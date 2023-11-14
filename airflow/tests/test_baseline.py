import os
from datetime import datetime
from textwrap import dedent

import pandas as pd
from include.tasks import ingest, split
from weaviate_provider.hooks.weaviate import WeaviateHook
from weaviate_provider.operators.weaviate import WeaviateCheckSchemaBranchOperator, WeaviateCreateSchemaOperator

from airflow.decorators import dag, task

seed_baseline_url = (
    "https://astronomer-demos-public-readonly.s3.us-west-2.amazonaws.com/ask-astro/baseline_data_v2.parquet"
)

ask_astro_env = os.environ.get("ASK_ASTRO_ENV", "")

test_doc_chunk1 = "# TEST TITLE\n## TEST SECTION\n" + "".join(["TEST " for a in range(0, 400)])
test_doc_chunk2 = "".join(["TEST " for a in range(0, 400)])
test_doc_content = "\n\n".join([test_doc_chunk1, test_doc_chunk2])

_WEAVIATE_CONN_ID = f"weaviate_{ask_astro_env}"


@dag(schedule_interval=None, start_date=datetime(2023, 9, 27), catchup=False, is_paused_upon_creation=True)
def test_ask_astro_load_baseline():
    """
    This DAG performs a test of the initial load of data from sources from a seed baseline.

    After the seed baseline is ingested an incremental load of a test document is ingested, changed for upsert
    and checked.

    """

    test_doc_link = "https://registry.astronomer.io/providers/apache-airflow/versions/2.7.2/modules/SmoothOperator"

    _check_schema = WeaviateCheckSchemaBranchOperator(
        task_id="check_schema",
        weaviate_conn_id=_WEAVIATE_CONN_ID,
        class_object_data="file://include/data/schema.json",
        follow_task_ids_if_true=["import_baseline"],
        follow_task_ids_if_false=["create_schema"],
        doc_md=dedent(
            """
        As the Weaviate schema may change over time this task checks if the most
        recent schema is in place before ingesting."""
        ),
    )

    _create_schema = WeaviateCreateSchemaOperator(
        task_id="create_schema",
        weaviate_conn_id=_WEAVIATE_CONN_ID,
        class_object_data="file://include/data/schema.json",
        existing="fail",
    )

    _import_baseline = task.weaviate_import(
        ingest.import_baseline, trigger_rule="none_failed", weaviate_conn_id=_WEAVIATE_CONN_ID
    )(class_name="Docs", seed_baseline_url=seed_baseline_url)

    @task()
    def get_existing_doc(doc_link: str = test_doc_link) -> list[pd.DataFrame]:
        """
        Import an existing document that was added from the baseline ingest.
        """

        weaviate_client = WeaviateHook(_WEAVIATE_CONN_ID).get_conn()

        existing_doc = (
            weaviate_client.query.get(properties=["docSource", "sha", "content", "docLink"], class_name="Docs")
            .with_limit(1)
            .with_additional(["id", "vector"])
            .with_where({"path": ["docLink"], "operator": "Equal", "valueText": doc_link})
            .do()
        )

        existing_doc = existing_doc["data"]["Get"]["Docs"][0]
        doc_additional = existing_doc.pop("_additional")
        existing_doc["uuid"] = doc_additional["id"]
        existing_doc["vector"] = doc_additional["vector"]

        return [pd.DataFrame([existing_doc])]

    @task()
    def create_test_object(original_doc: list[pd.DataFrame]) -> [pd.DataFrame]:
        """
        Create a test object with known data with sufficient size to be split into two chunks.
        """
        new_doc = original_doc[0][["docSource", "sha", "content", "docLink"]]
        new_doc["content"] = test_doc_content

        return [new_doc]

    @task()
    def check_test_objects(original_doc: list[pd.DataFrame], doc_link: str = test_doc_link) -> None:
        """
        Check the upserted doc against expected.
        """

        weaviate_client = WeaviateHook(_WEAVIATE_CONN_ID).get_conn()

        new_docs = (
            weaviate_client.query.get(properties=["docSource", "sha", "content", "docLink"], class_name="Docs")
            .with_limit(10)
            .with_additional(["id", "vector"])
            .with_where({"path": ["docLink"], "operator": "Equal", "valueText": test_doc_link})
            .do()
        )

        new_docs = new_docs["data"]["Get"]["Docs"]

        assert len(new_docs) == 2

        assert new_docs[0]["content"] + " " == test_doc_chunk1 or new_docs[0]["content"] + " " == test_doc_chunk2

        assert new_docs[1]["content"] + " " == test_doc_chunk1 or new_docs[1]["content"] + " " == test_doc_chunk2

        assert new_docs[0]["docLink"] == original_doc[0].docLink[0]
        assert new_docs[0]["docSource"] == original_doc[0].docSource[0]

        assert original_doc[0].uuid[0] != new_docs[0]["_additional"]["id"]
        assert original_doc[0].uuid[0] != new_docs[1]["_additional"]["id"]

    @task()
    def remove_test_objects(test_doc: pd.DataFrame) -> None:
        weaviate_client = WeaviateHook(_WEAVIATE_CONN_ID).get_conn()

        new_docs = (
            weaviate_client.query.get(properties=[], class_name="Docs")
            .with_limit(2)
            .with_additional(["id"])
            .with_where({"path": ["docLink"], "operator": "Equal", "valueText": test_doc_link})
            .do()
        )

        uuids = [doc["_additional"]["id"] for doc in new_docs["data"]["Get"]["Docs"]]

        for uuid in uuids:
            weaviate_client.data_object.delete(uuid=uuid, class_name="Docs")

    @task()
    def check_original_object(original_doc: list[pd.DataFrame], doc_link: str = test_doc_link) -> None:
        """
        Check the re-upserted doc against original.
        """

        weaviate_client = WeaviateHook(_WEAVIATE_CONN_ID).get_conn()

        new_docs = (
            weaviate_client.query.get(properties=["docSource", "sha", "content", "docLink"], class_name="Docs")
            .with_limit(10)
            .with_additional(["id", "vector"])
            .with_where({"path": ["docLink"], "operator": "Equal", "valueText": test_doc_link})
            .do()
        )

        new_docs = new_docs["data"]["Get"]["Docs"]

        assert len(new_docs) == 1

        original_doc[0].to_json()

    original_doc = get_existing_doc()
    test_doc = create_test_object(original_doc)

    split_md_docs = task(split.split_markdown).expand(dfs=[test_doc])

    _upsert_data = (
        task.weaviate_import(ingest.import_upsert_data, weaviate_conn_id=_WEAVIATE_CONN_ID)
        .partial(class_name="Docs", primary_key="docLink")
        .expand(dfs=[split_md_docs])
    )

    _check_test_objects = check_test_objects(original_doc=original_doc, doc_link=test_doc_link)

    _remove_test_objects = remove_test_objects(test_doc)

    _check_schema >> [_create_schema, _import_baseline]
    _create_schema >> _import_baseline
    _import_baseline >> original_doc >> test_doc
    _upsert_data >> _check_test_objects >> _remove_test_objects


test_ask_astro_load_baseline()
