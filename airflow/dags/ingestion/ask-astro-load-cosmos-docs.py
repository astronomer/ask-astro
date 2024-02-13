import os
from datetime import datetime

from include.tasks.extract.cosmos_docs import extract_cosmos_docs
from include.utils.slack import send_failure_notification

from airflow.decorators import dag, task
from airflow.providers.weaviate.operators.weaviate import WeaviateDocumentIngestOperator

ask_astro_env = os.environ.get("ASK_ASTRO_ENV", "dev")

_WEAVIATE_CONN_ID = f"weaviate_{ask_astro_env}"
WEAVIATE_CLASS = os.environ.get("WEAVIATE_CLASS", "DocsDev")


schedule_interval = os.environ.get("INGESTION_SCHEDULE", "0 5 * * 2") if ask_astro_env == "prod" else None


# @task
# def get_cosmos_docs_content():
#     from include.tasks.extract.cosmos_docs import extract_cosmos_docs

#     return extract_cosmos_docs()


@dag(
    schedule=schedule_interval,
    start_date=datetime(2023, 9, 27),
    catchup=False,
    is_paused_upon_creation=True,
    default_args={"retries": 3, "retry_delay": 30},
    on_failure_callback=send_failure_notification(
        dag_id="{{ dag.dag_id }}", execution_date="{{ dag_run.execution_date }}"
    ),
)
def ask_astro_load_cosmos_docs():
    """
    This DAG performs incremental load for any new Cosmos docs. Initial load via ask_astro_load_bulk imported
    data from a point-in-time data capture. By using the upsert logic of the weaviate_import decorator
    any existing documents that have been updated will be removed and re-added.
    """

    from include.tasks import split

    split_docs = task(split.split_html).expand(dfs=[extract_cosmos_docs()])

    _import_data = WeaviateDocumentIngestOperator.partial(
        class_name=WEAVIATE_CLASS,
        existing="replace",
        document_column="docLink",
        batch_config_params={"batch_size": 1000},
        verbose=True,
        conn_id=_WEAVIATE_CONN_ID,
        task_id="load_cosmos_docs_to_weaviate",
    ).expand(input_data=[split_docs])


ask_astro_load_cosmos_docs()
