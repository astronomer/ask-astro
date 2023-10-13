import os
from datetime import datetime

import pandas as pd
from include import extract, ingest, split
from weaviate.util import generate_uuid5
from weaviate_provider.hooks.weaviate import WeaviateHook
from weaviate_provider.operators.weaviate import WeaviateCheckSchemaBranchOperator, WeaviateCreateSchemaOperator

from airflow.decorators import dag, task

try:
    ask_astro_env = os.environ["ASK_ASTRO_ENV"]
except Exception:
    ask_astro_env = "NOOP"

if ask_astro_env == "prod":
    _WEAVIATE_CONN_ID = "weaviate_prod"
    _SLACK_CONN_ID = "slack_api_pgdev"
    _GITHUB_CONN_ID = "github_mpg"
elif ask_astro_env == "local":
    _WEAVIATE_CONN_ID = "weaviate_local"
    _SLACK_CONN_ID = "slack_api_pgdev"
    _GITHUB_CONN_ID = "github_mpg"
elif ask_astro_env == "dev":
    _WEAVIATE_CONN_ID = "weaviate_dev"
    _SLACK_CONN_ID = "slack_api_pgdev"
    _GITHUB_CONN_ID = "github_mpg"
elif ask_astro_env == "test":
    _WEAVIATE_CONN_ID = "weaviate_test"
    _SLACK_CONN_ID = "slack_api_pgdev"
    _GITHUB_CONN_ID = "github_mpg"
else:
    _WEAVIATE_CONN_ID = "weaviate_NOOP"
    _SLACK_CONN_ID = "slack_api_NOOP"
    _GITHUB_CONN_ID = "github_NOOP"

markdown_docs_sources = [
    {"doc_dir": "learn", "repo_base": "astronomer/docs"},
    {"doc_dir": "astro", "repo_base": "astronomer/docs"},
    {"doc_dir": "docs", "repo_base": "OpenLineage/docs"},
]
rst_docs_sources = [
    {"doc_dir": "docs", "repo_base": "apache/airflow"},
]
code_samples_sources = [
    {"doc_dir": "code-samples", "repo_base": "astronomer/docs"},
]
issues_docs_sources = [{"doc_dir": "issues", "repo_base": "apache/airflow"}]
slack_channel_sources = [
    {
        "channel_name": "troubleshooting",
        "channel_id": "CCQ7EGB1P",
        "team_id": "TCQ18L22Z",
        "team_name": "Airflow Slack Community",
        "slack_api_conn_id": "TBD",
    }
]

rst_exclude_docs = ["changelog.rst", "commits.rst"]

blog_cutoff_date = datetime.strptime("2023-01-19", "%Y-%m-%d")

stackoverflow_cutoff_date = "2021-09-01"
stackoverflow_tags = [
    "airflow",
]

weaviate_doc_count = {
    "Docs": 7113,
}


@dag(schedule_interval=None, start_date=datetime(2023, 9, 27), catchup=False, is_paused_upon_creation=True)
def ask_astro_load_bulk():
    """
    This DAG performs the initial load of data from sources.  While the code to generate these datasets
    is included for each function, the data is frozen as a parquet file for simple ingest and
    re-ingest for experimenting with different models, chunking strategies, vector DBs, etc.
    """

    _check_schema = WeaviateCheckSchemaBranchOperator(
        task_id="check_schema",
        weaviate_conn_id=_WEAVIATE_CONN_ID,
        class_object_data="file://include/data/schema.json",
        follow_task_ids_if_true=["check_object_count"],
        follow_task_ids_if_false=["create_schema"],
    )

    @task.branch
    def check_object_count(weaviate_doc_count: dict, class_name: str) -> str:
        try:
            weaviate_hook = WeaviateHook(_WEAVIATE_CONN_ID)
            weaviate_client = weaviate_hook.get_conn()
            response = weaviate_client.query.aggregate(class_name=class_name).with_meta_count().do()
        except Exception as e:
            if e.status_code == 422 and "no graphql provider present" in e.message:
                response = None

        if response and response["data"]["Aggregate"][class_name][0]["meta"]["count"] >= weaviate_doc_count[class_name]:
            print("Initial Upload complete. Skipping")
            return None
        else:
            return [
                "extract_github_markdown",
                "extract_github_rst",
                "extract_stack_overflow",
                "extract_slack",
                "extract_astro_registry_cell_types",
                "extract_github_issues",
                "extract_astro_blogs",
                "extract_github_python",
                "extract_astro_registry_dags",
            ]

    _create_schema = WeaviateCreateSchemaOperator(
        task_id="create_schema",
        weaviate_conn_id=_WEAVIATE_CONN_ID,
        class_object_data="file://include/data/schema.json",
        existing="fail",
    )

    @task(trigger_rule="none_failed")
    def extract_github_markdown(source: dict):
        # df = extract.extract_github_markdown(source, github_conn_id=_GITHUB_CONN_ID)

        # df.to_parquet(f"include/data/{source['repo_base']}/{source['doc_dir']}.parquet")
        df = pd.read_parquet(f"include/data/{source['repo_base']}/{source['doc_dir']}.parquet")

        return df

    @task(trigger_rule="none_failed")
    def extract_github_rst(source: dict):
        # df = extract.extract_github_rst(source, rst_exclude_docs, _GITHUB_CONN_ID)

        # df.to_parquet(f"include/data/{source['repo_base']}/{source['doc_dir']}.parquet")
        df = pd.read_parquet(f"include/data/{source['repo_base']}/{source['doc_dir']}.parquet")

        return df

    @task(trigger_rule="none_failed")
    def extract_github_python(source: dict):
        # df = extract.extract_github_python(source, _GITHUB_CONN_ID)

        # df.to_parquet(f"include/data/{source['repo_base']}/{source['doc_dir']}.parquet")
        df = pd.read_parquet(f"include/data/{source['repo_base']}/{source['doc_dir']}.parquet")

        return df

    @task(trigger_rule="none_failed")
    def extract_stack_overflow(tag: dict, stackoverflow_cutoff_date: str):
        # df = extract.extract_stack_overflow_archive(tag, stackoverflow_cutoff_date)

        # df.to_parquet('include/data/stackoverflow_base.parquet')
        df = pd.read_parquet("include/data/stackoverflow_base.parquet")

        return df

    @task(trigger_rule="none_failed")
    def extract_slack(source: dict):
        df = extract.extract_slack_archive(source)

        return df

    @task(trigger_rule="none_failed")
    def extract_github_issues(source: dict):
        # df = extract.extract_github_issues(source, _GITHUB_CONN_ID)

        # df.to_parquet(f"include/data/{source['repo_base']}/{source['doc_dir']}.parquet")
        df = pd.read_parquet(f"include/data/{source['repo_base']}/{source['doc_dir']}.parquet")

        return df

    @task(trigger_rule="none_failed")
    def extract_astro_registry_cell_types():
        # df = extract.extract_astro_registry_cell_types()[0]

        # df.to_parquet('include/data/registry_cells.parquet')
        df = pd.read_parquet("include/data/registry_cells.parquet")

        return [df]

    @task(trigger_rule="none_failed")
    def extract_astro_registry_dags():
        # df = extract.extract_astro_registry_dags()[0]

        # df.to_parquet('include/data/registry_dags.parquet')
        df = pd.read_parquet("include/data/registry_dags.parquet")

        return [df]

    @task(trigger_rule="none_failed")
    def extract_astro_blogs():
        # df = extract.extract_astro_blogs(blog_cutoff_date)[0]

        # df.to_parquet('include/data/astro_blogs.parquet')
        df = pd.read_parquet("include/data/astro_blogs.parquet")

        return [df]

    _check_object_count = check_object_count(weaviate_doc_count=weaviate_doc_count, class_name="Docs")

    md_docs = extract_github_markdown.expand(source=markdown_docs_sources)

    rst_docs = extract_github_rst.expand(source=rst_docs_sources)

    issues_docs = extract_github_issues.expand(source=issues_docs_sources)

    stackoverflow_docs = extract_stack_overflow.partial(stackoverflow_cutoff_date=stackoverflow_cutoff_date).expand(
        tag=stackoverflow_tags
    )

    slack_docs = extract_slack.expand(source=slack_channel_sources)

    registry_cells_docs = extract_astro_registry_cell_types()

    blogs_docs = extract_astro_blogs()

    registry_dags_docs = extract_astro_registry_dags()

    code_samples = extract_github_python.partial().expand(source=code_samples_sources)

    markdown_tasks = [md_docs, rst_docs, issues_docs, stackoverflow_docs, slack_docs, registry_cells_docs, blogs_docs]

    python_code_tasks = [registry_dags_docs, code_samples]

    split_md_docs = task(split.split_markdown).expand(df=markdown_tasks)

    split_code_docs = task(split.split_python).expand(df=python_code_tasks)

    task.weaviate_import(ingest.import_data, weaviate_conn_id=_WEAVIATE_CONN_ID).partial(class_name="Docs").expand(
        dfs=[split_md_docs, split_code_docs]
    )

    _check_schema >> [_check_object_count, _create_schema]
    _check_object_count >> markdown_tasks + python_code_tasks
    _create_schema >> markdown_tasks + python_code_tasks


ask_astro_load_bulk()


def test():
    from weaviate_provider.hooks.weaviate import WeaviateHook
    from weaviate_provider.operators.weaviate import WeaviateCreateSchemaOperator, WeaviateImportDataOperator

    WeaviateHook(_WEAVIATE_CONN_ID).get_conn().schema.delete_all()

    WeaviateCreateSchemaOperator(
        task_id="create_schema",
        weaviate_conn_id=_WEAVIATE_CONN_ID,
        class_object_data="file://include/data/schema.json",
        existing="fail",
    ).execute(context={})

    WeaviateHook(_WEAVIATE_CONN_ID).get_conn().query.aggregate(class_name="Docs").with_meta_count().do()

    for df in [
        pd.concat([extract.extract_github_markdown(source) for source in markdown_docs_sources]),
        pd.concat([extract.extract_github_rst(source) for source in rst_docs_sources]),
        pd.concat([extract.extract_stack_overflow(tag, stackoverflow_cutoff_date) for tag in stackoverflow_tags]),
        pd.concat([extract.extract_slack(source) for source in slack_channel_sources]),
        pd.concat([extract.extract_github_issues(source) for source in issues_docs_sources]),
        extract.extract_astro_blogs(),
        extract.extract_astro_registry_cell_types(),
    ]:
        df = split.split_markdown(df)
        df["uuid"] = df.apply(lambda x: generate_uuid5(x.to_dict()), axis=1)
        WeaviateImportDataOperator(
            task_id="test", class_name="Docs", data=df, uuid_column="uuid", weaviate_conn_id=_WEAVIATE_CONN_ID
        ).execute(context={})

    for df in [
        pd.concat([extract.extract_github_python(source) for source in code_samples_sources]),
        extract.extract_astro_registry_dags(),
    ]:
        df = split.split_python(df)
        df["uuid"] = df.apply(lambda x: generate_uuid5(x.to_dict()), axis=1)
        WeaviateImportDataOperator(
            task_id="test", class_name="Docs", data=df, uuid_column="uuid", weaviate_conn_id=_WEAVIATE_CONN_ID
        ).execute(context={})

    from weaviate_provider.hooks.weaviate import WeaviateHook

    weaviate_client = WeaviateHook(_WEAVIATE_CONN_ID).get_conn()

    help(weaviate_client.data)

    search = (
        weaviate_client.query.get(properties=["content"], class_name="Docs")
        .with_limit(2)
        .with_where({"path": ["docSource"], "operator": "Equal", "valueText": "astronomer registry modules"})
        .do()
    )

    print(search["data"]["Get"]["Docs"][0]["content"])
    len(search["data"]["Get"]["Docs"])

    WeaviateImportDataOperator(task_id="test", data=df, class_name="Docs", uuid_column="uuid")
