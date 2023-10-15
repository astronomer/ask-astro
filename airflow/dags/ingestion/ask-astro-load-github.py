import os
from datetime import datetime

from include import extract, ingest, split
from weaviate_provider.operators.weaviate import WeaviateCheckSchemaBranchOperator

from airflow.decorators import dag, task
from airflow.providers.slack.operators.slack import SlackAPIPostOperator

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

rst_exclude_docs = ["changelog.rst", "commits.rst"]


@dag(schedule_interval="0 5 * * *", start_date=datetime(2023, 9, 27), catchup=False, is_paused_upon_creation=True)
def ask_astro_load_github():
    """
    This DAG performs incremental load for any data sources that have changed.  Initial load via
    ask_astro_load_bulk imported data from a point-in-time data capture.

    This DAG checks to make sure the latest schema exists.  If it does not exist a slack message
    is sent to notify admins.
    """

    _check_schema = WeaviateCheckSchemaBranchOperator(
        task_id="check_schema",
        weaviate_conn_id=_WEAVIATE_CONN_ID,
        class_object_data="file://include/data/schema.json",
        follow_task_ids_if_true=[
            "extract_github_markdown",
            "extract_github_rst",
            "extract_github_issues",
            "extract_github_python",
        ],
        follow_task_ids_if_false=["slack_schema_alert"],
    )

    _slack_schema_alert = SlackAPIPostOperator(
        task_id="slack_schema_alert",
        channel="#airflow_notices",
        retries=0,
        slack_conn_id=_SLACK_CONN_ID,
        text="ask_astro_load_github DAG error.  Schema mismatch.",
    )

    md_docs = (
        task(extract.extract_github_markdown, trigger_rule="none_failed", retries=3)
        .partial(github_conn_id=_GITHUB_CONN_ID)
        .expand(source=markdown_docs_sources)
    )

    rst_docs = (
        task(extract.extract_github_rst, trigger_rule="none_failed", retries=3)
        .partial(github_conn_id=_GITHUB_CONN_ID, rst_exclude_docs=rst_exclude_docs)
        .expand(source=rst_docs_sources)
    )

    issues_docs = (
        task(extract.extract_github_issues, trigger_rule="none_failed", retries=3)
        .partial(github_conn_id=_GITHUB_CONN_ID)
        .expand(source=issues_docs_sources)
    )

    code_samples = (
        task(extract.extract_github_python, trigger_rule="none_failed", retries=3)
        .partial(github_conn_id=_GITHUB_CONN_ID)
        .expand(source=code_samples_sources)
    )

    markdown_tasks = [md_docs, rst_docs, issues_docs]
    python_code_tasks = [code_samples]

    split_md_docs = task(split.split_markdown, trigger_rule="none_failed").expand(df=markdown_tasks)

    split_code_docs = task(split.split_python, trigger_rule="none_failed").expand(df=python_code_tasks)

    task.weaviate_import(
        ingest.import_upsert_data, trigger_rule="none_failed", weaviate_conn_id=_WEAVIATE_CONN_ID
    ).partial(class_name="Docs", primary_key="docLink").expand(dfs=[split_md_docs, split_code_docs])

    _check_schema >> _slack_schema_alert
    _check_schema >> md_docs >> rst_docs >> issues_docs >> python_code_tasks


ask_astro_load_github()
