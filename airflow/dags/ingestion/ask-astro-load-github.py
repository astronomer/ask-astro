import datetime
import os

from dateutil.relativedelta import relativedelta
from include.tasks import ingest, split
from include.tasks.extract import github

from airflow.decorators import dag, task

ask_astro_env = os.environ.get("ASK_ASTRO_ENV", "")

_WEAVIATE_CONN_ID = f"weaviate_{ask_astro_env}"
_GITHUB_CONN_ID = "github_ro"
WEAVIATE_CLASS = os.environ.get("WEAVIATE_CLASS", "DocsProd")
markdown_docs_sources = [
    {"doc_dir": "learn", "repo_base": "astronomer/docs"},
    {"doc_dir": "astro", "repo_base": "astronomer/docs"},
    {"doc_dir": "", "repo_base": "OpenLineage/docs"},
    {"doc_dir": "", "repo_base": "OpenLineage/OpenLineage"},
]
code_samples_sources = [
    {"doc_dir": "code-samples", "repo_base": "astronomer/docs"},
]
issues_docs_sources = [
    {
        "repo_base": "apache/airflow",
        "cutoff_date": datetime.date.today() - relativedelta(months=1),
        "cutoff_issue_number": 30000,
    }
]

default_args = {"retries": 3, "retry_delay": 30}

schedule_interval = "0 5 * * *" if ask_astro_env == "prod" else None


@dag(
    schedule_interval=schedule_interval,
    start_date=datetime.datetime(2023, 9, 27),
    catchup=False,
    is_paused_upon_creation=True,
    default_args=default_args,
)
def ask_astro_load_github():
    """
    This DAG performs incremental load for any new docs. Initial load via ask_astro_load_bulk imported
    data from a point-in-time data capture. By using the upsert logic of the weaviate_import decorator
    any existing documents that have been updated will be removed and re-added.
    """

    md_docs = (
        task(github.extract_github_markdown)
        .partial(github_conn_id=_GITHUB_CONN_ID)
        .expand(source=markdown_docs_sources)
    )

    issues_docs = (
        task(github.extract_github_issues).partial(github_conn_id=_GITHUB_CONN_ID).expand(source=issues_docs_sources)
    )

    code_samples = (
        task(github.extract_github_python).partial(github_conn_id=_GITHUB_CONN_ID).expand(source=code_samples_sources)
    )

    split_md_docs = task(split.split_markdown).expand(dfs=[md_docs, issues_docs])

    split_code_docs = task(split.split_python).expand(dfs=[code_samples])

    task.weaviate_import(
        ingest.import_upsert_data,
        weaviate_conn_id=_WEAVIATE_CONN_ID,
    ).partial(
        class_name=WEAVIATE_CLASS, primary_key="docLink"
    ).expand(dfs=[split_md_docs, split_code_docs])


ask_astro_load_github()
