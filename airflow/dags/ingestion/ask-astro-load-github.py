import datetime
import os

from include.tasks import split
from include.tasks.extract import github
from include.tasks.extract.utils.weaviate.ask_astro_weaviate_hook import AskAstroWeaviateHook

from airflow.decorators import dag, task

ask_astro_env = os.environ.get("ASK_ASTRO_ENV", "dev")

_WEAVIATE_CONN_ID = f"weaviate_{ask_astro_env}"
_GITHUB_CONN_ID = "github_ro"
WEAVIATE_CLASS = os.environ.get("WEAVIATE_CLASS", "DocsDev")
_GITHUB_ISSUE_CUTOFF_DATE = os.environ.get("GITHUB_ISSUE_CUTOFF_DATE", "2022-1-1")

ask_astro_weaviate_hook = AskAstroWeaviateHook(_WEAVIATE_CONN_ID)

markdown_docs_sources = [
    {"doc_dir": "", "repo_base": "OpenLineage/docs"},
    {"doc_dir": "", "repo_base": "OpenLineage/OpenLineage"},
]

issues_docs_sources = [
    "apache/airflow",
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
        task(github.extract_github_issues)
        .partial(github_conn_id=_GITHUB_CONN_ID, cutoff_date=_GITHUB_ISSUE_CUTOFF_DATE)
        .expand(repo_base=issues_docs_sources)
    )

    split_md_docs = task(split.split_markdown).expand(dfs=[md_docs, issues_docs])

    _import_data = (
        task(ask_astro_weaviate_hook.ingest_data, retries=10)
        .partial(
            class_name=WEAVIATE_CLASS,
            existing="upsert",
            doc_key="docLink",
            batch_params={"batch_size": 1000},
            verbose=True,
        )
        .expand(dfs=[split_md_docs])
    )


ask_astro_load_github()
