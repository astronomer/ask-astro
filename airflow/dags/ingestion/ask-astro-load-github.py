import os
from datetime import datetime

from include.tasks import ingest, split
from include.tasks.extract import github

from airflow.decorators import dag, task

ask_astro_env = os.environ.get("ASK_ASTRO_ENV", "")

_WEAVIATE_CONN_ID = f"weaviate_{ask_astro_env}"
_GITHUB_CONN_ID = "github_ro"

markdown_docs_sources = [
    {"doc_dir": "learn", "repo_base": "astronomer/docs"},
    {"doc_dir": "astro", "repo_base": "astronomer/docs"},
    {"doc_dir": "", "repo_base": "OpenLineage/docs"},
    {"doc_dir": "", "repo_base": "OpenLineage/OpenLineage"},
]
rst_docs_sources = [
    {"doc_dir": "docs", "repo_base": "apache/airflow", "exclude_docs": ["changelog.rst", "commits.rst"]},
]
code_samples_sources = [
    {"doc_dir": "code-samples", "repo_base": "astronomer/docs"},
]
issues_docs_sources = [
    "apache/airflow",
]


@dag(schedule_interval="0 5 * * *", start_date=datetime(2023, 9, 27), catchup=False, is_paused_upon_creation=True)
def ask_astro_load_github():
    """
    This DAG performs incremental load for any new docs.  Initial load via ask_astro_load_bulk imported
    data from a point-in-time data capture.  By using the upsert logic of the weaviate_import decorator
    any existing documents that have been updated will be removed and re-added.
    """

    md_docs = (
        task(github.extract_github_markdown, retries=3)
        .partial(github_conn_id=_GITHUB_CONN_ID)
        .expand(source=markdown_docs_sources)
    )

    rst_docs = (
        task(github.extract_github_rst, retries=3)
        .partial(github_conn_id=_GITHUB_CONN_ID)
        .expand(source=rst_docs_sources)
    )

    issues_docs = (
        task(github.extract_github_issues, retries=3)
        .partial(github_conn_id=_GITHUB_CONN_ID)
        .expand(repo_base=issues_docs_sources)
    )

    code_samples = (
        task(github.extract_github_python, retries=3)
        .partial(github_conn_id=_GITHUB_CONN_ID)
        .expand(source=code_samples_sources)
    )

    markdown_tasks = [md_docs, rst_docs, issues_docs]

    split_md_docs = task(split.split_markdown).expand(dfs=markdown_tasks)

    split_code_docs = task(split.split_python).expand(dfs=[code_samples])

    task.weaviate_import(
        ingest.import_upsert_data,
        weaviate_conn_id=_WEAVIATE_CONN_ID,
        retries=10,
        retry_delay=30,
    ).partial(class_name="Docs", primary_key="docLink").expand(dfs=[split_md_docs, split_code_docs])

    issues_docs >> md_docs >> rst_docs >> code_samples


ask_astro_load_github()
