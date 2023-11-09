import os
from datetime import datetime
from textwrap import dedent

import pandas as pd
from include.tasks import ingest, split
from include.tasks.extract import blogs, github, registry, stack_overflow
from weaviate_provider.operators.weaviate import WeaviateCheckSchemaBranchOperator, WeaviateCreateSchemaOperator

from airflow.decorators import dag, task

seed_baseline_url = None

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
rst_docs_sources = [
    {"doc_dir": "docs", "repo_base": "apache/airflow", "exclude_docs": ["changelog.rst", "commits.rst"]},
]
code_samples_sources = [
    {"doc_dir": "code-samples", "repo_base": "astronomer/docs"},
]
issues_docs_sources = [
    "apache/airflow",
]
slack_channel_sources = [
    {
        "channel_name": "troubleshooting",
        "channel_id": "CCQ7EGB1P",
        "team_id": "TCQ18L22Z",
        "team_name": "Airflow Slack Community",
        "slack_api_conn_id": "slack_api_ro",
    }
]

blog_cutoff_date = datetime.strptime("2023-01-19", "%Y-%m-%d")

stackoverflow_cutoff_date = "2021-09-01"
stackoverflow_tags = [
    "airflow",
]

schedule_interval = "@daily" if ask_astro_env == "prod" else None


@dag(schedule_interval=schedule_interval, start_date=datetime(2023, 9, 27), catchup=False, is_paused_upon_creation=True)
def ask_astro_load_bulk():
    """
    This DAG performs the initial load of data from sources.

    If seed_baseline_url (set above) points to a parquet file with pre-embedded data it will be
    ingested.  Otherwise new data is extracted, split, embedded and ingested.

    The first time this DAG runs (without seeded baseline) it will take at lease 20 minutes to
    extract data from all sources. Extracted data is then serialized to disk in the project
    directory in order to simplify later iterations of ingest with different chunking strategies,
    vector databases or embedding models.

    """

    _check_schema = WeaviateCheckSchemaBranchOperator(
        task_id="check_schema",
        weaviate_conn_id=_WEAVIATE_CONN_ID,
        class_object_data="file://include/data/schema.json",
        follow_task_ids_if_true=["check_seed_baseline"],
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
        existing="ignore",
    )

    @task.branch(trigger_rule="none_failed")
    def check_seed_baseline() -> str:
        """
        Check if we will ingest from pre-embedded baseline or extract each source.
        """

        if seed_baseline_url is not None:
            return "import_baseline"
        else:
            return [
                "extract_github_markdown",
                "extract_github_rst",
                "extract_stack_overflow",
                # "extract_slack_archive",
                "extract_astro_registry_cell_types",
                "extract_github_issues",
                "extract_astro_blogs",
                "extract_github_python",
                "extract_astro_registry_dags",
            ]

    @task(trigger_rule="none_skipped")
    def extract_github_markdown(source: dict):
        try:
            df = pd.read_parquet(f"include/data/{source['repo_base']}/{source['doc_dir']}.parquet")
        except Exception:
            df = github.extract_github_markdown(source, github_conn_id=_GITHUB_CONN_ID)
            df.to_parquet(f"include/data/{source['repo_base']}/{source['doc_dir']}.parquet")

        return df

    @task(trigger_rule="none_skipped")
    def extract_github_rst(source: dict):
        try:
            df = pd.read_parquet(f"include/data/{source['repo_base']}/{source['doc_dir']}.parquet")
        except Exception:
            df = github.extract_github_rst(source=source, github_conn_id=_GITHUB_CONN_ID)
            df.to_parquet(f"include/data/{source['repo_base']}/{source['doc_dir']}.parquet")

        return df

    @task(trigger_rule="none_failed")
    def extract_github_python(source: dict):
        try:
            df = pd.read_parquet(f"include/data/{source['repo_base']}/{source['doc_dir']}.parquet")
        except Exception:
            df = github.extract_github_python(source, _GITHUB_CONN_ID)
            df.to_parquet(f"include/data/{source['repo_base']}/{source['doc_dir']}.parquet")

        return df

    @task(trigger_rule="none_failed")
    def extract_stack_overflow(tag: str, stackoverflow_cutoff_date: str):
        try:
            df = pd.read_parquet("include/data/stack_overflow/base.parquet")
        except Exception:
            df = stack_overflow.extract_stack_overflow_archive(
                tag=tag, stackoverflow_cutoff_date=stackoverflow_cutoff_date
            )
            df.to_parquet("include/data/stack_overflow/base.parquet")

        return df

    # @task(trigger_rule="none_failed")
    # def extract_slack_archive(source: dict):
    #     try:
    #         df = pd.read_parquet("include/data/slack/troubleshooting.parquet")
    #     except Exception:
    #         df = slack.extract_slack_archive(source)
    #         df.to_parquet("include/data/slack/troubleshooting.parquet")
    #
    #     return df

    @task(trigger_rule="none_failed")
    def extract_github_issues(repo_base: str):
        try:
            df = pd.read_parquet(f"include/data/{repo_base}/issues.parquet")
        except Exception:
            df = github.extract_github_issues(repo_base, _GITHUB_CONN_ID)
            df.to_parquet(f"include/data/{repo_base}/issues.parquet")

        return df

    @task(trigger_rule="none_failed")
    def extract_astro_registry_cell_types():
        try:
            df = pd.read_parquet("include/data/astronomer/registry/registry_cells.parquet")
        except Exception:
            df = registry.extract_astro_registry_cell_types()[0]
            df.to_parquet("include/data/astronomer/registry/registry_cells.parquet")

        return [df]

    @task(trigger_rule="none_failed")
    def extract_astro_registry_dags():
        try:
            df = pd.read_parquet("include/data/astronomer/registry/registry_dags.parquet")
        except Exception:
            df = registry.extract_astro_registry_dags()[0]
            df.to_parquet("include/data/astronomer/registry/registry_dags.parquet")

        return [df]

    @task(trigger_rule="none_failed")
    def extract_astro_blogs():
        try:
            df = pd.read_parquet("include/data/astronomer/blogs/astro_blogs.parquet")
        except Exception:
            df = blogs.extract_astro_blogs(blog_cutoff_date)[0]
            df.to_parquet("include/data/astronomer/blogs/astro_blogs.parquet")

        return [df]

    _check_seed_baseline = check_seed_baseline()

    md_docs = extract_github_markdown.expand(source=markdown_docs_sources)

    rst_docs = extract_github_rst.expand(source=rst_docs_sources)

    issues_docs = extract_github_issues.expand(repo_base=issues_docs_sources)

    stackoverflow_docs = extract_stack_overflow.partial(stackoverflow_cutoff_date=stackoverflow_cutoff_date).expand(
        tag=stackoverflow_tags
    )

    # slack_docs = extract_slack_archive.expand(source=slack_channel_sources)

    registry_cells_docs = extract_astro_registry_cell_types()

    blogs_docs = extract_astro_blogs()

    registry_dags_docs = extract_astro_registry_dags()

    code_samples = extract_github_python.partial().expand(source=code_samples_sources)

    markdown_tasks = [
        md_docs,
        rst_docs,
        issues_docs,
        stackoverflow_docs,
        # slack_docs,
        blogs_docs,
        registry_cells_docs,
    ]

    python_code_tasks = [registry_dags_docs, code_samples]

    split_md_docs = task(split.split_markdown).expand(dfs=markdown_tasks)

    split_code_docs = task(split.split_python).expand(dfs=python_code_tasks)

    task.weaviate_import(ingest.import_data, weaviate_conn_id=_WEAVIATE_CONN_ID, retries=10, retry_delay=30).partial(
        class_name=WEAVIATE_CLASS
    ).expand(dfs=[split_md_docs, split_code_docs])

    _import_baseline = task.weaviate_import(
        ingest.import_baseline, trigger_rule="none_failed", weaviate_conn_id=_WEAVIATE_CONN_ID
    )(class_name=WEAVIATE_CLASS, seed_baseline_url=seed_baseline_url)

    _check_schema >> [_check_seed_baseline, _create_schema]

    _create_schema >> markdown_tasks + python_code_tasks + [_check_seed_baseline]

    _check_seed_baseline >> issues_docs >> rst_docs >> md_docs
    # (
    #     _check_seed_baseline
    #     >> [stackoverflow_docs, slack_docs, blogs_docs, registry_cells_docs, _import_baseline] + python_code_tasks
    # )

    (
        _check_seed_baseline
        >> [stackoverflow_docs, blogs_docs, registry_cells_docs, _import_baseline] + python_code_tasks
    )


ask_astro_load_bulk()
