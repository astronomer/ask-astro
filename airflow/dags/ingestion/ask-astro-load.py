import datetime
import json
import os
from pathlib import Path
from textwrap import dedent

import pandas as pd
from include.tasks import ingest, split
from include.tasks.extract import airflow_docs, blogs, github, registry, stack_overflow
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
code_samples_sources = [
    {"doc_dir": "code-samples", "repo_base": "astronomer/docs"},
]
issues_docs_sources = [
    {"repo_base": "apache/airflow", "cutoff_date": datetime.date(2020, 1, 1), "cutoff_issue_number": 30000}
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

blog_cutoff_date = datetime.date(2023, 1, 19)

stackoverflow_cutoff_date = "2021-09-01"
stackoverflow_tags = [
    "airflow",
]

airflow_docs_base_url = "https://airflow.apache.org/docs/"

default_args = {"retries": 3, "retry_delay": 30}


@dag(
    schedule_interval=None,
    start_date=datetime.datetime(2023, 9, 27),
    catchup=False,
    is_paused_upon_creation=True,
    default_args=default_args,
)
def ask_astro_load_bulk():
    """
    This DAG performs the initial load of data from sources.

    If seed_baseline_url (set above) points to a parquet file with pre-embedded data it will be
    ingested.  Otherwise new data is extracted, split, embedded and ingested.

    The first time this DAG runs (without seeded baseline) it will take at lease 90 minutes to
    extract data from all sources. Extracted data is then serialized to disk in the project
    directory in order to simplify later iterations of ingest with different chunking strategies,
    vector databases or embedding models.

    """

    class_object_data = json.loads(Path("include/data/schema.json").read_text())
    class_object_data["classes"][0].update({"class": WEAVIATE_CLASS})
    class_object_data = json.dumps(class_object_data)

    _check_schema = WeaviateCheckSchemaBranchOperator(
        task_id="check_schema",
        weaviate_conn_id=_WEAVIATE_CONN_ID,
        class_object_data=class_object_data,
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
        class_object_data=class_object_data,
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
                "extract_airflow_docs",
                "extract_stack_overflow",
                # "extract_slack_archive",
                "extract_astro_registry_cell_types",
                "extract_github_issues",
                "extract_astro_blogs",
                "extract_github_python",
                "extract_astro_registry_dags",
            ]

    @task(trigger_rule="none_failed")
    def extract_github_markdown(source: dict):
        try:
            df = pd.read_parquet(f"include/data/{source['repo_base']}/{source['doc_dir']}.parquet")
        except Exception:
            df = github.extract_github_markdown(source, github_conn_id=_GITHUB_CONN_ID)
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
    def extract_airflow_docs():
        try:
            df = pd.read_parquet("include/data/apache/airflow/docs.parquet")
        except Exception:
            df = airflow_docs.extract_airflow_docs(docs_base_url=airflow_docs_base_url)[0]
            df.to_parquet("include/data/apache/airflow/docs.parquet")

        return [df]

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
    def extract_github_issues(source: dict):
        try:
            df = pd.read_parquet(f"include/data/{source['repo_base']}/issues.parquet")
        except Exception:
            df = github.extract_github_issues(source, _GITHUB_CONN_ID)
            df.to_parquet(f"include/data/{source['repo_base']}/issues.parquet")

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

    issues_docs = extract_github_issues.expand(source=issues_docs_sources)

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
        issues_docs,
        stackoverflow_docs,
        # slack_docs,
        blogs_docs,
        registry_cells_docs,
    ]

    extracted_airflow_docs = extract_airflow_docs()

    html_tasks = [extracted_airflow_docs]

    python_code_tasks = [registry_dags_docs, code_samples]

    split_md_docs = task(split.split_markdown).expand(dfs=markdown_tasks)

    split_code_docs = task(split.split_python).expand(dfs=python_code_tasks)

    split_html_docs = task(split.split_html).expand(dfs=html_tasks)

    task.weaviate_import(ingest.import_data, weaviate_conn_id=_WEAVIATE_CONN_ID, retries=10).partial(
        class_name=WEAVIATE_CLASS
    ).expand(dfs=[split_md_docs, split_code_docs, split_html_docs])

    _import_baseline = task.weaviate_import(
        ingest.import_baseline, trigger_rule="none_failed", weaviate_conn_id=_WEAVIATE_CONN_ID
    )(class_name=WEAVIATE_CLASS, seed_baseline_url=seed_baseline_url)

    _check_schema >> [_check_seed_baseline, _create_schema]

    _create_schema >> markdown_tasks + python_code_tasks + html_tasks + [_check_seed_baseline]

    _check_seed_baseline >> markdown_tasks + python_code_tasks + html_tasks + [_import_baseline]


ask_astro_load_bulk()
