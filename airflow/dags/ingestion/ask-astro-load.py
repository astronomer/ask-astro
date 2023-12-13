from __future__ import annotations

import datetime
import json
import logging
import os
from pathlib import Path

import pandas as pd
from include.tasks import split
from include.tasks.extract import airflow_docs, astro_cli_docs, blogs, github, registry, stack_overflow
from include.tasks.extract.utils.weaviate.ask_astro_weaviate_hook import AskAstroWeaviateHook
from include.tasks.extract.astro_sdk_docs import extract_astro_sdk_docs
from include.tasks.extract.astronomer_providers_docs import extract_provider_docs

from airflow.decorators import dag, task

seed_baseline_url = None
stackoverflow_cutoff_date = "2021-09-01"
ask_astro_env = os.environ.get("ASK_ASTRO_ENV", "dev")

_WEAVIATE_CONN_ID = f"weaviate_{ask_astro_env}"
_GITHUB_CONN_ID = "github_ro"
WEAVIATE_CLASS = os.environ.get("WEAVIATE_CLASS", "DocsDev")

ask_astro_weaviate_hook = AskAstroWeaviateHook(_WEAVIATE_CONN_ID)

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

blog_cutoff_date = datetime.date(2023, 1, 19)

stackoverflow_tags = [{"airflow": "2021-09-01"}]

airflow_docs_base_url = "https://airflow.apache.org/docs/"

default_args = {"retries": 3, "retry_delay": 30}
logger = logging.getLogger("airflow.task")


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
    ingested. Otherwise, new data is extracted, split, embedded and ingested.

    The first time this DAG runs (without seeded baseline) it will take at lease 90 minutes to
    extract data from all sources. Extracted data is then serialized to disk in the project
    directory in order to simplify later iterations of ingest with different chunking strategies,
    vector databases or embedding models.

    """

    @task
    def get_schema_and_process(schema_file: str) -> list:
        """
        Retrieves and processes the schema from a given JSON file.

        :param schema_file: path to the schema JSON file
        """
        try:
            class_objects = json.loads(Path(schema_file).read_text())
        except FileNotFoundError:
            logger.error(f"Schema file {schema_file} not found.")
            raise
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in the schema file {schema_file}.")
            raise

        class_objects["classes"][0].update({"class": WEAVIATE_CLASS})

        if "classes" not in class_objects:
            class_objects = [class_objects]
        else:
            class_objects = class_objects["classes"]

        logger.info("Schema processing completed.")
        return class_objects

    @task.branch
    def check_schema(class_objects: list) -> list[str]:
        """
        Check if the current schema includes the requested schema.  The current schema could be a superset
        so check_schema_subset is used recursively to check that all objects in the requested schema are
        represented in the current schema.

        :param class_objects: Class objects to be checked against the current schema.
        """
        return (
            ["check_seed_baseline"]
            if ask_astro_weaviate_hook.check_schema(class_objects=class_objects)
            else ["create_schema"]
        )

    @task(trigger_rule="none_failed")
    def create_schema(class_objects: list, existing: str = "ignore") -> None:
        """
        Creates or updates the schema in Weaviate based on the given class objects.

        :param class_objects: A list of class objects for schema creation or update.
        :param existing: Strategy to handle existing classes ('ignore' or 'replace'). Defaults to 'ignore'.
        """
        ask_astro_weaviate_hook.create_schema(class_objects=class_objects, existing=existing)

    @task.branch(trigger_rule="none_failed")
    def check_seed_baseline(seed_baseline_url: str = None) -> str | set:
        """
        Check if we will ingest from pre-embedded baseline or extract each source.
        """

        if seed_baseline_url is not None:
            return "import_baseline"
        else:
            return {
                "extract_github_markdown",
                "extract_airflow_docs",
                "extract_stack_overflow",
                "extract_astro_registry_cell_types",
                "extract_github_issues",
                "extract_astro_blogs",
                "extract_github_python",
                "extract_astro_registry_dags",
                "extract_astro_cli_docs",
                "extract_astro_sdk_doc",
                "extract_astro_provider_doc",
            }

    @task(trigger_rule="none_failed")
    def extract_github_markdown(source: dict):
        parquet_file = f"include/data/{source['repo_base']}/{source['doc_dir']}.parquet"

        if os.path.isfile(parquet_file):
            if os.access(parquet_file, os.R_OK):
                df = pd.read_parquet(parquet_file)
            else:
                raise Exception("Parquet file exists locally but is not readable.")
        else:
            df = github.extract_github_markdown(source, github_conn_id=_GITHUB_CONN_ID)
            df.to_parquet(parquet_file)

        return df

    @task(trigger_rule="none_failed")
    def extract_github_python(source: dict):
        parquet_file = f"include/data/{source['repo_base']}/{source['doc_dir']}.parquet"

        if os.path.isfile(parquet_file):
            if os.access(parquet_file, os.R_OK):
                df = pd.read_parquet(parquet_file)
            else:
                raise Exception("Parquet file exists locally but is not readable.")
        else:
            df = github.extract_github_python(source, _GITHUB_CONN_ID)
            df.to_parquet(parquet_file)

        return df

    @task(trigger_rule="none_failed")
    def extract_airflow_docs():
        parquet_file = "include/data/apache/airflow/docs.parquet"

        if os.path.isfile(parquet_file):
            if os.access(parquet_file, os.R_OK):
                df = pd.read_parquet(parquet_file)
            else:
                raise Exception("Parquet file exists locally but is not readable.")
        else:
            df = airflow_docs.extract_airflow_docs(docs_base_url=airflow_docs_base_url)[0]
            df.to_parquet(parquet_file)

        return [df]

    @task(trigger_rule="none_failed")
    def extract_astro_cli_docs():
        astro_cli_parquet_path = "include/data/astronomer/docs/astro-cli.parquet"
        try:
            df = pd.read_parquet(astro_cli_parquet_path)
        except Exception:
            df = astro_cli_docs.extract_astro_cli_docs()[0]
            df.to_parquet(astro_cli_parquet_path)

        return [df]

    @task(trigger_rule="none_failed")
    def extract_astro_sdk_doc():
        astro_sdk_parquet_path = "include/data/astronomer/docs/astro-sdk.parquet"
        try:
            df = pd.read_parquet(astro_sdk_parquet_path)
        except Exception:
            df = extract_astro_sdk_docs()
            df.to_parquet(df)

        return [df]

    @task(trigger_rule="none_failed")
    def extract_astro_provider_doc():
        astro_sdk_parquet_path = "include/data/astronomer/docs/astro-provider.parquet"
        try:
            df = pd.read_parquet(astro_sdk_parquet_path)
        except Exception:
            df = extract_provider_docs()
            df.to_parquet(df)

        return [df]

    @task(trigger_rule="none_failed")
    def extract_stack_overflow(tag: str, stackoverflow_cutoff_date: str = stackoverflow_cutoff_date):
        parquet_file = "include/data/stack_overflow/base.parquet"

        if os.path.isfile(parquet_file):
            if os.access(parquet_file, os.R_OK):
                df = pd.read_parquet(parquet_file)
            else:
                raise Exception("Parquet file exists locally but is not readable.")
        else:
            df = stack_overflow.extract_stack_overflow_archive(
                tag=tag, stackoverflow_cutoff_date=stackoverflow_cutoff_date
            )
            df.to_parquet(parquet_file)

        return df

    @task(trigger_rule="none_failed")
    def extract_github_issues(repo_base: str):
        parquet_file = f"include/data/{repo_base}/issues.parquet"

        if os.path.isfile(parquet_file):
            if os.access(parquet_file, os.R_OK):
                df = pd.read_parquet(parquet_file)
            else:
                raise Exception("Parquet file exists locally but is not readable.")
        else:
            df = github.extract_github_issues(repo_base, _GITHUB_CONN_ID)
            df.to_parquet(parquet_file)

        return df

    @task(trigger_rule="none_failed")
    def extract_astro_registry_cell_types():
        parquet_file = "include/data/astronomer/registry/registry_cells.parquet"

        if os.path.isfile(parquet_file):
            if os.access(parquet_file, os.R_OK):
                df = pd.read_parquet(parquet_file)
            else:
                raise Exception("Parquet file exists locally but is not readable.")
        else:
            df = registry.extract_astro_registry_cell_types()[0]
            df.to_parquet(parquet_file)

        return [df]

    @task(trigger_rule="none_failed")
    def extract_astro_registry_dags():
        parquet_file = "include/data/astronomer/registry/registry_dags.parquet"

        if os.path.isfile(parquet_file):
            if os.access(parquet_file, os.R_OK):
                df = pd.read_parquet(parquet_file)
            else:
                raise Exception("Parquet file exists locally but is not readable.")
        else:
            df = registry.extract_astro_registry_dags()[0]
            df.to_parquet(parquet_file)

        return [df]

    @task(trigger_rule="none_failed")
    def extract_astro_blogs():
        parquet_file = "include/data/astronomer/blogs/astro_blogs.parquet"

        if os.path.isfile(parquet_file):
            if os.access(parquet_file, os.R_OK):
                df = pd.read_parquet(parquet_file)
            else:
                raise Exception("Parquet file exists locally but is not readable.")
        else:
            df = blogs.extract_astro_blogs(blog_cutoff_date)[0]
            df.to_parquet(parquet_file)

        return [df]

    md_docs = extract_github_markdown.expand(source=markdown_docs_sources)
    issues_docs = extract_github_issues.expand(repo_base=issues_docs_sources)
    stackoverflow_docs = extract_stack_overflow.expand(tag=stackoverflow_tags)
    registry_cells_docs = extract_astro_registry_cell_types()
    blogs_docs = extract_astro_blogs()
    registry_dags_docs = extract_astro_registry_dags()
    code_samples = extract_github_python.expand(source=code_samples_sources)
    _airflow_docs = extract_airflow_docs()
    _astro_cli_docs = extract_astro_cli_docs()
    _extract_astro_sdk_docs = extract_astro_sdk_doc()
    _extract_astro_providers_docs = extract_astro_provider_doc()

    _get_schema = get_schema_and_process(schema_file="include/data/schema.json")
    _check_schema = check_schema(class_objects=_get_schema)
    _create_schema = create_schema(class_objects=_get_schema)
    _check_seed_baseline = check_seed_baseline(seed_baseline_url=seed_baseline_url)

    markdown_tasks = [
        md_docs,
        issues_docs,
        stackoverflow_docs,
        blogs_docs,
        registry_cells_docs,
    ]

    html_tasks = [_airflow_docs, _astro_cli_docs, _extract_astro_sdk_docs, _extract_astro_providers_docs]

    python_code_tasks = [registry_dags_docs, code_samples]

    split_md_docs = task(split.split_markdown).expand(dfs=markdown_tasks)

    split_code_docs = task(split.split_python).expand(dfs=python_code_tasks)

    split_html_docs = task(split.split_html).expand(dfs=html_tasks)

    _import_data = (
        task(ask_astro_weaviate_hook.ingest_data, retries=10)
        .partial(
            class_name=WEAVIATE_CLASS,
            existing="skip",
            doc_key="docLink",
            batch_params={"batch_size": 1000},
            verbose=True,
        )
        .expand(dfs=[split_md_docs, split_code_docs, split_html_docs])
    )

    _import_baseline = task(ask_astro_weaviate_hook.import_baseline, trigger_rule="none_failed")(
        seed_baseline_url=seed_baseline_url,
        class_name=WEAVIATE_CLASS,
        existing="error",
        doc_key="docLink",
        uuid_column="id",
        vector_column="vector",
        batch_params={"batch_size": 1000},
        verbose=True,
    )

    _check_schema >> [_check_seed_baseline, _create_schema]

    _create_schema >> markdown_tasks + python_code_tasks + html_tasks + [_check_seed_baseline]

    _check_seed_baseline >> markdown_tasks + python_code_tasks + html_tasks + [_import_baseline]


ask_astro_load_bulk()
