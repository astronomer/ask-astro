from __future__ import annotations

import datetime
import json
import logging
import os
from pathlib import Path

import pandas as pd
from include.utils.slack import send_failure_notification

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.providers.weaviate.operators.weaviate import WeaviateDocumentIngestOperator

seed_baseline_url = None
stackoverflow_cutoff_date = "2021-09-01"
ask_astro_env = os.environ.get("ASK_ASTRO_ENV", "dev")

_WEAVIATE_CONN_ID = f"weaviate_{ask_astro_env}"
_GITHUB_CONN_ID = "github_ro"
WEAVIATE_CLASS = os.environ.get("WEAVIATE_CLASS", "DocsDev")
_GITHUB_ISSUE_CUTOFF_DATE = os.environ.get("GITHUB_ISSUE_CUTOFF_DATE", "2022-1-1")

markdown_docs_sources = [
    {"doc_dir": "", "repo_base": "OpenLineage/docs"},
    {"doc_dir": "", "repo_base": "OpenLineage/OpenLineage"},
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
    on_failure_callback=send_failure_notification(
        dag_id="{{ dag.dag_id }}", execution_date="{{ dag_run.execution_date }}"
    ),
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

    from include.tasks import split

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
        from airflow.providers.weaviate.hooks.weaviate import WeaviateHook

        ask_astro_weaviate_hook = WeaviateHook(conn_id=_WEAVIATE_CONN_ID)
        return (
            ["check_seed_baseline"]
            if ask_astro_weaviate_hook.check_subset_of_schema(classes_objects=class_objects)
            else ["create_schema"]
        )

    @task(trigger_rule="none_failed")
    def create_schema(class_objects: list, existing: str = "ignore") -> None:
        """
        Creates or updates the schema in Weaviate based on the given class objects.

        :param class_objects: A list of class objects for schema creation or update.
        :param existing: Strategy to handle existing classes ('ignore' or 'replace'). Defaults to 'ignore'.
        """
        from airflow.providers.weaviate.hooks.weaviate import WeaviateHook

        ask_astro_weaviate_hook = WeaviateHook(conn_id=_WEAVIATE_CONN_ID)
        ask_astro_weaviate_hook.create_or_replace_classes(
            schema_json={cls["class"]: cls for cls in class_objects}, existing=existing
        )

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
                "extract_astro_registry_dags",
                "extract_astro_cli_docs",
                "extract_astro_provider_doc",
                "extract_astro_forum_doc",
                "extract_astronomer_docs",
                "extract_cosmos_docs",
            }

    @task(trigger_rule="none_failed")
    def extract_github_markdown(source: dict):
        from include.tasks.extract import github

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
        from include.tasks.extract import github

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
        from include.tasks.extract import airflow_docs

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
        from include.tasks.extract import astro_cli_docs

        astro_cli_parquet_path = "include/data/astronomer/docs/astro-cli.parquet"
        try:
            df = pd.read_parquet(astro_cli_parquet_path)
        except Exception:
            df = astro_cli_docs.extract_astro_cli_docs()[0]
            df.to_parquet(astro_cli_parquet_path)

        return [df]

    @task(trigger_rule="none_failed")
    def extract_astro_provider_doc():
        from include.tasks.extract.astronomer_providers_docs import extract_provider_docs

        astro_provider_parquet_path = "include/data/astronomer/docs/astro-provider.parquet"
        try:
            df = pd.read_parquet(astro_provider_parquet_path)
        except Exception:
            df = extract_provider_docs()[0]
            df.to_parquet(astro_provider_parquet_path)

        return [df]

    @task(trigger_rule="none_failed")
    def extract_stack_overflow(tag: str, stackoverflow_cutoff_date: str = stackoverflow_cutoff_date):
        from include.tasks.extract import stack_overflow

        try:
            df = pd.read_parquet("include/data/stack_overflow/base.parquet")
        except Exception:
            df = stack_overflow.extract_stack_overflow(tag=tag, stackoverflow_cutoff_date=stackoverflow_cutoff_date)
            df.to_parquet("include/data/stack_overflow/base.parquet")

        return df

    @task(trigger_rule="none_failed")
    def extract_astro_forum_doc():
        from include.tasks.extract.astro_forum_docs import get_forum_df

        astro_forum_parquet_path = "include/data/astronomer/docs/astro-forum.parquet"
        try:
            df = pd.read_parquet(astro_forum_parquet_path)
        except Exception:
            df = get_forum_df()[0]
            df.to_parquet(astro_forum_parquet_path)

        return [df]

    @task(trigger_rule="none_failed")
    def extract_github_issues(repo_base: str):
        from include.tasks.extract import github

        parquet_file = f"include/data/{repo_base}/issues.parquet"

        if os.path.isfile(parquet_file):
            if os.access(parquet_file, os.R_OK):
                df = pd.read_parquet(parquet_file)
            else:
                raise Exception("Parquet file exists locally but is not readable.")
        else:
            df = github.extract_github_issues(repo_base, _GITHUB_CONN_ID, _GITHUB_ISSUE_CUTOFF_DATE)
            df.to_parquet(parquet_file)

        return df

    @task(trigger_rule="none_failed")
    def extract_astro_registry_cell_types():
        from include.tasks.extract import registry

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
        from include.tasks.extract import registry

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
        from include.tasks.extract import blogs

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

    @task(trigger_rule="none_failed")
    def extract_cosmos_docs():
        from include.tasks.extract import cosmos_docs

        parquet_file_path = "include/data/astronomer/cosmos/cosmos_docs.parquet"

        try:
            df = pd.read_parquet(parquet_file_path)
        except Exception:
            df = cosmos_docs.extract_cosmos_docs()[0]
            df.to_parquet(parquet_file_path)

        return [df]

    @task(trigger_rule="none_failed")
    def extract_astronomer_docs():
        from include.tasks.extract.astro_docs import extract_astro_docs

        parquet_file = "include/data/astronomer/blogs/astro_docs.parquet"

        if os.path.isfile(parquet_file):
            if not os.access(parquet_file, os.R_OK):
                raise AirflowException("Parquet file exists locally but is not readable.")
            df = pd.read_parquet(parquet_file)
        else:
            df = extract_astro_docs()[0]
            df.to_parquet(parquet_file)

        return [df]

    @task(trigger_rule="none_failed")
    def import_baseline(
        document_column: str,
        class_name: str,
        seed_baseline_url: str | None = None,
        existing: str = "error",
        uuid_column: str | None = None,
        vector_column: str = "Vector",
        batch_config_params: dict | None = None,
        verbose: bool = True,
    ):
        from airflow.providers.weaviate.hooks.weaviate import WeaviateHook

        ask_astro_weaviate_hook = WeaviateHook(conn_id=_WEAVIATE_CONN_ID)
        seed_filename = f"include/data/{seed_baseline_url.split('/')[-1]}"

        if os.path.isfile(seed_filename):
            if not os.access(seed_filename, os.R_OK):
                raise AirflowException("Baseline file exists locally but is not readable.")
            df = pd.read_parquet(seed_filename)
        else:
            df = pd.read_parquet(seed_baseline_url)
            df.to_parquet(seed_filename)

        return ask_astro_weaviate_hook.create_or_replace_document_objects(
            data=df,
            class_name=class_name,
            existing=existing,
            document_column=document_column,
            uuid_column=uuid_column,
            vector_column=vector_column,
            verbose=verbose,
            batch_config_params=batch_config_params,
        )

    md_docs = extract_github_markdown.expand(source=markdown_docs_sources)
    issues_docs = extract_github_issues.expand(repo_base=issues_docs_sources)
    stackoverflow_docs = extract_stack_overflow.expand(tag=stackoverflow_tags)
    registry_cells_docs = extract_astro_registry_cell_types()
    blogs_docs = extract_astro_blogs()
    registry_dags_docs = extract_astro_registry_dags()
    _astro_docs = extract_astronomer_docs()
    _airflow_docs = extract_airflow_docs()
    _astro_cli_docs = extract_astro_cli_docs()
    _extract_astro_providers_docs = extract_astro_provider_doc()
    _astro_forum_docs = extract_astro_forum_doc()
    _cosmos_docs = extract_cosmos_docs()

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

    html_tasks = [
        _airflow_docs,
        _astro_cli_docs,
        _extract_astro_providers_docs,
        _astro_forum_docs,
        _astro_docs,
        _cosmos_docs,
    ]

    python_code_tasks = [registry_dags_docs]

    split_md_docs = task(split.split_markdown).expand(dfs=markdown_tasks)

    split_code_docs = task(split.split_python).expand(dfs=python_code_tasks)

    split_html_docs = task(split.split_html).expand(dfs=html_tasks)

    _import_data = WeaviateDocumentIngestOperator.partial(
        class_name=WEAVIATE_CLASS,
        existing="replace",
        document_column="docLink",
        batch_config_params={"batch_size": 1000},
        verbose=True,
        conn_id=_WEAVIATE_CONN_ID,
        task_id="WeaviateDocumentIngestOperator",
    ).expand(input_data=[split_md_docs, split_code_docs, split_html_docs])

    _import_baseline = import_baseline(
        seed_baseline_url=seed_baseline_url,
        class_name=WEAVIATE_CLASS,
        existing="error",
        document_column="docLink",
        uuid_column="id",
        vector_column="vector",
        batch_config_params={"batch_size": 1000},
        verbose=True,
    )

    _check_schema >> [_check_seed_baseline, _create_schema]

    _create_schema >> markdown_tasks + python_code_tasks + html_tasks + [_check_seed_baseline]

    _check_seed_baseline >> markdown_tasks + python_code_tasks + html_tasks + [_import_baseline]


ask_astro_load_bulk()
