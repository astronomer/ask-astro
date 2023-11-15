import datetime
import json
import os
from pathlib import Path

import pandas as pd
from include.tasks import ingest, split
from include.tasks.extract import airflow_docs, blogs, github, registry, stack_overflow
from include.tasks.utils.schema import check_schema_subset
from weaviate.exceptions import UnexpectedStatusCodeException

from airflow.decorators import dag, task, task_group
from airflow.providers.weaviate.hooks.weaviate import WeaviateHook

# from weaviate_provider.operators.weaviate import WeaviateCheckSchemaBranchOperator, WeaviateCreateSchemaOperator

seed_baseline_url = None

ask_astro_env = os.environ.get("ASK_ASTRO_ENV", "")

_WEAVIATE_CONN_ID = f"weaviate_{ask_astro_env}"
_GITHUB_CONN_ID = "github_ro"
WEAVIATE_CLASS = os.environ.get("WEAVIATE_CLASS", "DocsProd")

weaviate_hook = WeaviateHook(_WEAVIATE_CONN_ID)
weaviate_client = weaviate_hook.get_client()

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

stackoverflow_tags = [{"airflow": "2021-09-01"}]

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

    @task
    def get_schema(schema_file: str) -> dict:
        """
        Get the schema object for this DAG.
        """

        class_objects = json.loads(Path(schema_file).read_text())
        class_objects["classes"][0].update({"class": WEAVIATE_CLASS})

        if "classes" not in class_objects:
            class_objects = [class_objects]
        else:
            class_objects = class_objects["classes"]

        return class_objects

    @task.branch
    def check_schema(class_objects: dict) -> str:
        """
        Check if the current schema includes the requested schema.  The current schema could be a superset
        so check_schema_subset is used recursively to check that all objects in the requested schema are
        represented in the current schema.
        """

        missing_objects = []

        for class_object in class_objects:
            try:
                class_schema = weaviate_client.schema.get(class_object.get("class", ""))
                if not check_schema_subset(class_object=class_object, class_schema=class_schema):
                    missing_objects.append(class_object["class"])
            except Exception as e:
                if isinstance(e, UnexpectedStatusCodeException):
                    if e.status_code == 404 and "with response body: None." in e.message:
                        missing_objects.append(class_object["class"])
                    else:
                        raise (e)
                else:
                    raise (e)

        if missing_objects:
            print(f"Classes {missing_objects} are not in the current schema.")
            return ["create_schema"]
        else:
            return ["check_seed_baseline"]

    @task(trigger_rule="none_failed")
    def create_schema(class_objects: dict, existing: str = "ignore"):
        for class_object in class_objects:
            try:
                current_class = weaviate_client.schema.get(class_name=class_object.get("class", ""))
            except Exception:
                current_class = None

            if current_class is not None:
                if existing == "replace":
                    print(f"Deleting existing class {class_object['class']}")
                    weaviate_client.schema.delete_class(class_name=class_object["class"])

                elif existing == "ignore":
                    print(f"Ignoring existing class {class_object['class']}")
                    continue

            weaviate_client.schema.create_class(class_object)
            print(f"Created class {class_object['class']}")

    @task.branch(trigger_rule="none_failed")
    def check_seed_baseline(seed_baseline_url: str = None) -> str:
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

    @task_group
    def extract_data_sources():
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

        md_docs = extract_github_markdown.expand(source=markdown_docs_sources)
        issues_docs = extract_github_issues.expand(source=issues_docs_sources)
        stackoverflow_docs = extract_stack_overflow.expand(tag=stackoverflow_tags)
        # slack_docs = extract_slack_archive.expand(source=slack_channel_sources)
        registry_cells_docs = extract_astro_registry_cell_types()
        blogs_docs = extract_astro_blogs()
        registry_dags_docs = extract_astro_registry_dags()
        code_samples = extract_github_python.expand(source=code_samples_sources)
        _airflow_docs = extract_airflow_docs()

        return (
            md_docs,
            issues_docs,
            stackoverflow_docs,
            registry_cells_docs,
            blogs_docs,
            registry_dags_docs,
            code_samples,
            _airflow_docs,
        )

    _get_schema = get_schema(schema_file="include/data/schema.json")
    _check_schema = check_schema(class_objects=_get_schema)
    _create_schema = create_schema(class_objects=_get_schema)
    _check_seed_baseline = check_seed_baseline(seed_baseline_url=seed_baseline_url)

    (
        md_docs,
        issues_docs,
        stackoverflow_docs,
        registry_cells_docs,
        blogs_docs,
        registry_dags_docs,
        code_samples,
        _airflow_docs,
    ) = extract_data_sources()

    markdown_tasks = [
        md_docs,
        issues_docs,
        stackoverflow_docs,
        # slack_docs,
        blogs_docs,
        registry_cells_docs,
    ]

    html_tasks = [_airflow_docs]

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
