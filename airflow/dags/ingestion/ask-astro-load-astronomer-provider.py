import datetime
import os

from airflow.decorators import dag, task
from airflow.providers.weaviate.operators.weaviate import WeaviateDocumentIngestOperator

ask_astro_env = os.environ.get("ASK_ASTRO_ENV", "dev")

_WEAVIATE_CONN_ID = f"weaviate_{ask_astro_env}"
WEAVIATE_CLASS = os.environ.get("WEAVIATE_CLASS", "DocsDev")

blog_cutoff_date = datetime.date(2023, 1, 19)

default_args = {"retries": 3, "retry_delay": 30}

schedule_interval = "0 5 * * *" if ask_astro_env == "prod" else None


@task
def get_provider_content():
    from include.tasks.extract.astronomer_providers_docs import extract_provider_docs

    dfs = extract_provider_docs()
    return dfs


@dag(
    schedule_interval=schedule_interval,
    start_date=datetime.datetime(2023, 9, 27),
    catchup=False,
    is_paused_upon_creation=True,
    default_args=default_args,
)
def ask_astro_load_astronomer_providers():
    """
    This DAG performs incremental load for any new docs. Initial load via ask_astro_load_bulk imported
    data from a point-in-time data capture. By using the upsert logic of the weaviate_import decorator
    any existing documents that have been updated will be removed and re-added.
    """

    _import_data = WeaviateDocumentIngestOperator.partial(
        class_name=WEAVIATE_CLASS,
        existing="replace",
        document_column="docLink",
        batch_config_params={"batch_size": 1000},
        verbose=True,
        conn_id=_WEAVIATE_CONN_ID,
        task_id="WeaviateDocumentIngestOperator",
    ).expand(input_data=[get_provider_content()])


ask_astro_load_astronomer_providers()
