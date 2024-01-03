import os
from datetime import datetime

from include.tasks import split
from include.tasks.extract import airflow_docs
from include.tasks.extract.utils.html_utils import urls_to_dataframe
from include.tasks.extract.utils.weaviate.ask_astro_weaviate_hook import AskAstroWeaviateHook

from airflow.decorators import dag, task

ask_astro_env = os.environ.get("ASK_ASTRO_ENV", "dev")

_WEAVIATE_CONN_ID = f"weaviate_{ask_astro_env}"
WEAVIATE_CLASS = os.environ.get("WEAVIATE_CLASS", "DocsDev")

ask_astro_weaviate_hook = AskAstroWeaviateHook(_WEAVIATE_CONN_ID)

airflow_docs_base_url = "https://airflow.apache.org/docs/"

default_args = {"retries": 3, "retry_delay": 30}

schedule_interval = "0 5 * * *" if ask_astro_env == "prod" else None


@task
def split_docs(urls, chunk_size=100) -> list[list]:
    chunked_urls = split.split_list(list(urls), chunk_size=chunk_size)
    return [
        [urls_to_dataframe(chunk_url)]
        for chunk_url in chunked_urls
    ]


@dag(
    schedule_interval=schedule_interval,
    start_date=datetime(2023, 9, 27),
    catchup=False,
    is_paused_upon_creation=True,
    default_args=default_args,
)
def ask_astro_load_airflow_docs():
    """
    This DAG performs incremental load for any new Airflow docs. Initial load via ask_astro_load_bulk imported
    data from a point-in-time data capture. By using the upsert logic of the weaviate_import decorator
    any existing documents that have been updated will be removed and re-added.
    """

    extracted_airflow_docs = task(airflow_docs.extract_airflow_docs)(docs_base_url=airflow_docs_base_url)

    _import_data = (
        task(ask_astro_weaviate_hook.ingest_data, retries=10)
        .partial(
            class_name=WEAVIATE_CLASS,
            existing="upsert",
            doc_key="docLink",
            batch_params={"batch_size": 1000},
            verbose=True,
        )
        .expand(dfs=split_docs(extracted_airflow_docs, chunk_size=100))
    )


ask_astro_load_airflow_docs()
