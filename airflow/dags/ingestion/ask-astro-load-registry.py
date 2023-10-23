import os
from datetime import datetime

from include.tasks import ingest, split
from include.tasks.extract import registry

from airflow.decorators import dag, task

ask_astro_env = os.environ.get("ASK_ASTRO_ENV", "")

_WEAVIATE_CONN_ID = f"weaviate_{ask_astro_env}"


@dag(schedule_interval="0 5 * * *", start_date=datetime(2023, 9, 27), catchup=False, is_paused_upon_creation=True)
def ask_astro_load_registry():
    """
    This DAG performs incremental load for any new docs.  Initial load via ask_astro_load_bulk imported
    data from a point-in-time data capture.  By using the upsert logic of the weaviate_import decorator
    any existing documents that have been updated will be removed and re-added.
    """

    registry_cells_docs = task(registry.extract_astro_registry_cell_types, retries=3)()

    registry_dags_docs = task(registry.extract_astro_registry_dags, retries=3)()

    split_md_docs = task(split.split_markdown).expand(dfs=[registry_cells_docs])

    split_code_docs = task(split.split_python).expand(dfs=[registry_dags_docs])

    task.weaviate_import(
        ingest.import_upsert_data,
        weaviate_conn_id=_WEAVIATE_CONN_ID,
        retries=10,
        retry_delay=30,
    ).partial(class_name="Docs", primary_key="docLink").expand(dfs=[split_md_docs, split_code_docs])


ask_astro_load_registry()
