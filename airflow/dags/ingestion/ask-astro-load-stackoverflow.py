import os
from datetime import datetime

from include.tasks import ingest, split
from include.tasks.extract import stack_overflow

from airflow.decorators import dag, task

ask_astro_env = os.environ.get("ASK_ASTRO_ENV", "")

_WEAVIATE_CONN_ID = f"weaviate_{ask_astro_env}"
WEAVIATE_CLASS = os.environ.get("WEAVIATE_CLASS", "DocsProd")
stackoverflow_cutoff_date = "2021-09-01"

stackoverflow_tags = [
    "airflow",
]

default_args = {"retries": 3, "retry_delay": 30}

schedule_interval = "0 5 * * *" if ask_astro_env == "prod" else None


@dag(
    schedule_interval=schedule_interval,
    start_date=datetime(2023, 9, 27),
    catchup=False,
    is_paused_upon_creation=True,
    default_args=default_args,
)
def ask_astro_load_stackoverflow():
    """
    This DAG performs incremental load for any new docs.  Initial load via ask_astro_load_bulk imported
    data from a point-in-time data capture.  By using the upsert logic of the weaviate_import decorator
    any existing documents that have been updated will be removed and re-added.
    """

    stack_overflow_docs = (
        task(stack_overflow.extract_stack_overflow)
        .partial(stackoverflow_cutoff_date=stackoverflow_cutoff_date)
        .expand(tag=stackoverflow_tags)
    )

    split_md_docs = task(split.split_markdown).expand(dfs=[stack_overflow_docs])

    task.weaviate_import(
        ingest.import_upsert_data,
        weaviate_conn_id=_WEAVIATE_CONN_ID,
    ).partial(
        class_name=WEAVIATE_CLASS, primary_key="docLink"
    ).expand(dfs=[split_md_docs])


ask_astro_load_stackoverflow()
