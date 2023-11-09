import os
from datetime import datetime

from include.tasks import ingest, split
from include.tasks.extract import blogs

from airflow.decorators import dag, task

ask_astro_env = os.environ.get("ASK_ASTRO_ENV", "")

_WEAVIATE_CONN_ID = f"weaviate_{ask_astro_env}"
WEAVIATE_CLASS = os.environ.get("WEAVIATE_CLASS", "DocsProd")

blog_cutoff_date = datetime.strptime("2023-01-19", "%Y-%m-%d")


@dag(schedule_interval="0 5 * * *", start_date=datetime(2023, 9, 27), catchup=False, is_paused_upon_creation=True)
def ask_astro_load_blogs():
    """
    This DAG performs incremental load for any new docs.  Initial load via ask_astro_load_bulk imported
    data from a point-in-time data capture.  By using the upsert logic of the weaviate_import decorator
    any existing documents that have been updated will be removed and re-added.
    """

    blogs_docs = task(blogs.extract_astro_blogs, retries=3)(blog_cutoff_date=blog_cutoff_date)

    split_md_docs = task(split.split_markdown).expand(dfs=[blogs_docs])

    task.weaviate_import(
        ingest.import_upsert_data,
        weaviate_conn_id=_WEAVIATE_CONN_ID,
        retries=10,
        retry_delay=30,
    ).partial(class_name=WEAVIATE_CLASS, primary_key="docLink").expand(dfs=[split_md_docs])


ask_astro_load_blogs()
