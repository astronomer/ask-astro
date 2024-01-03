import datetime
import os

from airflow.decorators import dag, task

ask_astro_env = os.environ.get("ASK_ASTRO_ENV", "dev")

_WEAVIATE_CONN_ID = f"weaviate_{ask_astro_env}"
WEAVIATE_CLASS = os.environ.get("WEAVIATE_CLASS", "DocsDev")


default_args = {"retries": 3, "retry_delay": 30}

schedule_interval = "0 5 * * *" if ask_astro_env == "prod" else None


@dag(
    schedule_interval=schedule_interval,
    start_date=datetime.datetime(2023, 9, 27),
    catchup=False,
    is_paused_upon_creation=True,
    default_args=default_args,
)
def ask_astro_load_astronomer_docs():
    """
    This DAG performs incremental load for any new docs in astronomer docs.
    """
    from include.tasks import split
    from include.tasks.extract.astro_docs import extract_astro_docs
    from include.tasks.extract.utils.weaviate.ask_astro_weaviate_hook import AskAstroWeaviateHook

    ask_astro_weaviate_hook = AskAstroWeaviateHook(_WEAVIATE_CONN_ID)
    astro_docs = task(extract_astro_docs)()

    split_md_docs = task(split.split_markdown).expand(dfs=[astro_docs])

    _import_data = (
        task(ask_astro_weaviate_hook.ingest_data, retries=10)
        .partial(
            class_name=WEAVIATE_CLASS,
            existing="upsert",
            doc_key="docLink",
            batch_params={"batch_size": 1000},
            verbose=True,
        )
        .expand(dfs=[split_md_docs])
    )


ask_astro_load_astronomer_docs()
