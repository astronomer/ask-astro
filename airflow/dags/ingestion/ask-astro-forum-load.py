import datetime
import os

from include.tasks.extract.astro_forum_docs import get_forum_df
from include.tasks.extract.utils.weaviate.ask_astro_weaviate_hook import AskAstroWeaviateHook

from airflow.decorators import dag, task

ask_astro_env = os.environ.get("`ASK_ASTRO_ENV", "dev")

_WEAVIATE_CONN_ID = f"weaviate_{ask_astro_env}"
WEAVIATE_CLASS = os.environ.get("WEAVIATE_CLASS", "DocsDev")
ask_astro_weaviate_hook = AskAstroWeaviateHook(_WEAVIATE_CONN_ID)

blog_cutoff_date = datetime.date(2022, 1, 1)

default_args = {"retries": 3, "retry_delay": 30}

schedule_interval = "0 5 * * *" if ask_astro_env == "prod" else None


@task
def get_astro_forum_content():
    dfs = get_forum_df()
    return dfs


@dag(
    schedule_interval=schedule_interval,
    start_date=datetime.datetime(2023, 9, 27),
    catchup=False,
    is_paused_upon_creation=True,
    default_args=default_args,
)
def ask_astro_load_astro_forum():
    _import_data = (
        task(ask_astro_weaviate_hook.ingest_data, retries=10)
        .partial(
            class_name=WEAVIATE_CLASS,
            existing="upsert",
            doc_key="docLink",
            batch_params={"batch_size": 1000},
            verbose=True,
        )
        .expand(dfs=[get_astro_forum_content()])
    )


ask_astro_load_astro_forum()
