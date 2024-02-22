import datetime
import os

from include.utils.slack import send_failure_notification

from airflow.decorators import dag, task
from airflow.providers.weaviate.operators.weaviate import WeaviateDocumentIngestOperator

ask_astro_env = os.environ.get("ASK_ASTRO_ENV", "dev")

_WEAVIATE_CONN_ID = f"weaviate_{ask_astro_env}"
WEAVIATE_CLASS = os.environ.get("WEAVIATE_CLASS", "DocsDev")


default_args = {"retries": 3, "retry_delay": 30}

schedule_interval = os.environ.get("INGESTION_SCHEDULE", "0 5 * * 2") if ask_astro_env == "prod" else None


@dag(
    schedule_interval=schedule_interval,
    start_date=datetime.datetime(2023, 9, 27),
    catchup=False,
    is_paused_upon_creation=True,
    default_args=default_args,
    on_failure_callback=send_failure_notification(
        dag_id="{{ dag.dag_id }}", execution_date="{{ dag_run.execution_date }}"
    ),
)
def ask_astro_load_astronomer_docs():
    """
    This DAG performs incremental load for any new docs in astronomer docs.
    """
    from include.tasks import split
    from include.tasks.extract.astro_docs import extract_astro_docs

    astro_docs = task(extract_astro_docs)()

    split_html_docs = task(split.split_html).expand(dfs=[astro_docs])

    _import_data = WeaviateDocumentIngestOperator.partial(
        class_name=WEAVIATE_CLASS,
        existing="replace",
        document_column="docLink",
        batch_config_params={"batch_size": 6, "dynamic": False},
        verbose=True,
        conn_id=_WEAVIATE_CONN_ID,
        task_id="WeaviateDocumentIngestOperator",
    ).expand(input_data=[split_html_docs])


ask_astro_load_astronomer_docs()
