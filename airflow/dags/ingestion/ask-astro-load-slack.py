import os
from datetime import datetime

from include.utils.slack import send_failure_notification

from airflow.decorators import dag, task
from airflow.providers.weaviate.operators.weaviate import WeaviateDocumentIngestOperator

ask_astro_env = os.environ.get("ASK_ASTRO_ENV", "dev")

_WEAVIATE_CONN_ID = f"weaviate_{ask_astro_env}"
WEAVIATE_CLASS = os.environ.get("WEAVIATE_CLASS", "DocsDev")

slack_channel_sources = [
    {
        "channel_name": "troubleshooting",
        "channel_id": "CCQ7EGB1P",
        "team_id": "TCQ18L22Z",
        "team_name": "Airflow Slack Community",
        "slack_api_conn_id": "slack_api_ro",
    }
]

default_args = {"retries": 3, "retry_delay": 30}

schedule_interval = os.environ.get("INGESTION_SCHEDULE", "0 5 * * 2") if ask_astro_env == "prod" else None


@dag(
    schedule_interval=schedule_interval,
    start_date=datetime(2023, 9, 27),
    catchup=False,
    is_paused_upon_creation=True,
    default_args=default_args,
    on_failure_callback=send_failure_notification(
        dag_id="{{ dag.dag_id }}", execution_date="{{ dag_run.execution_date }}"
    ),
)
def ask_astro_load_slack():
    """
    This DAG performs incremental load for any new slack threads. The slack archive is a point-in-time capture.  This
    DAG should run nightly to capture threads between archive periods. By using the upsert logic of the
    weaviate_import decorator any existing documents that have been updated will be removed and re-added.
    """
    from include.tasks import split
    from include.tasks.extract import slack

    slack_docs = task(slack.extract_slack).expand(source=slack_channel_sources)

    split_md_docs = task(split.split_markdown).expand(dfs=[slack_docs])

    _import_data = WeaviateDocumentIngestOperator.partial(
        class_name=WEAVIATE_CLASS,
        existing="replace",
        document_column="docLink",
        batch_config_params={"batch_size": 1000},
        verbose=True,
        conn_id=_WEAVIATE_CONN_ID,
        task_id="WeaviateDocumentIngestOperator",
    ).expand(input_data=[split_md_docs])


ask_astro_load_slack()
