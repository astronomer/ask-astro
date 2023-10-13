import os
from datetime import datetime

from include import extract, ingest, split
from weaviate_provider.operators.weaviate import WeaviateCheckSchemaBranchOperator

from airflow.decorators import dag, task
from airflow.providers.slack.operators.slack import SlackAPIPostOperator

try:
    ask_astro_env = os.environ["ASK_ASTRO_ENV"]
except Exception:
    ask_astro_env = "NOOP"

if ask_astro_env == "prod":
    _WEAVIATE_CONN_ID = "weaviate_prod"
    _SLACK_CONN_ID = "slack_api_pgdev"
    _GITHUB_CONN_ID = "github_mpg"
elif ask_astro_env == "local":
    _WEAVIATE_CONN_ID = "weaviate_local"
    _SLACK_CONN_ID = "slack_api_pgdev"
    _GITHUB_CONN_ID = "github_mpg"
elif ask_astro_env == "dev":
    _WEAVIATE_CONN_ID = "weaviate_dev"
    _SLACK_CONN_ID = "slack_api_pgdev"
    _GITHUB_CONN_ID = "github_mpg"
elif ask_astro_env == "test":
    _WEAVIATE_CONN_ID = "weaviate_test"
    _SLACK_CONN_ID = "slack_api_pgdev"
    _GITHUB_CONN_ID = "github_mpg"
else:
    _WEAVIATE_CONN_ID = "weaviate_NOOP"
    _SLACK_CONN_ID = "slack_api_NOOP"
    _GITHUB_CONN_ID = "github_NOOP"

slack_channel_sources = [
    {
        "channel_name": "troubleshooting",
        "channel_id": "CCQ7EGB1P",
        "team_id": "TCQ18L22Z",
        "team_name": "Airflow Slack Community",
        "slack_api_conn_id": "TBD",
    }
]


@dag(schedule_interval="0 5 * * *", start_date=datetime(2023, 9, 27), catchup=False, is_paused_upon_creation=True)
def ask_astro_load_slack():
    """
    This DAG performs incremental load for any data sources that have changed.  Initial load via
    ask_astro_load_bulk imported data from a point-in-time data capture.

    This DAG checks to make sure the latest schema exists.  If it does not exist a slack message
    is sent to notify admins.
    """

    _check_schema = WeaviateCheckSchemaBranchOperator(
        task_id="check_schema",
        weaviate_conn_id=_WEAVIATE_CONN_ID,
        class_object_data="file://include/data/schema.json",
        follow_task_ids_if_true=[
            "extract_slack_archive",
        ],
        follow_task_ids_if_false=["slack_schema_alert"],
    )

    _slack_schema_alert = SlackAPIPostOperator(
        task_id="slack_schema_alert",
        channel="#airflow_notices",
        retries=0,
        slack_conn_id=_SLACK_CONN_ID,
        text="ask_astro_load_blogs DAG error.  Schema mismatch.",
    )

    slack_docs = task(extract.extract_slack_archive, trigger_rule="none_failed", retries=3).expand(
        source=slack_channel_sources
    )

    markdown_tasks = [slack_docs]

    split_md_docs = task(split.split_markdown).expand(df=markdown_tasks)

    task.weaviate_import(
        ingest.import_upsert_data, trigger_rule="none_failed", weaviate_conn_id=_WEAVIATE_CONN_ID
    ).partial(class_name="Docs", primary_key="docLink").expand(dfs=[split_md_docs])

    _check_schema >> [_slack_schema_alert] + markdown_tasks


ask_astro_load_slack()


def test():
    from weaviate_provider.hooks.weaviate import WeaviateHook
    from weaviate_provider.operators.weaviate import WeaviateCreateSchemaOperator

    WeaviateHook(_WEAVIATE_CONN_ID).get_conn().schema.delete_all()

    WeaviateCreateSchemaOperator(
        task_id="create_schema",
        weaviate_conn_id=_WEAVIATE_CONN_ID,
        class_object_data="file://include/data/schema.json",
        existing="fail",
    ).execute(context={})

    WeaviateHook(_WEAVIATE_CONN_ID).get_conn().query.aggregate(class_name="Docs").with_meta_count().do()
