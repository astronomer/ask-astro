import os
from datetime import datetime

import firebase_admin
import requests
from weaviate_provider.hooks.weaviate import WeaviateHook

from airflow.decorators import dag, task
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.trigger_rule import TriggerRule

monitoring_interval = os.environ.get("MONITORING_INTERVAL", "@daily")

weaviate_conn_id = os.environ.get("WEAVIATE_CONN_ID", WeaviateHook.default_conn_name)
weaviate_class = os.environ.get("WEAVIATE_CLASS", "DocsProd")

firestore_app_name = os.environ.get("FIRESTORE_APP_NAME", "[DEFAULT]")

slack_channel = os.environ.get("SLACK_CHANNEL", "")
slack_webhook_conn = os.environ.get("SLACK_WEBHOOK_CONN")
slack_username = os.getenv("SLACK_USERNAME", "airflow_app")


@task(trigger_rule=TriggerRule.ALL_DONE)
def slack_status(**context):
    tis_dagrun = context["ti"].get_dagrun().get_task_instances()

    task_status = []
    for ti in tis_dagrun:
        # Ignore status of slack task
        if ti.task_id == "slack_status":
            continue

        if ti.state == "success":
            continue
        elif ti.state == "failed":
            task = f":red_circle: {ti.task_id}"
        else:
            task = f":black_circle: {ti.task_id}"
        task_status.append(task)

    if task_status:
        service_status = "\n@here\n"
        service_status += "\n".join(task_status)
    else:
        service_status = "All service are up!"

    print("********************")
    print(service_status)
    print("********************")

    SlackWebhookOperator(
        task_id="slack_alert",
        slack_webhook_conn_id=slack_webhook_conn,
        message=service_status,
        channel=slack_channel,
        username=slack_username,
    ).execute(context=context)


@task(trigger_rule=TriggerRule.ALL_DONE)
def check_ui_status():
    endpoint = "https://ask.astronomer.io"
    response = requests.get(endpoint)
    if response.status_code != 200:
        raise response


@task(trigger_rule=TriggerRule.ALL_DONE)
def check_weaviate_status():
    weaviate_hook = WeaviateHook(weaviate_conn_id)
    client = weaviate_hook.get_conn()
    schemas = client.query.aggregate(weaviate_class).with_meta_count().do()
    schema = schemas["data"]["Aggregate"]["DocsProd"]
    count = 0
    for v in schema:
        metadata = v.get("meta")
        if metadata:
            count = metadata.get("count")
            break
    if count == 0:
        print(f"Weavaite class {weaviate_class} is empty!")
    else:
        print(f"{count} record found in Weavaite class {weaviate_class}")


@task(trigger_rule=TriggerRule.ALL_DONE)
def check_firestore_status():
    firebase_admin.initialize_app()

    app = firebase_admin.get_app(name=firestore_app_name)

    print(f"{app.name} found!")


@dag(
    schedule_interval=monitoring_interval, start_date=datetime(2023, 9, 27), catchup=False, is_paused_upon_creation=True
)
def monitoring_dag():
    [check_ui_status(), check_weaviate_status(), check_firestore_status()] >> slack_status()


monitoring_dag()
