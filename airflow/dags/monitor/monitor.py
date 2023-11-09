from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import datetime

import firebase_admin
import requests
from weaviate_provider.hooks.weaviate import WeaviateHook

from airflow.decorators import dag, task
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.trigger_rule import TriggerRule

# Set up the logger
logger = logging.getLogger("airflow.task")

monitoring_interval = os.environ.get("MONITORING_INTERVAL", "@daily")

weaviate_conn_id = os.environ.get("WEAVIATE_CONN_ID", WeaviateHook.default_conn_name)
weaviate_class = os.environ.get("WEAVIATE_CLASS", "DocsProd")

firestore_app_name = os.environ.get("FIRESTORE_APP_NAME", "[DEFAULT]")

slack_webhook_conn = os.environ.get("SLACK_WEBHOOK_CONN", "slack_webhook_default")

google_service_account_json_value = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_VALUE", None)

ASK_ASTRO_API_BASE_URL = os.environ.get("ASK_ASTRO_API_BASE_URL", None)

# request ids to monitor from Ask-Astro database.
ASK_ASTRO_REQUEST_ID_1 = os.environ.get("ASK_ASTRO_REQUEST_ID_1", "05d8882e-56ac-11ee-a818-4200a9fe0102")
ASK_ASTRO_REQUEST_ID_2 = os.environ.get("ASK_ASTRO_REQUEST_ID_2", "f2d6524c-56ab-11ee-a818-4200a9fe0102")


class APIMonitoring:
    """
    A class to test API endpoints using GET or POST requests and ensure that they return HTTP 200 status codes.
    """

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url

    def test_endpoint(
        self, endpoint: str, method: str = "GET", data: dict | None = None, headers: dict | None = None
    ) -> int:
        """
        Test an endpoint with the specified method, data, and headers.

        :param endpoint: The endpoint to test.
        :param method: The HTTP method to use. Defaults to 'GET'.
        :param data: The data to send in the request. Defaults to None.
        :param headers: The headers to send in the request. Defaults to None.
        """
        try:
            url = f"{self.base_url}{endpoint}"
            if method.upper() == "GET":
                response = requests.get(url, headers=headers)
            elif method.upper() == "POST":
                response = requests.post(url, json=data, headers=headers)
            else:
                logger.error(f"Unsupported method: {method}")
                raise Exception(f"Unsupported method: {method}")
            response.raise_for_status()
            logger.info(f"Success: {method} {endpoint} returned status code {response.status_code}")

        except requests.exceptions.HTTPError as err:
            logger.error(f"Failed: {method} {endpoint} returned status code {err.response.status_code}")


@task(trigger_rule=TriggerRule.ALL_DONE)
def slack_status(**context):
    """
    Post service status on slack.

    If any upstream task fail or DAG is trigger manually send immediate notification to slack.
    """
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

    # If some task fail then always publish result otherwise publish if trigger externally
    publish_result = False
    service_status = None
    if task_status:
        service_status = "\n<!here>\n"
        service_status += "\n".join(task_status)
        publish_result = True
    elif context["dag_run"].external_trigger:
        service_status = "\n:large_green_circle: All service are up!\n"
        publish_result = True

    if publish_result and service_status:
        SlackWebhookOperator(
            task_id="slack_alert",
            slack_webhook_conn_id=slack_webhook_conn,
            message=service_status,
        ).execute(context=context)


@task(trigger_rule=TriggerRule.ALL_DONE)
def check_ui_status():
    """Check UI respond with 200 status code."""
    endpoint = "https://ask.astronomer.io"
    response = requests.get(endpoint)
    if response.status_code != 200:
        raise Exception(f"UI check failed with status code: {response.status_code}")
    logger.info(f"UI check passed with status code: {response.status_code}")


@task(trigger_rule=TriggerRule.ALL_DONE)
def check_weaviate_status():
    """Check weaviate class exist."""
    weaviate_hook = WeaviateHook(weaviate_conn_id)
    client = weaviate_hook.get_conn()
    schemas = client.query.aggregate(weaviate_class).with_meta_count().do()
    schema = schemas["data"]["Aggregate"][weaviate_class]
    count = 0
    for v in schema:
        metadata = v.get("meta")
        if metadata:
            count = metadata.get("count")
            break
    if count == 0:
        logger.error(f"Weaviate class {weaviate_class} is empty!")
    else:
        logger.info(f"{count} record found in Weaviate class {weaviate_class}")


@task(trigger_rule=TriggerRule.ALL_DONE)
def check_firestore_status():
    """Check firestore app exist."""
    with tempfile.NamedTemporaryFile(mode="w+") as tf:
        if google_service_account_json_value:
            json.dump(google_service_account_json_value, tf)
            tf.flush()
            os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", tf.name)

        firebase_admin.initialize_app()
        app = firebase_admin.get_app(name=firestore_app_name)

        logger.info(f"{app.name} found!")


@task(trigger_rule=TriggerRule.ALL_DONE)
def monitor_apis():
    """
    Monitor a set of predefined API endpoints using the APIMonitoring class and report on their HTTP status codes.
    This task will test each API endpoint defined in the endpoints list and will print the status code for each.
    If any endpoint does not return a 200 status, an exception will be raised.
    """
    if not ASK_ASTRO_API_BASE_URL:
        raise ValueError("base_url cannot be empty.")

    api_monitor = APIMonitoring(base_url=ASK_ASTRO_API_BASE_URL)

    headers = {"accept": "*/*", "Content-Type": "application/json"}
    request_body = {"prompt": "Example prompt"}
    feedback_body = {"positive": True}

    endpoints_to_monitor = [
        ("/slack/install", "GET", headers),
        ("/requests", "POST", headers, request_body),
        ("/requests", "GET", headers),
    ]

    request_uuids = [ASK_ASTRO_REQUEST_ID_1, ASK_ASTRO_REQUEST_ID_2]
    for request_id in request_uuids:
        endpoints_to_monitor.append((f"/requests/{request_id}", "GET", headers))
        endpoints_to_monitor.append((f"/requests/{request_id}/feedback", "POST", headers, feedback_body))

    # Monitor each API endpoint
    for endpoint_info in endpoints_to_monitor:
        try:
            # Unpack the tuple with a default value for 'body'
            endpoint, method, headers, data = (*endpoint_info, None)[:4]
            status_code = api_monitor.test_endpoint(endpoint=endpoint, method=method, headers=headers, data=data)
            logger.info(f"Endpoint {endpoint} returned status code {status_code}")
        except requests.HTTPError as e:
            logger.error(f"Endpoint {endpoint_info[0]} failed with status code: {e.response.status_code}")
            raise Exception(f"Endpoint {endpoint_info[0]} failed with status code: {e.response.status_code}")


@dag(
    schedule_interval=monitoring_interval, start_date=datetime(2023, 9, 27), catchup=False, is_paused_upon_creation=True
)
def monitoring_dag():
    [check_ui_status(), check_weaviate_status(), check_firestore_status(), monitor_apis()] >> slack_status()


monitoring_dag()
