from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import datetime
from typing import Any

import firebase_admin
import requests

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models import TaskInstance
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.weaviate.hooks.weaviate import WeaviateHook
from airflow.utils.context import Context
from airflow.utils.trigger_rule import TriggerRule

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


def aggregate_task_statuses(tis_dagrun: list[TaskInstance]) -> tuple[list[str], list[str]]:
    """
    Collects the statuses of tasks in the DAG run.

    :param tis_dagrun: A list of TaskInstance objects.
    """
    task_status = []
    failed_tasks = []
    for ti in tis_dagrun:
        if ti.task_id == "slack_status":
            continue
        if ti.state == "success":
            task_status.append(f":large_green_circle: {ti.task_id}")
        elif ti.state == "failed":
            failed_tasks.append(f":red_circle: {ti.task_id}")
    return task_status, failed_tasks


def aggregate_error_messages(context: Context, error_keys: list[str]) -> list[str]:
    """
    Aggregates error messages from XCom based on provided keys.

    :param context: The task context provided by Airflow.
    :param error_keys: A list of keys to pull from XCom.
    """
    task_instance = context["task_instance"]
    error_messages = []
    for key in error_keys:
        errors = task_instance.xcom_pull(key=key)
        if errors:
            if isinstance(errors, list):
                error_messages.extend(errors)
            else:
                error_messages.append(errors)
    return error_messages


def construct_service_status_message(
    task_status: list[str], failed_tasks: list[str], error_messages: list[str], external_trigger: bool
) -> str:
    """
    Constructs the status message for Slack based on task statuses and error messages.

    :param task_status: A list of strings representing the successful tasks.
    :param failed_tasks: A list of strings representing the failed tasks.
    :param error_messages: A list of error messages from failed task executions.
    :param external_trigger: A boolean indicating if the DAG was triggered externally.
    """
    service_status_lines = []

    if failed_tasks:
        failed_tasks_formatted = "\n".join(failed_tasks)
        service_status_lines.append(f"*Failed Tasks:*\n{failed_tasks_formatted}")

    # Format error messages as a code block for better readability
    if error_messages:
        # Slack code block formatting with triple backticks
        error_messages_formatted = "\n".join([f"```{error}```" for error in error_messages])
        service_status_lines.append(f"*Error Details:*\n{error_messages_formatted}")

    # If the DAG was externally triggered and there are no failures, show all services are up
    if external_trigger and not failed_tasks:
        service_status_lines.append(":large_green_circle: *All services are up!*")

    # If not all services are up, list the successful tasks
    if not external_trigger or failed_tasks:
        successful_tasks_formatted = "\n".join(
            [f":large_green_circle: {task}" for task in task_status if task not in failed_tasks]
        )
        if successful_tasks_formatted:
            service_status_lines.append(f"*Successful Tasks:*\n{successful_tasks_formatted}")

    # Join all parts into the final service status message
    service_status = "\n\n".join(service_status_lines)

    return service_status


@task(trigger_rule=TriggerRule.ALL_DONE)
def slack_status(**context: Any) -> None:
    """
    Sends a status message to Slack. This task will execute even if upstream tasks fail.

    :param context: The context dictionary provided by Airflow.
    """
    tis_dagrun = context["ti"].get_dagrun().get_task_instances()
    task_status, failed_tasks = aggregate_task_statuses(tis_dagrun)
    error_keys = ["api_errors", "ui_check_error", "weaviate_check_error", "firestore_check_error"]
    error_messages = aggregate_error_messages(context, error_keys)

    publish_result = bool(failed_tasks) or context["dag_run"].external_trigger
    service_status = construct_service_status_message(
        task_status, failed_tasks, error_messages, context["dag_run"].external_trigger
    )

    # Send the Slack message if needed
    if publish_result:
        SlackWebhookOperator(
            task_id="slack_alert",
            slack_webhook_conn_id=slack_webhook_conn,
            message=service_status,
        ).execute(context=context)


@task()
def check_ui_status(**context) -> None:
    """
    Check UI respond with 200 status code.

    :param context: The context dictionary provided by Airflow.
    """
    try:
        endpoint = "https://ask.astronomer.io"
        response = requests.get(endpoint)
        response.raise_for_status()
        logger.info(f"UI check passed with status code: {response.status_code}")
    except requests.exceptions.HTTPError as err:
        error_message = f"UI Check Error: {err}"
        context["task_instance"].xcom_push(key="ui_check_error", value=error_message)
        raise AirflowException(error_message)


@task()
def check_weaviate_status(**context) -> None:
    """
    Check weaviate class exist.

    :param context: The context dictionary provided by Airflow.
    """
    try:
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
    except Exception as err:
        error_message = f"Weaviate Check Error: {err}"
        context["task_instance"].xcom_push(key="weaviate_check_error", value=error_message)
        raise AirflowException(error_message)


@task()
def check_firestore_status(**context) -> None:
    """
    Check firestore app exist.

    :param context: The context dictionary provided by Airflow.
    """
    try:
        with tempfile.NamedTemporaryFile(mode="w+") as tf:
            if google_service_account_json_value:
                json.dump(google_service_account_json_value, tf)
                tf.flush()
                os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", tf.name)

            firebase_admin.initialize_app()
            app = firebase_admin.get_app(name=firestore_app_name)

            logger.info(f"{app.name} found!")
    except Exception as err:
        error_message = f"Firestore Check Error: {err}"
        context["task_instance"].xcom_push(key="firestore_check_error", value=error_message)
        raise AirflowException(error_message)


@task()
def monitor_apis(**context) -> None:
    """
    Monitor a set of predefined API endpoints using the APIMonitoring class and report on their HTTP status codes.
    This task will test each API endpoint defined in the endpoints list and will print the status code for each.
    If any endpoint does not return a 200 status, the errors will be collected and reported.

    :param context: The context dictionary provided by Airflow.
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

    errors = []

    # Monitor each API endpoint
    for endpoint_info in endpoints_to_monitor:
        endpoint, method, headers, data = (*endpoint_info, None)[:4]
        try:
            api_monitor.test_endpoint(endpoint=endpoint, method=method, headers=headers, data=data)
            logger.info(f"Endpoint {endpoint} returned status code 200")
        except requests.HTTPError as http_err:
            error_message = f"HTTPError for {endpoint}: {http_err.response.status_code}"
            errors.append(error_message)
            logger.error(error_message)
        except Exception as err:
            error_message = f"Error for {endpoint}: {err}"
            errors.append(error_message)
            logger.error(error_message)
    if errors:
        context["task_instance"].xcom_push(key="api_errors", value=errors)
        raise AirflowException("API Monitoring encountered errors.")

    return errors


@dag(
    schedule_interval=monitoring_interval, start_date=datetime(2023, 9, 27), catchup=False, is_paused_upon_creation=True
)
def monitoring_dag():
    [check_ui_status(), check_weaviate_status(), check_firestore_status(), monitor_apis()] >> slack_status()


monitoring_dag()
