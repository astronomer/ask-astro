import os

from airflow.providers.slack.notifications.slack import send_slack_notification

AIRFLOW__WEBSERVER__BASE_URL = os.environ.get("AIRFLOW__WEBSERVER__BASE_URL", "http://localhost:8080")
ASK_ASTRO_ALERT_SLACK_CHANNEL_NAME = os.environ.get("ASK_ASTRO_ALERT_SLACK_CHANNEL_NAME", "#ask-astro-alert")
ASK_ASTRO_ALERT_SLACK_CONN_ID = os.environ.get("ASK_ASTRO_ALERT_SLACK_CONN_ID", "slack_api_default")


def send_failure_notification(dag_id, execution_date):
    dag_link = f"{AIRFLOW__WEBSERVER__BASE_URL}/dags/{dag_id}/grid?search={dag_id}"
    notification_text = f":red_circle: The DAG <{dag_link}|{dag_id}> with execution date `{execution_date}` failed."
    return send_slack_notification(
        slack_conn_id=ASK_ASTRO_ALERT_SLACK_CONN_ID, channel=ASK_ASTRO_ALERT_SLACK_CHANNEL_NAME, text=notification_text
    )
