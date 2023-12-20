from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any

from airflow.decorators import dag, task
from airflow.models import DagBag
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.cli import process_subdir

logger = logging.getLogger("airflow.task")

slack_webhook_conn = os.environ.get("SLACK_WEBHOOK_CONN", "slack_webhook_default")


ingestion_dags = [
    "ask_astro_load_airflow_docs",
    "ask_astro_load_astro_cli_docs",
    "ask_astro_load_astronomer_providers",
    "ask_astro_load_astro_sdk" "ask_astro_load_blogs",
    "ask_astro_load_bulk",
    "ask_astro_load_github",
    "ask_astro_load_registry",
    # "ask_astro_load_slack",
    "ask_astro_load_stackoverflow",
]


@task
def check_ingestion_dags(**context: Any):
    airflow_home = os.environ.get("AIRFLOW_HOME")
    dagbag = DagBag(process_subdir(f"{airflow_home}/dags"))
    data = []
    for filename, errors in dagbag.import_errors.items():
        data.append({"filepath": filename, "error": errors})

    if data:
        logger.info("************DAG Import Error*************")
        logger.error(data)
        logger.info("******************************")
        message = ":red_circle: Import Error in DAG"

    ingestion_dag_exist = False
    if set(ingestion_dags).issubset(set(dagbag.dag_ids)):
        ingestion_dag_exist = True
        message = ":red_circle: Some Ingestion DAG's are missing"

    if not ingestion_dag_exist or data:
        SlackWebhookOperator(
            task_id="slack_alert",
            slack_webhook_conn_id=slack_webhook_conn,
            message=message,
        ).execute(context=context)


@dag(schedule_interval="@daily", start_date=datetime(2023, 9, 27), catchup=False, is_paused_upon_creation=True)
def monitor_ingestion_dags():
    check_ingestion_dags()


monitor_ingestion_dags()
