"""
This DAG is responsible for pulling requests from Firestore and writing them
to snowflake.
"""

import os
import time
from datetime import date, datetime, timedelta

import snowflake.connector
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter
from include.utils.slack import send_failure_notification

from airflow.decorators import dag, task

METRICS_SNOWFLAKE_DB_USER = os.environ.get("METRICS_SNOWFLAKE_DB_USER")
METRICS_SNOWFLAKE_DB_PASSWORD = os.environ.get("METRICS_SNOWFLAKE_DB_PASSWORD")
METRICS_SNOWFLAKE_DB_ACCOUNT = os.environ.get("METRICS_SNOWFLAKE_DB_ACCOUNT")
METRICS_SNOWFLAKE_DB_DATABASE = os.environ.get("METRICS_SNOWFLAKE_DB_DATABASE")
METRICS_SNOWFLAKE_DB_SCHEMA = os.environ.get("METRICS_SNOWFLAKE_DB_SCHEMA")

FIRESTORE_REQUESTS_COLLECTION = os.environ.get("FIRESTORE_REQUESTS_COLLECTION")

default_args = {"retries": 3, "retry_delay": 30}

snowflake.connector.paramstyle = "qmark"


@dag(
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    default_args=default_args,
    catchup=False,
    on_failure_callback=send_failure_notification(
        dag_id="{{ dag.dag_id }}", execution_date="{{ dag_run.execution_date }}"
    ),
)
def load_firestore_to_snowflake():
    @task()
    def load_request_data_from_firestore() -> list[tuple[str, int, bool, int]]:
        firestore_client = firestore.Client(project="ask-astro")

        requests_col = firestore_client.collection(FIRESTORE_REQUESTS_COLLECTION)

        yesterday = date.today() - timedelta(days=1)
        one_day_before_yesterday = date.today() - timedelta(days=2)
        start_ts = time.mktime(one_day_before_yesterday.timetuple())
        end_ts = time.mktime(yesterday.timetuple())

        docs = (
            requests_col.where(filter=FieldFilter("response_received_at", ">=", start_ts))
            .where(filter=FieldFilter("response_received_at", "<", end_ts))
            .stream()
        )

        rows: list[tuple[str, Optional[int], bool, datetime]] = []
        for doc in docs:
            doc_dict = doc.to_dict()
            uuid = doc_dict["uuid"]
            score = doc_dict.get("score")
            status = doc_dict.get("status") == "complete"
            response_received_at = datetime.fromtimestamp(doc_dict.get("response_received_at"))
            rows.append((uuid, score, status, response_received_at))

        return rows

    @task()
    def write_request_data_to_snowflake(rows: list[tuple[str, int, bool, int]]) -> None:
        conn = snowflake.connector.connect(
            user=METRICS_SNOWFLAKE_DB_USER,
            password=METRICS_SNOWFLAKE_DB_PASSWORD,
            account=METRICS_SNOWFLAKE_DB_ACCOUNT,
            database=METRICS_SNOWFLAKE_DB_DATABASE,
            schema=METRICS_SNOWFLAKE_DB_SCHEMA,
        )

        insert_sql = f"""
            INSERT INTO
                {METRICS_SNOWFLAKE_DB_DATABASE}.{METRICS_SNOWFLAKE_DB_SCHEMA}.request(uuid, score, success, created_at)
            VALUES
                (?, ?, ?, ?)
        """
        conn.cursor().executemany(insert_sql, rows)

    write_request_data_to_snowflake(load_request_data_from_firestore())


load_firestore_to_snowflake()
