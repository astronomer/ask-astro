"""
This DAG is responsible for pulling requests from Firestore and writing them
to snowflake.
"""

import os
import time
from datetime import datetime

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
    def load_request_data_from_firestore(
        data_interval_start, data_interval_end
    ) -> list[tuple[str, str, str, str, bool, datetime, str]]:
        firestore_client = firestore.Client(project="ask-astro")

        requests_col = firestore_client.collection(FIRESTORE_REQUESTS_COLLECTION)

        start_ts = time.mktime(data_interval_start.timetuple())
        end_ts = time.mktime(data_interval_end.timetuple())

        print(f"Processing documents from {data_interval_start} to {data_interval_end}")

        docs = (
            requests_col.where(filter=FieldFilter("sent_at", ">=", start_ts))
            .where(filter=FieldFilter("sent_at", "<", end_ts))
            .stream()
        )

        rows: list[tuple[str, str, str, str, bool, datetime, str]] = []
        for doc in docs:
            doc_dict = doc.to_dict()
            uuid = doc_dict["uuid"]
            score = doc_dict.get("score")
            prompt = doc_dict.get("prompt")
            response = doc_dict.get("response")
            status = doc_dict.get("status") == "complete"
            sent_at = datetime.fromtimestamp(doc_dict.get("sent_at"))
            client = doc_dict.get("client")
            rows.append((uuid, score, prompt, response, status, sent_at, client))

        print(f"Found {len(rows)} rows")

        return rows

    @task()
    def write_request_data_to_snowflake(rows: list[tuple[str, str, str, str, bool, datetime, str]]) -> None:
        conn = snowflake.connector.connect(
            user=METRICS_SNOWFLAKE_DB_USER,
            password=METRICS_SNOWFLAKE_DB_PASSWORD,
            account=METRICS_SNOWFLAKE_DB_ACCOUNT,
            database=METRICS_SNOWFLAKE_DB_DATABASE,
            schema=METRICS_SNOWFLAKE_DB_SCHEMA,
        )

        insert_sql = f"""
        MERGE INTO {METRICS_SNOWFLAKE_DB_DATABASE}.{METRICS_SNOWFLAKE_DB_SCHEMA}.request AS target
        USING (
            SELECT
                ? AS uuid,
                ? AS score,
                ? AS prompt,
                ? AS response,
                ? AS success,
                ? AS created_at,
                ? AS client
        ) AS source
        ON target.uuid = source.uuid
        WHEN MATCHED THEN
            UPDATE SET
                score = source.score,
                prompt = source.prompt,
                response = source.response,
                success = source.success,
                created_at = source.created_at,
                client = source.client
        WHEN NOT MATCHED THEN
            INSERT (uuid, score, prompt, response, success, created_at, client)
            VALUES (source.uuid, source.score, source.prompt, source.response, source.success, source.created_at, source.client);
        """
        conn.cursor().executemany(insert_sql, rows)

    write_request_data_to_snowflake(load_request_data_from_firestore())


load_firestore_to_snowflake()
