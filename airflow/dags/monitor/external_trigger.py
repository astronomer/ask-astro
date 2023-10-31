from datetime import datetime
from airflow.decorators import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@dag(
    schedule_interval="@daily", start_date=datetime(2023, 9, 27), catchup=False, is_paused_upon_creation=True
)
def external_trigger_monitoring_dag():
    TriggerDagRunOperator(
        task_id="run_monitoring_dag",
        trigger_dag_id="monitoring_dag",
    )


external_trigger_monitoring_dag()
