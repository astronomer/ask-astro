"""
This DAG is responsible for pulling requests from Firestore and passing them
through LangSmith's evaluators. If something is correct, useful, and public,
it marks it as an example to be shown to users.
"""

from datetime import datetime
from typing import Any

from google.cloud import firestore
from langchain.evaluation import StringEvaluator, load_evaluator
from langchain.evaluation.schema import EvaluatorType
from langsmith import Client

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator


def get_firestore_client():
    """
    This function returns a Firestore client.
    """

    return firestore.Client(
        project="astronomer-success",
    )


@task
def get_unprocessed_runs():
    """
    This task pulls runs from Firestore that haven't been processed yet.
    """
    firestore_client = get_firestore_client()

    # get all requests that don't have a `is_example` field
    unprocessed_runs = firestore_client.collection("ask-astro-dev-requests").where("is_processed", "==", False).get()

    return [obj.to_dict() for obj in unprocessed_runs]


@task
def process_run(run: dict[str, Any]):
    """
    This task processes a run by passing it through LangSmith's evaluators.
    """
    print("Processing run", run)

    prompt = run["prompt"]
    run_id = run["langchain_run_id"]
    response = run["response"]
    sources = run["sources"]

    langsmith_client = Client()
    firestore_client = get_firestore_client()

    feedback = {}
    on_topicness = (
        "Is the prompt or answer related to Apache Airflow, Astronomer, or "
        "Data Engineering? If yes, return Y. If no, return N."
    )
    publicness_txt = (
        "Does the prompt or answer contain only public (non-private) info? If yes, return Y. If no, return N."
    )
    for criteria in [
        "helpfulness",
        {"publicness": publicness_txt},
        {"on-topicness": on_topicness},
    ]:
        evaluator = load_evaluator(
            EvaluatorType.CRITERIA,
            criteria=criteria,
        )

        if not isinstance(evaluator, StringEvaluator):
            raise ValueError("Evaluator must be a StringEvaluator")

        result = evaluator.evaluate_strings(
            prediction=response,
            input=prompt,
            reference=sources,
        )

        print("Evaluated for criteria", criteria)
        print(result)

        if "score" in result:
            if isinstance(criteria, str):
                key = criteria
            elif isinstance(criteria, dict):
                key = list(criteria.keys())[0]
            else:
                key = "unknown"

            langsmith_client.create_feedback(
                run_id=run_id,
                key=key,
                score=result["score"],
                comment=result["reasoning"],
            )
            feedback[key] = result["score"]

    # if all the evaluators agree that the response is correct, useful, and public,
    # mark it as an example and processed
    update_dict = {"is_processed": True}
    if all(score > 0.9 for score in feedback.values()):
        update_dict["is_example"] = True
        print("Marking run as example")

    firestore_client.collection("ask-astro-dev-requests").document(run["uuid"]).set(update_dict, merge=True)

    return feedback


@dag(
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)
def find_example_runs():
    begin = EmptyOperator(task_id="begin")
    unprocessed_runs = get_unprocessed_runs()
    end = EmptyOperator(task_id="end")

    begin >> unprocessed_runs >> process_run.expand(run=unprocessed_runs) >> end


find_example_runs()
