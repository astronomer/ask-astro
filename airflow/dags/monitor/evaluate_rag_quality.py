from __future__ import annotations

import ast
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from textwrap import dedent

import pandas as pd
from include.tasks.extract.utils.evaluate_helpers import (
    generate_answer,
    get_or_create_drive_folder,
)

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.providers.google.suite.hooks.drive import GoogleDriveHook
from airflow.providers.google.suite.hooks.sheets import GSheetsHook
from airflow.providers.weaviate.hooks.weaviate import WeaviateHook

ask_astro_env = os.environ.get("ASK_ASTRO_ENV", "dev")
test_questions_sheet_id = os.environ.get("TEST_QUESTIONS_SHEET_ID")
askastro_endpoint_url = os.environ.get("ASK_ASTRO_ENDPOINT_URL")
langchain_org_id = os.environ.get("LANGCHAIN_ORG")
langchain_project_id = os.environ.get(f"LANGCHAIN_PROJECT_ID_{ask_astro_env.upper()}", None)
azure_endpoint = os.environ.get("AZURE_OPENAI_USEAST_PARAMS")
google_domain_id = os.environ.get("GOOGLE_DOMAIN_ID")

drive_folder = f"ask_astro/tests_{ask_astro_env}"

_WEAVIATE_CONN_ID = f"weaviate_{ask_astro_env}"
_DRIVE_CONN_ID = "google_cloud_drive"

WEAVIATE_CLASS = os.environ.get("WEAVIATE_CLASS", "DocsDev")

logger = logging.getLogger("airflow.task")

test_question_template_path = "include/data/test_questions_template.csv"

default_args = {"retries": 3, "retry_delay": 30, "trigger_rule": "none_failed"}


@dag(
    schedule_interval=None,
    start_date=datetime(2023, 9, 27),
    catchup=False,
    is_paused_upon_creation=True,
    default_args=default_args,
    params={
        "question_number_subset": Param(
            default="",
            title="Subset of test numbers to run.",
            description=dedent(
                """
                A set of test ID numbers to run instead of the entire test set.
                Format should be a bracketed list of integers
                (ie. [14, 19, 21, 29, 32, 38, 50, 52])."""
            ),
        )
    },
)
def evaluate_rag_quality(question_number_subset: str):
    """
    This DAG performs a test of document retrieval from Ask Astro's vector database.

    It downloads a set of test questions as a CSV file from Google Sheets.

    Environment variables needed:
    - ASK_ASTRO_ENV the environment name (ie. 'dev', 'prod', 'local')
    - WEAVIATE_CLASS schema class name (ie. 'DocsLocal', 'DocsProd', etc.)
    - AIRFLOW_CONN_WEAVIATE_<env> Airflow connection string for the weaviate instance
    - AZURE_OPENAI_USEAST_PARAMS Airflow connection string for Azure OpenAI instance used for
        multiquery retriever tests.
    - AIRFLOW_CONN_GOOGLE_CLOUD_DRIVE Airflow connection string for Google Drive (with scope
        "https://www.googleapis.com/auth/drive") for the Ask Astro Project test pipeline
        service account.
    - LANGCHAIN_ORG Organization ID for creating the link to the Langsmith run
    - LANGCHAIN_PROJECT_ID_<env> Project ID for creating the link to the Langsmith run
    - TEST_QUESTIONS_SHEET_ID the Google sheet ID for the test questions this should be in the
        Google project account.
    - ASK_ASTRO_ENDPOINT_URL the endpoint URL for the Ask Astro frontend to be tested.
    - GOOGLE_DOMAIN_ID the Google Cloud domain ID to grant access to for reading the results.

    Retrieved answers and references are saved as CSV and uploaded to a Google Spreadsheet.  The
    `upload_results` task logs print a link to the uploaded sheet.

    Additionally, this DAG is parameterized to allow running a subset of the questions.  The
    estimated cost is approximately $.50 per question.  If the number of questions is large and a
    specific change to the ingest only impacts a subset of questions it may be advantageous to test
    only that subset first (potentially multiple times) before testing the entire question set for
    regressions.

    :param question_number_subset: A json string of a list of integers representing a subset of
    test numbers from the test question template spreadsheet.
    """

    @task
    def create_drive_folders(drive_folder: str) -> list:
        """
        This task creates Google Drive folders for the test results and returns the folder ID.
        If the folders already exist it returns the folder ID.

        :param drive_folder:  A fully-qualified path name for the folders. ie. "ask_astro/tests_dev"
        """

        gd_hook = GoogleDriveHook(gcp_conn_id=_DRIVE_CONN_ID)

        drive_folder_parts = drive_folder.split("/")

        parent_id = get_or_create_drive_folder(gd_hook=gd_hook, folder_name=drive_folder_parts[0], parent_id=None)

        folder_id = get_or_create_drive_folder(gd_hook=gd_hook, folder_name=drive_folder_parts[1], parent_id=parent_id)

        gd_hook.get_conn().permissions().create(
            fileId=parent_id, body={"type": "domain", "domain": google_domain_id, "role": "writer"}
        ).execute()

        return folder_id

    @task
    def get_schema(schema_file: str = "include/data/schema.json") -> list:
        """
        Get the schema object for this DAG.
        """

        class_objects = json.loads(Path(schema_file).read_text())
        class_objects["classes"][0].update({"class": WEAVIATE_CLASS})

        class_objects = class_objects.get("classes", [class_objects])

        return class_objects

    @task
    def check_schema(class_objects: dict) -> bool:
        """
        Check if the current schema includes the requested schema.  The current schema could be a superset
        so check_schema_subset is used recursively to check that all objects in the requested schema are
        represented in the current schema.
        """

        if WeaviateHook(_WEAVIATE_CONN_ID).check_subset_of_schema(classes_objects=class_objects):
            return True

        raise AirflowException(
            """
            Class does not exist in current schema. Create it with
            'WeaviateHook(_WEAVIATE_CONN_ID).check_subset_of_schema(class_objects=class_objects)'
            """
        )

    @task
    def download_test_questions(test_questions_sheet_id: str):
        gs_hook = GSheetsHook(_DRIVE_CONN_ID)

        test_questions_sheet = gs_hook.get_spreadsheet(test_questions_sheet_id)

        values = gs_hook.get_values(spreadsheet_id=test_questions_sheet.get("spreadsheetId"), range_="test_questions")
        if values:
            pd.DataFrame(values[1:], columns=values[0]).to_csv(test_question_template_path, index=False)

            return test_question_template_path
        raise ValueError("Could not download test question sheet.")

    @task
    def generate_test_answers(test_question_template_path: Path, ts_nodash=None, **context):
        """
        Given a set of test questions (csv) add columns with references and answers with
        various methods.  Saves results in a csv file name with the DAG run timestamp.
        """

        question_number_subset = context["params"]["question_number_subset"]

        if question_number_subset:
            question_number_subset = ast.literal_eval(question_number_subset)

        results_file = f"include/data/test_questions_{ts_nodash}.csv"

        csv_columns = [
            "test_number",
            "question",
            "expected_references",
            "askastro_answer",
            "askastro_references",
            "langsmith_link",
        ]

        questions_df = pd.read_csv(test_question_template_path)

        if question_number_subset:
            questions_df = questions_df[questions_df.test_number.isin(question_number_subset)]

        questions_df[["askastro_answer", "askastro_references", "langsmith_link"]] = questions_df.question.apply(
            lambda x: pd.Series(
                generate_answer(
                    askastro_endpoint_url=askastro_endpoint_url,
                    question=x,
                    langchain_org_id=langchain_org_id,
                    langchain_project_id=langchain_project_id,
                )
            )
        )

        questions_df[csv_columns].to_csv(results_file, index=False)

        return results_file

    @task
    def upload_results(results_file: str, drive_id: str, ts_nodash: str = None):
        gs_hook = GSheetsHook(_DRIVE_CONN_ID)
        gd_hook = GoogleDriveHook(gcp_conn_id=_DRIVE_CONN_ID)

        results_sheet = (
            gd_hook.get_conn()
            .files()
            .create(
                body={
                    "name": f"test_results_{ts_nodash}",
                    "mimeType": "application/vnd.google-apps.spreadsheet",
                    "parents": [drive_id],
                },
                fields="id",
            )
            .execute()
        )

        results = pd.read_csv(results_file).fillna("NULL")

        values = results.T.reset_index().values.T.tolist()

        existing_data = gs_hook.get_values(spreadsheet_id=results_sheet.get("id"), range_="A1")

        if existing_data:
            raise ValueError("Spreadsheet exists. Not overwriting")

        gs_hook.append_values(
            spreadsheet_id=results_sheet.get("id"), range_="A1", values=values, include_values_in_response=False
        )

        logger.info(f"Test results are available at: https://drive.google.com/drive/folders/{drive_id}")

    _results_folder_id = create_drive_folders(drive_folder)
    _get_schema = get_schema()
    _check_schema = check_schema(_get_schema)
    _download_questions = download_test_questions(test_questions_sheet_id)
    _results_file = generate_test_answers(_download_questions)
    _upload_results = upload_results(results_file=_results_file, drive_id=_results_folder_id)

    _check_schema >> _results_file >> _upload_results


evaluate_rag_quality(question_number_subset=None)
