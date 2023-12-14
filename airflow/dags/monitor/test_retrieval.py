
from datetime import datetime
import json
import logging
import os
import pandas as pd
from pathlib import Path
import requests

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.suite.hooks.sheets import GSheetsHook
# from airflow.providers.weaviate.hooks.weaviate import WeaviateHook
from include.tasks.extract.utils.weaviate.ask_astro_weaviate_hook import AskAstroWeaviateHook
from include.tasks.extract.utils.retrieval_tests import (
    weaviate_search, 
    weaviate_search_mqr, 
    generate_answer
)

ask_astro_env = os.environ.get("ASK_ASTRO_ENV", "dev")
test_questions_sheet_id=os.environ.get("TEST_QUESTIONS_SHEET_ID")
askastro_endpoint_url = os.environ.get("ASK_ASTRO_ENDPOINT_URL")

_WEAVIATE_CONN_ID = f"weaviate_{ask_astro_env}"
_GCP_CONN_ID = "google_cloud_gcp"
_DRIVE_CONN_ID = "google_cloud_drive"

WEAVIATE_CLASS = os.environ.get("WEAVIATE_CLASS", "DocsDev")

logger = logging.getLogger("airflow.task")

test_question_template_path = Path("include/data/test_questions_template.csv")

expected_doc_count = 36860
question_number_subset = {14, 19, } #21, 29, 32, 38, 50, 52}

default_args = {"retries": 3, "retry_delay": 30, "trigger_rule": "none_failed"}

@dag(
    schedule_interval=None, 
    start_date=datetime(2023, 9, 27), 
    catchup=False, 
    is_paused_upon_creation=True, 
    default_args=default_args)
def test_retrieval():
    """
    This DAG performs a test of the Ask Astro document ingest.  

    It downloads a set of test questions as a CSV file from Google Sheets.  
    Set TEST_QUESTIONS_SHEET_ID environment variable to specify the source of test questions.
    
    Retrieved answers and references are saved as CSV and uploaded to a Google Spreadsheet.
    """

    @task
    def get_schema(schema_file: str = "include/data/schema.json") -> list:
        """
        Get the schema object for this DAG.
        """

        class_objects = json.loads(Path(schema_file).read_text())
        class_objects["classes"][0].update({"class": WEAVIATE_CLASS})

        if "classes" not in class_objects:
            class_objects = [class_objects]
        else:
            class_objects = class_objects["classes"]

        return class_objects

    @task
    def check_schema(class_objects: dict) -> bool:
        """
        Check if the current schema includes the requested schema.  The current schema could be a superset
        so check_schema_subset is used recursively to check that all objects in the requested schema are
        represented in the current schema.
        """
        
        if AskAstroWeaviateHook(_WEAVIATE_CONN_ID).check_schema(class_objects=class_objects):
            return True
        else:
            raise AirflowException(f"""
                Class does not exist in current schema. Create it with 
                'AskAstroWeaviateHook(_WEAVIATE_CONN_ID).create_schema(class_objects=class_objects, existing="error")'
                """)
    
    @task
    def check_doc_count(expected_count: int) -> bool:
        """
        Check if the vectordb has AT LEAST expected_count objects.
        """
        
        count = AskAstroWeaviateHook(_WEAVIATE_CONN_ID)\
                    .client.query.aggregate(WEAVIATE_CLASS).with_meta_count().do()
        
        doc_count = count["data"]["Aggregate"][WEAVIATE_CLASS][0]["meta"]["count"]

        if doc_count >= expected_count:
            return True
        else:
            raise AirflowException("Unknown vectordb state. Ingest baseline or change expected_count.")
    
    @task
    def download_test_questions(test_questions_sheet_id: str):
        gs_hook = GSheetsHook(_DRIVE_CONN_ID)

        test_questions_sheet = gs_hook.get_spreadsheet(test_questions_sheet_id)
        
        values = gs_hook.get_values(
            spreadsheet_id=test_questions_sheet.get("spreadsheetId"),
            range_='test_questions'
            )
        if values:
            test_question_template_path.write_text(data=json.dumps(values))

            return test_question_template_path.absolute()
        else:
            raise ValueError("Could not download test question sheet.")
    
    @task
    def generate_test_answers(test_question_template_path:Path, ts_nodash=None):
        """
        Given a set of test questions (csv) add columns with references and answers with 
        various methods.  Saves results in a csv file name with the DAG run timestamp. 
        """
    
        results_file = f"include/data/test_questions_{ts_nodash}.csv"

        csv_columns = [
            "test_number",
            "question",
            "expected_references",
            "weaviate_search_references",
            "weaviate_mqr_references",
            "askastro_answer",
            "askastro_references",
            "langsmith_link",
        ]

        weaviate_client = AskAstroWeaviateHook(_WEAVIATE_CONN_ID).client

        questions_df=pd.read_csv(test_question_template_path)

        if question_number_subset: 
            questions_df = questions_df[questions_df.test_number.isin(question_number_subset)]
        
        questions_df['weaviate_search_references'] = questions_df\
            .question.apply(lambda x: weaviate_search(
                weaviate_client=weaviate_client, 
                question=x, 
                class_name=WEAVIATE_CLASS))
        
        questions_df['weaviate_mqr_references'] = questions_df\
            .question.apply(lambda x: weaviate_search_mqr(
                weaviate_client=weaviate_client, 
                question=x, 
                class_name=WEAVIATE_CLASS))
        
        questions_df[
            ["askastro_answer", "askastro_references", "langsmith_link"]
            ] = questions_df\
            .question.apply(lambda x: pd.Series(generate_answer(
                askastro_endpoint_url=askastro_endpoint_url,
                question=x)))
        
        questions_df[csv_columns].to_csv(results_file, index=False)

        return results_file

    @task
    def upload_results(results_file: str, ts_nodash: str = None):
        
        gs_hook = GSheetsHook(_DRIVE_CONN_ID)

        values=pd.read_csv(results_file).values

        results_sheet = {
            'properties': {
                'title': 'askastro_test_results_'+ts_nodash,
                'locale': 'en_US',
                'timeZone': 'Etc/GMT'
                },
            'sheets': [
                {'properties': {
                    # 'sheetId': 1762228914,
                    'title': 'test_results_'+ts_nodash,
                    'index': 0,
                    'sheetType': 'GRID',
                    }
                },
            ],
        }
        
        results_sheet_dict = gs_hook.create_spreadsheet(results_sheet)

        existing_data = gs_hook.get_values(
            spreadsheet_id=results_sheet_dict.get("spreadsheetId"),
            range_="A1")
        
        if existing_data:
            raise ValueError("Spreadsheet exists. Not overwriting")

        response = gs_hook.append_values(
            spreadsheet_id=results_sheet_dict.get("spreadsheetId"),
            range_="A1",
            values=values.tolist(),
            include_values_in_response=False
            )
        
        if (
            response["updates"]["updatedRows"], 
            response["updates"]['updatedColumns']) != values.shape:
            raise ValueError("Not all values uploaded to spreadsheet.")

        logger.info("Test results are available at: "+results_sheet_dict.get("spreadsheetUrl"))

    _get_schema = get_schema()
    _check_schema = check_schema(_get_schema)
    _check_doc_count = check_doc_count(expected_doc_count)
    _download_questions = download_test_questions(test_questions_sheet_id)
    _results_file = generate_test_answers(_download_questions)
    _upload_results = upload_results(_results_file)
    
    _check_schema >> _check_doc_count >> _download_questions
    
test_retrieval()
