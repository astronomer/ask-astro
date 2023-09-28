from datetime import datetime 
import os

from include import extract, split, ingest

from airflow.decorators import dag, task
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from weaviate_provider.operators.weaviate import WeaviateCheckSchemaBranchOperator
from weaviate.util import generate_uuid5

try:
    ask_astro_env = os.environ['ASK_ASTRO_ENV']
except:
    ask_astro_env = 'NOOP'

if ask_astro_env == 'prod':
    _WEAVIATE_CONN_ID = 'weaviate_prod'
    _SLACK_CONN_ID = 'slack_api_pgdev'
    _GITHUB_CONN_ID = 'github_mpg'
elif ask_astro_env == 'dev':
    _WEAVIATE_CONN_ID = 'weaviate_local'
    _SLACK_CONN_ID = 'slack_api_pgdev'
    _GITHUB_CONN_ID = 'github_mpg'
elif ask_astro_env == 'test':
    _WEAVIATE_CONN_ID = 'weaviate_test'
    _SLACK_CONN_ID = 'slack_api_pgdev'
    _GITHUB_CONN_ID = 'github_mpg'
else:
    _WEAVIATE_CONN_ID = 'weaviate_NOOP'
    _SLACK_CONN_ID = 'slack_api_NOOP'
    _GITHUB_CONN_ID = 'github_NOOP'
    
blog_cutoff_date = datetime.strptime('2023-01-19', '%Y-%m-%d')

@dag(schedule_interval="0 5 * * *", start_date=datetime(2023, 9, 27), catchup=False, is_paused_upon_creation=True)
def ask_astro_load_blogs():
    """
    This DAG performs incremental load for any data sources that have changed.  Initial load via 
    ask_astro_load_bulk imported data from a point-in-time data capture.

    This DAG checks to make sure the latest schema exists.  If it does not exist a slack message
    is sent to notify admins.
    """

    _check_schema = WeaviateCheckSchemaBranchOperator(task_id='check_schema', 
                                                      weaviate_conn_id=_WEAVIATE_CONN_ID,
                                                      class_object_data='file://include/data/schema.json',
                                                      follow_task_ids_if_true=["extract_astro_blogs",
                                                                               ],
                                                      follow_task_ids_if_false=["slack_schema_alert"])

    _slack_schema_alert = SlackAPIPostOperator(task_id='slack_schema_alert', 
                                               channel='#airflow_notices',
                                               retries=0,
                                               slack_conn_id = _SLACK_CONN_ID,
                                               text='ask_astro_load_blogs DAG error.  Schema mismatch.')

    blogs_docs = task(extract.extract_astro_blogs, 
                      trigger_rule='none_failed', 
                      retries=3)(blog_cutoff_date=blog_cutoff_date)

    markdown_tasks = [blogs_docs]

    split_md_docs = task(split.split_markdown).expand(df=markdown_tasks)
    
    task.weaviate_import(ingest.import_upsert_data, 
                         trigger_rule='none_failed', 
                         weaviate_conn_id=_WEAVIATE_CONN_ID)\
            .partial(class_name='Docs', 
                     primary_key='docLink')\
            .expand(dfs=[split_md_docs])
    
    _check_schema >> [_slack_schema_alert] + markdown_tasks

ask_astro_load_blogs()