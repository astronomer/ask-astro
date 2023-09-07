from datetime import datetime 
import pandas as pd
import html2text
import requests
from bs4 import BeautifulSoup
import lxml

from typing import List

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from weaviate_provider.hooks.weaviate import WeaviateHook
from weaviate_provider.operators.weaviate import (
    WeaviateCheckSchemaOperator,
    WeaviateRetrieveAllOperator,
)
from weaviate.util import generate_uuid5
from langchain.text_splitter import (
    MarkdownHeaderTextSplitter, 
    RecursiveCharacterTextSplitter
)
from langchain.schema import Document

_WEAVIATE_CONN_ID = 'weaviate_test'
_SLACK_CONN_ID = 'slack_api_default'

default_args = {
    "retries": 3,
    }

# @dag(schedule_interval="0 5 * * *", start_date=datetime(2023, 9, 11), catchup=False, default_args=default_args)
@dag(schedule_interval=None, start_date=datetime(2023, 9, 11), catchup=False, default_args=default_args)
def ask_astro_load_blogs():
    """
    This DAG performs incremental load for any data sources that have changed.  Initial load via 
    ask_astro_load_bulk imported data from a point-in-time data capture.

    This DAG checks to make sure the latest schema exists.  If it does not exist a slack message
    is sent to notify admins.
    """

    def remove_existing_objects(loaded_docs_file_path:str, new_df:pd.DataFrame, class_name:str):
        """
        Helper function to check if existing content needs to 
        be deleted before update. 

        Existing objects (based on 'docLink') with differing uuid or sha 
        will be deleted.

        Returned df includes only the objects that need to be (re)imported.
        """

        weaviate_hook = WeaviateHook(_WEAVIATE_CONN_ID)
        weaviate_hook.client = weaviate_hook.get_conn()

        current_objects_df = pd.read_parquet(loaded_docs_file_path)\
                               .drop(['vector'], axis=1)\
                               .groupby('docLink').agg(set)

        update_objects_df = new_df.groupby('docLink').agg(set)\
                              .join(current_objects_df, rsuffix='_old')
        update_objects_df = update_objects_df[update_objects_df['sha'] != update_objects_df['sha_old']]

        #remove existing objects
        update_objects_df['id'].dropna()\
            .apply(lambda x: [weaviate_hook.client.data_object.delete(uuid=uuid, 
                                                                      class_name=class_name) 
                                for uuid in list(x)])

        objects_to_import = new_df.merge(update_objects_df.reset_index()['docLink'], on='docLink', how='right')

        return objects_to_import

    _check_schema = WeaviateCheckSchemaOperator(task_id='check_schema', 
                                                weaviate_conn_id=_WEAVIATE_CONN_ID,
                                                class_object_data='file://include/data/schema.json')

    @task.branch(retries=0)
    def alert_schema_branch(schema_exists:bool) -> str:
        """
        Check if schema is no longer valid
        """
        # WeaviateHook(_WEAVIATE_CONN_ID).get_conn().schema.delete_all()
        if schema_exists:
            return [
                "extract_astro_blogs",
                "fetch_loaded_docs"
            ]
        elif not schema_exists:
            return ["slack_schema_alert"]
        else:
            return None

    _slack_schema_alert = SlackAPIPostOperator(task_id='slack_schema_alert', 
                                               channel='#airflow_notices',
                                               retries=0,
                                               slack_conn_id = _SLACK_CONN_ID,
                                               text='ask_astro_load_http897 DAG error.  Schema mismatch.')

    @task(trigger_rule='none_failed')
    def extract_astro_blogs():

        cutoff_date = datetime.strptime('2023-01-19', '%Y-%m-%d')

        base_url = "https://www.astronomer.io"
        page_url = base_url+"/blog/{page}/#archive"
        headers={}
        links=[]
        dates=[]
        page=1
        
        response = requests.get(page_url.format(page=page), headers=headers)
        while response.ok:

            soup = BeautifulSoup(response.text, 'lxml')
            cards = soup.find_all(class_="post-card__cover")
            links.extend([base_url+card.find('a', href=True)['href'] for card in cards])
            meta = soup.find_all(class_="post-card__meta")
            dates.extend([post.find('time')['datetime'] for post in meta])

            page = page + 1
            response = requests.get(page_url.format(page=page), headers=headers)

        df = pd.DataFrame(zip(links, dates), columns=['docLink','date'])
        df['date'] = pd.to_datetime(df['date']).dt.date
        df = df[df['date'] > cutoff_date.date()]
        df.drop('date', inplace=True, axis=1)
        df.drop_duplicates(inplace=True)

        df['content'] = df['docLink'].apply(lambda x: requests.get(x).text)
        df['title'] = df['content'].apply(lambda x: BeautifulSoup(x, 'lxml')\
                                          .find(class_="post-card__meta")\
                                          .find(class_='title').get_text())
        
        df['content'] = df['content'].apply(lambda x: BeautifulSoup(x, 'lxml').find(class_='prose').get_text())
        df['content'] = df.apply(lambda x: x.title + ' ' + html2text.html2text(html=x.content), axis=1)
        df.drop('title', axis=1, inplace=True)
        df['sha'] = df['content'].apply(generate_uuid5)
        df['docSource'] = 'astro blog'
        df.reset_index(drop=True, inplace=True)

        blog_dfs = df[['docSource', 'sha', 'content', 'docLink']]

        return blog_dfs
    
    @task(trigger_rule='none_failed')
    def split_data(blog_dfs:List[pd.DataFrame]):
        """
        This task concatenates multiple dataframes from upstream dynamic tasks and 
        splits markdown content on markdown headers.

        Dataframe fields are:
        'docSource': ie. 'astro', 'learn', 'docs', etc.
        'sha': the github sha for the document
        'docLink': URL for the specific document in github.
        'content': Chunked content in markdown format.

        """

        df = pd.concat([blog_dfs], axis=0, ignore_index=True)

        splitter = RecursiveCharacterTextSplitter()
        df['doc_chunks'] = df['content'].apply(lambda x: splitter.split_documents([Document(page_content=x)]))

        df = df[df['doc_chunks'].apply(lambda x: len(x))>0].reset_index(drop=True)
        df = df.explode('doc_chunks', ignore_index=True)
        df['content'] = df['doc_chunks'].apply(lambda x: html2text.html2text(x.page_content).replace('\n',' '))
        df['content'] = df['content'].apply(lambda x: x.replace('\\',''))

        df.drop(['doc_chunks'], inplace=True, axis=1)
        df.reset_index(inplace=True, drop=True)

        return df

    @task.weaviate_import(weaviate_conn_id=_WEAVIATE_CONN_ID)
    def import_data(blog_docs:pd.DataFrame, class_name:str, loaded_docs_file_path:str):
        """
        This task concatenates multiple dataframes from upstream dynamic tasks and 
        vectorizes with import to weaviate.

        A 'uuid' is generated based on the content and metadata (the git sha, document url,  
        the document source (ie. astro) and a concatenation of the headers).

        Any existing documents with the same docLink but differing UUID or sha will be 
        deleted prior to import.

        Vectorization includes the headers for bm25 search.
        """
        df = pd.concat([blog_docs], ignore_index=True)

        df['uuid'] = df.apply(lambda x: generate_uuid5(x.to_dict()), axis=1)

        df = remove_existing_objects(loaded_docs_file_path=loaded_docs_file_path, 
                                     new_df=df, 
                                     class_name=class_name)

        print(f"Passing {len(df)} objects for import.")

        return {"data": df, "class_name": class_name, "uuid_column": "uuid", "error_threshold": 10}
    
    _alert_schema_branch = alert_schema_branch(_check_schema.output)

    @task
    def fail_schema():
        raise AirflowException('Failing DAG for schema failure.')
    
    _fail_schema = fail_schema()

    blog_docs = extract_astro_blogs()
    
    split_md_docs = split_data(blog_dfs=blog_docs)

    _loaded_docs = WeaviateRetrieveAllOperator(task_id='fetch_loaded_docs', 
                                               weaviate_conn_id=_WEAVIATE_CONN_ID,
                                               trigger_rule='none_failed',
                                               class_name='Docs', 
                                               replace_existing=True,
                                               output_file='file://include/data/loaded_docs.parquet')
    
    _unimported_md = import_data(blog_docs=split_md_docs,
                                 class_name='Docs', 
                                 loaded_docs_file_path=_loaded_docs.output)

    _check_schema >> \
        _alert_schema_branch >> \
            [_slack_schema_alert, blog_docs]
    
    _loaded_docs >> _unimported_md
    _slack_schema_alert >> _fail_schema

ask_astro_load_blogs()