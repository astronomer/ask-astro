from datetime import datetime
from typing import List

import html2text
import numpy as np
import pandas as pd
from langchain.schema import Document
from langchain.text_splitter import RecursiveCharacterTextSplitter
from weaviate.util import generate_uuid5
from weaviate_provider.hooks.weaviate import WeaviateHook
from weaviate_provider.operators.weaviate import (
    WeaviateCheckSchemaOperator,
    WeaviateRetrieveAllOperator,
)

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.providers.slack.hooks.slack import SlackHook
from airflow.providers.slack.operators.slack import SlackAPIPostOperator

_WEAVIATE_CONN_ID = 'weaviate_default' #'weaviate_wcs' #
_GITHUB_CONN_ID = 'github_default'
_SLACK_CONN_ID = 'slack_api_default'

slack_archive_hostname = 'http://apache-airflow.slack-archives.org/'

slack_channel_sources = [
    {'channel_name': 'troubleshooting',
      'channel_id': 'CCQ7EGB1P',
      'team_id': 'TCQ18L22Z',
      'team_name' : 'Airflow Slack Community',
      'slack_api_conn_id' : _SLACK_CONN_ID},
    {'channel_name': 'dev',
      'channel_id': 'C05LWJWSV9P',
      'team_id': 'T04GW479DRB',
      'team_name' : 'dev',
      'slack_api_conn_id' : _SLACK_CONN_ID},
]

default_args = {
    "retries": 3,
    }

@dag(schedule_interval="0 5 * * *", start_date=datetime(2023, 9, 11), catchup=False, default_args=default_args)
def ask_astro_load_slack():
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
                "extract_slack",
            ]
        elif not schema_exists:
            return ["slack_schema_alert"]
        else:
            return None

    _slack_schema_alert = SlackAPIPostOperator(task_id='slack_schema_alert',
                                               channel='#airflow_notices',
                                               retries=0,
                                               slack_conn_id = _SLACK_CONN_ID,
                                               text='ask_astro_load_incremental DAG error.  Schema mismatch.')

    @task(trigger_rule='none_failed')
    def extract_slack(source:dict):
        """
        This task downloads archived slack messages as documents in a pandas dataframe.

        Dataframe fields are:
        'docSource': slack team and channel names
        'docLink': URL for the specific message/reply
        'content': The message/reply content in markdown format.
        'header': document type. (ie. 'question' or 'answer')

        Code is provided for the processing of questions and answers but is
        commented out as the historical data is provided as a parquet file.
        """
        #### THIS IS A PLACEHOLDER AS WE WAIT FOR SLACK BOT ACCESS TO AIRFLOW ####

        message_md_format = "# slack: {team_name}\n\n## {channel_name}\n\n{content}"
        reply_md_format = "### [{ts}] <@{user}>\n\n{text}"
        link_format = "https://app.slack.com/client/{team_id}/{channel_id}/p{ts}"

        slack_client = SlackHook(slack_conn_id = source['slack_api_conn_id']).client

        channel_info = slack_client.conversations_info(channel=source['channel_id']).data['channel']
        assert channel_info['is_member'] or not channel_info['is_private']

        history = slack_client.conversations_history(channel=source['channel_id'])
        replies = []
        df = pd.DataFrame(history.data['messages'])

        #if channel has no replies yet thread_ts will not be present
        if 'thread_ts' not in df:
            df['thread_ts'] = np.nan
        else:
            for ts in df[df['thread_ts'].notna()]['thread_ts']:
                reply=slack_client.conversations_replies(channel=source['channel_id'], ts=ts)
                replies.extend(reply.data['messages'])
            df = pd.concat([df, pd.DataFrame(replies)], axis=0)

        df = df[['user', 'text', 'ts', 'thread_ts', 'client_msg_id', 'type']]\
                .drop_duplicates()\
                .reset_index(drop=True)

        df['thread_ts'] = df['thread_ts'].astype(float)
        df['ts'] = df['ts'].astype(float)
        df['thread_ts'].fillna(value=df.ts, inplace=True)

        df['content'] = df.apply(lambda x: reply_md_format.format(ts=datetime.fromtimestamp(x.ts),
                                                                  user=x.user,
                                                                  text=x.text), axis=1)

        df = df.sort_values('ts').groupby('thread_ts').agg({'content': '\n'.join}).reset_index()

        df['content'] = df['content'].apply(lambda x: message_md_format.format(team_name=source['team_name'],
                                                                     channel_name=source['channel_name'],
                                                                     content=x))

        df['docLink'] = df['thread_ts'].apply(lambda x: link_format.format(team_id=source['team_id'],
                                                                    channel_id=source['channel_id'],
                                                                    ts=str(x).replace('.','')))
        df['docSource'] = source['channel_name']

        df['sha'] = df['content'].apply(generate_uuid5)

        return df[['docSource', 'sha', 'content', 'docLink']]
        # return pd.DataFrame()

    @task(trigger_rule='none_failed')
    def split_data(slack_dfs:List[pd.DataFrame]):
        """
        This task concatenates multiple dataframes from upstream dynamic tasks and
        splits markdown content on markdown headers.

        Dataframe fields are:
        'docSource': ie. 'astro', 'learn', 'docs', etc.
        'sha': the github sha for the document
        'docLink': URL for the specific document in github.
        'content': Chunked content in markdown format.

        """

        df = pd.concat(slack_dfs, axis=0, ignore_index=True)

        # headers_to_split_on = [
        #     ("#", "Header 1"),
        #     ("##", "Header 2"),
        #     # ("###", "Header 3"),
        # ]

        # splitter = MarkdownHeaderTextSplitter(headers_to_split_on=headers_to_split_on)
        # df['doc_chunks'] = df['content'].apply(lambda x: splitter.split_text(x))

        splitter = RecursiveCharacterTextSplitter()
        df['doc_chunks'] = df['content'].apply(lambda x: splitter.split_documents([Document(page_content=x)]))

        df = df[df['doc_chunks'].apply(lambda x: len(x))>0].reset_index(drop=True)
        # _ = df['doc_chunks'].apply(lambda x: x[0].metadata.update({'Header 1': 'Summary'}) if x[0].metadata == {} else x[0] )
        df = df.explode('doc_chunks', ignore_index=True)
        df['content'] = df['doc_chunks'].apply(lambda x: html2text.html2text(x.page_content).replace('\n',' '))
        df['content'] = df['content'].apply(lambda x: x.replace('\\',''))
        # df['header'] = df['doc_chunks'].apply(lambda x: '. '.join(list(x.metadata.values())))

        df.drop(['doc_chunks'], inplace=True, axis=1)
        df.reset_index(inplace=True, drop=True)

        return df

    @task.weaviate_import(weaviate_conn_id=_WEAVIATE_CONN_ID)
    def import_data(md_docs:pd.DataFrame, class_name:str, loaded_docs_file_path:str):
        """
        This task concatenates multiple dataframes from upstream dynamic tasks and
        vectorizes with import to weaviate.

        A 'uuid' is generated based on the content and metadata (the git sha, document url,
        the document source (ie. astro) and a concatenation of the headers).

        Any existing documents with the same docLink but differing UUID or sha will be
        deleted prior to import.

        Vectorization includes the headers for bm25 search.
        """

        df = pd.concat([md_docs], ignore_index=True)

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

    slack_md = extract_slack.partial().expand(source=slack_channel_sources)

    split_md_docs = split_data(slack_dfs=slack_md)

    _loaded_docs = WeaviateRetrieveAllOperator(task_id='fetch_loaded_docs',
                                               weaviate_conn_id=_WEAVIATE_CONN_ID,
                                               trigger_rule='none_failed',
                                               class_name='Docs',
                                               replace_existing=True,
                                               output_file='file://include/data/loaded_docs.parquet')

    _unimported_md = import_data(md_docs=split_md_docs,
                                 class_name='Docs',
                                 loaded_docs_file_path=_loaded_docs.output)

    _check_schema >> \
        _alert_schema_branch >> \
            [_slack_schema_alert, slack_md]

    _loaded_docs >> _unimported_md
    _slack_schema_alert >> _fail_schema

ask_astro_load_slack()
