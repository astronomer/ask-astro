import re
from datetime import datetime
from pathlib import Path
from typing import List

import html2text
import pandas as pd
import pypandoc
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
from airflow.providers.github.hooks.github import GithubHook
from airflow.providers.slack.operators.slack import SlackAPIPostOperator

_WEAVIATE_CONN_ID = 'weaviate_test'
_GITHUB_CONN_ID = 'github_default'
_SLACK_CONN_ID = 'slack_api_default'

markdown_docs_sources = [
    {'doc_dir': 'learn', 'repo_base': 'astronomer/docs'},
    {'doc_dir': 'astro', 'repo_base': 'astronomer/docs'}
    ]
rst_docs_sources = [
    {'doc_dir': 'docs', 'repo_base': 'apache/airflow'},
    ]
code_samples_sources = [
    {'doc_dir': 'code-samples', 'repo_base': 'astronomer/docs'},
    ]
issues_docs_sources = [
    {'doc_dir': 'issues', 'repo_base': 'apache/airflow'}
]

rst_exclude_docs = ['changelog.rst', 'commits.rst']

default_args = {
    "retries": 3,
    }

# @dag(schedule_interval="0 5 * * *", start_date=datetime(2023, 9, 11), catchup=False, default_args=default_args)
@dag(schedule_interval=None, start_date=datetime(2023, 9, 11), catchup=False, default_args=default_args)
def ask_astro_load_github():
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
                "extract_github_markdown",
                "extract_github_rst",
                "extract_github_python",
                "extract_github_issues",
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
    def extract_github_markdown(source:dict):
        """
        This task downloads github content as markdown documents in a
        pandas dataframe.

        Dataframe fields are:
        'docSource': ie. 'astro', 'learn', etc.
        'sha': the github sha for the document
        'docLink': URL for the specific document in github.
        'content': Entire document content in markdown format.
        """

        downloaded_docs = []

        gh_hook = GithubHook(_GITHUB_CONN_ID)

        repo = gh_hook.client.get_repo(source['repo_base'])
        contents = repo.get_contents(source['doc_dir'])

        while contents:

            file_content = contents.pop(0)
            if file_content.type == "dir":
                contents.extend(repo.get_contents(file_content.path))

            elif Path(file_content.name).suffix == '.md':

                print(file_content.name)

                row = {
                    "docLink": file_content.html_url,
                    "sha": file_content.sha,
                    "content": file_content.decoded_content.decode(),
                    "docSource": source['doc_dir'],
                }

                downloaded_docs.append(row)

        df = pd.DataFrame(downloaded_docs)

        # df.to_parquet(f"include/data/{source['repo_base']}/{source['doc_dir']}.parquet")
        # df = pd.read_parquet(f"include/data/{source['repo_base']}/{source['doc_dir']}.parquet")

        return df

    @task(trigger_rule='none_failed')
    def extract_github_rst(source:dict):
        """
        This task downloads github content as rst documents
        in a pandas dataframe.

        The 'content' field is converted from RST to Markdown (via pypandoc).  After
        removing the preamble (apache license), any empty lines and 'include' footers
        any empty docs are removed.  Document links and references are not included
        in the content.

        Dataframe fields are:
        'docSource': ie. 'docs'
        'sha': the github sha for the document
        'docLink': URL for the specific document in github.
        'content': Entire document in markdown format.
        """

        downloaded_docs = []

        gh_hook = GithubHook(_GITHUB_CONN_ID)

        repo = gh_hook.client.get_repo(source['repo_base'])
        contents = repo.get_contents(source['doc_dir'])

        apache_license_text = Path('include/data/apache_license.rst').read_text()

        while contents:

            file_content = contents.pop(0)
            if file_content.type == "dir":
                contents.extend(repo.get_contents(file_content.path))

            elif Path(file_content.name).suffix == '.rst' and file_content.name not in rst_exclude_docs:

                print(file_content.name)

                row = {
                    "docLink": file_content.html_url,
                    "sha": file_content.sha,
                    "content": file_content.decoded_content.decode(),
                    "docSource": source['doc_dir'],
                }

                downloaded_docs.append(row)

        df = pd.DataFrame(downloaded_docs)

        df['content'] = df['content'].apply(lambda x: x.replace(apache_license_text, ''))
        df['content'] = df['content'].apply(lambda x: re.sub(r".*include.*", "", x))
        df['content'] = df['content'].apply(lambda x: re.sub(r'^\s*$', "", x))
        df = df[df['content']!='']
        df['content'] = df['content'].apply(lambda x: pypandoc.convert_text(source=x, to='md',
                                                                            format='rst',
                                                                            extra_args=['--atx-headers']))

        # df.to_parquet(f"include/data/{source['repo_base']}/{source['doc_dir']}.parquet")
        # df = pd.read_parquet(f"include/data/{source['repo_base']}/{source['doc_dir']}.parquet")

        return df

    @task(trigger_rule='none_failed')
    def extract_github_python(source:dict):
        """
        This task downloads github content as python code in a pandas dataframe.

        The 'content' field of the dataframe is currently not split as the context
        window is large enough. Code for splitting is provided but commented out.

        Dataframe fields are:
        'docSource': ie. 'code-samples'
        'sha': the github sha for the document
        'docLink': URL for the specific document in github.
        'content': The python code
        'header': a placeholder of 'python' for bm25 search
        """

        downloaded_docs = []

        gh_hook = GithubHook(_GITHUB_CONN_ID)

        repo = gh_hook.client.get_repo(source['repo_base'])
        contents = repo.get_contents(source['doc_dir'])

        while contents:
            file_content = contents.pop(0)

            if file_content.type == "dir":
                contents.extend(repo.get_contents(file_content.path))

            elif Path(file_content.name).suffix == '.py':
                print(file_content.name)

                row = {
                    "docLink": file_content.html_url,
                    "sha": file_content.sha,
                    "content": file_content.decoded_content.decode(),
                    "docSource": source['doc_dir'],
                    "header": 'python',
                }

                downloaded_docs.append(row)

        df = pd.DataFrame(downloaded_docs)

        # df.to_parquet(f"include/data/{source['repo_base']}/{source['doc_dir']}.parquet")
        # df = pd.read_parquet(f"include/data/{source['repo_base']}/{source['doc_dir']}.parquet")

        return df

    @task(trigger_rule='none_failed')
    def extract_github_issues(source:dict):
        """
        This task downloads github issues as markdown documents in a pandas dataframe.

        Dataframe fields are:
        'docSource': repo name + 'issues'
        'docLink': URL for the specific question/answer.
        'content': The base64 encoded content of the question/answer in markdown format.
        'header': document type. (ie. 'question' or 'answer')
        """

        gh_hook = GithubHook(_GITHUB_CONN_ID)

        repo = gh_hook.client.get_repo(source['repo_base'])
        issues = repo.get_issues()

        issue_autoresponse_text = 'Thanks for opening your first issue here!'
        pr_autoresponse_text = 'Congratulations on your first Pull Request and welcome to the Apache Airflow community!'
        drop_content = [issue_autoresponse_text, pr_autoresponse_text]

        issue_markdown_template = "## ISSUE TITLE: {title}\nDATE: {date}\nBY: {user}\nSTATE: {state}\n{body}\n{comments}"
        comment_markdown_template = "#### COMMENT: {user} on {date}\n{body}\n"

        downloaded_docs = []
        page_num = 0

        page = issues.get_page(page_num)

        while page:

            for issue in page:
                print(issue.number)
                comments=[]
                for comment in issue.get_comments():
                    #TODO: this is very slow.  Look for vectorized approach.
                    if not any(substring in comment.body for substring in drop_content):
                        comments.append(comment_markdown_template.format(user=comment.user.login,
                                                                         date=issue.created_at.strftime("%m-%d-%Y"),
                                                                         body=comment.body))
                downloaded_docs.append({
                    "docLink": issue.html_url,
                    "sha": '',
                    "content": issue_markdown_template.format(title=issue.title,
                                                              date=issue.created_at.strftime("%m-%d-%Y"),
                                                              user=issue.user.login,
                                                              state=issue.state,
                                                              body=issue.body,
                                                              comments='\n'.join(comments)),
                    "docSource": f"{source['repo_base']} {source['doc_dir']}",
                    "header": f"{source['repo_base']} issue",
                })
            page_num=page_num+1
            page = issues.get_page(page_num)

        df = pd.DataFrame(downloaded_docs)

        # df.to_parquet(f"include/data/{source['repo_base']}/{source['doc_dir']}.parquet")
        # df = pd.read_parquet(f"include/data/{source['repo_base']}/{source['doc_dir']}.parquet")
        df['sha'] = df.apply(generate_uuid5, axis=1)

        return df

    @task(trigger_rule='none_failed')
    def split_data(md_dfs:List[pd.DataFrame],
                   rst_dfs:List[pd.DataFrame],
                   code_dfs:List[pd.DataFrame],
                   issues_dfs:List[pd.DataFrame]):
        """
        This task concatenates multiple dataframes from upstream dynamic tasks and
        splits markdown content on markdown headers.

        Dataframe fields are:
        'docSource': ie. 'astro', 'learn', 'docs', etc.
        'sha': the github sha for the document
        'docLink': URL for the specific document in github.
        'content': Chunked content in markdown format.

        """

        md_df = pd.concat(md_dfs, axis=0, ignore_index=True)
        rst_df = pd.concat(rst_dfs, axis=0, ignore_index=True)
        issues_df = pd.concat(issues_dfs, axis=0, ignore_index=True)
        code_df = pd.concat(code_dfs, axis=0, ignore_index=True)
        df = pd.concat([md_df, rst_df, issues_df, code_df], axis=0, ignore_index=True)

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
    def import_data(md_df:pd.DataFrame, class_name:str, loaded_docs_file_path:str):
        """
        This task concatenates multiple dataframes from upstream dynamic tasks and
        vectorizes with import to weaviate.

        A 'uuid' is generated based on the content and metadata (the git sha, document url,
        the document source (ie. astro) and a concatenation of the headers).

        Any existing documents with the same docLink but differing UUID or sha will be
        deleted prior to import.

        Vectorization includes the headers for bm25 search.
        """

        df = pd.concat([md_df], ignore_index=True)

        df['uuid'] = df.apply(lambda x: generate_uuid5(x.to_dict()), axis=1)

        df = remove_existing_objects(loaded_docs_file_path=loaded_docs_file_path, new_df=df, class_name=class_name)

        print(f"Passing {len(df)} objects for import.")

        return {"data": df, "class_name": class_name, "uuid_column": "uuid", "error_threshold": 10}

    _alert_schema_branch = alert_schema_branch(_check_schema.output)

    @task
    def fail_schema():
        raise AirflowException('Failing DAG for schema failure.')

    _fail_schema = fail_schema()

    md_docs = extract_github_markdown.partial().expand(source=markdown_docs_sources)
    rst_docs = extract_github_rst.partial().expand(source=rst_docs_sources)
    issues_docs = extract_github_issues.partial().expand(source=issues_docs_sources)
    code_samples = extract_github_python.partial().expand(source=code_samples_sources)

    split_md_docs = split_data(md_dfs=md_docs,
                               rst_dfs=rst_docs,
                               code_dfs=code_samples,
                               issues_dfs=issues_docs)

    _loaded_docs = WeaviateRetrieveAllOperator(task_id='fetch_loaded_docs',
                                               weaviate_conn_id=_WEAVIATE_CONN_ID,
                                               trigger_rule='none_failed',
                                               class_name='Docs',
                                               replace_existing=True,
                                               output_file='file://include/data/loaded_docs.parquet')

    _unimported_data = import_data(md_df=split_md_docs,
                                 class_name='Docs',
                                 loaded_docs_file_path=_loaded_docs.output)

    _check_schema >> \
        _alert_schema_branch >> \
            [_slack_schema_alert, md_docs, rst_docs, code_samples, issues_docs]

    _loaded_docs >> _unimported_data
    _slack_schema_alert >> _fail_schema

ask_astro_load_github()
