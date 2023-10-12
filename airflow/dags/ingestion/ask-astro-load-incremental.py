import re
from datetime import datetime
from pathlib import Path
from typing import List

import html2text
import pandas as pd
import pypandoc
import requests
from langchain.text_splitter import RecursiveCharacterTextSplitter
from stackapi import StackAPI
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

_WEAVIATE_CONN_ID = 'weaviate_default' #'weaviate_wcs' #
_GITHUB_CONN_ID = 'github_default'
_SLACK_CONN_ID = 'slack_api_default'

slack_archive_hostname = 'http://apache-airflow.slack-archives.org/'

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
slack_channel_sources = [
    {'channel_name': 'troubleshooting',
      'channel_id': 'CCQ7EGB1P',
      'team_id': 'TCQ18L22Z',
      'team_name' : 'Airflow Slack Community',
      'slack_api_conn_id' : _SLACK_CONN_ID},
]
http_json_sources = [
    {'name': 'registry_cell_types',
     'base_url': 'https://api.astronomer.io/registryV2/v1alpha1/organizations/public/modules?limit=1000',
     'headers': {},
     'count_field': 'totalCount'}
]

rst_exclude_docs = ['changelog.rst', 'commits.rst']

stackoverflow_cutoff_date = '2023-06-04'
stackoverflow_tags = [
    'airflow',
]

default_args = {
    "retries": 3,
    }

@dag(schedule_interval="0 5 * * *", start_date=datetime(2023, 9, 11), catchup=False, default_args=default_args)
def ask_astro_load_incremental():
    """
    This DAG performs incremental load for any data sources that have changed.  Initial load via
    ask_astro_load_bulk imported data from a point-in-time data capture specified by
    'stackoverflow_cutoff_date'.

    The DAG checks to make sure the latest schema exists.  If it does not exist a slack message
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
                "extract_stack_overflow",
                "extract_slack",
                "extract_github_issues",
                "extract_registry",
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
    def extract_stack_overflow(tag:dict, stackoverflow_cutoff_date:str):
        """
        This task generates stack overflow questions and answers as markdown
        documents in a pandas dataframe.

        Dataframe fields are:
        'docSource': 'stackoverflow' plus the tag name (ie. 'airflow')
        'docLink': URL for the specific question/answer.
        'content': The base64 encoded content of the question/answer in markdown format.
        'header': document type. (ie. 'question' or 'answer')
        """

        question_template = "TITLE: {title}\nDATE: {date}\nBY: {user}\nSCORE: {score}\n{body}{question_comments}"
        answer_template = "DATE: {date}\nBY: {user}\nSCORE: {score}\n{body}{answer_comments}"
        comment_template = "{user} on {date} [Score: {score}]: {body}\n"

        SITE = StackAPI(name='stackoverflow', max_pagesize=100, max_pages=10000000)

        fromdate = datetime.strptime(stackoverflow_cutoff_date, "%Y-%m-%d")
        filter = '!-(5KXGCFLp3w9.-7QsAKFqaf5yFPl**9q*_hsHzYGjJGQ6BxnCMvDYijFE'

        questions_dict = SITE.fetch(endpoint='questions', tagged=tag, fromdate=fromdate, filter=filter)
        items = questions_dict.pop('items')

        #TODO: add backoff logic.  For now just fail the task if we can't fetch all results due to api rate limits.
        assert not questions_dict['has_more']

        questions_df = pd.DataFrame(items)
        questions_df = questions_df[questions_df['answer_count']>=1]
        questions_df = questions_df[questions_df['score']>=1]
        questions_df.reset_index(inplace=True, drop=True)

        answers_df = questions_df.explode('answers')

        #consolidate comments for the original question
        questions_df['comments'] = questions_df['comments'].fillna('')
        questions_df['comments'] = questions_df['comments'].apply(lambda x: [comment_template.format(
            user=comment['owner']['user_id'],
            date=datetime.fromtimestamp(comment['creation_date']).strftime("%Y-%m-%d"),
            score=comment['score'],
            body=comment['body_markdown']) for comment in x])
        questions_df['question_comments'] = questions_df['comments'].apply(lambda x: ''.join(x))

        #format question content
        questions_df['question_text'] = questions_df.apply(lambda x: question_template.format(
            title=x.title,
            user=x.owner['user_id'],
            date=datetime.fromtimestamp(x.creation_date).strftime("%Y-%m-%d"),
            score=x.score,
            body=x.body_markdown,
            question_comments=x.question_comments), axis=1)
        questions_df = questions_df[['link', 'question_id', 'question_text']].set_index('question_id')

        questions_df['docSource'] = f'stackoverflow {tag}'
        questions_df = questions_df[['docSource', 'link', 'question_text']]
        questions_df.columns = ['docSource', 'docLink', 'content']
        questions_df['header'] = 'question'


        # consolidate comments for each answer
        answers_df['answer_comments'] = answers_df['answers'].apply(lambda x: [comment_template.format(
            user=comment['owner']['user_id'],
            date=datetime.fromtimestamp(comment['creation_date']).strftime("%Y-%m-%d"),
            score=comment['score'],
            body=comment['body_markdown']) for comment in x.get('comments', '')])
        answers_df['answer_comments'] = answers_df['answer_comments'].apply(lambda x: ''.join(x))

        answers_df['answer_text'] = answers_df[['answers', 'answer_comments']].apply(lambda x: answer_template.format(
            score=x.answers['score'],
            date=datetime.fromtimestamp(x.answers['creation_date']).strftime("%Y-%m-%d"),
            user=x.answers['owner']['user_id'],
            body=x.answers['body_markdown'],
            answer_comments=x.answer_comments), axis=1)
        answers_df = answers_df.groupby('question_id')['answer_text'].apply(lambda x: ''.join(x))

        answers_df = questions_df.join(answers_df).apply(lambda x: pd.Series([
            f'stackoverflow {tag}',
            x.docLink,
            x.answer_text]), axis=1)
        answers_df.columns=['docSource', 'docLink','content']
        answers_df['header'] = 'answer'

        df = pd.concat([questions_df, answers_df], axis=0).reset_index(drop=True)

        df['content'] = df['content'].apply(lambda x: html2text.html2text(x).replace('\n',' '))
        df['content'] = df['content'].apply(lambda x: x.replace('\\',''))

        # df.to_parquet('include/data/stackoverflow_incremental.parquet')
        # df = pd.read_parquet('include/data/stackoverflow_incremental.parquet')
        df['sha'] = df.apply(generate_uuid5, axis=1)

        return df

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

        # slack_client = SlackHook(slack_conn_id = _SLACK_CONN_ID).client

        # channel_info = slack_client.conversations_info(channel=source['channel_id']).data['channel']
        # assert channel_info['is_member'] or not channel_info['is_private']

        # history = slack_client.conversations_history(channel=source['channel_id'])
        # df = pd.DataFrame(history.data['messages'])

        # #if channel has no replies yet they will not be in the messages
        # if 'thread_ts' not in df:
        #     df['thread_ts'] = ''

        # df = df[['user', 'text', 'ts', 'thread_ts', 'client_msg_id', 'type']].drop_duplicates()
        # df.rename({'client_msg_id': 'sha'}, axis=1, inplace=True)
        # df['docLink'] = df['ts'].apply(lambda x: f"https://app.slack.com/client/{source['team_id']}/{source['channel_id']}/p{str(x).replace('.','')}")
        # df['docSource'] = source['channel_name']
        # df['type'] = df.apply(lambda x: 'question' if x.ts == x.thread_ts else 'answer', axis=1)
        # df['ts'] = df['ts'].apply(lambda x: datetime.fromtimestamp(float(x)))
        # df['content'] = df.apply(lambda x: f"# slack {source['team_name']} {source['channel_name']}\n## {x.type} \n### [{x.ts}] <@{x.user}>\n\n{x.text}", axis=1)

        # return df[['docSource', 'sha', 'content', 'docLink']]
        return pd.DataFrame()

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
    def extract_registry(source:dict):

        data_class = source['base_url'].split('/')[-1].split('?')[0]

        response = requests.get(source['base_url'], headers=source['headers']).json()
        total_count = response[source['count_field']]
        data = response.get(data_class, [])
        while len(data) < total_count-1:
            response = requests.get(f"{source['base_url']}&offset={len(data)+1}").json()
            data.extend(response.get(data_class, []))

        df = pd.DataFrame(data)
        df.rename({'githubUrl': 'docLink', 'searchId': 'sha'}, axis=1, inplace=True)
        df['docSource'] = source['name']
        df['description'] = df['description'].apply(lambda x: html2text.html2text(x) if x else 'No Description')
        df['content'] = df.apply(lambda x: f"# Registry\n## {x.providerName}__{x.version}__{x['name']}\n\n{x.description})", axis=1)

        df = df[['docSource', 'sha', 'content', 'docLink']]

        return df

    @task(trigger_rule='none_failed')
    def split_markdown(md_dfs:List[pd.DataFrame], rst_dfs:List[pd.DataFrame], slack_dfs:List[pd.DataFrame], reg_dfs:List[pd.DataFrame]):
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
        slack_df = pd.concat(slack_dfs, axis=0, ignore_index=True)
        reg_df = pd.concat(reg_dfs, axis=0, ignore_index=True)
        df = pd.concat([md_df, rst_df, slack_df, reg_df], axis=0, ignore_index=True)


        # splitter = MarkdownHeaderTextSplitter(headers_to_split_on=headers_to_split_on)
        splitter = RecursiveCharacterTextSplitter()

        df['doc_chunks'] = df['content'].apply(lambda x: splitter.split_text(x))
        df = df[df['doc_chunks'].apply(lambda x: len(x))>0].reset_index(drop=True)
        _ = df['doc_chunks'].apply(lambda x: x[0].metadata.update({'Header 1': 'Summary'}) if x[0].metadata == {} else x[0] )
        df = df.explode('doc_chunks', ignore_index=True)
        df['content'] = df['doc_chunks'].apply(lambda x: html2text.html2text(x.page_content).replace('\n',' '))
        df['content'] = df['content'].apply(lambda x: x.replace('\\',''))
        df['header'] = df['doc_chunks'].apply(lambda x: '. '.join(list(x.metadata.values())))

        df.drop(['doc_chunks'], inplace=True, axis=1)
        df.reset_index(inplace=True, drop=True)

        return df

    @task()
    def split_python(python_dfs:List[pd.DataFrame]):
        """
        This concatenates multiple dataframes from upstream dynamic tasks and
        splits python code in a pandas dataframe.

        This task is a concatenation and passthru. The 'content' field of the dataframe
        is currently not split as the context window is large enough. Code for splitting
        is provided but commented out.

        Dataframe fields are:
        'docSource': ie. 'code-samples'
        'sha': the github sha for the document
        'docLink': URL for the specific document in github.
        'content': The base64 encoded python code
        'header': a placeholder of 'python' for bm25 search
        """

        df = pd.concat(python_dfs, axis=0, ignore_index=True)

        #chunk code and use each chunk as a separate doc for vectorization
        # python_splitter = RecursiveCharacterTextSplitter.from_language(
        #         language=Language.PYTHON, chunk_size=50, chunk_overlap=0
        #     )
        # df['doc_chunks'] = df['content'].apply(lambda x: python_splitter.create_documents([base64.b64decode(x).decode()]))
        # df = df.explode('doc_chunks', ignore_index=True)
        # df['content'] = df['doc_chunks'].apply(lambda x: x.page_content.replace('\n',' '))
        # df.drop(['doc_chunks'], inplace=True, axis=1)

        # df.to_parquet(f"include/data/{source['repo_base']}/{source['doc_dir']}.parquet")
        # df = pd.read_parquet(f"include/data/{source['repo_base']}/{source['doc_dir']}.parquet")

        return df

    @task.weaviate_import(weaviate_conn_id=_WEAVIATE_CONN_ID)
    def import_md(md_docs:pd.DataFrame,
                  stack_docs:pd.DataFrame,
                  issues_docs:pd.DataFrame,
                  class_name:str,
                  loaded_docs_file_path:str):
        """
        This task concatenates multiple dataframes from upstream dynamic tasks and
        vectorizes with import to weaviate.

        A 'uuid' is generated based on the content and metadata (the git sha, document url,
        the document source (ie. astro) and a concatenation of the headers).

        Any existing documents with the same docLink but differing UUID or sha will be
        deleted prior to import.

        Vectorization includes the headers for bm25 search.
        """

        stack_df = pd.concat(stack_docs, axis=0, ignore_index=True)
        issues_df = pd.concat(issues_docs, axis=0, ignore_index=True)
        df = pd.concat([stack_df, issues_df, md_docs], ignore_index=True)

        df['uuid'] = df.apply(lambda x: generate_uuid5(x.to_dict()), axis=1)

        df = remove_existing_objects(loaded_docs_file_path=loaded_docs_file_path,
                                     new_df=df,
                                     class_name=class_name)

        print(f"Passing {len(df)} objects for import.")

        return {"data": df, "class_name": class_name, "uuid_column": "uuid", "error_threshold": 10}

    @task.weaviate_import(weaviate_conn_id=_WEAVIATE_CONN_ID)
    def import_python_code(code_dfs: List[pd.DataFrame], class_name:str, loaded_docs_file_path:str):
        """
        This task concatenates multiple dataframes from upstream dynamic tasks and
        vectorizes with import to weaviate.

        A 'uuid' is generated based on the content and metadata which is comprised of
        the git sha, document url,  the document source (ie. code-samples) and a simulated
        header (ie. python).

        Any existing documents with the same docLink but differing UUID or sha will be
        deleted prior to import.
        """

        df = pd.concat(code_dfs, axis=0, ignore_index=True)

        df['uuid'] = df.apply(generate_uuid5, axis=1)

        df = remove_existing_objects(loaded_docs_file_path=loaded_docs_file_path,
                                     new_df=df,
                                     class_name=class_name)

        print(f"Passing {len(df)} objects for import.")

        return {"data": df, "class_name": class_name, "uuid_column": "uuid", "error_threshold": 0}

    _alert_schema_branch = alert_schema_branch(_check_schema.output)

    @task
    def fail_schema():
        raise AirflowException('Failing DAG for schema failure.')
    _fail_schema = fail_schema()

    md_docs = extract_github_markdown.partial().expand(source=markdown_docs_sources)
    rst_docs = extract_github_rst.partial().expand(source=rst_docs_sources)
    issues_md = extract_github_issues.partial().expand(source=issues_docs_sources)
    code_samples = extract_github_python.partial().expand(source=code_samples_sources)
    stackoverflow_md = extract_stack_overflow.partial(stackoverflow_cutoff_date=stackoverflow_cutoff_date).expand(tag=stackoverflow_tags)
    slack_md = extract_slack.partial().expand(source=slack_channel_sources)
    registry_md = extract_registry.partial().expand(source=http_json_sources)

    split_md_docs = split_markdown(md_dfs=md_docs, rst_dfs=rst_docs, slack_dfs=slack_md, reg_dfs=registry_md)
    split_python_code = split_python(code_samples)

    _loaded_docs = WeaviateRetrieveAllOperator(task_id='fetch_loaded_docs',
                                               weaviate_conn_id=_WEAVIATE_CONN_ID,
                                               trigger_rule='none_failed',
                                               class_name='Docs',
                                               replace_existing=True,
                                               output_file='file://include/data/loaded_docs.parquet')

    _unimported_md = import_md(md_docs=split_md_docs,
                               stack_docs=stackoverflow_md,
                               issues_docs=issues_md,
                               class_name='Docs',
                               loaded_docs_file_path=_loaded_docs.output)

    _unimported_code = import_python_code(code_dfs=[split_python_code],
                                          class_name='Docs',
                                          loaded_docs_file_path=_loaded_docs.output)

    _check_schema >> \
        _alert_schema_branch >> \
            [_slack_schema_alert, md_docs, rst_docs, code_samples, stackoverflow_md, issues_md, slack_md, registry_md]

    _loaded_docs >> [_unimported_md, _unimported_code]
    _slack_schema_alert >> _fail_schema

ask_astro_load_incremental()


# _backup = WeaviateBackupOperator(task_id='backup_to_fs',
#                                  weaviate_conn_id='weaviate_default',
#                                  backend='filesystem',
#                                  id="backup_base",
#                                 #  id="backup_fs_"+"{{ ts_nodash }}",
#                                  include=['Docs'],
#                                  )
#Restore only works with local weaviate
# _restore = WeaviateRestoreOperator(task_id='restore_from_fs',
#                                     weaviate_conn_id='weaviate_default',
#                                     backend='filesystem',
#                                     id='backup_base',
#                                     include=['Docs'],
#                                     replace_existing=True)
