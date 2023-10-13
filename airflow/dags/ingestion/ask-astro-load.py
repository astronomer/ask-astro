from datetime import datetime

import html2text
import pandas as pd
from langchain.schema import Document
from langchain.text_splitter import (
    RecursiveCharacterTextSplitter,
)
from weaviate.util import generate_uuid5
from weaviate_provider.hooks.weaviate import WeaviateHook
from weaviate_provider.operators.weaviate import (
    WeaviateCheckSchemaOperator,
    WeaviateCreateSchemaOperator,
)

from airflow.decorators import dag, task

_WEAVIATE_CONN_ID = "weaviate_test"
_GITHUB_CONN_ID = "github_default"
_SLACK_CONN_ID = "slack_api_default"

# doc_dir:baseurl pairs for dynamic tasks
markdown_docs_sources = [
    {"doc_dir": "learn", "repo_base": "astronomer/docs"},
    {"doc_dir": "astro", "repo_base": "astronomer/docs"},
]
rst_docs_sources = [
    {"doc_dir": "docs", "repo_base": "apache/airflow"},
]
code_samples_sources = [
    {"doc_dir": "code-samples", "repo_base": "astronomer/docs"},
]
issues_docs_sources = [{"doc_dir": "issues", "repo_base": "apache/airflow"}]
slack_channel_sources = [
    {
        "channel_name": "troubleshooting",
        "channel_id": "CCQ7EGB1P",
        "team_id": "TCQ18L22Z",
        "team_name": "Airflow Slack Community",
        "slack_api_conn_id": "TBD",
    }
]
http_json_sources = [
    {
        "name": "registry_cell_types",
        "base_url": "https://api.astronomer.io/registryV2/v1alpha1/organizations/public/modules?limit=1000",
        "headers": {},
        "count_field": "totalCount",
    }
]

rst_exclude_docs = ["changelog.rst", "commits.rst"]

stackoverflow_cutoff_date = "2021-09-01"
stackoverflow_tags = [
    "airflow",
]

weaviate_doc_count = {
    "Docs": 7307,
}

default_args = {
    "retries": 3,
}


@dag(schedule_interval=None, start_date=datetime(2023, 8, 1), catchup=False, default_args=default_args)
def ask_astro_load_bulk():
    """
    This DAG performs the initial load of data from sources.  While the code to generate these datasets
    is included for each function, the data is frozen as a parquet file for simple ingest and the steps
    to create are commented out.
    """
    _check_schema = WeaviateCheckSchemaOperator(
        task_id="check_schema",
        weaviate_conn_id=_WEAVIATE_CONN_ID,
        class_object_data="file://include/data/schema.json",
    )

    @task.branch
    def recreate_schema_branch(schema_exists: bool) -> str:
        # WeaviateHook(_WEAVIATE_CONN_ID).get_conn().schema.delete_all()
        if schema_exists:
            return ["check_object_count"]
        elif not schema_exists:
            return ["create_schema"]
        else:
            return None

    @task.branch
    def check_object_count(weaviate_doc_count: dict, class_name: str) -> str:
        try:
            weaviate_hook = WeaviateHook(_WEAVIATE_CONN_ID)
            weaviate_hook.client = weaviate_hook.get_conn()
            response = weaviate_hook.run(f"{{Aggregate {{ {class_name} {{ meta {{ count }} }} }} }}")
        except Exception as e:
            if e.status_code == 422 and "no graphql provider present" in e.message:
                response = None

        if response and response["data"]["Aggregate"][class_name][0]["meta"]["count"] >= weaviate_doc_count[class_name]:
            print("Initial Upload complete. Skipping")
            return None
        else:
            return [
                "extract_github_markdown",
                "extract_github_rst",
                "extract_github_python",
                "extract_stack_overflow",
                "extract_slack",
                "extract_registry",
                "extract_github_issues",
            ]

    _create_schema = WeaviateCreateSchemaOperator(
        task_id="create_schema",
        weaviate_conn_id=_WEAVIATE_CONN_ID,
        class_object_data="file://include/data/schema.json",
        existing="fail",
    )

    @task(trigger_rule="none_failed")
    def extract_github_markdown(source: dict):
        """
        This task downloads github content as markdown documents in a
        pandas dataframe.

        Dataframe fields are:
        'docSource': ie. 'astro', 'learn', etc.
        'sha': the github sha for the document
        'docLink': URL for the specific document in github.
        'content': Entire document content in markdown format.

        Code is provided for the processing of questions and answers but is
        commented out as the historical data is provided as a parquet file.
        """

        # downloaded_docs = []

        # gh_hook = GithubHook(_GITHUB_CONN_ID)

        # repo = gh_hook.client.get_repo(source['repo_base'])
        # contents = repo.get_contents(source['doc_dir'])

        # while contents:

        #     file_content = contents.pop(0)
        #     if file_content.type == "dir":
        #         contents.extend(repo.get_contents(file_content.path))

        #     elif Path(file_content.name).suffix == '.md':

        #         print(file_content.name)

        #         row = {
        #             "docLink": file_content.html_url,
        #             "sha": file_content.sha,
        #             "content": file_content.decoded_content.decode(),
        #             "docSource": source['doc_dir'],
        #         }

        #         downloaded_docs.append(row)

        # df = pd.DataFrame(downloaded_docs)

        # df.to_parquet(f"include/data/{source['repo_base']}/{source['doc_dir']}.parquet")
        df = pd.read_parquet(f"include/data/{source['repo_base']}/{source['doc_dir']}.parquet")

        return df

    @task(trigger_rule="none_failed")
    def extract_github_rst(source: dict):
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

        Code is provided for the processing of questions and answers but is
        commented out as the historical data is provided as a parquet file.
        """

        # downloaded_docs = []

        # gh_hook = GithubHook(_GITHUB_CONN_ID)

        # repo = gh_hook.client.get_repo(source['repo_base'])
        # contents = repo.get_contents(source['doc_dir'])

        # apache_license_text = Path('include/data/apache_license.rst').read_text()

        # while contents:

        #     file_content = contents.pop(0)
        #     if file_content.type == "dir":
        #         contents.extend(repo.get_contents(file_content.path))

        #     elif Path(file_content.name).suffix == '.rst' and file_content.name not in rst_exclude_docs:

        #         print(file_content.name)

        #         row = {
        #             "docLink": file_content.html_url,
        #             "sha": file_content.sha,
        #             "content": file_content.decoded_content.decode(),
        #             "docSource": source['doc_dir'],
        #         }

        #         downloaded_docs.append(row)

        # df = pd.DataFrame(downloaded_docs)

        # df['content'] = df['content'].apply(lambda x: x.replace(apache_license_text, ''))
        # df['content'] = df['content'].apply(lambda x: re.sub(r".*include.*", "", x))
        # df['content'] = df['content'].apply(lambda x: re.sub(r'^\s*$', "", x))
        # df = df[df['content']!='']
        # df['content'] = df['content'].apply(lambda x: pypandoc.convert_text(source=x, to='md',
        #                                                                     format='rst',
        #                                                                     extra_args=['--atx-headers']))

        # df.to_parquet(f"include/data/{source['repo_base']}/{source['doc_dir']}.parquet")
        df = pd.read_parquet(f"include/data/{source['repo_base']}/{source['doc_dir']}.parquet")

        return df

    @task(trigger_rule="none_failed")
    def extract_github_python(source: dict):
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

        Code is provided for the processing of questions and answers but is
        commented out as the historical data is provided as a parquet file.
        """

        # downloaded_docs = []

        # gh_hook = GithubHook(_GITHUB_CONN_ID)

        # repo = gh_hook.client.get_repo(source['repo_base'])
        # contents = repo.get_contents(source['doc_dir'])

        # while contents:
        #     file_content = contents.pop(0)

        #     if file_content.type == "dir":
        #         contents.extend(repo.get_contents(file_content.path))

        #     elif Path(file_content.name).suffix == '.py':
        #         print(file_content.name)

        #         row = {
        #             "docLink": file_content.html_url,
        #             "sha": file_content.sha,
        #             "content": file_content.decoded_content.decode(),
        #             "docSource": source['doc_dir'],
        #             "header": 'python',
        #         }

        #         downloaded_docs.append(row)

        # df = pd.DataFrame(downloaded_docs)

        # df.to_parquet(f"include/data/{source['repo_base']}/{source['doc_dir']}.parquet")
        df = pd.read_parquet(f"include/data/{source['repo_base']}/{source['doc_dir']}.parquet")

        return df

    @task(trigger_rule="none_failed")
    def extract_stack_overflow(tag: dict, stackoverflow_cutoff_date: str):
        """
        This task generates stack overflow questions and answers as markdown
        documents in a pandas dataframe.

        Dataframe fields are:
        'docSource': 'stackoverflow' plus the tag name (ie. 'airflow')
        'docLink': URL for the specific question/answer.
        'content': The base64 encoded content of the question/answer in markdown format.
        'header': document type. (ie. 'question' or 'answer')

        Code is provided for the processing of questions and answers but is
        commented out as the historical data is provided as a parquet file.
        """
        df = pd.read_parquet("include/data/stackoverflow_base.parquet")
        df["sha"] = df.apply(generate_uuid5, axis=1)

        return df

    @task(trigger_rule="none_failed")
    def extract_slack(source: dict):
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

        df = pd.read_parquet("include/data/slack/troubleshooting.parquet")

        message_md_format = "# slack: {team_name}\n\n## {channel_name}\n\n{content}"
        reply_md_format = "### [{ts}] <@{user}>\n\n{text}"
        link_format = "https://app.slack.com/client/{team_id}/{channel_id}/p{ts}"

        df = df[["user", "text", "ts", "thread_ts", "client_msg_id", "type"]].drop_duplicates().reset_index(drop=True)

        df["thread_ts"] = df["thread_ts"].astype(float)
        df["ts"] = df["ts"].astype(float)

        df["thread_ts"].fillna(value=df.ts, inplace=True)

        df["content"] = df.apply(
            lambda x: reply_md_format.format(ts=datetime.fromtimestamp(x.ts), user=x.user, text=x.text),
            axis=1,
        )

        df = df.sort_values("ts").groupby("thread_ts").agg({"content": "\n".join}).reset_index()

        df["content"] = df["content"].apply(
            lambda x: message_md_format.format(
                team_name=source["team_name"], channel_name=source["channel_name"], content=x
            )
        )

        df["docLink"] = df["thread_ts"].apply(
            lambda x: link_format.format(
                team_id=source["team_id"], channel_id=source["channel_id"], ts=str(x).replace(".", "")
            )
        )
        df["docSource"] = source["channel_name"]

        df["sha"] = df["content"].apply(generate_uuid5)

        df = df[["docSource", "sha", "content", "docLink"]]

        return df

    @task(trigger_rule="none_failed")
    def extract_github_issues(source: dict):
        """
        This task downloads github issues as markdown documents in a pandas dataframe.

        Dataframe fields are:
        'docSource': repo name + 'issues'
        'docLink': URL for the specific question/answer.
        'content': The base64 encoded content of the question/answer in markdown format.
        'header': document type. (ie. 'airflow issue')

        Code is provided for the processing of questions and answers but is
        commented out as the historical data is provided as a parquet file.
        """
        # gh_hook = GithubHook(_GITHUB_CONN_ID)
        #
        # repo = gh_hook.client.get_repo(source["repo_base"])
        # issues = repo.get_issues()
        #
        # issue_autoresponse_text = "Thanks for opening your first issue here!"
        # pr_autoresponse_text = (
        #     "Congratulations on your first Pull Request and welcome to the Apache Airflow community!"
        # )
        # drop_content = [issue_autoresponse_text, pr_autoresponse_text]
        #
        # issue_markdown_template = (
        #     "## ISSUE TITLE: {title}\nDATE: {date}\nBY: {user}\nSTATE: {state}\n{body}\n{comments}"
        # )
        # comment_markdown_template = "#### COMMENT: {user} on {date}\n{body}\n"
        #
        # downloaded_docs = []
        # page_num = 0
        #
        # page = issues.get_page(page_num)
        #
        # while page:
        #     for issue in page:
        #         print(issue.number)
        #         comments = []
        #         for comment in issue.get_comments():
        #             # TODO: this is very slow.  Look for vectorized approach.
        #             if not any(substring in comment.body for substring in drop_content):
        #                 comments.append(
        #                     comment_markdown_template.format(
        #                         user=comment.user.login,
        #                         date=issue.created_at.strftime("%m-%d-%Y"),
        #                         body=comment.body,
        #                     )
        #                 )
        #         downloaded_docs.append(
        #             {
        #                 "docLink": issue.html_url,
        #                 "sha": "",
        #                 "content": issue_markdown_template.format(
        #                     title=issue.title,
        #                     date=issue.created_at.strftime("%m-%d-%Y"),
        #                     user=issue.user.login,
        #                     state=issue.state,
        #                     body=issue.body,
        #                     comments="\n".join(comments),
        #                 ),
        #                 "docSource": f"{source['repo_base']} {source['doc_dir']}",
        #                 "header": f"{source['repo_base']} issue",
        #             }
        #         )
        #     page_num = page_num + 1
        #     page = issues.get_page(page_num)
        #
        # df = pd.DataFrame(downloaded_docs)

        # df.to_parquet(f"include/data/{source['repo_base']}/{source['doc_dir']}.parquet")
        df = pd.read_parquet(f"include/data/{source['repo_base']}/{source['doc_dir']}.parquet")
        df["sha"] = df.apply(generate_uuid5, axis=1)

        return df

    @task(trigger_rule="none_failed")
    def extract_registry(source: dict):
        # data_class = source['base_url'].split('/')[-1].split('?')[0]

        # response = requests.get(source['base_url'], headers=source['headers']).json()
        # total_count = response[source['count_field']]
        # data = response.get(data_class, [])
        # while len(data) < total_count-1:
        #     response = requests.get(f"{source['base_url']}&offset={len(data)+1}").json()
        #     data.extend(response.get(data_class, []))

        # df = pd.DataFrame(data)
        # df.rename({'githubUrl': 'docLink', 'searchId': 'sha'}, axis=1, inplace=True)
        # df['docSource'] = source['name']
        # df['description'] = df['description'].apply(lambda x: html2text.html2text(x) if x else 'No Description')
        # df['content'] = df.apply(lambda x: md_template.format(providerName=x.providerName,
        #                                                       version=x.version,
        #                                                       name=x.name,
        #                                                       description=x.description), axis=1)

        # df = df[['docSource', 'sha', 'content', 'docLink']]

        # df.to_parquet('include/data/registry.parquet')

        df = pd.read_parquet("include/data/registry.parquet")

        return df

    @task()
    def split_data(
        md_dfs: list[pd.DataFrame],
        rst_dfs: list[pd.DataFrame],
        slack_dfs: list[pd.DataFrame],
        stackoverflow_dfs: list[pd.DataFrame],
        code_dfs: list[pd.DataFrame],
        issues_dfs: list[pd.DataFrame],
        reg_dfs: list[pd.DataFrame],
    ):
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
        stackoverflow_df = pd.concat(stackoverflow_dfs, axis=0, ignore_index=True)
        code_df = pd.concat(code_dfs, axis=0, ignore_index=True)
        issues_df = pd.concat(issues_dfs, axis=0, ignore_index=True)
        reg_df = pd.concat(reg_dfs, axis=0, ignore_index=True)

        df = pd.concat(
            [
                md_df,
                rst_df,
                slack_df,
                reg_df,
                code_df,
                stackoverflow_df,
                issues_df,
            ],
            axis=0,
            ignore_index=True,
        )

        # headers_to_split_on = [
        #     ("#", "Header 1"),
        #     ("##", "Header 2"),
        #     # ("###", "Header 3"),
        # ]

        # splitter = MarkdownHeaderTextSplitter(headers_to_split_on=headers_to_split_on)
        splitter = RecursiveCharacterTextSplitter()
        df["doc_chunks"] = df["content"].apply(lambda x: splitter.split_documents([Document(page_content=x)]))

        df = df[df["doc_chunks"].apply(lambda x: len(x)) > 0].reset_index(drop=True)
        # _ = df["doc_chunks"].apply(
        #     lambda x: x[0].metadata.update({"Header 1": "Summary"}) if x[0].metadata == {} else x[0]
        # )
        df = df.explode("doc_chunks", ignore_index=True)
        df["content"] = df["doc_chunks"].apply(lambda x: html2text.html2text(x.page_content).replace("\n", " "))
        df["content"] = df["content"].apply(lambda x: x.replace("\\", ""))
        # df['header'] = df['doc_chunks'].apply(lambda x: '. '.join(list(x.metadata.values())))

        df.drop(["doc_chunks"], inplace=True, axis=1)
        df.drop(["header"], inplace=True, axis=1)
        df.reset_index(inplace=True, drop=True)

        return df

    @task.weaviate_import(weaviate_conn_id=_WEAVIATE_CONN_ID)
    def import_data(md_docs: pd.DataFrame, class_name: str):
        """
        This task concatenates multiple dataframes from upstream dynamic tasks and
        vectorizes with import to weaviate.

        A 'uuid' is generated based on the content and metadata (the git sha, document url,
        the document source (ie. astro) and a concatenation of the headers).

        Vectorization includes the headers for bm25 search.
        """

        df = pd.concat([md_docs], ignore_index=True)

        df["uuid"] = df.apply(lambda x: generate_uuid5(x.to_dict()), axis=1)

        print(f"Passing {len(df)} objects for import.")

        return {
            "data": df,
            "class_name": class_name,
            "uuid_column": "uuid",
            "batch_size": 1000,
            "error_threshold": 12,
        }

    _recreate_schema_branch = recreate_schema_branch(_check_schema.output)
    _check_object_count = check_object_count(weaviate_doc_count, "Docs")

    md_docs = extract_github_markdown.partial().expand(source=markdown_docs_sources)
    rst_docs = extract_github_rst.partial().expand(source=rst_docs_sources)
    issues_md = extract_github_issues.partial().expand(source=issues_docs_sources)
    code_samples = extract_github_python.partial().expand(source=code_samples_sources)
    stackoverflow_md = extract_stack_overflow.partial(stackoverflow_cutoff_date=stackoverflow_cutoff_date).expand(
        tag=stackoverflow_tags
    )
    slack_md = extract_slack.partial().expand(source=slack_channel_sources)
    registry_md = extract_registry.partial().expand(source=http_json_sources)

    split_md_docs = split_data(
        md_dfs=md_docs,
        rst_dfs=rst_docs,
        stackoverflow_dfs=stackoverflow_md,
        code_dfs=code_samples,
        slack_dfs=slack_md,
        issues_dfs=issues_md,
        reg_dfs=registry_md,
    )

    _unimported_md = import_data(md_docs=split_md_docs, class_name="Docs")

    _check_schema >> _recreate_schema_branch >> [_create_schema, _check_object_count]
    _check_object_count >> [
        md_docs,
        rst_docs,
        code_samples,
        stackoverflow_md,
        issues_md,
        slack_md,
        registry_md,
    ]
    _create_schema >> [md_docs, rst_docs, code_samples, stackoverflow_md, issues_md, slack_md, registry_md]


ask_astro_load_bulk()


def test():
    from weaviate_provider.hooks.weaviate import WeaviateHook

    weaviate_client = WeaviateHook(_WEAVIATE_CONN_ID).get_conn()
    search = (
        weaviate_client.query.get(properties=["content"], class_name="Docs")
        .with_limit(2000)
        .with_where({"path": ["docSource"], "operator": "Equal", "valueText": "registry_cell_types"})
        .do()
    )
    len(search["data"]["Get"]["Docs"])

    # TODO: The following is broken as df is not defined
    # WeaviateImportDataOperator(task_id="test", data=df, class_name="Docs", uuid_column="uuid")
