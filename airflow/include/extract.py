import re
from datetime import datetime
from pathlib import Path

import html2text
import numpy as np
import pandas as pd
import pypandoc
import requests
from bs4 import BeautifulSoup
from weaviate.util import generate_uuid5

from airflow.providers.github.hooks.github import GithubHook
from airflow.providers.slack.hooks.slack import SlackHook


def extract_github_markdown(source: dict, github_conn_id: str):
    """
    This task downloads github content as markdown documents in a
    pandas dataframe.

    Dataframe fields are:
    'docSource': ie. 'astro', 'learn', etc.
    'sha': the github sha for the document
    'docLink': URL for the specific document in github.
    'content': Entire document content in markdown format.
    """

    _GITHUB_CONN_ID = github_conn_id

    downloaded_docs = []

    gh_hook = GithubHook(_GITHUB_CONN_ID)

    repo = gh_hook.client.get_repo(source["repo_base"])
    contents = repo.get_contents(source["doc_dir"])

    while contents:
        file_content = contents.pop(0)
        if file_content.type == "dir":
            contents.extend(repo.get_contents(file_content.path))

        elif Path(file_content.name).suffix == ".md":
            print(file_content.name)

            row = {
                "docLink": file_content.html_url,
                "sha": file_content.sha,
                "content": file_content.decoded_content.decode(),
                "docSource": source["doc_dir"],
            }

            downloaded_docs.append(row)

    df = pd.DataFrame(downloaded_docs)

    df["content"] = df["content"].apply(lambda x: BeautifulSoup(x, "lxml").text)

    return df


def extract_github_rst(source: dict, rst_exclude_docs: list, github_conn_id: str):
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

    _GITHUB_CONN_ID = github_conn_id

    downloaded_docs = []

    gh_hook = GithubHook(_GITHUB_CONN_ID)

    repo = gh_hook.client.get_repo(source["repo_base"])
    contents = repo.get_contents(source["doc_dir"])

    apache_license_text = Path("include/data/apache_license.rst").read_text()

    while contents:
        file_content = contents.pop(0)
        if file_content.type == "dir":
            contents.extend(repo.get_contents(file_content.path))

        elif Path(file_content.name).suffix == ".rst" and file_content.name not in rst_exclude_docs:
            print(file_content.name)

            row = {
                "docLink": file_content.html_url,
                "sha": file_content.sha,
                "content": file_content.decoded_content.decode(),
                "docSource": source["doc_dir"],
            }

            downloaded_docs.append(row)

    df = pd.DataFrame(downloaded_docs)

    df["content"] = df["content"].apply(lambda x: x.replace(apache_license_text, ""))
    df["content"] = df["content"].apply(lambda x: re.sub(r".*include.*", "", x))
    df["content"] = df["content"].apply(lambda x: re.sub(r"^\s*$", "", x))
    df = df[df["content"] != ""]

    df["content"] = df["content"].apply(
        lambda x: pypandoc.convert_text(source=x, to="md", format="rst", extra_args=["--atx-headers"])
    )
    return df


def extract_github_python(source: dict, github_conn_id: str):
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

    _GITHUB_CONN_ID = github_conn_id

    downloaded_docs = []

    gh_hook = GithubHook(_GITHUB_CONN_ID)

    repo = gh_hook.client.get_repo(source["repo_base"])
    contents = repo.get_contents(source["doc_dir"])

    while contents:
        file_content = contents.pop(0)

        if file_content.type == "dir":
            contents.extend(repo.get_contents(file_content.path))

        elif Path(file_content.name).suffix == ".py":
            print(file_content.name)

            row = {
                "docLink": file_content.html_url,
                "sha": file_content.sha,
                "content": file_content.decoded_content.decode(),
                "docSource": source["doc_dir"],
            }

            downloaded_docs.append(row)

    df = pd.DataFrame(downloaded_docs)

    return df


def extract_stack_overflow_archive(tag: dict, stackoverflow_cutoff_date: str):
    """
    This task generates stack overflow questions and answers as markdown
    documents in a pandas dataframe.

    Dataframe fields are:
    'docSource': 'stackoverflow' plus the tag name (ie. 'airflow')
    'docLink': URL for the specific question/answer.
    'content': The base64 encoded content of the question/answer in markdown format.

    Code is provided for the processing of questions and answers but is
    commented out as the historical data is provided as a parquet file.
    """
    question_template = "TITLE: {title}\nDATE: {date}\nBY: {user}\nSCORE: {score}\n{body}{question_comments}"
    answer_template = "DATE: {date}\nBY: {user}\nSCORE: {score}\n{body}{answer_comments}"
    comment_template = "{user} on {date} [Score: {score}]: {body}\n"

    post_types = {
        "1": "Question",
        "2": "Answer",
        "3": "Wiki",
        "4": "TagWikiExcerpt",
        "5": "TagWiki",
        "6": "ModeratorNomination",
        "7": "WikiPlaceholder",
        "8": "PrivilegeWiki",
    }
    posts_columns = {
        "_COL_0": "post_id",
        "_COL_1": "type",
        "_COL_3": "parent_id",
        "_COL_4": "created_on",
        "_COL_6": "score",
        "_COL_8": "body",
        "_COL_9": "user_id",
        "_COL_10": "user_name",
        "_COL_15": "title",
        "_COL_17": "answer_count",
    }
    comments_columns = {
        "_COL_0": "comment_id",
        "_COL_1": "post_id",
        "_COL_2": "comment_score",
        "_COL_3": "comment_body",
        "_COL_4": "comment_created_on",
        "_COL_5": "comment_user_name",
        "_COL_6": "comment_user_id",
    }

    posts_df = pd.read_parquet("include/data/StackOverflow/Posts/data_0_0_0.snappy.parquet")[posts_columns.keys()]
    comments_df = pd.concat(
        [
            pd.read_parquet("include/data/StackOverflow/Comments/data_0_0_0.snappy.parquet")[comments_columns.keys()],
            pd.read_parquet("include/data/StackOverflow/Comments/data_0_1_0.snappy.parquet")[comments_columns.keys()],
        ],
        ignore_index=True,
    )

    posts_df.rename(posts_columns, axis=1, inplace=True)
    posts_df["type"] = posts_df["type"].apply(lambda x: post_types[x])
    posts_df["created_on"] = pd.to_datetime(posts_df["created_on"])
    posts_df[["post_id", "parent_id", "user_id", "user_name"]] = posts_df[
        ["post_id", "parent_id", "user_id", "user_name"]
    ].astype(str)
    posts_df = posts_df[posts_df["created_on"] >= stackoverflow_cutoff_date]
    posts_df["user_id"] = posts_df.apply(lambda x: x.user_id or x.user_name or "Unknown User", axis=1)
    posts_df.reset_index(inplace=True, drop=True)

    comments_df.rename(comments_columns, axis=1, inplace=True)
    comments_df["comment_created_on"] = pd.to_datetime(comments_df["comment_created_on"])
    comments_df["comment_user_id"] = comments_df.apply(
        lambda x: x.comment_user_id or x.comment_user_name or "Unknown User", axis=1
    )
    comments_df[["post_id", "comment_user_id", "comment_user_name"]] = comments_df[
        ["post_id", "comment_user_id", "comment_user_name"]
    ].astype(str)
    comments_df[["comment_score"]] = comments_df[["comment_score"]].astype(int)
    comments_df["comment_text"] = comments_df.apply(
        lambda x: comment_template.format(
            user=x.comment_user_id, date=x.comment_created_on, score=x.comment_score, body=x.comment_body
        ),
        axis=1,
    )
    comments_df = comments_df[["post_id", "comment_text"]].groupby("post_id").agg(list)
    comments_df["comment_text"] = comments_df["comment_text"].apply(lambda x: "\n".join(x))
    comments_df.reset_index(inplace=True)

    questions_df = posts_df[posts_df["type"] == "Question"]
    questions_df = questions_df.drop("parent_id", axis=1)
    questions_df.rename({"body": "question_body", "post_id": "question_id"}, axis=1, inplace=True)
    questions_df[["answer_count", "score"]] = questions_df[["answer_count", "score"]].astype(int)
    questions_df = questions_df[questions_df["score"] >= 1]
    questions_df = questions_df[questions_df["answer_count"] >= 1]
    questions_df.reset_index(inplace=True, drop=True)
    questions_df = pd.merge(questions_df, comments_df, left_on="question_id", right_on="post_id", how="left")
    questions_df["comment_text"].fillna("", inplace=True)
    questions_df.drop("post_id", axis=1, inplace=True)
    questions_df["link"] = questions_df["question_id"].apply(lambda x: f"https://stackoverflow.com/questions/{x}")
    questions_df["question_text"] = questions_df.apply(
        lambda x: question_template.format(
            title=x.title,
            user=x.user_id,
            date=x.created_on,
            score=x.score,
            body=html2text.html2text(x.question_body),
            question_comments=x.comment_text,
        ),
        axis=1,
    )
    questions_df = questions_df[["link", "question_id", "question_text"]].set_index("question_id")
    questions_df["docSource"] = f"stackoverflow {tag}"
    questions_df = questions_df[["docSource", "link", "question_text"]]
    questions_df.columns = ["docSource", "docLink", "content"]

    answers_df = posts_df[posts_df["type"] == "Answer"][
        ["created_on", "score", "user_id", "post_id", "parent_id", "body"]
    ]
    answers_df.rename({"body": "answer_body", "post_id": "answer_id", "parent_id": "question_id"}, axis=1, inplace=True)
    answers_df.reset_index(inplace=True, drop=True)
    answers_df = pd.merge(answers_df, comments_df, left_on="answer_id", right_on="post_id", how="left")
    answers_df["comment_text"].fillna("", inplace=True)
    answers_df.drop("post_id", axis=1, inplace=True)
    answers_df["link"] = answers_df["question_id"].apply(lambda x: f"https://stackoverflow.com/questions/{x}")
    answers_df["answer_text"] = answers_df.apply(
        lambda x: answer_template.format(
            user=x.user_id,
            date=x.created_on,
            score=x.score,
            body=html2text.html2text(x.answer_body),
            answer_comments=x.comment_text,
        ),
        axis=1,
    )
    answers_df = answers_df.groupby("question_id")["answer_text"].apply(lambda x: "".join(x))

    df = questions_df.join(answers_df)
    df = df.apply(
        lambda x: pd.Series([f"stackoverflow {tag}", x.docLink, "\n".join([x.content, x.answer_text])]), axis=1
    )
    df.columns = ["docSource", "docLink", "content"]

    df.reset_index(inplace=True, drop=True)
    df["sha"] = df.apply(generate_uuid5, axis=1)

    return df


def extract_slack_archive(source: dict):
    """
    This task downloads archived slack messages as documents in a pandas dataframe.

    Dataframe fields are:
    'docSource': slack team and channel names
    'docLink': URL for the specific message/reply
    'content': The message/reply content in markdown format.
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
        lambda x: reply_md_format.format(ts=datetime.fromtimestamp(x.ts), user=x.user, text=x.text), axis=1
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


def extract_slack(source: dict):
    """
    This task downloads slack messages as documents in a pandas dataframe.

    Dataframe fields are:
    'docSource': slack team and channel names
    'docLink': URL for the specific message/reply
    'content': The message/reply content in markdown format.
    """
    message_md_format = "# slack: {team_name}\n\n## {channel_name}\n\n{content}"
    reply_md_format = "### [{ts}] <@{user}>\n\n{text}"
    link_format = "https://app.slack.com/client/{team_id}/{channel_id}/p{ts}"

    slack_client = SlackHook(slack_conn_id=source["slack_api_conn_id"]).client

    channel_info = slack_client.conversations_info(channel=source["channel_id"]).data["channel"]
    assert channel_info["is_member"] or not channel_info["is_private"]

    history = slack_client.conversations_history(channel=source["channel_id"])
    replies = []
    df = pd.DataFrame(history.data["messages"])

    # if channel has no replies yet thread_ts will not be present
    if "thread_ts" not in df:
        df["thread_ts"] = np.nan
    else:
        for ts in df[df["thread_ts"].notna()]["thread_ts"]:
            reply = slack_client.conversations_replies(channel=source["channel_id"], ts=ts)
            replies.extend(reply.data["messages"])
        df = pd.concat([df, pd.DataFrame(replies)], axis=0)

    df = df[["user", "text", "ts", "thread_ts", "client_msg_id", "type"]].drop_duplicates().reset_index(drop=True)

    df["thread_ts"] = df["thread_ts"].astype(float)
    df["ts"] = df["ts"].astype(float)
    df["thread_ts"].fillna(value=df.ts, inplace=True)

    df["content"] = df.apply(
        lambda x: reply_md_format.format(ts=datetime.fromtimestamp(x.ts), user=x.user, text=x.text), axis=1
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


def extract_github_issues(source: dict, github_conn_id: str):
    """
    This task downloads github issues as markdown documents in a pandas dataframe.

    Dataframe fields are:
    'docSource': repo name + 'issues'
    'docLink': URL for the specific question/answer.
    'content': The base64 encoded content of the question/answer in markdown format.
    'header': document type. (ie. 'question' or 'answer')
    """
    _GITHUB_CONN_ID = github_conn_id

    gh_hook = GithubHook(_GITHUB_CONN_ID)

    repo = gh_hook.client.get_repo(source["repo_base"])
    issues = repo.get_issues()

    issue_autoresponse_text = "Thanks for opening your first issue here!"
    pr_autoresponse_text = "Congratulations on your first Pull Request and welcome to the Apache Airflow community!"
    drop_content = [issue_autoresponse_text, pr_autoresponse_text]

    issues_drop_text = [
        "<\\!--\r\n.*Licensed to the Apache Software Foundation \\(ASF\\) under one.*under the License\\.\r\n -->",
        "<!-- Please keep an empty line above the dashes. -->",
        "<!--\r\nThank you.*http://chris.beams.io/posts/git-commit/\r\n-->",
        r"\*\*\^ Add meaningful description above.*newsfragments\)\.",
    ]

    issue_markdown_template = "## ISSUE TITLE: {title}\nDATE: {date}\nBY: {user}\nSTATE: {state}\n{body}\n{comments}"
    comment_markdown_template = "#### COMMENT: {user} on {date}\n{body}\n"

    downloaded_docs = []
    page_num = 0

    page = issues.get_page(page_num)

    while page:
        for issue in page:
            print(issue.number)
            comments = []
            for comment in issue.get_comments():
                if not any(substring in comment.body for substring in drop_content):
                    comments.append(
                        comment_markdown_template.format(
                            user=comment.user.login, date=issue.created_at.strftime("%m-%d-%Y"), body=comment.body
                        )
                    )
            downloaded_docs.append(
                {
                    "docLink": issue.html_url,
                    "sha": "",
                    "content": issue_markdown_template.format(
                        title=issue.title,
                        date=issue.created_at.strftime("%m-%d-%Y"),
                        user=issue.user.login,
                        state=issue.state,
                        body=issue.body,
                        comments="\n".join(comments),
                    ),
                    "docSource": f"{source['repo_base']} {source['doc_dir']}",
                }
            )
        page_num = page_num + 1
        page = issues.get_page(page_num)

    df = pd.DataFrame(downloaded_docs)

    for _text in issues_drop_text:
        df["content"] = df["content"].apply(lambda x: re.sub(_text, "", x, flags=re.DOTALL))
    df["sha"] = df.apply(generate_uuid5, axis=1)

    return df


def extract_astro_registry_cell_types():
    """
    This task downloads a list of Astro Cloud IDE cell types from the Astronomer registry
    and creates markdown documents in a pandas dataframe.

    Dataframe fields are:
    'docSource': 'registry_cell_types'
    'docLink': URL for the operator code in github
    'content': A small markdown document with cell type description
    'sha': A reference to the registry searchID.
    """

    base_url = "https://api.astronomer.io/registryV2/v1alpha1/organizations/public/modules?limit=1000"
    headers = {}

    json_data_class = "modules"
    response = requests.get(base_url, headers=headers).json()
    total_count = response["totalCount"]
    data = response.get(json_data_class, [])

    while len(data) < total_count - 1:
        response = requests.get(f"{base_url}&offset={len(data)+1}").json()
        data.extend(response.get(json_data_class, []))

    md_template = "# Registry\n## Provider: {providerName}\nVersion: {version}\nModule: {module}\nModule Description: {description}"  # noqa: E501
    link_template = "https://registry.astronomer.io/providers/{providerName}/versions/{version}/modules/{_name}"

    df = pd.DataFrame(data)
    df["docLink"] = df.apply(
        lambda x: link_template.format(providerName=x.providerName, version=x.version, _name=x["name"]), axis=1
    )
    df.rename({"searchId": "sha"}, axis=1, inplace=True)
    df["docSource"] = "astronomer registry modules"

    df["description"] = df["description"].apply(lambda x: html2text.html2text(x) if x else "No Description")
    df["content"] = df.apply(
        lambda x: md_template.format(
            providerName=x.providerName, version=x.version, module=x["name"], description=x.description
        ),
        axis=1,
    )

    df = df[["docSource", "sha", "content", "docLink"]]

    return [df]


def extract_astro_registry_dags():
    """
    This task downloads DAG code from the Astronomer registry
    and returns a pandas dataframe.

    Dataframe fields are:
    'docSource': 'registry_dags'
    'docLink': URL for the Registry location
    'content': The python DAG code
    'sha': A reference to the registry searchID.
    """

    base_url = "https://api.astronomer.io/registryV2/v1alpha1/organizations/public/dags?limit=1000"
    headers = {}

    json_data_class = "dags"
    response = requests.get(base_url, headers=headers).json()
    total_count = response["totalCount"]
    data = response.get(json_data_class, [])

    while len(data) < total_count - 1:
        response = requests.get(f"{base_url}&offset={len(data)+1}").json()
        data.extend(response.get(json_data_class, []))

    df = pd.DataFrame(data)

    df["docLink"] = df.apply(
        lambda x: f"https://registry.astronomer.io/dags/{x['name']}/versions/{x['version']}", axis=1
    )
    df.rename({"searchId": "sha"}, axis=1, inplace=True)
    df["docSource"] = "astronomer registry dags"

    df["content"] = df["githubRawSourceUrl"].apply(lambda x: requests.get(x).text)

    df = df[["docSource", "sha", "content", "docLink"]]

    return [df]


def extract_astro_blogs(blog_cutoff_date: datetime):
    """
    This task downloads Blogs from the Astronomer website
    and returns a pandas dataframe.

    Dataframe fields are:
    'docSource': 'astro blog'
    'docLink': URL for the blog post
    'content': Markdown encoded content of the blog.
    'sha': A UUID from the other fields

    """
    blog_format = "# {title}\n\n## {content}"

    base_url = "https://www.astronomer.io"
    page_url = base_url + "/blog/{page}/#archive"
    headers = {}
    links = []
    dates = []
    page = 1

    response = requests.get(page_url.format(page=page), headers=headers)
    while response.ok:
        soup = BeautifulSoup(response.text, "lxml")
        cards = soup.find_all(class_="post-card__cover")
        links.extend([base_url + card.find("a", href=True)["href"] for card in cards])
        meta = soup.find_all(class_="post-card__meta")
        dates.extend([post.find("time")["datetime"] for post in meta])

        page = page + 1
        response = requests.get(page_url.format(page=page), headers=headers)

    df = pd.DataFrame(zip(links, dates), columns=["docLink", "date"])

    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df[df["date"] > blog_cutoff_date.date()]
    df.drop("date", inplace=True, axis=1)
    df.drop_duplicates(inplace=True)

    df["content"] = df["docLink"].apply(lambda x: requests.get(x).content)
    df["title"] = df["content"].apply(
        lambda x: BeautifulSoup(x, "lxml").find(class_="post-card__meta").find(class_="title").get_text()
    )

    df["content"] = df["content"].apply(lambda x: BeautifulSoup(x, "lxml").find(class_="prose").get_text())
    df["content"] = df.apply(lambda x: blog_format.format(title=x.title, content=x.content), axis=1)

    df.drop("title", axis=1, inplace=True)
    df["sha"] = df["content"].apply(generate_uuid5)
    df["docSource"] = "astro blog"
    df.reset_index(drop=True, inplace=True)

    df = df[["docSource", "sha", "content", "docLink"]]

    return [df]
