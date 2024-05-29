from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from textwrap import dedent

import pandas as pd
import pypandoc
from bs4 import BeautifulSoup
from weaviate.util import generate_uuid5

from airflow.providers.github.hooks.github import GithubHook


def extract_github_markdown(source: dict, github_conn_id: str) -> pd.DataFrame:
    """
    This task downloads github content as markdown documents in a
    pandas dataframe.

    param source: A dictionary specifying a github repository "repo_base" as organization/repository
    (ie. "astronomer/docs") and optionally a subdirectory ("doc_dir") within that repository (ie. "learn")
    from which to extract.
    type source: dict

    param github_conn_id: The connection ID to use with the GithubHook
    param github_conn_id: str

    The returned data includes the following fields:
    'docSource': ie. 'astronomer/docs/astro', 'astronomer/docs/learn', etc.
    'sha': the github sha for the document
    'docLink': URL for the specific document in github.
    'content': Entire document content in markdown format.
    """

    gh_hook = GithubHook(github_conn_id)

    repo = gh_hook.client.get_repo(source["repo_base"])
    contents = repo.get_contents(source.get("doc_dir"))

    downloaded_docs = []
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
                "docSource": "/".join([source["repo_base"], source.get("doc_dir")]),
            }

            downloaded_docs.append(row)

    df = pd.DataFrame(downloaded_docs)

    df["content"] = df["content"].apply(lambda x: BeautifulSoup(x, "lxml").text)

    # column order matters for uuid generation
    df = df[["docSource", "sha", "content", "docLink"]]

    return df


def extract_github_rst(source: dict, github_conn_id: str) -> pd.DataFrame:
    """
    This task downloads github content as rst documents in a pandas dataframe.  The 'content' field is converted from
    RST to Markdown (via pypandoc).  After removing the preamble (apache license), any empty lines and include'
    footers any empty docs are removed.  Document links and references are not included in the content.

    param source: A dictionary specifying a github repository "repo_base" as organization/repository
    (ie. "apache/airflow"), optionally a subdirectory ("doc_dir") within that repository (ie. "docs")
    from which to extract, and a list of file names to exclude in "exclude_docs".
    type source: dict

    param github_conn_id: The connection ID to use with the GithubHook
    param github_conn_id: str

    The returned data includes the following fields:
    'docSource': ie. 'astronomer/docs/astro', 'astronomer/docs/learn', etc.
    'sha': the github sha for the document
    'docLink': URL for the specific document in github.
    'content': Entire document content in markdown format.

    """

    gh_hook = GithubHook(github_conn_id)

    repo = gh_hook.client.get_repo(source["repo_base"])
    contents = repo.get_contents(source.get("doc_dir"))

    apache_license_text = Path("include/data/apache/apache_license.rst").read_text()

    downloaded_docs = []
    while contents:
        file_content = contents.pop(0)
        if file_content.type == "dir":
            contents.extend(repo.get_contents(file_content.path))

        elif Path(file_content.name).suffix == ".rst" and file_content.name not in source["exclude_docs"]:
            print(file_content.name)

            row = {
                "docLink": file_content.html_url,
                "sha": file_content.sha,
                "content": file_content.decoded_content.decode(),
                "docSource": "/".join([source["repo_base"], source.get("doc_dir")]),
            }

            downloaded_docs.append(row)

    df = pd.DataFrame(downloaded_docs)

    df["content"] = df["content"].apply(lambda x: x.replace(apache_license_text, ""))
    df["content"] = df["content"].apply(lambda x: re.sub(r".*include.*", "", x))
    df["content"] = df["content"].apply(lambda x: re.sub(r"^\s*$", "", x))
    df = df[df["content"] != ""].reset_index()

    df["content"] = df["content"].apply(
        lambda x: pypandoc.convert_text(source=x, to="md", format="rst", extra_args=["--atx-headers"])
    )

    # column order matters for uuid generation
    df = df[["docSource", "sha", "content", "docLink"]]

    return df


def extract_github_python(source: dict, github_conn_id: str) -> pd.DataFrame:
    """
    This task downloads github content as python code in a pandas dataframe.

    param source: A dictionary specifying a github repository "repo_base" as organization/repository
    (ie. "astronomer/docs") and optionally a subdirectory ("doc_dir") within that repository (ie. "docs")
    from which to extract.
    type source: dict

    param github_conn_id: The connection ID to use with the GithubHook
    param github_conn_id: str

    The returned data includes the following fields:
    'docSource': ie. 'astronomer/docs/astro', 'astronomer/docs/learn', etc.
    'sha': the github sha for the document
    'docLink': URL for the specific document in github.
    'content': Entire document content in markdown format.
    """

    gh_hook = GithubHook(github_conn_id)

    repo = gh_hook.client.get_repo(source["repo_base"])
    contents = repo.get_contents(source.get("doc_dir"))

    downloaded_docs = []
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
                "docSource": "/".join([source["repo_base"], source.get("doc_dir")]),
            }

            downloaded_docs.append(row)

    df = pd.DataFrame(downloaded_docs)

    # column order matters for uuid generation
    df = df[["docSource", "sha", "content", "docLink"]]

    return df


def extract_github_issues(repo_base: str, github_conn_id: str, cutoff_date: str = "2022-1-1") -> pd.DataFrame:
    """
    This task downloads github issues as markdown documents in a pandas dataframe.  Text from templated
    auto responses for issues are removed while building a markdown document for each issue.

    param repo_base: The name of organization/repository (ie. "apache/airflow") from which to extract
    issues.
    type repo_base: str

    param github_conn_id: The connection ID to use with the GithubHook
    param github_conn_id: str
    param cutoff_date: The cutoff date (format: Y-m-d) to extract issues

    The returned data includes the following fields:
    'docSource': ie. 'astronomer/docs/astro', 'astronomer/docs/learn', etc.
    'sha': the github sha for the document
    'docLink': URL for the specific document in github.
    'content': Entire document content in markdown format.

    """

    gh_hook = GithubHook(github_conn_id)

    repo = gh_hook.client.get_repo(repo_base)
    issues = repo.get_issues(state="all", since=datetime.strptime(cutoff_date, "%Y-%m-%d"))

    issue_autoresponse_text = "Thanks for opening your first issue here!"
    pr_autoresponse_text = "Congratulations on your first Pull Request and welcome to the Apache Airflow community!"
    drop_content = [issue_autoresponse_text, pr_autoresponse_text]

    issues_drop_text = [
        dedent(
            """                     <\\!--\r
                         .*?Licensed to the Apache Software Foundation \\(ASF\\) under one.*?under the License\\.\r
                          -->"""
        ),
        "<!-- Please keep an empty line above the dashes. -->",
        "<!--\r\nThank you.*?http://chris.beams.io/posts/git-commit/\r\n-->",
        r"\*\*\^ Add meaningful description above.*?newsfragments\)\.",
    ]

    issue_markdown_template = dedent(
        """
        ## ISSUE TITLE: {title}
        DATE: {date}
        BY: {user}
        STATE: {state}
        {body}
        {comments}"""
    )

    comment_markdown_template = dedent(
        """
        #### COMMENT: {user} on {date}
        {body}\n"""
    )

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
                    "docSource": f"{repo_base}/issues",
                }
            )
        page_num = page_num + 1
        page = issues.get_page(page_num)

    df = pd.DataFrame(downloaded_docs)

    for _text in issues_drop_text:
        df["content"] = df["content"].apply(lambda x: re.sub(_text, "", x, flags=re.DOTALL))

    df["content"] = df["content"].apply(lambda x: re.sub(r"\r\n+", "\n\n", x).strip())
    df["content"] = df["content"].apply(lambda x: re.sub(r"\n+", "\n\n", x).strip())

    df["sha"] = df.apply(generate_uuid5, axis=1)

    # column order matters for uuid generation
    df = df[["docSource", "sha", "content", "docLink"]]

    return df
