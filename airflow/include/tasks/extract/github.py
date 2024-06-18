from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
import pypandoc
from bs4 import BeautifulSoup

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
