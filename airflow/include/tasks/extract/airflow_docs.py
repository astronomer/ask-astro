from __future__ import annotations

import re
import urllib.parse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from include.tasks.extract.utils.html_helpers import get_all_links
from weaviate.util import generate_uuid5


def extract_airflow_docs(docs_base_url: str) -> list[pd.DataFrame]:
    """
    This task scrapes docs from the Airflow website and returns a list of pandas dataframes. Return
    type is a list in order to map to upstream dynamic tasks.  The code recursively generates a list
    of html files relative to 'docs_base_url' and then extracts each as text.

    Note: Only the (class_: body, role: main) tag and children are extracted.

    Note: This code will also pickup https://airflow.apache.org/howto/*
    which are also referenced in the docs pages.  These are useful for Ask Astro and also they are relatively few
    pages so we leave them in.

    param docs_base_url: Base URL to start extract.
    type docs_base_url: str

    The returned data includes the following fields:
    'docSource': 'apache/airflow/docs'
    'docLink': URL for the page
    'content': HTML content of the page
    'sha': A UUID from the other fields
    """

    # we exclude the following docs which are not useful and/or too large for easy processing.
    exclude_docs = [
        "changelog.html",
        "commits.html",
        "docs/apache-airflow/stable/release_notes.html",
        "docs/stable/release_notes.html",
        "_api",
        "_modules",
        "installing-providers-from-sources.html",
        "apache-airflow/1.",
        "apache-airflow/2.",
        "example",
        "cli-and-env-variables-ref.html",
    ]

    docs_url_parts = urllib.parse.urlsplit(docs_base_url)
    docs_url_base = f"{docs_url_parts.scheme}://{docs_url_parts.netloc}"

    all_links = {docs_base_url}
    get_all_links(url=list(all_links)[0], all_links=all_links, exclude_docs=exclude_docs)

    # make sure we didn't accidentally pickup any unrelated links in recursion
    non_doc_links = {link if docs_url_base not in link else "" for link in all_links}
    docs_links = all_links - non_doc_links

    df = pd.DataFrame(docs_links, columns=["docLink"])

    df["html_content"] = df["docLink"].apply(lambda x: requests.get(x).content)

    df["content"] = df["html_content"].apply(
        lambda x: str(BeautifulSoup(x, "html.parser").find(class_="body", role="main"))
    )
    df["content"] = df["content"].apply(lambda x: re.sub("¶", "", x))

    df["sha"] = df["content"].apply(generate_uuid5)
    df["docSource"] = "apache/airflow/docs"
    df.reset_index(drop=True, inplace=True)

    # column order matters for uuid generation
    df = df[["docSource", "sha", "content", "docLink"]]

    return [df]
