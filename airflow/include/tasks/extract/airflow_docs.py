from __future__ import annotations
from bs4 import BeautifulSoup

import pandas as pd
import requests
import urllib.parse
from weaviate.util import generate_uuid5
import re

from include.tasks.extract.utils.html_utils import get_internal_links


def extract_airflow_docs(docs_base_url: str) -> list[pd.DataFrame]:
    """
    This task return all internal url for Airflow docs
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

    all_links = get_internal_links(docs_base_url, exclude_literal=exclude_docs)
    
    docs_url_parts = urllib.parse.urlsplit(docs_base_url)
    docs_url_base = f"{docs_url_parts.scheme}://{docs_url_parts.netloc}"
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
