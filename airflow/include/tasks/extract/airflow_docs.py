from __future__ import annotations

import pandas as pd

from include.tasks.extract.utils.html_utils import get_internal_links


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

    all_links = get_internal_links(docs_base_url, exclude_literal=exclude_docs)

    return all_links
