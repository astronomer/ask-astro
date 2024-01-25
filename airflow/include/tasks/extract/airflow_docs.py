from __future__ import annotations

import pandas as pd

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

    all_links = list(get_internal_links(docs_base_url, exclude_literal=exclude_docs))

    return all_links
