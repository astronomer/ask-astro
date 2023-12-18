from __future__ import annotations

import pandas as pd

from include.tasks.extract.utils.html_url_extractor import extract_internal_url, url_to_df


def extract_provider_docs() -> list[pd.DataFrame]:
    exclude_docs = ["_api", "_modules", "_sources", "changelog.html", "genindex.html", "py-modindex.html", "#"]
    base_url = "https://astronomer-providers.readthedocs.io/en/stable/"

    urls = extract_internal_url(base_url, exclude_docs)
    df = url_to_df(urls, "astronomer-providers")
    return [df]
