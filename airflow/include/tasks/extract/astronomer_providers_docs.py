from __future__ import annotations

import pandas as pd

from include.tasks.extract.utils.html_utils import get_internal_links, urls_to_dataframe


def extract_provider_docs() -> list[pd.DataFrame]:
    exclude_docs = ["_api", "_modules", "_sources", "changelog.html", "genindex.html", "py-modindex.html", "#"]
    base_url = "https://astronomer-providers.readthedocs.io/en/stable/"

    urls = get_internal_links(base_url, exclude_docs)
    df = urls_to_dataframe(urls, "astronomer-providers")
    return [df]
