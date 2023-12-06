from __future__ import annotations

import re

import pandas as pd

from include.tasks.extract.utils.html_url_extractor import extract_internal_url, url_to_df


def extract_astro_sdk_docs() -> list[pd.DataFrame]:
    exclude_docs = ["autoapi", "genindex.html", "py-modindex.html", ".md", ".py"]
    base_url = "https://astro-sdk-python.readthedocs.io/en/stable/"

    urls = extract_internal_url(base_url, exclude_docs)
    version_pattern = re.compile(r"[0-9]+(\.[0-9]+){1,2}")

    new_urls = [url for url in urls if not version_pattern.search(url)]
    print("******ingesting****")
    print(new_urls)
    print("*********************")
    df = url_to_df(new_urls, "astro-sdk")

    return [df]
