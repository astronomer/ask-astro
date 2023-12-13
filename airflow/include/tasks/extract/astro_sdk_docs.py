from __future__ import annotations

import logging
import re

import pandas as pd

from include.tasks.extract.utils.html_url_extractor import extract_internal_url, url_to_df

logger = logging.getLogger("airflow.task")


def extract_astro_sdk_docs() -> list[pd.DataFrame]:
    exclude_docs = ["autoapi", "genindex.html", "py-modindex.html", ".md", ".py"]
    base_url = "https://astro-sdk-python.readthedocs.io/en/stable/"

    urls = extract_internal_url(base_url, exclude_docs)

    new_urls = [url for url in urls if "stable" in url]
    logger.info("******ingesting****")
    logger.info(new_urls)
    logger.info("*********************")
    df = url_to_df(new_urls, "astro-sdk")

    return [df]
