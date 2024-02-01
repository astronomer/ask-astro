from __future__ import annotations

import urllib.parse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from weaviate.util import generate_uuid5

from include.tasks.extract.utils.html_utils import get_internal_links


def extract_cosmos_docs(docs_base_url: str = "https://astronomer.github.io/astronomer-cosmos/") -> list[pd.DataFrame]:
    """
    This task return a dataframe containing the extracted data for cosmos docs.
    """

    # we exclude the following docs which are not useful and/or too large for easy processing.
    exclude_docs = [
        "_sources",
    ]

    all_links = get_internal_links(docs_base_url, exclude_literal=exclude_docs)
    html_links = {url for url in all_links if url.endswith(".html")}

    docs_url_parts = urllib.parse.urlsplit(docs_base_url)
    docs_url_base = f"{docs_url_parts.scheme}://{docs_url_parts.netloc}"
    # make sure we didn't accidentally pickup any unrelated links in recursion
    non_doc_links = {link if docs_url_base not in link else "" for link in html_links}
    docs_links = html_links - non_doc_links

    df = pd.DataFrame(docs_links, columns=["docLink"])

    df["html_content"] = df["docLink"].apply(lambda url: requests.get(url).content)

    # Only keep the main article content,
    df["content"] = df["html_content"].apply(lambda x: str(BeautifulSoup(x, "html.parser").find(name="article")))

    df["sha"] = df["content"].apply(generate_uuid5)
    df["docSource"] = "cosmos docs"
    df.reset_index(drop=True, inplace=True)

    # column order matters for uuid generation
    df = df[["docSource", "sha", "content", "docLink"]]

    return [df]
