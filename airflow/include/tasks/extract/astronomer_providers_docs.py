from __future__ import annotations

import re
import urllib.parse

import pandas as pd
from bs4 import BeautifulSoup
from weaviate.util import generate_uuid5

from include.tasks.extract.utils.html_utils import fetch_page_content, get_internal_links


def extract_provider_docs() -> list[pd.DataFrame]:
    exclude_docs = ["_api", "_modules", "_sources", "changelog.html", "genindex.html", "py-modindex.html", "#"]
    base_url = "https://astronomer-providers.readthedocs.io/en/stable/"

    all_links = get_internal_links(base_url, exclude_docs)

    docs_url_parts = urllib.parse.urlsplit(base_url)
    docs_url_base = f"{docs_url_parts.scheme}://{docs_url_parts.netloc}"

    # make sure we didn't accidentally pickup any unrelated links in recursion
    # get rid of older versions of the docs, only care about "stable" version docs

    old_version_doc_pattern = r"/(\d+\.)*\d+/"

    def is_doc_link_invalid(link):
        return (docs_url_base not in link) or re.search(old_version_doc_pattern, link) or "/latest/" in link

    invalid_doc_links = {link if is_doc_link_invalid(link) else "" for link in all_links}
    docs_links = all_links - invalid_doc_links

    df = pd.DataFrame(docs_links, columns=["docLink"])

    df["html_content"] = df["docLink"].apply(fetch_page_content)

    df["content"] = df["html_content"].apply(
        lambda x: str(BeautifulSoup(x, "html.parser").find(class_="body", role="main"))
    )
    df["content"] = df["content"].apply(lambda x: re.sub("¶", "", x))

    df["sha"] = df["content"].apply(generate_uuid5)
    df["docSource"] = "astronomer-providers"
    df.reset_index(drop=True, inplace=True)

    # column order matters for uuid generation
    df = df[["docSource", "sha", "content", "docLink"]]
    return [df]
