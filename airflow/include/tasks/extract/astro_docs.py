from __future__ import annotations

import re

import pandas as pd
from bs4 import BeautifulSoup
from weaviate.util import generate_uuid5

from include.tasks.extract.utils.html_utils import fetch_page_content, get_internal_links

base_url = "https://docs.astronomer.io/"


def process_astro_doc_page_content(page_content: str) -> str:
    soup = BeautifulSoup(page_content, "html.parser")

    # Find the main article container
    main_container = soup.find("main", class_="docMainContainer_TBSr")

    content_of_interest = main_container if main_container else soup
    for nav_tag in content_of_interest.find_all("nav"):
        nav_tag.decompose()

    for script_or_style in content_of_interest.find_all(["script", "style", "button", "img", "svg"]):
        script_or_style.decompose()

    feedback_widget = content_of_interest.find("div", id="feedbackWidget")
    if feedback_widget:
        feedback_widget.decompose()

    newsletter_form = content_of_interest.find("form", id="newsletterForm")
    if newsletter_form:
        newsletter_form.decompose()

    sidebar = content_of_interest.find("ul", class_=lambda value: value and "table-of-contents" in value)
    if sidebar:
        sidebar.decompose()

    footers = content_of_interest.find_all("footer")
    for footer in footers:
        footer.decompose()

    # The actual article in almost all pages of Astro Docs website is in the following HTML container
    container_div = content_of_interest.find("div", class_=lambda value: value and "container" in value)

    if container_div:
        row_div = container_div.find("div", class_="row")

        if row_div:
            col_div = row_div.find("div", class_=lambda value: value and "col" in value)

            if col_div:
                content_of_interest = str(col_div)

    return str(content_of_interest).strip()


def extract_astro_docs(base_url: str = base_url) -> list[pd.DataFrame]:
    """
    Extract documentation pages from docs.astronomer.io and its subdomains.

    :return: A list of pandas dataframes with extracted data.
    """
    all_links = get_internal_links(base_url, exclude_literal=["learn/tags"])

    # for software references, we only want latest docs, ones with version number (old) is removed
    old_version_doc_pattern = r"^https://docs\.astronomer\.io/software/\d+\.\d+/.+$"
    # remove duplicate xml files, we only want html pages
    non_doc_links = {
        link if link.endswith("xml") or re.match(old_version_doc_pattern, link) else "" for link in all_links
    }
    docs_links = all_links - non_doc_links

    df = pd.DataFrame(docs_links, columns=["docLink"])

    df["html_content"] = df["docLink"].apply(lambda url: fetch_page_content(url))

    # Only keep the main article content
    df["content"] = df["html_content"].apply(process_astro_doc_page_content)

    df["sha"] = df["content"].apply(generate_uuid5)
    df["docSource"] = "astro docs"
    df.reset_index(drop=True, inplace=True)

    df = df[["docSource", "sha", "content", "docLink"]]
    return [df]


extract_astro_docs()
