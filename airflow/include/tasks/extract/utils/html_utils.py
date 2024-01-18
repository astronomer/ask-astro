from __future__ import annotations

import logging
import time
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from weaviate.util import generate_uuid5

logger = logging.getLogger("airflow.task")


internal_urls = set()


def is_valid_url(url: str) -> bool:
    """
    Check if the given URL is valid by ensuring it has a valid scheme and network location.

    param url (str): The URL to be validated.
    """
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)


def fetch_page_content(url: str) -> str:
    """
    Fetch the content of a html page

    param url: The url of a page
    """
    try:
        response = requests.get(url, headers={"User-agent": "Ask Astro"})
        response.raise_for_status()  # Raise an HTTPError for bad responses
        return response.content
    except requests.RequestException:
        logger.info("Error fetching content for %s: %s", url, url)
        return ""


def is_excluded_url(url: str, exclude_literal: list[str]) -> bool:
    """
    Check if url can be excluded

    param url: URL which we want to check
    param exclude_literal: Exclude URL that contain pattern from this list
    """
    url_path = urlparse(url).path
    return any(literal in url_path for literal in exclude_literal)


def clean_tags(text_content: str, tags: list[str] | None = None) -> str | None:
    """
    Clean the HTML content by removing script and style tags, collapsing whitespaces, and extracting text.

    param text_content (str): The HTML content to be cleaned.
    param tags: List of html tag want to clean
    """
    if tags is None:
        tags = ["script", "style"]
    soup = BeautifulSoup(text_content, "html.parser").find("body")

    if soup is None:
        return
    # Remove script and style tags
    for script_or_style in soup(tags):
        script_or_style.extract()

    # Get text and handle whitespaces
    text = " ".join(soup.stripped_strings)

    return text


def truncate_tokens(text: str, encoding_name: str = "gpt-3.5-turbo", max_length: int = 8192) -> str | None:
    """
    Truncates a text string based on the maximum number of tokens.

    param string (str): The input text string to be truncated.
    param encoding_name (str): The name of the encoding model.
    param max_length (int): The maximum number of tokens allowed. Default is 8192.
    """
    import tiktoken

    try:
        encoding = tiktoken.encoding_for_model(encoding_name)
        encoded_string = encoding.encode(text, disallowed_special=())

        num_tokens = len(encoded_string)

        if num_tokens > max_length:
            text = encoding.decode(encoded_string[:max_length])
        return text
    except Exception as e:
        logger.info("Unable to encode text %s", text)
        logger.info(e)


def get_page_links(url: str, exclude_literal: list[str]) -> set[str]:
    """
    Extract all valid and internal links from the given URL.

    param url (str): The URL to extract links from.
    param exclude_docs (list): List of strings to exclude from the URL path.
    """
    urls = set()
    domain_name = urlparse(url).netloc
    soup = BeautifulSoup(requests.get(url).content, "html.parser")
    for a_tag in soup.findAll("a"):
        href = a_tag.attrs.get("href")
        if href == "" or href is None:
            continue
        href = urljoin(url, href)
        parsed_href = urlparse(href)
        href = parsed_href.scheme + "://" + parsed_href.netloc + parsed_href.path
        if (
            not is_valid_url(href)
            or href in internal_urls
            or domain_name not in href
            or is_excluded_url(href, exclude_literal)
        ):
            continue
        urls.add(href)
        logger.info(href)
        internal_urls.add(href)

    return urls


def get_internal_links(base_url: str, exclude_literal: list[str] | None = None) -> set[str]:
    """
    Extract the internal links of website

    param base_url: The base URL of site
    param exclude_literal: Exclude URL that contain pattern from this list
    """
    if exclude_literal is None:
        exclude_literal = []

    links = get_page_links(base_url, exclude_literal)

    for link in links:
        get_page_links(link, exclude_literal)

    return internal_urls


def process_url(url, doc_source="", clean_tag: bool = True, truncate_text: bool = True) -> dict | None:
    """
    Process a URL by fetching its content, cleaning it, and generating a unique identifier (SHA) based on the cleaned content.

    param url: The URL to be processed.
    param clean_tag: Clean HTML tags or not
    param truncate_text: Truncate text if text is bigger for given embedding model
    """
    html_text = fetch_page_content(url)
    if html_text:
        if clean_tag:
            html_text = clean_tags(html_text)
        if truncate_text:
            html_text = truncate_tokens(html_text)
        sha = generate_uuid5(html_text)
        return {"docSource": doc_source, "sha": sha, "content": html_text, "docLink": url}
    else:
        return None


def urls_to_dataframe(
    urls: set[str], doc_source: str = "", clean_tag: bool = True, truncate_text: bool = True
) -> pd.DataFrame:
    """
    Create a DataFrame from a list of URLs by processing each URL and organizing the results.

    param urls: A list of URLs to be processed.
    param doc_source: A document source
    param clean_tag: Clean HTML tags or not
    param truncate_text: Truncate text if text is bigger for given embedding model
    """
    content_list = []
    for url in urls:
        time.sleep(1)
        data = process_url(url, doc_source, clean_tag, truncate_text)
        if data:
            content_list.append(data)
    df = pd.DataFrame(content_list)
    df = df[["docSource", "sha", "content", "docLink"]]  # Reorder columns if needed
    return df
