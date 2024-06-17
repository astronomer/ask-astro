from __future__ import annotations

import logging
import time
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tenacity import RetryCallState, retry, retry_if_exception_type, stop_after_attempt, wait_exponential
from weaviate.util import generate_uuid5

logger = logging.getLogger("airflow.task")

attempted_urls = set()
internal_urls = set()
internal_page_hashset = set()


def is_valid_url(url: str) -> bool:
    """
    Check if the given URL is valid by ensuring it has a valid scheme and network location.

    param url (str): The URL to be validated.
    """
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)


def _fetch_page_content_retry_default_return(retry_state: RetryCallState) -> str:
    logger.info(
        "Error fetching content for %s. May be expected if making attempts to validate unknown URLs.",
        retry_state.args[0],
    )
    return ""


@retry(
    retry=retry_if_exception_type(requests.RequestException),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, max=10),
    retry_error_callback=_fetch_page_content_retry_default_return,
)
def fetch_page_content(url: str) -> str:
    """
    Fetch the content of a html page

    param url: The url of a page
    """
    response = requests.get(url, headers={"User-agent": "Ask Astro"})
    response.raise_for_status()  # Raise an HTTPError for bad responses
    return response.content


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
    Clean the HTML content by removing unrelated tags, collapsing whitespaces, and extracting text.

    param text_content (str): The HTML content to be cleaned.
    param tags: List of html tag want to clean
    """
    if tags is None:
        tags = ["script", "style", "button", "img", "svg"]
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


def get_page_links(
    url: str, current_page_content: bytes, exclude_literal: list[str], prefix_url: str | None = None
) -> None:
    """
    Recursively extract all valid and internal links from the given URL.
    Deduplicates any links with the exact same page content in the process.

    param url (str): The URL to extract links from.
    param current_page_content: Bytes of the content of the url passed in for hashing.
    param exclude_docs (list): List of strings to exclude from the URL path.
    param prefix_url (str | None): Ensure all urls scrapped begins with this prefix url. None for skipping this check.
    """
    domain_name = urlparse(url).netloc
    page_content_hash = generate_uuid5(current_page_content)
    internal_page_hashset.add(page_content_hash)
    soup = BeautifulSoup(current_page_content, "html.parser")
    for a_tag in soup.findAll("a"):
        href = a_tag.attrs.get("href")
        if href == "" or href is None:
            continue
        href = urljoin(url, href)
        parsed_href = urlparse(href)
        href = parsed_href.scheme + "://" + parsed_href.netloc + parsed_href.path
        if href in attempted_urls:
            continue
        attempted_urls.add(href)
        if (
            not is_valid_url(href)
            or not href.startswith("https")
            or href in internal_urls
            or domain_name not in href
            or is_excluded_url(href, exclude_literal)
            or (prefix_url and not href.startswith(prefix_url))
        ):
            continue
        new_page_content = fetch_page_content(href)
        if (not new_page_content) or generate_uuid5(new_page_content) in internal_page_hashset:
            continue
        logger.info(href)
        internal_urls.add(href)
        get_page_links(href, new_page_content, exclude_literal, prefix_url)


def get_internal_links(
    base_url: str, exclude_literal: list[str] | None = None, prefix_url: str | None = None
) -> set[str]:
    """
    Extract the internal links of website

    param base_url: The base URL of site
    param exclude_literal: Exclude URL that contain pattern from this list
    param prefix_url (str | None): Ensure all urls scrapped begins with this prefix url. None for skipping this check.
    """
    if exclude_literal is None:
        exclude_literal = []

    page_content = fetch_page_content(base_url)
    get_page_links(
        url=base_url, current_page_content=page_content, exclude_literal=exclude_literal, prefix_url=prefix_url
    )
    internal_urls.add(base_url)

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
