from __future__ import annotations

from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from weaviate.util import generate_uuid5

internal_urls = set()


def is_valid_url(url):
    """
    Check if the given URL is valid by ensuring it has a valid scheme and network location.

    param url (str): The URL to be validated.
    """
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)


def exclude_path(url, exclude_docs=None):
    """
    Check if the URL path contains any of the specified strings to be excluded.

    param url (str): The URL to check.
    param exclude_docs (list): List of strings to exclude from the URL path.
    """
    if exclude_docs is None:
        exclude_docs = []
    url_path = urlparse(url).path
    return any(docs in url_path for docs in exclude_docs)


def get_all_links(url, exclude_docs=None):
    """
    Extract all valid and internal links from the given URL.

    param url (str): The URL to extract links from.
    param exclude_docs (list): List of strings to exclude from the URL path.
    """
    if exclude_docs is None:
        exclude_docs = []

    urls = set()
    domain_name = urlparse(url).netloc

    with requests.Session() as session:
        response = session.get(url)
        soup = BeautifulSoup(response.content, "html.parser")

    for a_tag in soup.find_all("a"):
        href = a_tag.attrs.get("href", "")
        href = urljoin(url, href)
        if (
            not is_valid_url(href)
            or href in internal_urls
            or domain_name not in href
            or exclude_path(href, exclude_docs)
        ):
            continue

        urls.add(href)
        print(href)
        internal_urls.add(href)

    return urls


def extract_internal_url(url, exclude_docs=None):
    """
    Recursively extract all valid and internal links from the given URL.

    param url (str): The URL to start the extraction from.
    param exclude_docs (list): List of strings to exclude from the URL path.
    """
    if exclude_docs is None:
        exclude_docs = []

    links = get_all_links(url, exclude_docs)

    for link in links:
        extract_internal_url(link, exclude_docs)

    return internal_urls


def clean_content(text_content: str) -> str:
    """
    Clean the HTML content by removing script and style tags, collapsing whitespaces, and extracting text.

    param text_content (str): The HTML content to be cleaned.
    """
    soup = BeautifulSoup(text_content, "html.parser").find("body")

    # Remove script and style tags
    for script_or_style in soup(["script", "style"]):
        script_or_style.extract()

    # Get text and handle whitespaces
    text = " ".join(soup.stripped_strings)

    return text


def fetch_url_content(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an HTTPError for bad responses
        return response.content
    except requests.RequestException as e:
        print(f"Error fetching content for {url}: {e}")
        return None


def process_url(url):
    """
    Process a URL by fetching its content, cleaning it, and generating a unique identifier (SHA) based on the cleaned content.

    param url (str): The URL to be processed.
    """
    content = fetch_url_content(url)
    if content is not None:
        cleaned_content = clean_content(content)
        sha = generate_uuid5(cleaned_content)
        return {"docSource": "", "sha": sha, "content": cleaned_content, "docLink": url}
    else:
        return None


def url_to_df(urls):
    """
    Create a DataFrame from a list of URLs by processing each URL and organizing the results.

    param urls (list): A list of URLs to be processed.
    """
    df_data = [process_url(url) for url in urls]
    df_data = [entry for entry in df_data if entry is not None]  # Remove failed entries
    df = pd.DataFrame(df_data)
    df = df[["docSource", "sha", "content", "docLink"]]  # Reorder columns if needed
    return df
