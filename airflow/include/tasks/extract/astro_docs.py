from __future__ import annotations

import logging
from urllib.parse import urldefrag, urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup
from weaviate.util import generate_uuid5

base_url = "https://docs.astronomer.io/"


def fetch_page_content(url: str) -> str:
    """
    Fetches the content of a given URL.

    :param url: URL of the page to fetch.
    :return: HTML content of the page.
    """
    try:
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        if response.status_code == 200:
            return response.content
    except requests.RequestException as e:
        logging.error(f"Error fetching {url}: {e}")
    return ""


def extract_links(soup: BeautifulSoup, base_url: str) -> list[str]:
    """
    Extracts all valid links from a BeautifulSoup object.

    :param soup: BeautifulSoup object to extract links from.
    :param base_url: Base URL for relative links.
    :return: List of extracted URLs.
    """
    links = []
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if not href.startswith("http"):
            href = urljoin(base_url, href)
        if href.startswith(base_url):
            links.append(href)
    return links


def scrape_page(url: str, visited_urls: set, docs_data: list) -> None:
    """
    Recursively scrapes a webpage and its subpages.

    :param url: URL of the page to scrape.
    :param visited_urls: Set of URLs already visited.
    :param docs_data: List to append extracted data to.
    """
    if url in visited_urls or not url.startswith(base_url):
        return

    # Normalize URL by stripping off the fragment
    base_url_no_fragment, frag = urldefrag(url)

    # If the URL is the base URL plus a fragment, ignore it
    if base_url_no_fragment == base_url and frag:
        return

    visited_urls.add(url)

    logging.info(f"Scraping : {url}")

    page_content = fetch_page_content(url)
    if not page_content:
        return

    soup = BeautifulSoup(page_content, "lxml")
    content = soup.get_text(strip=True)
    sha = generate_uuid5(content)
    docs_data.append({"docSource": "astro docs", "sha": sha, "content": content, "docLink": url})
    # Recursively scrape linked pages
    for link in extract_links(soup, base_url):
        scrape_page(link, visited_urls, docs_data)


def extract_astro_docs(base_url: str = base_url) -> list[pd.DataFrame]:
    """
    Extract documentation pages from docs.astronomer.io and its subdomains.

    :return: A list of pandas dataframes with extracted data.
    """
    visited_urls = set()
    docs_data = []

    scrape_page(base_url, visited_urls, docs_data)

    df = pd.DataFrame(docs_data)
    df.drop_duplicates(subset="sha", inplace=True)
    df.reset_index(drop=True, inplace=True)

    return [df]
