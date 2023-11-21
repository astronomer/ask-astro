from __future__ import annotations

import logging
import urllib.parse
from time import sleep

import requests
from bs4 import BeautifulSoup


def get_links(url: str, exclude_docs: list) -> set:
    """
    Given a HTML url this function scrapes the page for any HTML links (<a> tags) and returns a set of links which:
    a) starts with the same base (ie. scheme + netloc)
    b) is a relative link from the currently read page
    Relative links are converted to absolute links.Note that the absolute link may not be unique due to redirects.

    :param url: The url to scrape for links.
    :param exclude_docs: A list of strings to exclude from the returned links.
    """
    response = requests.get(url)
    data = response.text
    soup = BeautifulSoup(data, "lxml")

    url_parts = urllib.parse.urlsplit(url)
    url_base = f"{url_parts.scheme}://{url_parts.netloc}"

    links = set()
    for link in soup.find_all("a"):
        link_url = link.get("href")

        if link_url.endswith(".html"):
            if link_url.startswith(url_base) and not any(substring in link_url for substring in exclude_docs):
                links.add(link_url)
            elif not link_url.startswith("http"):
                absolute_url = urllib.parse.urljoin(url, link_url)
                if not any(substring in absolute_url for substring in exclude_docs):
                    links.add(absolute_url)

    return links


def get_all_links(url: str, all_links: set, exclude_docs: list, retry_count: int = 0, max_retries: int = 5):
    """
    Recursive function to find all sub-pages of a webpage.

    :param url: The url to scrape for links.
    :param all_links: A set of all links found so far.
    :param exclude_docs: A list of strings to exclude from the returned links.
    :param retry_count: Current retry attempt.
    :param max_retries: Maximum number of retries allowed for a single URL.
    """
    try:
        links = get_links(url=url, exclude_docs=exclude_docs)
        for link in links:
            # check if the linked page actually exists and get the redirect which is hopefully unique

            response = requests.head(link, allow_redirects=True)
            if response.ok:
                redirect_url = response.url
                if redirect_url not in all_links:
                    logging.info(redirect_url)
                    all_links.add(redirect_url)
                    get_all_links(url=redirect_url, all_links=all_links, exclude_docs=exclude_docs)
    except requests.exceptions.ConnectionError as ce:
        if retry_count < max_retries:
            logging.warning(f"Connection error for {url}: {ce}. Retrying ({retry_count + 1}/{max_retries})")
            sleep(2**retry_count)  # Exponential backoff
            get_all_links(
                url=url,
                all_links=all_links,
                exclude_docs=exclude_docs,
                retry_count=retry_count + 1,
                max_retries=max_retries,
            )
        else:
            logging.warning(f"Max retries reached for {url}. Skipping this URL.")
