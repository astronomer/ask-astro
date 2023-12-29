from __future__ import annotations

import logging
from datetime import datetime

import pandas as pd
import pytz
import requests
from bs4 import BeautifulSoup

from include.tasks.extract.utils.html_utils import fetch_page_content, urls_to_dataframe

cutoff_date = datetime(2022, 1, 1, tzinfo=pytz.UTC)

logger = logging.getLogger("airflow.task")


def get_questions_urls(html_content: str) -> list[str]:
    """
    Extracts question URLs from HTML content using BeautifulSoup.

    param html_content (str): The HTML content of a web page.
    """
    soup = BeautifulSoup(html_content, "html.parser")
    return [a_tag.attrs.get("href") for a_tag in soup.findAll("a", class_="title raw-link raw-topic-link")]


def get_publish_date(html_content) -> datetime:
    """
    Extracts and parses the publish date from HTML content

    html_content (str): The HTML content of a web page.
    """
    soup = BeautifulSoup(html_content, "html.parser")
    # TODO: use og:article:tag for tag filter
    publish_date = soup.find("meta", property="article:published_time")["content"]
    publish_date = datetime.fromisoformat(publish_date)
    return publish_date


def filter_cutoff_questions(questions_urls: list[str]) -> list[str]:
    """
    Filters a list of question URLs based on the publish dates.

    param questions_urls (list[str]): A list of question URLs.
    """
    filter_questions_urls = []

    for question_url in questions_urls:
        try:
            html_content = requests.get(question_url).content
        except requests.RequestException as e:
            logger.error(f"Error fetching content for {question_url}: {e}")
            continue  # Move on to the next iteration

        soup = BeautifulSoup(html_content, "html.parser")
        reply = soup.find("div", itemprop="comment")
        if not reply:
            logger.info(f"No response, Ignoring {question_url}")
            continue

        if get_publish_date(html_content) >= cutoff_date:
            filter_questions_urls.append(question_url)

    return filter_questions_urls


def get_cutoff_questions(forum_url: str) -> set[str]:
    """
    Retrieves a set of valid question URLs from a forum page.

    param forum_url (str): The URL of the forum.
    """
    page_number = 0
    base_url = f"{forum_url}?page="
    all_valid_url = []
    while True:
        page_url = f"{base_url}{page_number}"
        logger.info(page_url)
        page_number = page_number + 1
        html_content = fetch_page_content(page_url)
        questions_urls = get_questions_urls(html_content)
        if not questions_urls:  # reached at the end of page
            return set(all_valid_url)
        filter_questions_urls = filter_cutoff_questions(questions_urls)
        all_valid_url.extend(filter_questions_urls)


def get_forum_df() -> list[pd.DataFrame]:
    """
    Retrieves question links from a forum, converts them into a DataFrame, and returns a list containing the DataFrame.
    """
    questions_links = get_cutoff_questions("https://forum.astronomer.io/latest")
    logger.info(questions_links)
    df = urls_to_dataframe(questions_links, "astro-forum")
    return [df]
