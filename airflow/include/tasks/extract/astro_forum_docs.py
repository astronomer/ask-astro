from __future__ import annotations

import logging
from datetime import datetime

import pandas as pd
import pytz
import requests
from bs4 import BeautifulSoup
from weaviate.util import generate_uuid5

cutoff_date = datetime(2022, 1, 1, tzinfo=pytz.UTC)

logger = logging.getLogger("airflow.task")


def get_questions_urls(html_content: str) -> list[str]:
    soup = BeautifulSoup(html_content, "html.parser")
    return [a_tag.attrs.get("href") for a_tag in soup.findAll("a", class_="title raw-link raw-topic-link")]


def get_publish_date(html_content) -> datetime:
    soup = BeautifulSoup(html_content, "html.parser")
    # TODO: use og:article:tag for tag filter
    publish_date = soup.find("meta", property="article:published_time")["content"]
    publish_date = datetime.fromisoformat(publish_date)
    return publish_date


def filter_cutoff_questions(questions_urls: list[str]) -> list[str]:
    return [
        question_url
        for question_url in questions_urls
        if get_publish_date(requests.get(question_url).content) >= cutoff_date
    ]


def get_cutoff_questions(forum_url: str) -> set[str]:
    """
    Retrieves a set of valid question URLs from a forum page.

    Parameters:
    - forum_url (str): The URL of the forum.
    """
    page_number = 0
    base_url = f"{forum_url}?page="
    all_valid_url = []
    while True:
        page_url = f"{base_url}{page_number}"
        logger.info(page_url)
        page_number = page_number + 1
        html_content = requests.get(page_url).content
        questions_urls = get_questions_urls(html_content)
        if not questions_urls:  # reached at the end of page
            return set(all_valid_url)
        filter_questions_urls = filter_cutoff_questions(questions_urls)
        all_valid_url.extend(filter_questions_urls)


def truncate_tokens(text: str, encoding_name: str, max_length: int = 8192) -> str:
    """
    Truncates a text string based on the maximum number of tokens.

    Parameters:
    - string (str): The input text string to be truncated.
    - encoding_name (str): The name of the encoding model.
    - max_length (int): The maximum number of tokens allowed. Default is 8192.
    """
    import tiktoken

    try:
        encoding = tiktoken.encoding_for_model(encoding_name)
    except ValueError as e:
        raise ValueError(f"Invalid encoding_name: {e}")

    encoded_string = encoding.encode(text)
    num_tokens = len(encoded_string)

    if num_tokens > max_length:
        text = encoding.decode(encoded_string[:max_length])

    return text


def clean_content(row_content) -> str | None:
    soup = BeautifulSoup(row_content, "html.parser").find("body")

    if soup is None:
        return
    # Remove script and style tags
    for script_or_style in soup(["script", "style"]):
        script_or_style.extract()

    # Get text and handle whitespaces
    text = " ".join(soup.stripped_strings)
    # Need to truncate because in some cases the token size
    # exceeding the max token size. Better solution can be get summary and ingest it.
    return truncate_tokens(text, "gpt-3.5-turbo", 7692)


def fetch_url_content(url) -> str | None:
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an HTTPError for bad responses
        return response.content
    except requests.RequestException:
        logger.info("Error fetching content for %s: %s", url, url)
        return None


def process_url(url: str, doc_source: str = "") -> dict | None:
    """
    Process a URL by fetching its content, cleaning it, and generating a unique identifier (SHA) based on the cleaned content.
    param url (str): The URL to be processed.
    """
    content = fetch_url_content(url)
    if content is not None:
        cleaned_content = clean_content(content)
        sha = generate_uuid5(cleaned_content)
        return {"docSource": doc_source, "sha": sha, "content": cleaned_content, "docLink": url}


def url_to_df(urls: set[str], doc_source: str = "") -> pd.DataFrame:
    """
    Create a DataFrame from a list of URLs by processing each URL and organizing the results.
    param urls (list): A list of URLs to be processed.
    """
    df_data = [process_url(url, doc_source) for url in urls]
    df_data = [entry for entry in df_data if entry is not None]  # Remove failed entries
    df = pd.DataFrame(df_data)
    df = df[["docSource", "sha", "content", "docLink"]]  # Reorder columns if needed
    return df


def get_forum_df() -> list[pd.DataFrame]:
    questions_links = get_cutoff_questions("https://forum.astronomer.io/latest")
    logger.info(questions_links)
    df = url_to_df(questions_links, "astro-forum")
    return [df]
