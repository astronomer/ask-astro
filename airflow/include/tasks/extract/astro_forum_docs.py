import logging
from datetime import datetime

import pandas as pd
import pytz
import requests
from bs4 import BeautifulSoup
from weaviate.util import generate_uuid5

cutoff_date = datetime(2022, 1, 1, tzinfo=pytz.UTC)

logger = logging.getLogger("airflow.task")


def get_questions_urls(html_content: str):
    soup = BeautifulSoup(html_content, "html.parser")
    url_list = []
    for a_tag in soup.findAll("a", class_="title raw-link raw-topic-link"):
        url_list.append(a_tag.attrs.get("href"))
    return url_list


def get_publish_date(html_content):
    soup = BeautifulSoup(html_content, "html.parser")
    # TODO: use og:article:tag for tag filter
    publish_date = soup.find("meta", property="article:published_time")["content"]
    publish_date = datetime.fromisoformat(publish_date)
    return publish_date


def filter_cutoff_questions(questions_urls):
    valid_urls = []
    for question_url in questions_urls:
        html_content = requests.get(question_url).content
        if get_publish_date(html_content) > cutoff_date:
            return valid_urls
        valid_urls.append(question_url)
    return valid_urls


def get_cutoff_questions(forum_url):
    page_number = 0
    base_url = f"{forum_url}?page="
    all_valid_url = []
    while True:
        page_url = f"{base_url}{page_number}"
        page_number = page_number + 1
        html_content = requests.get(page_url).content
        questions_urls = get_questions_urls(html_content)
        if len(questions_urls) == 0:  # reached at the end of page
            return all_valid_url
        filter_questions_urls = filter_cutoff_questions(questions_urls)
        all_valid_url.extend(questions_urls)
        if len(questions_urls) > len(filter_questions_urls):  # reached cutoff date
            return all_valid_url


def clean_content(row_content):
    soup = BeautifulSoup(row_content, "html.parser").find("body")

    if soup is None:
        return
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
    except requests.RequestException:
        logger.info("Error fetching content for %s: %s", url, url)
        return None


def process_url(url, doc_source=""):
    """
    Process a URL by fetching its content, cleaning it, and generating a unique identifier (SHA) based on the cleaned content.
    param url (str): The URL to be processed.
    """
    content = fetch_url_content(url)
    if content is not None:
        cleaned_content = clean_content(content)
        sha = generate_uuid5(cleaned_content)
        return {"docSource": doc_source, "sha": sha, "content": cleaned_content, "docLink": url}
    else:
        return None


def url_to_df(urls, doc_source=""):
    """
    Create a DataFrame from a list of URLs by processing each URL and organizing the results.
    param urls (list): A list of URLs to be processed.
    """
    df_data = [process_url(url, doc_source) for url in urls]
    df_data = [entry for entry in df_data if entry is not None]  # Remove failed entries
    df = pd.DataFrame(df_data)
    df = df[["docSource", "sha", "content", "docLink"]]  # Reorder columns if needed
    return df


def get_forum_df():
    questions_link = get_cutoff_questions("https://forum.astronomer.io/latest")
    df = url_to_df(questions_link, "astro-forum")
    return [df]
