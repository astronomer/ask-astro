from __future__ import annotations

import datetime
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup
from weaviate.util import generate_uuid5

blog_format = "# {title}\n\n## {content}"

base_url = "https://www.astronomer.io"
page_url = base_url + "/blog/{page}/#archive"


def extract_astro_blogs(blog_cutoff_date: datetime.date) -> list[pd.DataFrame]:
    """
    This task downloads Blogs from the Astronomer website and returns a list of pandas dataframes. Return
    type is a list in order to map to upstream dynamic tasks.

    param blog_cutoff_date: Blog posts dated before this date will not be ingested.
    type blog_cutoff_date: datetime

    The returned data includes the following fields:
    'docSource': 'astro blog'
    'docLink': URL for the blog post
    'content': Markdown encoded content of the blog.
    'sha': A UUID from the other fields
    """

    headers = {}
    links = []
    page = 1

    response = requests.get(page_url.format(page=page), headers=headers)
    while response.ok:
        soup = BeautifulSoup(response.text, "lxml")
        for card in soup.find_all(class_="post-card__meta"):
            blog_date = datetime.datetime.strptime(card.find("time")["datetime"], "%Y-%m-%dT%H:%M:%S.%fZ")
            if blog_date.date() >= blog_cutoff_date:
                url = urljoin(base_url, card.find("a", href=True)["href"])
                response = requests.head(url, allow_redirects=True)
                if response.ok:
                    links.append(
                        {
                            "docLink": response.url,
                            "title": card.find(class_="title").get_text(),
                        }
                    )

        page = page + 1
        response = requests.get(page_url.format(page=page), headers=headers)

    df = pd.DataFrame(links)
    df.drop_duplicates(inplace=True)

    df["content"] = df["docLink"].apply(lambda x: requests.get(x).content)
    df["content"] = df["content"].apply(lambda x: BeautifulSoup(x, "lxml").find(class_="prose").get_text())
    df["content"] = df.apply(lambda x: blog_format.format(title=x.title, content=x.content), axis=1)

    df["sha"] = df["content"].apply(generate_uuid5)
    df["docSource"] = "astro blog"
    df.reset_index(drop=True, inplace=True)

    # column order matters for uuid generation
    df = df[["docSource", "sha", "content", "docLink"]]

    return [df]
