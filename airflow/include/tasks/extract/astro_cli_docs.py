from __future__ import annotations

import re

import pandas as pd
import requests
from bs4 import BeautifulSoup
from weaviate.util import generate_uuid5


def extract_astro_cli_docs() -> list[pd.DataFrame]:
    astronomer_base_url = "https://docs.astronomer.io"
    astro_cli_overview_endpoint = "/astro/cli/overview"

    response = requests.get(f"{astronomer_base_url}/{astro_cli_overview_endpoint}")
    soup = BeautifulSoup(response.text, "lxml")
    astro_cli_links = {
        f"{astronomer_base_url}{link.get('href')}"
        for link in soup.find_all("a")
        if link.get("href").startswith("/astro/cli")
    }

    df = pd.DataFrame(astro_cli_links, columns=["docLink"])
    df["html_content"] = df["docLink"].apply(lambda x: requests.get(x).content)

    df["content"] = df["html_content"].apply(lambda x: str(BeautifulSoup(x, "html.parser").find("body")))
    df["content"] = df["content"].apply(lambda x: re.sub("¶", "", x))

    df["sha"] = df["content"].apply(generate_uuid5)
    df["docSource"] = "apache/airflow/docs"
    df.reset_index(drop=True, inplace=True)

    # column order matters for uuid generation
    df = df[["docSource", "sha", "content", "docLink"]]

    return [df]
