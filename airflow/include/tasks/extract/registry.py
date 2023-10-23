from __future__ import annotations

from textwrap import dedent

import pandas as pd
import requests
from html2text import html2text

modules_url = "https://api.astronomer.io/registryV2/v1alpha1/organizations/public/modules?limit=1000"
modules_link_template = "https://registry.astronomer.io/providers/{providerName}/versions/{version}/modules/{_name}"

dags_url = "https://api.astronomer.io/registryV2/v1alpha1/organizations/public/dags?limit=1000"
dags_link_template = "https://registry.astronomer.io/dags/{_name}/versions/{version}"

registry_cell_md_template = dedent(
    """
    # Registry
    ## Provider: {providerName}
    Version: {version}
    Module: {module}
    Module Description: {description}"""
)


def extract_astro_registry_cell_types() -> list[pd.DataFrame]:
    """
    This task downloads a list of Astro Cloud IDE cell types from the Astronomer registry and creates markdown
    documents in a list of pandas dataframes. Return type is a list in order to map to upstream dynamic tasks.

    Returned dataframe fields are:
    'docSource': 'registry_cell_types'
    'docLink': URL for the operator code in github
    'content': A small markdown document with cell type description
    'sha': A reference to the registry searchID.
    """

    json_data_class = "modules"
    response = requests.get(modules_url).json()
    total_count = response["totalCount"]
    data = response.get(json_data_class, [])

    while len(data) < total_count - 1:
        response = requests.get(f"{modules_url}&offset={len(data)+1}").json()
        data.extend(response.get(json_data_class, []))

    df = pd.DataFrame(data)
    df["docLink"] = df.apply(
        lambda x: modules_link_template.format(providerName=x.providerName, version=x.version, _name=x["name"]), axis=1
    )
    df.rename({"searchId": "sha"}, axis=1, inplace=True)
    df["docSource"] = "astronomer registry modules"

    df["description"] = df["description"].apply(lambda x: html2text(x) if x else "No Description")
    df["content"] = df.apply(
        lambda x: registry_cell_md_template.format(
            providerName=x.providerName, version=x.version, module=x["name"], description=x.description
        ),
        axis=1,
    )

    # column order matters for uuid generation
    df = df[["docSource", "sha", "content", "docLink"]]

    return [df]


def extract_astro_registry_dags() -> list[pd.DataFrame]:
    """
    This task downloads DAG code from the Astronomer registry and returns a list of pandas dataframes. Return
    type is a list in order to map to upstream dynamic tasks.

    Dataframe fields are:
    'docSource': 'registry_dags'
    'docLink': URL for the Registry location
    'content': The python DAG code
    'sha': A reference to the registry searchID.
    """

    json_data_class = "dags"
    response = requests.get(dags_url).json()
    total_count = response["totalCount"]
    data = response.get(json_data_class, [])

    while len(data) < total_count - 1:
        response = requests.get(f"{dags_url}&offset={len(data)+1}").json()
        data.extend(response.get(json_data_class, []))

    df = pd.DataFrame(data)

    df["docLink"] = df.apply(lambda x: dags_link_template.format(_name=x["name"], version=x.version), axis=1)

    df.rename({"searchId": "sha"}, axis=1, inplace=True)
    df["docSource"] = "astronomer registry dags"

    df["content"] = df["githubRawSourceUrl"].apply(lambda x: requests.get(x).text)

    # column order matters for uuid generation
    df = df[["docSource", "sha", "content", "docLink"]]

    return [df]
