from __future__ import annotations

from textwrap import dedent

import pandas as pd
import requests
from html2text import html2text

modules_url = "https://api.astronomer.io/registryV2/v1alpha1/organizations/public/modules?limit=1000"
modules_link_template = "https://registry.astronomer.io/providers/{providerName}/versions/{version}/modules/{_name}"
module_info_url_template = "https://api.astronomer.io/registryV2/v1alpha1/organizations/public/providers/{provider_name}/versions/latest/modules/{module_name}"

dags_url = "https://api.astronomer.io/registryV2/v1alpha1/organizations/public/dags?limit=1000"
dags_link_template = "https://registry.astronomer.io/dags/{_name}/versions/{version}"


def get_individual_module_detail(provider_name, module_name):
    data = requests.get(module_info_url_template.format(provider_name=provider_name, module_name=module_name)).json()
    import_path = data["importPath"]

    module_name = data["name"]
    version = data["version"]
    provider_name = data["providerName"]
    description = html2text(data["description"]).strip() if data["description"] else "No Description"
    description = description.replace("\n", " ")
    parameters = data["parameters"]

    param_details = []
    param_usage = []

    for param in parameters:
        param_name = param["name"]
        param_type = param.get("type", "UNKNOWN")
        if param_type == "UNKNOWN" and "typeDef" in param and "rawAnnotation" in param["typeDef"]:
            param_type = param["typeDef"]["rawAnnotation"]
        required = "(REQUIRED) " if param["required"] else ""
        param_details.append(
            f"{param_name} ({param_type}): {required}{param.get('description', 'No Param Description')}"
        )
        param_usage.append(f"\t{param_name}=MY_{param_name.upper()}")

    param_details_str = "\n\t".join(param_details)
    param_usage_str = ",\n\t".join(param_usage)

    # Format the final string
    module_info = dedent(
        f"""
    Module Name: {module_name}
    Version: {version}
    Provider Name: {provider_name}
    Import Statement: `from {import_path} import {module_name}`
    Module Description: {description}

    Parameters:
        {param_details_str}

    Usage Example:
        f = AsyncKubernetesHook(
            {param_usage_str}
        )"""
    )

    return module_info


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

    df["content"] = df.apply(lambda x: pd.Series(get_individual_module_detail(x.providerName, x["name"])), axis=1)

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
