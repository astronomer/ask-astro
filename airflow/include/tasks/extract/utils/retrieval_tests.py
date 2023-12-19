import asyncio
import json

import aiohttp
from langchain.chat_models import AzureChatOpenAI
from langchain.retrievers import MultiQueryRetriever
from langchain.vectorstores import Weaviate as WeaviateVectorStore
from weaviate.client import Client as WeaviateClient

from airflow.providers.google.suite.hooks.drive import GoogleDriveHook


async def get_answer(askastro_endpoint_url: str, request_payload: dict) -> str:
    """
    This function posts a question to the Ask Astro endpoint asynchronously and returns an answer.

    :param askastro_endpoint_url:
    :param request_payload:
    """
    async with aiohttp.ClientSession() as session:
        async with session.post(url=askastro_endpoint_url + "/requests", json=request_payload) as response:
            assert response.status == 200

            json_response = await response.json()
            request_id = json_response.get("request_uuid")

            assert request_id

        while True:
            async with session.get(url=askastro_endpoint_url + f"/requests/{request_id}") as response:
                assert response.status == 200

                json_response = await response.json()
                if json_response.get("response"):
                    return json_response
                else:
                    await asyncio.sleep(1)


def generate_answer(
    askastro_endpoint_url: str, question: str, langchain_org_id: str, langchain_project_id: str
) -> (str, str, str):
    """
    This function uses Ask Astro frontend to answer questions.

    :param askastro_endpoint_url: HTTP url for the running ask astro service
    :param question: A question.
    :param langchain_org_id: The organization ID for generating langsmith links
    :param langchain_project_id: The project ID for generating langsmith links
    :return: A list of strings for answers and references
    """

    langsmith_link_template = "https://smith.langchain.com/o/{org}/projects/p/{project}?peek={run_id}"

    try:
        response = asyncio.run(
            get_answer(askastro_endpoint_url=askastro_endpoint_url, request_payload={"prompt": question})
        )

        assert response.get("status") == "complete"

        answer = response.get("response")
        references = {source["name"] for source in response.get("sources")}
        references = "\n".join(references)
        langsmith_link = langsmith_link_template.format(
            org=langchain_org_id, project=langchain_project_id, run_id=response.get("langchain_run_id")
        )

    except Exception as e:
        print(e)
        answer = ""
        references = ""
        langsmith_link = ""

    return (answer, references, langsmith_link)


def weaviate_search(weaviate_client: WeaviateClient, question: str, class_name: str) -> str:
    """
    This function uses Weaviate's
    [Similarity Search](https://weaviate.io/developers/weaviate/search/similarity)
    and returns a pandas series of reference documents.  This is a one-shot retrieval unlike
    Ask Astro frontend which uses LangChain's MultiQueryRetrieval.

    :param weaviate_client: An instantiated weaviate client to use for the search.
    :param question: A question.
    :param class_name: The name of the class to search.
    """

    try:
        results = (
            weaviate_client.query.get(class_name=class_name, properties=["docLink"])
            .with_near_text(
                {
                    "concepts": question,
                }
            )
            .with_limit(5)
            .with_additional(["id", "certainty"])
            .do()["data"]["Get"][class_name]
        )

        references = "\n".join(
            [f"{result['docLink']} [{round(result['_additional']['certainty'], 3)}]" for result in results]
        )

    except Exception as e:
        print(e)
        references = []

    return references


def weaviate_search_multiquery_retriever(
    weaviate_client: WeaviateClient, question: str, class_name: str, azure_endpoint: str
) -> str:
    """
    This function uses LangChain's
    [MultiQueryRetriever](https://api.python.langchain.com/en/latest/retrievers/langchain.retrievers.multi_query.MultiQueryRetriever.html)
    to retrieve a set of documents based on a question.

    :param weaviate_client: An instantiated weaviate client to use for the search.
    :param question: A question.
    :param class_name: The name of the class to search.
    :param azure_gpt35_endpoint: Azure OpenAI endpoint to use for multi-query retrieval
    """

    docsearch = WeaviateVectorStore(
        client=weaviate_client,
        index_name=class_name,
        text_key="content",
        attributes=["docLink"],
    )

    retriever = MultiQueryRetriever.from_llm(
        llm=AzureChatOpenAI(
            **json.loads(azure_endpoint),
            deployment_name="gpt-35-turbo",
            temperature="0.0",
        ),
        retriever=docsearch.as_retriever(),
    )

    try:
        results = retriever.get_relevant_documents(query=question)

        references = {result.metadata["docLink"] for result in results}
        references = "\n".join(references)

    except Exception as e:
        print(e)
        references = []

    return references


def get_or_create_drive_folder(gd_hook: GoogleDriveHook, folder_name: str, parent_id: str | None) -> str:
    """
    Creates a google drive folder if it does not exist.

    :param gd_hook: An Google drive hook
    :param folder_name: Name of the folder to create if it does not exist
    :param parent_id: File ID for the parent or None for root.
    """

    current_file_list = gd_hook.get_conn().files().list().execute().get("files")

    existing_folder_ids = [
        file["id"]
        for file in current_file_list
        if file["name"] == folder_name and file["mimeType"] == "application/vnd.google-apps.folder"
    ]

    if len(existing_folder_ids) > 1:
        raise ValueError("More than one folder found.")
    elif len(existing_folder_ids) == 1:
        return existing_folder_ids[0]
    folder = (
        gd_hook.get_conn()
        .files()
        .create(
            body={"name": folder_name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent_id]},
            fields="id",
        )
        .execute()
    )
    return folder["id"]
