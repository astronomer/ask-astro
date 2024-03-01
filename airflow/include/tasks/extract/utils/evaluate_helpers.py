from __future__ import annotations

import asyncio
import logging

import aiohttp
import backoff

from airflow.providers.google.suite.hooks.drive import GoogleDriveHook

logger = logging.getLogger("airflow.task")


@backoff.on_exception(backoff.expo, aiohttp.ClientError, max_tries=10)
async def get_answer(askastro_endpoint_url: str, request_payload: dict) -> str:
    """
    This function posts a question to the Ask Astro endpoint asynchronously and returns an answer.

    :param askastro_endpoint_url:
    :param request_payload:
    """
    async with aiohttp.ClientSession() as session:
        async with session.post(
            url=f"{askastro_endpoint_url}/requests", json=request_payload, raise_for_status=True
        ) as response:
            if response.status == 200:
                json_response = await response.json()
                request_id = json_response.get("request_uuid")

                if request_id:
                    response.close()
                else:
                    logger.info(f"Request not accepted: {json_response}")
                    raise aiohttp.ClientError("Retrying")
            else:
                logger.info(f"Could not connect to ask astro frontend: {response.reason}")
                raise aiohttp.ClientError("Retrying")

        while True:
            async with session.get(url=f"{askastro_endpoint_url}/requests/{request_id}") as response:
                if response.status == 200:
                    json_response = await response.json()
                    if json_response.get("response"):
                        response.close()
                        session.close()
                        return json_response
                    else:
                        await asyncio.sleep(1)
                else:
                    logger.info(f"Could not connect to ask astro frontend to get response: {response.reason}")
                    raise aiohttp.ClientError("Retrying")


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
        references = [source["name"] for source in response.get("sources")]
        references = "\n".join(references)
        langsmith_link = langsmith_link_template.format(
            org=langchain_org_id, project=langchain_project_id, run_id=response.get("langchain_run_id")
        )

    except Exception as e:
        logger.info(e)
        answer = ""
        references = ""
        langsmith_link = ""

    return (answer, references, langsmith_link)


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
