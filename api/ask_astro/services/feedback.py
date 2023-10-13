"Handles app mention events from Slack"
import asyncio
from logging import getLogger
from typing import Any

from ask_astro.clients.firestore import firestore_client
from ask_astro.clients.langsmith_ import langsmith_client
from ask_astro.config import FirestoreCollections

logger = getLogger(__name__)


async def submit_feedback(request_id: str, correct: bool, source_info: dict[str, Any] | None):
    """
    Submits feedback for a request. Writes to firestore and langsmith.
    """
    logger.info("Submitting feedback for request %s: %s", request_id, correct)
    # first, get the request from the database
    request = await firestore_client.collection(FirestoreCollections.requests).document(request_id).get()

    if not request.exists:
        raise ValueError(f"Request {request_id} does not exist")

    langchain_run_id = request.to_dict().get("langchain_run_id")
    if not langchain_run_id:
        raise ValueError(f"Request {request_id} does not have a langchain run id")

    # update the db and langsmith
    async with asyncio.TaskGroup() as tg:
        # update just the score field
        tg.create_task(
            firestore_client.collection(FirestoreCollections.requests)
            .document(request_id)
            .update({"score": 1 if correct else 0})
        )

        tg.create_task(
            asyncio.to_thread(
                lambda: langsmith_client.create_feedback(
                    key="correctness",
                    run_id=langchain_run_id,
                    score=1 if correct else 0,
                    source_info=source_info,
                )
            )
        )
