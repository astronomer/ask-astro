"""Handles app mention events from Slack"""
from __future__ import annotations

import asyncio
from logging import getLogger
from typing import Any

from ask_astro.clients.firestore import firestore_client
from ask_astro.clients.langsmith_ import langsmith_client
from ask_astro.config import FirestoreCollections

logger = getLogger(__name__)


class FeedbackSubmissionError(Exception):
    """Exception raised when there's an error submitting feedback."""


async def submit_feedback(request_id: str, correct: bool, source_info: dict[str, Any] | None) -> None:
    """
    Submits feedback for a request. Writes to firestore and langsmith.

    :param request_id: The ID of the request for which feedback is provided.
    :param correct: Boolean indicating if the feedback is positive or not.
    :param source_info: Additional source information for the feedback.
    """
    logger.info("Submitting feedback for request %s: %s", request_id, correct)

    try:
        # first, get the request from the database
        request = await firestore_client.collection(FirestoreCollections.requests).document(request_id).get()

        if not request.exists:
            raise ValueError("Request %s does not exist", request_id)

        langchain_run_id = request.to_dict().get("langchain_run_id")
        if not langchain_run_id:
            raise ValueError("Request %s does not have a langchain run id", request_id)

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
    except Exception as e:
        logger.error("Error occurred while processing feedback for request %s: %s", request_id, str(e))
        raise FeedbackSubmissionError("Failed to submit feedback for request %s.", request_id) from e
