"""Handles app mention events from Slack"""
from __future__ import annotations

import asyncio
import sqlite3
from logging import getLogger
from typing import Any

from ask_astro.clients.firestore import firestore_client
from ask_astro.clients.langsmith_ import langsmith_client
from ask_astro.config import FirestoreCollections

logger = getLogger(__name__)


class FeedbackSubmissionError(Exception):
    """Exception raised when there's an error submitting feedback."""


def _update_metrics_db(request_id: str, score: int) -> None:
    logger.info("Update metrics db")
    con = sqlite3.connect("temp.db")
    cur = con.cursor()
    cur.execute(
        f"""
        UPDATE request
        SET score={score}
        WHERE
            uuid = '{request_id}'
        """
    )
    con.commit()


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
        score = 1 if correct else 0
        async with asyncio.TaskGroup() as tg:
            # update just the score field
            tg.create_task(
                firestore_client.collection(FirestoreCollections.requests).document(request_id).update({"score": score})
            )

            tg.create_task(
                asyncio.to_thread(
                    lambda: langsmith_client.create_feedback(
                        key="correctness",
                        run_id=langchain_run_id,
                        score=score,
                        source_info=source_info,
                    )
                )
            )

        _update_metrics_db(request_id, score)
    except Exception as e:
        logger.error("Error occurred while processing feedback for request %s: %s", request_id, e)
        raise FeedbackSubmissionError("Failed to submit feedback for request %s.", request_id) from e
