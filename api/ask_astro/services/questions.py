"""Handles app mention events from Slack"""
from __future__ import annotations

import asyncio
import time
from logging import getLogger

from ask_astro.clients.firestore import firestore_client
from ask_astro.config import FirestoreCollections
from ask_astro.models.request import AskAstroRequest, Source

logger = getLogger(__name__)


async def _update_firestore_request(request: AskAstroRequest) -> None:
    """
    Update the Firestore database with the given request.

    :param request: The AskAstroRequest object to update in Firestore.
    """
    await (
        firestore_client.collection(FirestoreCollections.requests)
        .document(str(request.uuid))
        .set(request.to_firestore())
    )


async def answer_question(request: AskAstroRequest) -> None:
    """
    Performs the actual question answering logic and updates the request object.

    :param request: The request to answer the question.
    """
    try:
        from langchain import callbacks

        from ask_astro.chains.answer_question import answer_question_chain

        # First, mark the request as in_progress and add it to the database
        request.status = "in_progress"
        await _update_firestore_request(request)

        # Run the question answering chain
        with callbacks.collect_runs() as cb:
            result = await asyncio.to_thread(
                lambda: answer_question_chain(
                    {
                        "question": request.prompt,
                        "chat_history": [],
                        "messages": request.messages,
                    },
                    metadata={"request_id": str(request.uuid)},
                )
            )
            request.langchain_run_id = cb.traced_runs[0].id

        logger.info("Question answering chain finished with result %s", result)

        # Update the request in the database
        request.status = "complete"
        request.response = result["answer"]
        request.response_received_at = int(time.time())
        request.sources = [
            Source(name=doc.metadata.get("docLink"), snippet=doc.page_content)
            for doc in result.get("source_documents", [])
            if doc.metadata.get("docLink", "").startswith("https://")
        ]

        await _update_firestore_request(request)

    except Exception as e:
        # If there's an error, mark the request as errored and add it to the database
        request.status = "error"
        request.response = "Sorry, something went wrong. Please try again later."
        await _update_firestore_request(request)

        # Propagate the error
        raise Exception("An error occurred during question answering.") from e
