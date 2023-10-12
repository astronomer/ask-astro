"Handles app mention events from Slack"
import asyncio
import time
from logging import getLogger

from ask_astro.chains.answer_question import answer_question_chain
from ask_astro.clients.firestore import firestore_client
from ask_astro.config import FirestoreCollections
from ask_astro.models.request import AskAstroRequest, Source
from langchain import callbacks

logger = getLogger(__name__)


async def answer_question(request: AskAstroRequest):
    """
    Performs the actual question answering logic. Writes to the request object.
    """
    try:
        # first, mark the request as in_progress and add it to the database
        request.status = "in_progress"
        await firestore_client.collection(FirestoreCollections.requests).document(
            str(request.uuid)
        ).set(request.to_firestore())

        # then, run the question answering chain
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

        # update the request in the database
        request.status = "complete"
        request.response = result["answer"]
        request.response_received_at = int(time.time())
        request.sources = [
            Source(
                name=doc.metadata.get("docLink"),
                snippet=doc.page_content,
            )
            for doc in result.get("source_documents", [])
            if doc.metadata.get("docLink", "").startswith("https://")
        ]

        await firestore_client.collection(FirestoreCollections.requests).document(
            str(request.uuid)
        ).set(request.to_firestore())

    except Exception as e:
        # if there's an error, mark the request as errored and add it to the database
        request.status = "error"
        request.response = "Sorry, something went wrong. Please try again later."
        await firestore_client.collection(FirestoreCollections.requests).document(
            str(request.uuid)
        ).set(request.to_firestore())

        # then propagate the error
        raise e
