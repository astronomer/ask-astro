"""Handles app mention events from Slack"""
from __future__ import annotations

import asyncio
import re
import time
from logging import getLogger

from tenacity import retry, retry_if_not_exception_type, stop_after_attempt, wait_exponential

from ask_astro.clients.firestore import firestore_client
from ask_astro.config import FirestoreCollections, PromptPreprocessingConfig
from ask_astro.models.request import AskAstroRequest, Source
from ask_astro.settings import SHOW_SERVICE_MAINTENANCE_BANNER

logger = getLogger(__name__)


class InvalidRequestPromptError(Exception):
    """Exception raised when the prompt string in the request object is invalid"""


class RequestDuringMaintenanceException(Exception):
    """Exception raised when a request is still somehow received on the backend when server is in maintenance"""


class QuestionAnsweringError(Exception):
    """Exception raised when an error occurs during question answering"""


UNCERTAIN_RESPONSE_PREFIX = "I cannot find documents that are directly helpful with your question, but I provided my best guess below. Please use caution as the answer below is more likely to contain incorrect information and your should always verify the answers with Astronomer support at www.astronomer.io/contact"


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


def _preprocess_request(request: AskAstroRequest) -> None:
    if SHOW_SERVICE_MAINTENANCE_BANNER:
        error_msg = "Ask Astro is currently undergoing maintenance and will be back shortly. We apologize for any inconvenience this may cause!"
        request.response = error_msg
        raise RequestDuringMaintenanceException(error_msg)
    if len(request.prompt) > PromptPreprocessingConfig.max_char:
        error_msg = "Question text is too long. Please try making a new thread and shortening your question."
        request.response = error_msg
        raise InvalidRequestPromptError(error_msg)
    if not request.prompt:
        error_msg = "Question text cannot be empty. Please try again with a different question."
        request.response = error_msg
        raise InvalidRequestPromptError(error_msg)
    # take the most recent 10 question and answers in the history
    if len(request.messages) > PromptPreprocessingConfig.max_chat_history_len:
        request.messages = request.messages[-10:]
    # Logic to change prompt for web apps
    if request.client is not None and request.client == "webapp":
        request.prompt = request.prompt.replace("Slack", "Markdown").replace(
            "Format links using this format: <URL|Text to display>. Examples: GOOD: <https://www.example.com|This message *is* a link>. BAD: [This message *is* a link](https://www.example.com).",
            "Format links using this format: [Text to display](URL). Examples: GOOD: [This message **is** a link](https://www.example.com). BAD: <https://www.example.com|This message **is** a link>.",
        )
    # parse out backslack escape character to prevent hybrid search erroring out with invalid syntax string
    request.prompt = re.sub(r"(?<!\\)\\(?!\\)", "", request.prompt)


@retry(
    retry=retry_if_not_exception_type(InvalidRequestPromptError),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, max=10),
    reraise=True,
)
async def answer_question(request: AskAstroRequest) -> None:
    """
    Performs the actual question answering logic and updates the request object.

    :param request: The request to answer the question.
    """
    try:
        from langchain import callbacks

        from ask_astro.chains.answer_question import slack_answer_question_chain, webapp_answer_question_chain

        # First, mark the request as in_progress and add it to the database
        request.status = "in_progress"
        await _update_firestore_request(request)

        # Preprocess request
        _preprocess_request(request=request)

        # Run the question answering chain
        with callbacks.collect_runs() as cb:
            if request.client == "slack":
                result = await asyncio.to_thread(
                    lambda: slack_answer_question_chain(
                        {
                            "question": request.prompt,
                            "chat_history": [],
                            "messages": request.messages,
                        },
                        metadata={"request_id": str(request.uuid), "client": str(request.client)},
                    )
                )
            else:
                result = await asyncio.to_thread(
                    lambda: webapp_answer_question_chain(
                        {
                            "question": request.prompt,
                            "chat_history": [],
                            "messages": request.messages,
                        },
                        metadata={"request_id": str(request.uuid), "client": str(request.client)},
                    )
                )
            request.langchain_run_id = cb.traced_runs[0].id

        logger.info("Question answering chain finished with result %s", result)

        # Update the request in the database
        request.status = "complete"
        request.response = result["answer"]
        request.response_received_at = int(time.time())
        docs = result.get("source_documents", [])
        request.sources = [
            Source(name=doc.metadata.get("docLink"), snippet=doc.page_content)
            for doc in docs
            if doc.metadata.get("docLink", "").startswith("https://")
        ]
        # somewhat hacky way to ensure the prefix is in front every time, as sometimes LLM forgets
        if "I cannot find documents that are directly helpful with your question" not in request.response and (
            len(request.sources) == 0
            or (
                # get max relevance_score field of every doc in docs variable list
                max([doc.metadata.get("relevance_score") for doc in docs]) < 0.8
                and not re.search(r"\[\d+\]", request.response)
            )
        ):
            request.response = UNCERTAIN_RESPONSE_PREFIX + "\n\n" + request.response

    except Exception as e:
        # If there's an error, mark the request as errored and add it to the database
        request.status = "error"
        if not isinstance(e, (InvalidRequestPromptError, RequestDuringMaintenanceException)):
            request.response = "Sorry, something went wrong. Please try again later."
            raise QuestionAnsweringError("An error occurred during question answering.") from e
        else:
            raise e

    finally:
        await _update_firestore_request(request)
