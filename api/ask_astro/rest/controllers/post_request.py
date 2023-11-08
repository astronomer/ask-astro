"""
Handles GET requests to the /ask/{question_id} endpoint.
"""
from __future__ import annotations

import uuid
from logging import getLogger

from langchain.schema import AIMessage, HumanMessage
from pydantic.v1 import BaseModel, Field
from sanic import Request, json
from sanic_ext import openapi

from ask_astro.clients.firestore import firestore_client
from ask_astro.config import FirestoreCollections
from ask_astro.models.request import AskAstroRequest

logger = getLogger(__name__)


class PostRequestResponse(BaseModel):
    """Model for the response when a POST request is made."""

    request_uuid: str = Field(..., description="The UUID of the request")


class PostRequestBody(BaseModel):
    """Model for the body of a POST request."""

    prompt: str = Field(..., description="The prompt for the request")
    from_request_uuid: str | None = Field(
        None,
        description="The UUID of the request to continue",
    )


@openapi.definition(
    response=PostRequestResponse.schema(),
    body=PostRequestBody.schema(),
)
async def on_post_request(request: Request) -> json:
    """
    Handles POST requests to the /requests endpoint.

    :param request: The Sanic request object.
    """
    try:
        from ask_astro.services.questions import answer_question

        if "prompt" not in request.json:
            return json({"error": "prompt is required"}, status=400)

        messages = []
        if "from_request_uuid" in request.json:
            from_request_uuid = request.json["from_request_uuid"]
            logger.info("Received request to continue %s", from_request_uuid)

            from_request = await (
                firestore_client.collection(FirestoreCollections.requests).document(from_request_uuid).get()
            )
            if not from_request.exists:
                return json(
                    {
                        "error": "from_request_uuid not found",
                        "from_request_uuid": from_request_uuid,
                    },
                    status=404,
                )

            from_request = AskAstroRequest.from_dict(from_request.to_dict())

            messages = from_request.messages if from_request else []
            messages.append(
                HumanMessage(
                    content=from_request.prompt,
                    additional_kwargs={
                        "ts": from_request.sent_at,
                    },
                )
            )
            messages.append(
                AIMessage(
                    content=from_request.response,
                    additional_kwargs={
                        "ts": from_request.response_received_at,
                        "ask_astro_request_uuid": from_request_uuid,
                    },
                )
            )

        req = AskAstroRequest(
            uuid=uuid.uuid1(),
            prompt=request.json["prompt"],
            status="in_progress",
            messages=messages,
        )

        request.app.add_task(lambda: answer_question(req))

        return json(
            PostRequestResponse(
                request_uuid=str(req.uuid),
            ).dict(),
            status=200,
        )
    except Exception as e:
        logger.error("An error occurred while processing the POST request: %s", e)
        return json({"error": "An internal error occurred."}, status=500)
