"""
Handles POST requests to the /requests/{question_id}/feedback endpoint.
"""
from __future__ import annotations

from logging import getLogger
from uuid import UUID

from pydantic.v1 import BaseModel, Field
from sanic import HTTPResponse, Request
from sanic_ext import openapi

from ask_astro.services.feedback import submit_feedback

logger = getLogger(__name__)


class PostRequestBody(BaseModel):
    """Model for the body of a POST feedback request."""

    positive: bool = Field(..., description="Whether the feedback is positive")


@openapi.definition(
    body=PostRequestBody.schema(),
)
async def on_submit_feedback(request: Request, request_id: UUID) -> HTTPResponse:
    """
    Handles POST requests to the /requests/{request_id}/feedback endpoint.

    :param request: The Sanic request object.
    :param request_id:  The unique identifier for the AskAstro request.
    """
    try:
        positive = request.json["positive"]

        await submit_feedback(str(request_id), positive, {"source": "api"})

        return HTTPResponse(status=200)
    except Exception as e:
        logger.error("Error occurred while processing feedback for request %s: %s", request_id, e)
        return HTTPResponse(text="An internal error occurred.", status=500)
