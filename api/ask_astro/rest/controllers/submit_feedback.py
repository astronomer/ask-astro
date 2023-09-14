"""
Handles POST requests to the /requests/{question_id}/feedback endpoint.
"""

from uuid import UUID

from sanic import Request, HTTPResponse
from sanic_ext import openapi

from pydantic.v1 import BaseModel, Field

from ask_astro.services.feedback import submit_feedback


class PostRequestBody(BaseModel):
    positive: bool = Field(..., description="Whether the feedback is positive")


@openapi.definition(
    body=PostRequestBody.schema(),
)
async def on_submit_feedback(request: Request, request_id: UUID):
    """
    Handles POST requests to the /requests/{request_id}/feedback endpoint.
    """
    positive = request.json["positive"]

    await submit_feedback(str(request_id), positive, {"source": "api"})

    return HTTPResponse(status=200)
