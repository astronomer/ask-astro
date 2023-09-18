"""
Handles GET requests to the /ask/{question_id} endpoint.
"""

from sanic import json, Request
from sanic_ext import openapi

from pydantic.v1 import BaseModel, Field

from ask_astro.config import FirestoreCollections
from ask_astro.clients.firestore import firestore_client
from ask_astro.models.request import AskAstroRequest


class RecentRequestsResponse(BaseModel):
    requests: list[AskAstroRequest] = Field(
        default_factory=list,
        description="The requests",
    )

    def to_dict(self):
        return {
            "requests": [request.to_firestore() for request in self.requests],
        }


@openapi.definition(
    response=RecentRequestsResponse.schema_json(),
)
async def on_list_recent_requests(_: Request):
    """
    Handles GET requests to the /requests endpoint.
    """
    # list the most recent 5 requests
    requests = await (
        firestore_client.collection(FirestoreCollections.requests)
        .order_by("sent_at", direction="DESCENDING")
        .where("status", "==", "complete")
        .where("is_example", "==", True)
        .limit(12)
        .get()
    )

    return json(
        RecentRequestsResponse(
            requests=[
                AskAstroRequest.from_dict(request.to_dict()) for request in requests
            ]
        ).to_dict(),
        status=200,
    )
