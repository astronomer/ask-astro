"""
Handles GET requests to the /ask/{question_id} endpoint.
"""
from __future__ import annotations

from logging import getLogger

from ask_astro.clients.firestore import firestore_client
from ask_astro.config import FirestoreCollections
from ask_astro.models.request import AskAstroRequest
from pydantic.v1 import BaseModel, Field
from sanic import Request, json
from sanic_ext import openapi

logger = getLogger(__name__)


class RecentRequestsResponse(BaseModel):
    """Data model for the list of recent requests returned in the API response."""

    requests: list[AskAstroRequest] = Field(
        default_factory=list,
        description="The requests",
    )

    def to_dict(self):
        """Convert the RecentRequestsResponse model to a dictionary."""
        return {
            "requests": [request.to_firestore() for request in self.requests],
        }


@openapi.definition(
    response=RecentRequestsResponse.schema_json(),
)
async def on_list_recent_requests(_: Request) -> json:
    """Handle GET requests to retrieve a list of recent completed requests marked as examples."""
    try:
        # Query the Firestore for the most recent 12 completed example requests
        query_results = await (
            firestore_client.collection(FirestoreCollections.requests)
            .order_by("sent_at", direction="DESCENDING")
            .where("status", "==", "complete")
            .where("is_example", "==", True)
            .limit(12)
            .get()
        )
        recent_requests = [AskAstroRequest.from_dict(request_doc.to_dict()) for request_doc in query_results]

        return json(RecentRequestsResponse(requests=recent_requests).to_dict(), status=200)
    except Exception as e:
        logger.error(f"Error while fetching recent requests: {e}")
        return json({"error": "An error occurred while processing your request."}, status=500)
