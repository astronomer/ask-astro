"""
Handles GET requests to the /ask/{question_id} endpoint.
"""

from uuid import UUID
from sanic import json, Request
from sanic_ext import openapi

from ask_astro.config import FirestoreCollections
from ask_astro.clients.firestore import firestore_client
from ask_astro.models.request import AskAstroRequest

from logging import getLogger

logger = getLogger(__name__)


@openapi.definition(response=AskAstroRequest.schema_json())
async def on_get_request(_: Request, request_id: UUID):
    """
    Handles GET requests to the /requests/{request_id} endpoint.
    """
    logger.info("Received GET request for request %s", request_id)
    request = await (
        firestore_client.collection(FirestoreCollections.requests)
        .document(str(request_id))
        .get()
    )
    logger.info("Request %s exists: %s", request_id, request.exists)

    if not request.exists:
        return json({"error": "Question not found"}, status=404)

    return json(request.to_dict(), status=200)
