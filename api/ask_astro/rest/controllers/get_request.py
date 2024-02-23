"""
Handles GET requests to the /ask/{question_id} endpoint.
"""
from __future__ import annotations

import re
from logging import getLogger
from uuid import UUID

from sanic import Request, json
from sanic_ext import openapi

from ask_astro.clients.firestore import firestore_client
from ask_astro.config import FirestoreCollections
from ask_astro.models.request import AskAstroRequest

logger = getLogger(__name__)


def replace_single_newline_pattern_with_double_newline(text):
    return re.sub(
        r"(.+?)\n•",
        lambda match: f"{match.group(1)}\n\n•" if "\n\n" not in match.group(1) else match.group(0),
        text,
    )


@openapi.definition(response=AskAstroRequest.schema_json())
async def on_get_request(request: Request, request_id: UUID) -> json:
    """
    Handles GET requests to the /requests/{request_id} endpoint.

    :param request: The Sanic request object.
    :param request_id: The unique identifier for the AskAstro request.
    """
    try:
        logger.info("Received GET request for request %s", request_id)
        request = await firestore_client.collection(FirestoreCollections.requests).document(str(request_id)).get()

        logger.info("Request %s exists: %s", request_id, request.exists)

        if not request.exists:
            return json({"error": "Question not found"}, status=404)

        request_dict = request.to_dict()
        if request_dict and "response" in request_dict and request_dict["response"] is not None:
            request_dict["response"] = replace_single_newline_pattern_with_double_newline(request_dict["response"])
        return json(request_dict, status=200)
    except Exception as e:
        logger.error("Error fetching data for request %s: %s", request_id, e)
        return json({"error": "Internal Server Error"}, status=500)
