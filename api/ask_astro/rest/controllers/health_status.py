"""
Handles GET requests to the /ask/{question_id} endpoint.
"""
from __future__ import annotations

from logging import getLogger

from sanic import Request, json
from sanic_ext import openapi

from ask_astro.models.request import HealthStatus
from ask_astro.settings import SERVICE_MAINTENANCE_BANNER_STATUS

logger = getLogger(__name__)


@openapi.definition(response=HealthStatus.schema_json())
async def on_get_health_status(request: Request) -> json:
    """
    Handles GET requests to the /health_status endpoint.

    :param request: The Sanic request object.
    """
    if SERVICE_MAINTENANCE_BANNER_STATUS:
        return json({"status": "maintenance"}, status=200)
    return json({"status": "healthy"}, status=200)
