"""Contains a function to register all controllers with the app."""
from __future__ import annotations

from dataclasses import dataclass
from logging import getLogger
from typing import Callable

from sanic import Sanic, response

from ask_astro.rest.controllers.get_request import on_get_request
from ask_astro.rest.controllers.health_status import on_get_health_status
from ask_astro.rest.controllers.list_recent_requests import on_list_recent_requests
from ask_astro.rest.controllers.post_request import on_post_request
from ask_astro.rest.controllers.submit_feedback import on_submit_feedback

logger = getLogger(__name__)


@dataclass
class RouteConfig:
    handler: Callable[..., response.BaseHTTPResponse]
    uri: str
    methods: list[str]
    name: str


def register_routes(api: Sanic):
    """Registers all controllers with the app."""

    routes: list[RouteConfig] = [
        RouteConfig(on_list_recent_requests, "/requests", ["GET"], "list_recent_requests"),
        RouteConfig(on_get_request, "/requests/<request_id:uuid>", ["GET"], "get_request"),
        RouteConfig(on_post_request, "/requests", ["POST"], "post_request"),
        RouteConfig(on_submit_feedback, "/requests/<request_id:uuid>/feedback", ["POST"], "submit_feedback"),
        RouteConfig(on_get_health_status, "/health_status", ["GET"], "health_status"),
    ]

    for route_config in routes:
        api.add_route(
            handler=route_config.handler,
            uri=route_config.uri,
            methods=route_config.methods,
            name=route_config.name,
        )
        logger.info("Registered %s %s controller", route_config.methods[0], route_config.uri)
