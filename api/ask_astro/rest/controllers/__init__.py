"Contains a function to register all controllers with the app."

from sanic import Sanic

from ask_astro.rest.controllers.list_recent_requests import on_list_recent_requests
from ask_astro.rest.controllers.get_request import on_get_request
from ask_astro.rest.controllers.post_request import on_post_request
from ask_astro.rest.controllers.submit_feedback import on_submit_feedback

from logging import getLogger

logger = getLogger(__name__)


def register_routes(api: Sanic):
    """
    Registers all controllers with the app.
    """

    api.add_route(
        on_list_recent_requests,
        "/requests",
        methods=["GET"],
        name="list_recent_requests",
    )
    logger.info("Registered GET /requests controller")

    api.add_route(
        on_get_request,
        "/requests/<request_id:uuid>",
        methods=["GET"],
        name="get_request",
    )
    logger.info("Registered GET /requests/<request_id> controller")

    api.add_route(
        on_post_request,
        "/requests",
        methods=["POST"],
        name="post_request",
    )
    logger.info("Registered POST /requests controller")

    api.add_route(
        on_submit_feedback,
        "/requests/<request_id:uuid>/feedback",
        methods=["POST"],
        name="submit_feedback",
    )
    logger.info("Registered POST /requests/<request_id>/feedback controller")
