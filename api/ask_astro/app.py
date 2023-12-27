"""
Initialize the Sanic app and route requests to the Slack app.
"""
import logging
import os
from logging import getLogger

from sanic import Request, Sanic, json
from sanic_limiter import Limiter, get_remote_address
from sanic_limiter.errors import RateLimitExceeded

from ask_astro.rest.controllers import register_routes
from ask_astro.rest.controllers.post_request import on_post_request
from ask_astro.slack.app import app_handler, slack_app
from ask_astro.slack.controllers import register_controllers

# set the logging level based on an env var
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

logger = getLogger(__name__)

api = Sanic(name="ask_astro")

limiter = Limiter(
    api,
    key_func=get_remote_address,
)
requests_per_day = os.environ.get("API_REQUESTS_PER_DAY", "50")
requests_per_hour = os.environ.get("API_REQUESTS_PER_HOUR", "20")
requests_per_minute = os.environ.get("API_REQUESTS_PER_MINUTE", "10")
requests_limit_value = f"{requests_per_day} per day, {requests_per_hour} per hour, {requests_per_minute} per minute"


# route slack requests to the slack app
@api.get("/slack/oauth_redirect", name="oauth_redirect")
@api.get("/slack/install", name="install")
@api.post("/slack/events", name="events")
async def endpoint(req: Request):
    """Forward requests to the Slack bolt handler."""
    return await app_handler.handle(req)


@api.exception(RateLimitExceeded)
async def catch_rate_limit(request, exception):
    logger.warning("Rate limit exceeded for IP %s at endpoint %s", request.ip, request.path)
    return json(
        {"error": "Rate limit exceeded"},
        status=429,
    )


server_port = int(os.environ.get("PORT", 8080))

register_controllers(slack_app)
register_routes(api)


# We want to rate limit requests to the /requests endpoint, but not to the other endpoints.
# Hence, we use a separate explicit route registration here.
@api.post("/requests", name="post_request")
@limiter.limit(requests_limit_value, key_func=get_remote_address)
async def post_request(req: Request):
    """Handle POST requests to the /requests endpoint."""
    return await on_post_request(req)


if __name__ == "__main__":
    api.run(host="0.0.0.0", port=server_port, auto_reload=True)
