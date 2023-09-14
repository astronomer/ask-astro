"""
Initialize the Sanic app and route requests to the Slack app.
"""
import os
import logging
from logging import getLogger

from sanic import Sanic, Request

from ask_astro.slack.app import slack_app, app_handler
from ask_astro.slack.controllers import register_controllers
from ask_astro.rest.controllers import register_routes

# set the logging level based on an env var
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

logger = getLogger(__name__)

api = Sanic(name="ask_astro")


# route slack requests to the slack app
@api.get("/slack/oauth_redirect", name="oauth_redirect")
@api.get("/slack/install", name="install")
@api.post("/slack/events", name="events")
async def endpoint(req: Request):
    "Forward requests to the Slack bolt hander"
    return await app_handler.handle(req)


server_port = int(os.environ.get("PORT", 8080))

register_controllers(slack_app)
register_routes(api)

if __name__ == "__main__":
    api.run(host="0.0.0.0", port=server_port)
