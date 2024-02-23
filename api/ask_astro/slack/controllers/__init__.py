"""Contains a function to register all controllers with the app."""
from __future__ import annotations

from logging import getLogger

from slack_bolt.async_app import AsyncApp

from ask_astro.slack.controllers.feedback.bad import handle_feedback_bad
from ask_astro.slack.controllers.feedback.good import handle_feedback_good
from ask_astro.slack.controllers.mention import on_mention

logger = getLogger(__name__)


def register_controllers(app: AsyncApp):
    """
    Registers all controllers with the app.

    :param app: The Slack AsyncApp instance where controllers need to be registered.
    """

    handlers = {
        "event:app_mention": on_mention,
        "action:feedback_good": handle_feedback_good,
        "action:feedback_bad": handle_feedback_bad,
    }

    for event_action, handler in handlers.items():
        event_type, identifier = event_action.split(":")

        if event_type == "event":
            app.event(identifier)(handler)
        elif event_type == "action":
            app.action(identifier)(handler)

        logger.info("Registered %s:%s controller", event_type, identifier)
