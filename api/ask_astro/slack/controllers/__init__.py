"Contains a function to register all controllers with the app."

from logging import getLogger

from ask_astro.slack.controllers.feedback.bad import handle_feedback_bad
from ask_astro.slack.controllers.feedback.good import handle_feedback_good
from ask_astro.slack.controllers.mention import on_mention
from slack_bolt.async_app import AsyncApp

logger = getLogger(__name__)


def register_controllers(app: AsyncApp):
    """
    Registers all controllers with the app.
    """

    app.event("app_mention")(on_mention)
    logger.info("Registered event:app_mention controller")

    app.action("feedback_good")(handle_feedback_good)
    logger.info("Registered action:feedback_good controller")

    app.action("feedback_bad")(handle_feedback_bad)
    logger.info("Registered action:feedback_bad controller")
