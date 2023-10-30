from __future__ import annotations

from asyncio import TaskGroup
from logging import getLogger
from typing import Any

from ask_astro.services.feedback import submit_feedback
from slack_bolt.async_app import AsyncAck, AsyncRespond
from slack_sdk.errors import SlackApiError
from slack_sdk.web.async_client import AsyncWebClient

logger = getLogger(__name__)


def extract_feedback_details(body: dict[str, Any]) -> dict[str, str]:
    """
    Extract necessary details from Slack body for feedback processing.

    :param body: The slack event body.
    """
    try:
        return {
            "user": body["user"]["id"],
            "channel": body["channel"]["id"],
            "thread_ts": body["message"]["thread_ts"],
            "message_ts": body["message"]["ts"],
            "value": body["actions"][0]["value"],
        }
    except KeyError as e:
        logger.error("Missing key: %s", e)
        return {}


async def handle_feedback_bad(
    body: dict[str, Any], ack: AsyncAck, respond: AsyncRespond, client: AsyncWebClient
) -> None:
    """
    Handle feedback received from Slack and send appropriate responses.

    :param body: The slack event body.
    :param ack: Acknowledgement object from slack_bolt.
    :param respond: Response object from slack_bolt.
    :param client: Slack API client.
    """
    await ack()

    details = extract_feedback_details(body)
    if not details:
        return

    request_id = details["value"].split(":")[0]
    await submit_feedback(request_id, False, source_info={"type": "slack", "user": details["user"]})

    async with TaskGroup() as tg:
        tg.create_task(_send_response(details, respond))
        tg.create_task(_update_reaction(details, client))


async def _send_response(details: dict[str, str], respond: AsyncRespond) -> None:
    """
    Send a response back to the user in Slack.

    :param details: The details extracted from the Slack body.
    :param respond: Response object from slack_bolt.
    """
    await respond(
        "😥 Thank you for your feedback, <@%s>!" % details["user"],
        thread_ts=details["thread_ts"],
        replace_original=False,
        response_type="ephemeral",
    )


async def _update_reaction(details: dict[str, str], client: AsyncWebClient) -> None:
    """
    Add a 'thumbsdown' reaction and remove the 'thumbsup' reaction from the original message.

    :param details: The details extracted from the Slack body.
    :param client: Slack API client.
    """
    try:
        await client.reactions_add(name="thumbsdown", channel=details["channel"], timestamp=details["message_ts"])
    except SlackApiError as e:
        # ignore the error if the reaction already exists
        if e.response["error"] != "already_reacted":
            raise e

    try:
        await client.reactions_remove(name="thumbsup", channel=details["channel"], timestamp=details["message_ts"])
    except Exception as e:
        logger.debug("Failed to remove thumbsup reaction: %s", e)
        pass
