from asyncio import TaskGroup
from logging import getLogger
from typing import Any

from ask_astro.services.feedback import submit_feedback
from slack_bolt.async_app import AsyncAck, AsyncRespond
from slack_sdk.errors import SlackApiError
from slack_sdk.web.async_client import AsyncWebClient

logger = getLogger(__name__)


async def handle_feedback_good(
    body: dict[str, Any], ack: AsyncAck, respond: AsyncRespond, client: AsyncWebClient
):
    await ack()

    try:
        user = body["user"]["id"]
        channel = body["channel"]["id"]
        thread_ts = body["message"]["thread_ts"]
        message_ts = body["message"]["ts"]
        value = body["actions"][0]["value"]
    except KeyError as e:
        logger.error(f"Missing key: {e}")
        return

    request_id = value.split(":")[0]

    await submit_feedback(request_id, True, source_info={"type": "slack", "user": user})

    async with TaskGroup() as tg:
        tg.create_task(
            respond(
                f"☺️ Thank you for your feedback, <@{user}>!",
                thread_ts=thread_ts,
                replace_original=False,
                response_type="ephemeral",
            )
        )

        async def update_reaction():
            try:
                await client.reactions_add(
                    name="thumbsup",
                    channel=channel,
                    timestamp=message_ts,
                )
            except SlackApiError as e:
                # ignore the error if the reaction already exists
                if e.response["error"] != "already_reacted":
                    raise e

            try:
                await client.reactions_remove(
                    name="thumbsdown",
                    channel=channel,
                    timestamp=message_ts,
                )
            except:
                pass

        tg.create_task(
            update_reaction(),
        )
