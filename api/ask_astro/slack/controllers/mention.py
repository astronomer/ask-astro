"""Handles app mention events from Slack"""
from __future__ import annotations

import uuid
from asyncio import TaskGroup
from logging import getLogger
from typing import Any

from langchain.schema import AIMessage, HumanMessage
from slack_bolt.async_app import AsyncAck, AsyncSay
from slack_sdk.errors import SlackApiError
from slack_sdk.web.async_client import AsyncWebClient

from ask_astro.models.request import AskAstroRequest
from ask_astro.services.questions import answer_question
from ask_astro.slack.utils import get_blocks, markdown_to_slack

logger = getLogger(__name__)

THOUGHT_BALLOON_REACTION = "thought_balloon"
FAILURE_REACTION = "x"
FAILURE_MESSAGE = "Sorry, I couldn't answer your question. Please try again later."


async def try_add_reaction(client: AsyncWebClient, name: str, channel_id: str, timestamp: str) -> None:
    """
    Try to add a reaction to a given Slack message.

    :param client: Slack API client.
    :param name: Name of the reaction emoji.
    :param channel_id: ID of the channel where the message is located.
    :param timestamp: Timestamp of the message.
    """
    try:
        await client.reactions_add(
            name=name,
            channel=channel_id,
            timestamp=timestamp,
        )
    except SlackApiError as exc:
        logger.warning("Failed to add %s reaction", name, exc_info=exc)


async def send_answer(request: AskAstroRequest, say: AsyncSay, ts: str):
    """
    Send an answer to a Slack thread using the provided request information.

    :param request: The AskAstro request object containing the response and other related data.
    :param say: Say object from slack_bolt to send messages.
    :param ts: Timestamp of the original message to which we're replying.
    """
    response = markdown_to_slack(request.response)

    source_links = ", ".join(
        f"<{link}|[{n_}]>"
        for n_, link in enumerate(
            {*[s.name for s in request.sources if s.name]},
            start=1,
        )
    )

    resp = get_blocks(
        "message.jinja2",
        message=f"{response[:2999]}…" if len(response) > 3000 else response,
        sources=f"📚 Related: {source_links}" if source_links else None,
        feedback_value=f"{request.uuid}:{request.langchain_run_id}",
    )

    await say(
        text=response,
        blocks=resp,
        thread_ts=ts,
        unfurl_links=False,
        unfurl_media=False,
    )


async def on_mention(body: dict[str, Any], ack: AsyncAck, say: AsyncSay, client: AsyncWebClient) -> None:
    """
    Handles Slack app mentions. When the app is mentioned, this function processes the mention,
    performs relevant operations and sends a response back.

    :param body: The Slack event body.
    :param ack: Acknowledgement object from slack_bolt.
    :param say: Say object from slack_bolt to send messages.
    :param client: Slack API client.
    """
    await ack()

    try:
        text = body["event"]["text"]
        bot_id = body["authorizations"][0]["user_id"]
        channel_id = body["event"]["channel"]
        ts = body["event"]["ts"]
    except KeyError as e:
        logger.error(f"Missing key: {e}")
        return

    logger.info("Received question (%s): %s", ts, text)
    thread_ts = body["event"].get("thread_ts")

    text = text.strip().removeprefix(f"<@{bot_id}>").strip()

    request = AskAstroRequest(uuid=uuid.uuid1(), prompt=text, status="in_progress")

    async with TaskGroup() as tg:
        tg.create_task(try_add_reaction(client, THOUGHT_BALLOON_REACTION, channel_id, ts))

    try:
        if thread_ts:
            slack_messages = await client.conversations_replies(channel=channel_id, ts=thread_ts, limit=100)

            request.messages = [
                (AIMessage if msg.get("bot_id") == bot_id else HumanMessage)(content=msg["text"], additional_kwargs=msg)
                for msg in slack_messages["messages"]
                if msg.get("type") == "message" and msg.get("ts") != ts
            ]

        await answer_question(request)

        async with TaskGroup() as tg:
            tg.create_task(client.reactions_remove(name=THOUGHT_BALLOON_REACTION, channel=channel_id, timestamp=ts))
            tg.create_task(send_answer(request, say, ts))

    except Exception as e:
        await client.reactions_remove(name=THOUGHT_BALLOON_REACTION, channel=channel_id, timestamp=ts)
        await try_add_reaction(client, FAILURE_REACTION, channel_id, ts)
        await say(text=FAILURE_MESSAGE, thread_ts=ts)

        raise e
