"Handles app mention events from Slack"

import uuid
from asyncio import TaskGroup
from logging import getLogger
from typing import Any

from ask_astro.models.request import AskAstroRequest
from ask_astro.services.questions import answer_question
from ask_astro.slack.utils import get_blocks, markdown_to_slack
from langchain.schema import AIMessage, HumanMessage
from slack_bolt.async_app import AsyncAck, AsyncSay
from slack_sdk.errors import SlackApiError
from slack_sdk.web.async_client import AsyncWebClient

logger = getLogger(__name__)


async def on_mention(
    body: dict[str, Any],
    ack: AsyncAck,
    say: AsyncSay,
    client: AsyncWebClient,
):
    await ack()

    # pull out the relevant fields from the body
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
    logger.info("In thread %s", thread_ts)

    # Preprocess text to remove leading bot mentions
    text = text.strip().removeprefix(f"<@{bot_id}>").strip()

    # create the request state object and add it to the database
    request = AskAstroRequest(
        uuid=uuid.uuid1(),
        prompt=text,
        status="in_progress",
    )

    async with TaskGroup() as tg:
        # add a thinking reaction for immediate feedback
        async def try_add_thinking_reaction():
            try:
                await client.reactions_add(
                    name="thought_balloon",
                    channel=channel_id,
                    timestamp=ts,
                )
            except SlackApiError as exc:
                # this is a non-fatal error, so we can just log it and move on
                logger.warning("Failed to add thought_balloon reaction", exc_info=exc)

        tg.create_task(try_add_thinking_reaction())

    logger.info("Acknowledged question")

    try:
        # get messages from the thread, if there is a thread
        if thread_ts:
            # TODO: what happens if there are more than 100 messages?
            slack_messages = await client.conversations_replies(
                channel=channel_id,
                ts=thread_ts,
                limit=100,
            )

            logger.debug("Messages in thread: %s", slack_messages)

            # turn the slack messages into a list of langchain messages
            request.messages = [
                (AIMessage if msg.get("bot_id") == bot_id else HumanMessage)(content=msg["text"], additional_kwargs=msg)
                for msg in slack_messages["messages"]
                if msg.get("type") == "message" and msg.get("ts") != ts
            ]

        await answer_question(request)
        response = markdown_to_slack(request.response)

        source_links = ", ".join(
            (
                f"<{link}|[{n_}]>"
                for n_, link in enumerate(
                    [*{s.name for s in request.sources if s.name}],
                    start=1,
                )
            )
        )

        resp = get_blocks(
            "message.jinja2",
            message=f"{response[:2999]}…" if len(response) > 3000 else response,
            sources=f"📚 Related: {source_links}" if source_links else None,
            feedback_value=f"{request.uuid}:{request.langchain_run_id}",
        )

        async with TaskGroup() as tg:
            # remove the reaction from the message
            tg.create_task(
                client.reactions_remove(
                    name="thought_balloon",
                    channel=channel_id,
                    timestamp=ts,
                )
            )

            # write the response
            tg.create_task(
                say(
                    text=response,
                    blocks=resp,
                    thread_ts=ts,
                    unfurl_links=False,
                    unfurl_media=False,
                )
            )

    except Exception as e:
        # remove the thinking reaction from the message
        await client.reactions_remove(
            name="thought_balloon",
            channel=channel_id,
            timestamp=ts,
        )

        # add a failure reaction
        await client.reactions_add(
            name="x",
            channel=channel_id,
            timestamp=ts,
        )

        # write the failure message
        await say(
            text="Sorry, I couldn't answer your question. Please try again later.",
            thread_ts=ts,
        )

        raise e
