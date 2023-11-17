from __future__ import annotations

from datetime import datetime

import numpy as np
import pandas as pd
import requests
from weaviate.util import generate_uuid5

from airflow.providers.slack.hooks.slack import SlackHook
from include.tasks.extract.utils.slack_helpers import get_slack_replies

slack_archive_host = "apache-airflow.slack-archives.org"
slack_base_url = "https://{slack_archive_host}/v1/messages?size={size}&team={team}&channel={channel}"

slack_message_md_format = "# slack: {team_name}\n\n## {channel_name}\n\n{content}"
slack_reply_md_format = "### [{ts}] <@{user}>\n\n{text}"
slack_link_format = "https://app.slack.com/client/{team_id}/{channel_id}/thread/{channel_id}-{ts}"


def extract_slack_archive(source: dict) -> pd.DataFrame:
    """
    This task downloads archived slack messages and returns documents in a pandas dataframe.

    param source: A dictionary specifying the channel, team connection id.

    For example
    {
        "channel_name": "troubleshooting",
        "channel_id": "CCQ7EGB1P",
        "team_id": "TCQ18L22Z",
        "team_name": "Airflow Slack Community",
        "slack_api_conn_id": "slack_api_ro",
    }
    type source: dict

    Returned dataframe fields are:
    'docSource': slack team and channel names
    'docLink': URL for the specific message/reply
    'content': The message/reply content in markdown format.
    'sha': A unique identifier of the client_msg_id
    """

    page_size = 500

    slack_archive_url = slack_base_url.format(
        slack_archive_host=slack_archive_host, size=page_size, team=source["team_id"], channel=source.get("channel_id")
    )

    total_messages = requests.get(slack_archive_url).json().get("total")

    messages = []
    for offset in range(0, total_messages, page_size):
        response = requests.get(slack_archive_url + "&offset=" + str(offset)).json()
        messages.extend(response.get("messages"))
        print(f"Fetched messages length is {len(messages)}")

    df = pd.DataFrame(messages)

    df = df[["user", "text", "ts", "thread_ts", "client_msg_id", "type"]].drop_duplicates().reset_index(drop=True)

    df["thread_ts"].fillna(value=df.ts, inplace=True)

    msg_ids = df[["ts", "client_msg_id"]]

    df["content"] = df.apply(
        lambda x: slack_reply_md_format.format(ts=datetime.fromtimestamp(float(x.ts)), user=x.user, text=x.text), axis=1
    )

    df = df.sort_values("ts").groupby("thread_ts").agg({"content": "\n".join}).reset_index()

    df = df.merge(msg_ids, left_on="thread_ts", right_on="ts", how="left")

    df["sha"] = df.apply(
        lambda x: x.client_msg_id if x.client_msg_id is not np.nan else generate_uuid5(x.content), axis=1
    )

    df["content"] = df["content"].apply(
        lambda x: slack_message_md_format.format(
            team_name=source["team_name"], channel_name=source["channel_name"], content=x
        )
    )

    df["docLink"] = df["thread_ts"].apply(
        lambda x: slack_link_format.format(team_id=source["team_id"], channel_id=source["channel_id"], ts=str(x))
    )
    df["docSource"] = source["channel_name"]

    # column order matters for uuid generation
    df = df[["docSource", "sha", "content", "docLink"]]

    return df


def extract_slack(source: dict) -> pd.DataFrame:
    """
    This task downloads messages from slack channels.

    param source: A dictionary specifying the channel, team connection id.

    For example
    {
        "channel_name": "troubleshooting",
        "channel_id": "CCQ7EGB1P",
        "team_id": "TCQ18L22Z",
        "team_name": "Airflow Slack Community",
        "slack_api_conn_id": "slack_api_ro",
    }
    type source: dict

    Returned dataframe fields are:
    'docSource': slack team and channel names
    'docLink': URL for the specific message/reply
    'content': The message/reply content in markdown format.
    'sha': A unique identifier of the client_msg_id
    """

    page_size = 100

    slack_client = SlackHook(slack_conn_id=source["slack_api_conn_id"]).client
    channel_info = slack_client.conversations_info(channel=source["channel_id"]).data["channel"]
    assert channel_info["is_member"] or not channel_info["is_private"]

    page = 1
    response = slack_client.conversations_history(channel=source["channel_id"], limit=page_size)
    messages = response["messages"]

    while response["has_more"]:
        page += 1
        response = slack_client.conversations_history(
            channel=source["channel_id"], limit=page_size, cursor=response["response_metadata"]["next_cursor"]
        )
        messages.extend(response["messages"])
        print(f"Fetched messages length is {len(messages)}")

    df = pd.DataFrame(messages)

    # if channel has no replies yet thread_ts will not be present
    if "thread_ts" not in df:
        df["thread_ts"] = np.nan
    else:
        replies = get_slack_replies(df=df, channel_id=source["channel_id"], slack_client=slack_client)

        df = pd.concat([df, pd.DataFrame(replies)], axis=0)

    df = df[["user", "text", "ts", "thread_ts", "client_msg_id", "type"]].drop_duplicates().reset_index(drop=True)

    df["thread_ts"].fillna(value=df.ts, inplace=True)

    msg_ids = df[["ts", "client_msg_id"]]

    df["content"] = df.apply(
        lambda x: slack_reply_md_format.format(ts=datetime.fromtimestamp(float(x.ts)), user=x.user, text=x.text), axis=1
    )

    df = df.sort_values("ts").groupby("thread_ts").agg({"content": "\n".join}).reset_index()

    df = df.merge(msg_ids, left_on="thread_ts", right_on="ts", how="left")

    df["sha"] = df.apply(
        lambda x: x.client_msg_id if x.client_msg_id is not np.nan else generate_uuid5(x.content), axis=1
    )

    df["content"] = df["content"].apply(
        lambda x: slack_message_md_format.format(
            team_name=source["team_name"], channel_name=source["channel_name"], content=x
        )
    )

    df["docLink"] = df["thread_ts"].apply(
        lambda x: slack_link_format.format(team_id=source["team_id"], channel_id=source["channel_id"], ts=str(x))
    )
    df["docSource"] = source["channel_name"]

    # column order matters for uuid generation
    df = df[["docSource", "sha", "content", "docLink"]]

    return df
