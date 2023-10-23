from time import sleep

import pandas as pd
from slack_sdk import errors as slack_errors
from slack_sdk.web.client import WebClient

from airflow.exceptions import AirflowException


def get_slack_replies(df: pd.DataFrame, channel_id: str, slack_client: WebClient) -> list:
    """
    This helper function reads replies for each thread in df.  Unlike the archive these need to
    be pulled separately.

    param df: Dataframe of slack messages.  These are the base messages before pulling the
    associated replies.
    type df: pd.DataFrame

    param channel_id: The channel ID to search for replies.
    type channel_id: str

    param slack_client: A slack client to use for reading.  It should be instantiated with a
    slack ID with read permissions for channel_id.
    type slack_client: slack_sdk.web.client.WebClient

    """
    replies = []

    threads_with_replies = df[df.reply_count > 0].thread_ts.to_list()

    for ts in threads_with_replies:
        print(f"Fetching replies for thread {ts}")
        for attempt in range(10):
            try:
                reply = slack_client.conversations_replies(channel=channel_id, ts=ts)
                replies.extend(reply.data["messages"])
            except Exception as e:
                if isinstance(e, slack_errors.SlackApiError) and e.response.get("error") == "ratelimited":
                    sleep_time = e.response.headers.get("retry-after")
                    print(f"Received ratelimit. Sleeping {sleep_time}")
                    sleep(int(sleep_time))
                else:
                    raise e
            else:
                break
        else:
            raise AirflowException("Retry count exceeded fetching replies.")

    return replies
