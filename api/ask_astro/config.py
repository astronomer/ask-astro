"""Exports config variables that are used throughout the code."""

# TODO: Refactor with https://sanic.dev/en/guide/deployment/configuration.html

import os
import json


class FirestoreCollections:
    "Contains the names of the collections in the Firestore database."
    installation_store = os.environ["FIRESTORE_INSTALLATION_STORE_COLLECTION"]
    state_store = os.environ["FIRESTORE_STATE_STORE_COLLECTION"]
    messages = os.environ["FIRESTORE_MESSAGES_COLLECTION"]
    mentions = os.environ["FIRESTORE_MENTIONS_COLLECTION"]
    actions = os.environ["FIRESTORE_ACTIONS_COLLECTION"]
    responses = os.environ["FIRESTORE_RESPONSES_COLLECTION"]
    reactions = os.environ["FIRESTORE_REACTIONS_COLLECTION"]
    shortcuts = os.environ["FIRESTORE_SHORTCUTS_COLLECTION"]
    teams = os.environ["FIRESTORE_TEAMS_COLLECTION"]
    requests = os.environ["FIRESTORE_REQUESTS_COLLECTION"]


class AzureOpenAIParams:
    "Contains the parameters for the Azure OpenAI API."
    us_east = json.loads(os.environ["AZURE_OPENAI_USEAST_PARAMS"])
    us_east2 = json.loads(os.environ["AZURE_OPENAI_USEAST2_PARAMS"])


class ZendeskConfig:
    "Contains the config variables for the Zendesk API."
    credentials = os.environ.get("ZENDESK_CREDENTIALS")
    assignee_group_id = os.environ["ZENDESK_ASSIGNEE_GROUP_ID"]


class SlackAppConfig:
    "Contains the config variables for the Slack app."
    client_id = os.environ["SLACK_CLIENT_ID"]
    client_secret = os.environ["SLACK_CLIENT_SECRET"]
    signing_secret = os.environ["SLACK_SIGNING_SECRET"]


class LangSmithConfig:
    "Contains the config variables for the Langsmith API."
    project_name = os.environ["LANGCHAIN_PROJECT"]


class WeaviateConfig:
    OpenAIApiKey = os.environ["OPENAI_API_KEY"]
    url = os.environ["WEAVIATE_URL"]
    api_key = os.environ["WEAVIATE_API_KEY"]
    index_name = os.environ["WEAVIATE_INDEX_NAME"]
    text_key = os.environ["WEAVIATE_TEXT_KEY"]
    attributes = os.environ.get("WEAVIATE_ATTRIBUTES", "").split(",")
