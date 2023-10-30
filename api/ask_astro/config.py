"""Exports config variables that are used throughout the code."""
import json

from ask_astro.app import api as app


class FirestoreCollections:
    """Contains the names of the collections in the Firestore database."""

    installation_store = app.config.FIRESTORE_INSTALLATION_STORE_COLLECTION
    state_store = app.config.FIRESTORE_STATE_STORE_COLLECTION
    messages = app.config.FIRESTORE_MESSAGES_COLLECTION
    mentions = app.config.FIRESTORE_MENTIONS_COLLECTION
    actions = app.config.FIRESTORE_ACTIONS_COLLECTION
    responses = app.config.FIRESTORE_RESPONSES_COLLECTION
    reactions = app.config.FIRESTORE_REACTIONS_COLLECTION
    shortcuts = app.config.FIRESTORE_SHORTCUTS_COLLECTION
    teams = app.config.FIRESTORE_TEAMS_COLLECTION
    requests = app.config.FIRESTORE_REQUESTS_COLLECTION


class AzureOpenAIParams:
    """Contains the parameters for the Azure OpenAI API."""

    us_east_raw = app.config.AZURE_OPENAI_USEAST_PARAMS
    us_east = json.loads(us_east_raw) if us_east_raw else {}

    us_east2_raw = app.config.AZURE_OPENAI_USEAST2_PARAMS
    us_east2 = json.loads(us_east2_raw) if us_east2_raw else {}


class ZendeskConfig:
    """Contains the config variables for the Zendesk API."""

    credentials = app.config.ZENDESK_CREDENTIALS
    assignee_group_id = app.config.ZENDESK_ASSIGNEE_GROUP_ID


class SlackAppConfig:
    """Contains the config variables for the Slack app."""

    client_id = app.config.SLACK_CLIENT_ID
    client_secret = app.config.SLACK_CLIENT_SECRET
    signing_secret = app.config.SLACK_SIGNING_SECRET


class LangSmithConfig:
    """Contains the config variables for the Langsmith API."""

    project_name = app.config.LANGCHAIN_PROJECT


class WeaviateConfig:
    """Contains the config variables for the Weaviate API."""

    OpenAIApiKey = app.config.OPENAI_API_KEY
    url = app.config.WEAVIATE_URL
    api_key = app.config.WEAVIATE_API_KEY
    index_name = app.config.WEAVIATE_INDEX_NAME
    text_key = app.config.WEAVIATE_TEXT_KEY
    attributes = app.config.WEAVIATE_ATTRIBUTES.split(",") if app.config.WEAVIATE_ATTRIBUTES else []
