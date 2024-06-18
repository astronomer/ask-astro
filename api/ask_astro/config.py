"""Exports config variables that are used throughout the code."""
import json
import os


class FirestoreCollections:
    """Contains the names of the collections in the Firestore database."""

    installation_store = os.environ.get("FIRESTORE_INSTALLATION_STORE_COLLECTION")
    state_store = os.environ.get("FIRESTORE_STATE_STORE_COLLECTION")
    messages = os.environ.get("FIRESTORE_MESSAGES_COLLECTION")
    mentions = os.environ.get("FIRESTORE_MENTIONS_COLLECTION")
    actions = os.environ.get("FIRESTORE_ACTIONS_COLLECTION")
    responses = os.environ.get("FIRESTORE_RESPONSES_COLLECTION")
    reactions = os.environ.get("FIRESTORE_REACTIONS_COLLECTION")
    shortcuts = os.environ.get("FIRESTORE_SHORTCUTS_COLLECTION")
    teams = os.environ.get("FIRESTORE_TEAMS_COLLECTION")
    requests = os.environ.get("FIRESTORE_REQUESTS_COLLECTION")


class AzureOpenAIParams:
    """Contains the parameters for the Azure OpenAI API."""

    us_east2_raw = os.environ.get("AZURE_OPENAI_USEAST2_PARAMS")
    us_east2 = json.loads(us_east2_raw) if us_east2_raw else {}


class ZendeskConfig:
    """Contains the config variables for the Zendesk API."""

    credentials = os.environ.get("ZENDESK_CREDENTIALS")
    assignee_group_id = os.environ.get("ZENDESK_ASSIGNEE_GROUP_ID")


class SlackAppConfig:
    "Contains the config variables for the Slack app."

    client_id = os.environ.get("SLACK_CLIENT_ID")
    client_secret = os.environ.get("SLACK_CLIENT_SECRET")
    signing_secret = os.environ.get("SLACK_SIGNING_SECRET")


class LangSmithConfig:
    """Contains the config variables for the Langsmith API."""

    project_name = os.environ.get("LANGCHAIN_PROJECT")
    tracing_v2 = os.environ.get("LANGCHAIN_TRACING_V2")
    endpoint = os.environ.get("LANGCHAIN_ENDPOINT")
    api_key = os.environ.get("LANGCHAIN_API_KEY")


class WeaviateConfig:
    """Contains the config variables for the Weaviate API."""

    OpenAIApiKey = os.environ.get("OPENAI_API_KEY")
    url = os.environ.get("WEAVIATE_URL")
    api_key = os.environ.get("WEAVIATE_API_KEY")
    index_name = os.environ.get("WEAVIATE_INDEX_NAME")
    text_key = os.environ.get("WEAVIATE_TEXT_KEY")
    attributes = os.environ.get("WEAVIATE_ATTRIBUTES", "").split(",")
    k = os.environ.get("WEAVIATE_HYBRID_SEARCH_TOP_K", 100)
    alpha = os.environ.get("WEAVIATE_HYBRID_SEARCH_ALPHA", 0.5)
    create_schema_if_missing = bool(os.environ.get("WEAVIATE_CREATE_SCHEMA_IF_MISSING", "").lower() == "true")


class CohereConfig:
    """Contains the config variables for the Cohere API."""

    rerank_top_n = int(os.environ.get("COHERE_RERANK_TOP_N", 8))


class PromptPreprocessingConfig:
    """Contains the config variables for user prompt's preprocessing function."""

    max_char = int(os.environ.get("PROMPT_PREPROCESSING_MAX_CHAR", 20000))
    max_chat_history_len = int(os.environ.get("PROMPT_PREPROCESSING_MAX_CHAT_HISTORY_LEN", 10))
