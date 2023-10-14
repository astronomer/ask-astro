"Generates the slack app and the slack app handler."

from ask_astro.config import FirestoreCollections, SlackAppConfig
from ask_astro.stores.installation_store import AsyncFirestoreInstallationStore
from ask_astro.stores.oauth_state_store import AsyncFirestoreOAuthStateStore
from slack_bolt.adapter.sanic import AsyncSlackRequestHandler
from slack_bolt.app.async_app import AsyncApp
from slack_bolt.oauth.async_oauth_settings import AsyncOAuthSettings

oauth_settings = AsyncOAuthSettings(
    client_id=SlackAppConfig.client_id,
    client_secret=SlackAppConfig.client_secret,
    scopes=[
        "commands",
        "app_mentions:read",
        "channels:read",
        "channels:history",
        "groups:read",
        "groups:history",
        "chat:write",
        "reactions:read",
        "reactions:write",
        "users:read",
        "users:read.email",
        "team:read",
        "im:history",
        "mpim:history",
        "files:read",
    ],
    installation_store=AsyncFirestoreInstallationStore(
        collection=FirestoreCollections.installation_store,
    ),
    state_store=AsyncFirestoreOAuthStateStore(
        expiration_seconds=600,
        collection=FirestoreCollections.state_store,
    ),
)


slack_app = AsyncApp(
    signing_secret=SlackAppConfig.signing_secret,
    oauth_settings=oauth_settings,
)
app_handler = AsyncSlackRequestHandler(slack_app)
