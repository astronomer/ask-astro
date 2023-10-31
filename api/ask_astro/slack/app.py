"""
Generates the Slack app and the Slack app handler.

This module sets up the Slack app's OAuth settings and creates an instance
of the Slack app and its handler.

.. note::
   **Scopes Required for Slack:**

   - `commands`: Add shortcuts and/or slash commands.
   - `app_mentions:read`: Read messages that directly mention the app in conversations.
   - `channels:read`: View basic information about public channels in the workspace.
   - `channels:history`: View messages and other content in public channels.
   - `groups:read`: View basic information about private channels.
   - `groups:history`: View messages and other content in private channels.
   - `chat:write`: Send messages as the app.
   - `reactions:read`: View emoji reactions and their associated messages in channels and conversations.
   - `reactions:write`: Add and remove emoji reactions to/from messages.
   - `users:read`: View people in the workspace.
   - `users:read.email`: View email addresses of people in the workspace.
   - `team:read`: View name, email domain, and icon for the workspace.
   - `im:history`: View messages and other content in direct messages.
   - `mpim:history`: View messages and other content in group direct messages.
   - `files:read`: View files shared in channels and conversations the app has access to.
"""

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
