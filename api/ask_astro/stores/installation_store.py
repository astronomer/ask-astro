import logging
from logging import Logger
from typing import Optional

import google.cloud.firestore

from slack_sdk.oauth.installation_store.async_installation_store import (
    AsyncInstallationStore,
)
from slack_sdk.oauth.installation_store.models.bot import Bot
from slack_sdk.oauth.installation_store.models.installation import Installation


class AsyncFirestoreInstallationStore(AsyncInstallationStore):
    def __init__(
        self,
        *,
        collection: str,
        historical_data_enabled: bool = True,
        client_id: Optional[str] = None,
        logger: Logger = logging.getLogger(__name__),
    ):
        firestore_client = google.cloud.firestore.AsyncClient()
        self.fp = firestore_client.field_path
        self.collection = firestore_client.collection(collection)
        self.historical_data_enabled = historical_data_enabled
        self.client_id = client_id
        self._logger = logger

    @property
    def logger(self) -> Logger:
        if self._logger is None:
            self._logger = logging.getLogger(__name__)
        return self._logger

    async def async_save(self, installation: Installation):
        none = "none"
        e_id = installation.enterprise_id or none
        t_id = installation.team_id or none

        doc_ref = self.collection.document(f"{e_id}-{t_id}")

        try:
            await doc_ref.create({})
        except google.cloud.exceptions.Conflict:
            pass

        await self.async_save_bot(installation.to_bot())

        if self.historical_data_enabled:
            history_version: str = str(installation.installed_at)

            # per workspace
            entity = installation.__dict__
            await doc_ref.update(
                {
                    self.fp("installer", "latest"): entity,
                    self.fp("installer", history_version): entity,
                },
            )

            # per workspace per user
            u_id = installation.user_id or none
            entity = installation.__dict__
            await doc_ref.update(
                {
                    self.fp("installer", u_id, "latest"): entity,
                    self.fp("installer", u_id, history_version): entity,
                },
            )
        else:
            u_id = installation.user_id or none
            entity = installation.__dict__
            await doc_ref.update(
                {self.fp("installer", u_id, "latest"): entity},
            )

    async def async_save_bot(self, bot: Bot):
        none = "none"
        e_id = bot.enterprise_id or none
        t_id = bot.team_id or none
        doc_ref = self.collection.document(f"{e_id}-{t_id}")

        if self.historical_data_enabled:
            history_version: str = str(bot.installed_at)

            entity = bot.__dict__
            await doc_ref.update(
                {
                    self.fp("bot", "latest"): entity,
                    self.fp(f"bot", history_version): entity,
                },
            )
        else:
            entity = bot.__dict__
            await doc_ref.update(
                {self.fp("bot", "latest"): entity},
            )

    async def async_find_bot(
        self,
        *,
        enterprise_id: Optional[str],
        team_id: Optional[str],
        is_enterprise_install: Optional[bool] = False,
    ) -> Optional[Bot]:
        none = "none"
        e_id = enterprise_id or none
        t_id = team_id or none
        if is_enterprise_install:
            t_id = none

        doc_ref = self.collection.document(f"{e_id}-{t_id}")

        if data := (await doc_ref.get([self.fp("bot", "latest")])).to_dict():
            return Bot(**data["bot"]["latest"])
        else:
            message = f"Installation data missing for enterprise: {e_id}, team: {t_id}"
            self.logger.debug(message)
            return None

    async def async_find_installation(
        self,
        *,
        enterprise_id: Optional[str],
        team_id: Optional[str],
        user_id: Optional[str] = None,
        is_enterprise_install: Optional[bool] = False,
    ) -> Optional[Installation]:
        none = "none"
        e_id = enterprise_id or none
        t_id = team_id or none
        if is_enterprise_install:
            t_id = none

        doc_ref = self.collection.document(f"{e_id}-{t_id}")

        if user_id:
            data = (
                (await doc_ref.get([self.fp("installer", user_id, "latest")]))
                .to_dict()
                .get("installer", {})
                .get(user_id, {})
                .get("latest")
            )
        else:
            data = (
                (await doc_ref.get([self.fp("installer", "latest")]))
                .to_dict()
                .get("installer", {})
                .get("latest")
            )

        if data:
            installation = Installation(**data) if data else None

            if installation is not None and user_id is not None:
                # Retrieve the latest bot token, just in case
                # See also: https://github.com/slackapi/bolt-python/issues/664
                latest_bot_installation = await self.async_find_installation(
                    enterprise_id=enterprise_id,
                    team_id=team_id,
                    is_enterprise_install=is_enterprise_install,
                )
                if (
                    latest_bot_installation is not None
                    and installation.bot_token != latest_bot_installation.bot_token
                ):
                    # NOTE: this logic is based on the assumption that every single installation has bot scopes
                    # If you need to installation patterns without bot scopes in the same S3 bucket,
                    # please fork this code and implement your own logic.
                    installation.bot_id = latest_bot_installation.bot_id
                    installation.bot_user_id = latest_bot_installation.bot_user_id
                    installation.bot_token = latest_bot_installation.bot_token
                    installation.bot_scopes = latest_bot_installation.bot_scopes
                    installation.bot_refresh_token = (
                        latest_bot_installation.bot_refresh_token
                    )
                    installation.bot_token_expires_at = (
                        latest_bot_installation.bot_token_expires_at
                    )

            return installation
        else:
            message = f"Installation data missing for enterprise: {e_id}, team: {t_id}"
            self.logger.debug(message)
            return None

    async def async_delete_bot(
        self, *, enterprise_id: Optional[str], team_id: Optional[str]
    ) -> None:
        none = "none"
        e_id = enterprise_id or none
        t_id = team_id or none
        doc_ref = self.collection.document(f"{e_id}-{t_id}")
        await doc_ref.update({"bot": google.cloud.firestore.DELETE_FIELD})

    async def async_delete_installation(
        self,
        *,
        enterprise_id: Optional[str],
        team_id: Optional[str],
        user_id: Optional[str] = None,
    ) -> None:
        none = "none"
        e_id = enterprise_id or none
        t_id = team_id or none
        doc_ref = self.collection.document(f"{e_id}-{t_id}")
        await doc_ref.update({"installer": google.cloud.firestore.DELETE_FIELD})
