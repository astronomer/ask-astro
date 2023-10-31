from __future__ import annotations

import datetime
import logging
from datetime import timedelta
from logging import Logger

import google.cloud.firestore
from slack_sdk.oauth.state_store.async_state_store import AsyncOAuthStateStore


class AsyncFirestoreOAuthStateStore(AsyncOAuthStateStore):
    """An async state store backed by Firestore for Slack OAuth flows."""

    def __init__(
        self,
        *,
        collection: str,
        expiration_seconds: int,
        client_id: str | None = None,
        logger: Logger = logging.getLogger(__name__),
    ):
        """
        Initialize the state store with given parameters.

        :param collection: Firestore collection name.
        :param expiration_seconds: Duration in seconds before a state becomes expired.
        :param client_id: The client ID for Slack OAuth. Default is None.
        :param logger: Logger instance. Defaults to the module's logger.
        """
        firestore_client = google.cloud.firestore.AsyncClient()
        self.collection = firestore_client.collection(collection)
        self.expiration_seconds = expiration_seconds

        self.client_id = client_id
        self._logger = logger

    @property
    def logger(self) -> Logger:
        """Logger property. If `_logger` is None, it initializes a new logger."""
        if self._logger is None:
            self._logger = logging.getLogger(__name__)
        return self._logger

    async def async_issue(self, *args, **kwargs) -> str:
        """Issue a new OAuth state and store it in Firestore."""
        doc_ref = self.collection.document()
        await doc_ref.set({"timestamp": datetime.datetime.now().astimezone()})
        return doc_ref.id

    async def async_consume(self, state: str) -> bool:
        """
        Consume the OAuth state by verifying its validity.

        :param state: The state string to verify.
        """
        doc_ref = self.collection.document(state)
        created = (await doc_ref.get()).get("timestamp")

        if created:
            expiration = created + timedelta(seconds=self.expiration_seconds)
            still_valid: bool = datetime.datetime.now().astimezone() < expiration
            return still_valid

        return False
