import datetime
import logging
from datetime import timedelta
from logging import Logger
from typing import Optional

import google.cloud.firestore
from slack_sdk.oauth.state_store.async_state_store import AsyncOAuthStateStore


class AsyncFirestoreOAuthStateStore(AsyncOAuthStateStore):
    def __init__(
        self,
        *,
        collection: str,
        expiration_seconds: int,
        client_id: Optional[str] = None,
        logger: Logger = logging.getLogger(__name__),
    ):
        firestore_client = google.cloud.firestore.AsyncClient()
        self.collection = firestore_client.collection(collection)
        self.expiration_seconds = expiration_seconds

        self.client_id = client_id
        self._logger = logger

    @property
    def logger(self) -> Logger:
        if self._logger is None:
            self._logger = logging.getLogger(__name__)
        return self._logger

    async def async_issue(self, *args, **kwargs) -> str:
        doc_ref = self.collection.document()
        await doc_ref.set({"timestamp": datetime.datetime.now().astimezone()})
        return doc_ref.id

    async def async_consume(self, state: str) -> bool:
        doc_ref = self.collection.document(state)
        created = (await doc_ref.get()).get("timestamp")

        if created:
            expiration = created + timedelta(seconds=self.expiration_seconds)
            still_valid: bool = datetime.datetime.now().astimezone() < expiration
            return still_valid

        return False
