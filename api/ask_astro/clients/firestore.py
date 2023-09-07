"Firestore client to handle database operations"

from google.cloud import firestore

# auth is handled implicitly by the environment
firestore_client = firestore.AsyncClient()

__all__ = ["firestore_client"]
