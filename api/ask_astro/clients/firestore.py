"""
Firestore client to handle database operations.

This module provides an asynchronous Firestore client instance for use across the application.
The client will authenticate using the credentials set in the environment.
"""

from google.cloud import firestore

# auth is handled implicitly by the environment.
# Singleton instance of the Firestore AsyncClient.
firestore_client = firestore.AsyncClient()

__all__ = ["firestore_client"]
