from unittest.mock import patch


@patch("google.cloud.firestore.AsyncClient")
def test_firestore_client_instance(mocked_async_client):
    """
    Test that firestore_client is an instance of the AsyncClient class from the google.cloud.firestore library.
    """
    from ask_astro.clients.firestore import firestore_client  # noqa

    assert mocked_async_client.called, "AsyncClient was not instantiated"
