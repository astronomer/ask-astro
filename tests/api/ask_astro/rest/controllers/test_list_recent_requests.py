from unittest.mock import AsyncMock, Mock, PropertyMock, patch

import pytest


def generate_mock_document(data):
    mock_document = Mock()
    mock_document.to_dict.return_value = data
    return mock_document


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_data,expected_status,expected_response",
    [
        ([], 500, {"error": "An error occurred while processing your request."}),
    ],
)
async def test_on_list_recent_requests(app, mock_data, expected_status, expected_response):
    with patch("ask_astro.config.FirestoreCollections.requests", new_callable=PropertyMock) as mock_collection:
        mock_collection.return_value = "mock_collection_name"
        with patch("google.cloud.firestore_v1.Client", new=AsyncMock()) as MockFirestoreClient:
            # Here, MockFirestoreClient will replace the actual Firestore Client everywhere in the code.
            mock_client_instance = MockFirestoreClient.return_value

            # Create the final result (from Firestore's get() method)
            mock_get = AsyncMock()
            mock_get.return_value = [generate_mock_document(doc) for doc in mock_data]

            # Mock the Firestore query methods
            mock_query = AsyncMock()
            mock_query.order_by.return_value = mock_query
            mock_query.where.return_value = mock_query
            mock_query.limit.return_value = mock_query
            mock_query.get = mock_get

            # Mock the Firestore collection call
            mock_client_instance.collection.return_value = mock_query

            _, response = await app.asgi_client.get("/requests")
            print(response.content)

            assert response.status == expected_status
            assert response.json == expected_response
