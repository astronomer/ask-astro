from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4

import pytest
from sanic import Sanic
from sanic_testing import TestManager

sanitized_name = __name__.replace(".", "_")


@pytest.fixture
def app():
    """Fixture that creates and configures a new Sanic application for testing."""
    app_instance = Sanic(sanitized_name)
    TestManager(app_instance)

    from ask_astro.rest.controllers.get_request import on_get_request

    app_instance.add_route(on_get_request, "/requests/<request_id:uuid>")

    return app_instance


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_exists,mock_data,expected_status,expected_response",
    [
        (True, {"title": "Sample Question"}, 200, {"title": "Sample Question"}),
        (False, None, 404, {"error": "Question not found"}),
        (None, None, 500, {"error": "Internal Server Error"}),
    ],
)
async def test_on_get_request(app, mock_exists, mock_data, expected_status, expected_response):
    """Test to validate get request behavior based on different Firestore responses."""
    with patch("ask_astro.rest.controllers.get_request.firestore_client") as mock_firestore:
        request_id = uuid4()

        mock_get = Mock()
        mock_get.exists = mock_exists if mock_exists is not None else True  # Ensure it's True for the error scenario
        mock_get.to_dict.return_value = mock_data

        mock_document = Mock()

        # Mock the async get() method behavior
        async def mock_get_async():
            if mock_exists is not None:
                return mock_get
            else:
                # Simulate an exception for 500 status code
                raise Exception("Simulated Error")

        mock_document.get = AsyncMock(side_effect=mock_get_async)

        mock_collection = Mock()
        mock_collection.document.return_value = mock_document

        mock_firestore.collection.return_value = mock_collection

        request, response = await app.asgi_client.get(f"/requests/{request_id}")

        assert response.status == expected_status
        assert response.json == expected_response
