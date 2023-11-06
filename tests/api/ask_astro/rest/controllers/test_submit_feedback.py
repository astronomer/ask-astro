import uuid
from unittest.mock import AsyncMock, patch

import pytest
from httpx import Response
from pytest_sanic.utils import TestClient
from sanic import Sanic


def create_sanic_app(name: str) -> Sanic:
    """Create a new instance of a Sanic application with the provided name."""
    Sanic.test_mode = True
    return Sanic(name)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "request_data,expected_status,expected_response_text",
    [
        ({"positive": True}, 200, None),
        ({"positive": False}, 200, None),
        ({}, 500, "An internal error occurred."),  # Missing positive key
    ],
)
async def test_on_submit_feedback(request_data, expected_status, expected_response_text):
    """
    Test the behavior of the on_submit_feedback route. This test validates the responses of the feedback submission
     route for various inputs.
    """
    from ask_astro.rest.controllers.submit_feedback import on_submit_feedback

    app_name = f"test_sanic_app_{uuid.uuid4().hex}"
    app = create_sanic_app(app_name)
    app.add_route(on_submit_feedback, "/requests/<request_id>/feedback", methods=["POST"])

    async def mock_post(*args, **kwargs):
        """Mock the POST request by returning a dummy response."""
        return Response(status_code=expected_status, text=expected_response_text or "")

    with patch("pytest_sanic.utils.TestClient.post", new=mock_post):
        test_manager = TestClient(app)

        with patch("ask_astro.rest.controllers.submit_feedback.submit_feedback", new_callable=AsyncMock):
            response = await test_manager.post("/requests/test_request_id/feedback", json=request_data)

            assert response.status_code == expected_status
            assert response.text == (expected_response_text or "")
