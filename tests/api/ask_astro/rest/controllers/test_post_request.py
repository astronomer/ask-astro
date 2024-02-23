from unittest.mock import AsyncMock, patch

import pytest


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "request_payload,expected_status,expected_response",
    [
        ({"prompt": "Tell me about space"}, 200, {"request_uuid": "test-uuid"}),
        ({"prompt": "What is quantum mechanics?"}, 200, {"request_uuid": "test-uuid"}),
        ({}, 400, {"error": "prompt is required"}),
    ],
)
async def test_on_post_request(app, request_payload, expected_status, expected_response):
    """Test the POST request endpoint behavior based on different input payloads."""
    with patch("ask_astro.services.questions.answer_question") as mock_answer_question, patch(
        "ask_astro.clients.firestore.firestore.AsyncClient"
    ) as mock_firestore, patch("google.cloud.firestore_v1.Client", new=AsyncMock()):
        mock_firestore.collection.return_value.document.return_value.get.return_value = AsyncMock()
        mock_answer_question.return_value = AsyncMock()

        request, response = await app.asgi_client.post("/requests", json=request_payload)

        assert response.status == expected_status
        # If expecting a 200 status
        if expected_status == 200:
            assert "request_uuid" in response.json
            assert isinstance(response.json.get("request_uuid"), str)

        # If expecting a 400 status
        elif expected_status == 400:
            assert "error" in response.json
            assert response.json["error"] == expected_response["error"]
