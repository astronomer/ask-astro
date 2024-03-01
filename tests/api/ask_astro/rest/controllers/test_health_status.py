from unittest.mock import patch

import pytest


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "show_maintenance_banner, expected_response",
    [
        (True, {"status": "maintenance"}),
        (False, {"status": "healthy"}),
    ],
)
async def test_health_status(app, show_maintenance_banner, expected_response):
    """
    Test the /health_status endpoint by mocking banner status environment variable
    """

    with patch("ask_astro.settings.SHOW_SERVICE_MAINTENANCE_BANNER", new=show_maintenance_banner):
        _, response = await app.asgi_client.get("/health_status")

        # Validating the response status code and content
        assert response.status == 200
        assert response.json == expected_response
