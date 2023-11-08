from __future__ import annotations

import requests


class APIMonitoring:
    """
    A class to test API endpoints using GET or POST requests and ensure that they return HTTP 200 status codes.
    """

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url
        if not self.base_url:
            raise ValueError("ASK_ASTRO_API_BASE_URL cannot be empty.")

    def test_endpoint(
        self, endpoint: str, method: str = "GET", data: dict | None = None, headers: dict | None = None
    ) -> int:
        """
        Test an endpoint with the specified method, data, and headers.

        :param endpoint: The endpoint to test.
        :param method: The HTTP method to use. Defaults to 'GET'.
        :param data: The data to send in the request. Defaults to None.
        :param headers: The headers to send in the request. Defaults to None.
        """
        url = f"{self.base_url}{endpoint}"
        if method.upper() == "GET":
            response = requests.get(url, headers=headers)
        elif method.upper() == "POST":
            response = requests.post(url, json=data, headers=headers)
        else:
            raise ValueError("Unsupported HTTP method.")

        response.raise_for_status()  # Will raise an exception for non-200 responses
        return response.status_code
