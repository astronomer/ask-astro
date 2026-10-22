import importlib
import sys
from unittest import mock


def test_weaviate_client_sends_integration_header():
    """The Weaviate client identifies ask-astro through the X-Weaviate-Client-Integration header."""
    sys.modules.pop("ask_astro.clients.weaviate_", None)
    with mock.patch("weaviate.Client") as client_cls, mock.patch("langchain.vectorstores.Weaviate"):
        weaviate_ = importlib.import_module("ask_astro.clients.weaviate_")
    sys.modules.pop("ask_astro.clients.weaviate_", None)

    headers = client_cls.call_args.kwargs["additional_headers"]
    assert headers["X-Weaviate-Client-Integration"] == f"ask-astro/{weaviate_._ask_astro_version()}"
    assert weaviate_._ask_astro_version() != "unknown"
    assert "X-Openai-Api-Key" in headers
