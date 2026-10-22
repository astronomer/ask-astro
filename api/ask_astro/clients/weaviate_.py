"""
This module provides configurations and initializations for the Weaviate client,
as well as text embeddings using the OpenAIEmbeddings from the LangChain library.
"""
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

import tomllib
import weaviate
from langchain.vectorstores import Weaviate
from weaviate import Client as WeaviateClient

from ask_astro.config import WeaviateConfig


def _ask_astro_version() -> str:
    """Return the ask-astro version from package metadata or pyproject.toml, or "unknown"."""
    try:
        return version("ask-astro")
    except PackageNotFoundError:
        pass
    try:
        # The API image installs dependencies with `--no-root`, so read the version from the copied pyproject.toml.
        pyproject = Path(__file__).resolve().parents[2] / "pyproject.toml"
        return tomllib.loads(pyproject.read_text())["tool"]["poetry"]["version"]
    except (OSError, KeyError, tomllib.TOMLDecodeError):
        return "unknown"


# Configure and initialize the Weaviate client.
client = WeaviateClient(
    url=WeaviateConfig.url,
    auth_client_secret=weaviate.AuthApiKey(api_key=WeaviateConfig.api_key),
    additional_headers={
        # Lets Weaviate attribute requests to this integration (read by Weaviate >= 1.38).
        "X-Weaviate-Client-Integration": f"ask-astro/{_ask_astro_version()}",
        "X-Openai-Api-Key": WeaviateConfig.OpenAIApiKey,
    },
)

# Create a Weaviate instance for search functionality using the initialized client.
docsearch = Weaviate(
    client=client,
    index_name=WeaviateConfig.index_name,
    text_key=WeaviateConfig.text_key,
    attributes=WeaviateConfig.attributes,
)
