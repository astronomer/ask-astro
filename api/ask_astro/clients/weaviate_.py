"""
This module provides configurations and initializations for the Weaviate client,
as well as text embeddings using the OpenAIEmbeddings from the LangChain library.
"""
import weaviate
from langchain.vectorstores import Weaviate
from weaviate import Client as WeaviateClient

from ask_astro.config import WeaviateConfig

# Configure and initialize the Weaviate client.
client = WeaviateClient(
    url=WeaviateConfig.url,
    auth_client_secret=weaviate.AuthApiKey(api_key=WeaviateConfig.api_key),
    additional_headers={
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
