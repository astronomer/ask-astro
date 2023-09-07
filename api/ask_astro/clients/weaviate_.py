import weaviate
from weaviate import Client as WeaviateClient
from ask_astro.config import AzureOpenAIParams, WeaviateConfig
from langchain.embeddings import OpenAIEmbeddings
from langchain.vectorstores import Weaviate

embeddings = OpenAIEmbeddings(
    **AzureOpenAIParams.us_east,
    deployment="text-embedding-ada-002",
    model="text-embedding-ada-002",
)

client = WeaviateClient(
    url=WeaviateConfig.url,
    auth_client_secret=weaviate.AuthApiKey(api_key=WeaviateConfig.api_key),
    additional_headers={
        "X-Openai-Api-Key": WeaviateConfig.OpenAIApiKey,
    },
)
docsearch = Weaviate(
    client=client,
    index_name=WeaviateConfig.index_name,
    text_key=WeaviateConfig.text_key,
    attributes=WeaviateConfig.attributes,
)
