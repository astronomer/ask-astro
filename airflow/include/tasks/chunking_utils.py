from __future__ import annotations

import logging

import pandas as pd
import tiktoken
from langchain.schema import Document
from langchain.text_splitter import (
    RecursiveCharacterTextSplitter,
)
from langchain_community.document_transformers import Html2TextTransformer

logger = logging.getLogger("airflow.task")

TARGET_CHUNK_SIZE = 2500


def enforce_max_token_len(text: str) -> str:
    encoding = tiktoken.get_encoding("cl100k_base")
    encoded_text = encoding.encode(text)
    if len(encoded_text) > 8191:
        logger.info("Token length of string exceeds the max content length of the tokenizer. Truncating...")
        return encoding.decode(encoded_text[:8191])
    return text


def split_markdown(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """
    This task concatenates multiple dataframes from upstream dynamic tasks and splits the documents before importing
    to a vector database.

    param dfs: A list of dataframes from downstream dynamic tasks
    type dfs: list[pd.DataFrame]

    Returned dataframe fields are:
    'docSource': ie. 'astro', 'learn', 'docs', etc.
    'sha': the github sha for the document
    'docLink': URL for the specific document in github.
    'content': Chunked content in markdown format.

    """

    df = pd.concat(dfs, axis=0, ignore_index=True)

    # directly from the langchain splitter library
    separators = ["\n\n", "\n", " ", ""]
    splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
        # cl100k_base is used for text ada 002 and later embedding models
        encoding_name="cl100k_base",
        chunk_size=TARGET_CHUNK_SIZE,
        chunk_overlap=200,
        separators=separators,
        is_separator_regex=True,
    )

    df["doc_chunks"] = df["content"].apply(lambda x: splitter.split_documents([Document(page_content=x)]))
    df = df.explode("doc_chunks", ignore_index=True)
    df["content"] = df["doc_chunks"].apply(lambda x: x.page_content)

    # Remove blank doc chunks
    df = df[~df["content"].apply(lambda x: x.isspace() or x == "")]

    df["content"] = df["content"].apply(enforce_max_token_len)
    df.drop(["doc_chunks"], inplace=True, axis=1)
    df.reset_index(inplace=True, drop=True)

    return df


def split_python(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """
    This task concatenates multiple dataframes from upstream dynamic tasks and splits python code before importing
    to a vector database.

    param dfs: A list of dataframes from downstream dynamic tasks
    type dfs: list[pd.DataFrame]

    Returned dataframe fields are:
    'docSource': ie. 'astro', 'learn', 'docs', etc.
    'sha': the github sha for the document
    'docLink': URL for the specific document in github.
    'content': Chunked content in markdown format.

    """

    df = pd.concat(dfs, axis=0, ignore_index=True)

    # directly from the langchain splitter library
    python_separators = [
        # First, try to split along class definitions
        "\nclass ",
        "\ndef ",
        "\n\tdef ",
        # Now split by the normal type of lines
        "\n\n",
        "\n",
        " ",
        "",
    ]

    splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
        # cl100k_base is used for text ada 002 and later embedding models
        encoding_name="cl100k_base",
        chunk_size=TARGET_CHUNK_SIZE,
        chunk_overlap=200,
        separators=python_separators,
        is_separator_regex=True,
    )

    df["doc_chunks"] = df["content"].apply(lambda x: splitter.split_documents([Document(page_content=x)]))
    df = df.explode("doc_chunks", ignore_index=True)
    df["content"] = df["doc_chunks"].apply(lambda x: x.page_content)

    # Remove blank doc chunks
    df = df[~df["content"].apply(lambda x: x.isspace() or x == "")]

    df["content"] = df["content"].apply(enforce_max_token_len)
    df.drop(["doc_chunks"], inplace=True, axis=1)
    df.reset_index(inplace=True, drop=True)

    return df


def split_html(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """
    This task concatenates multiple dataframes from upstream dynamic tasks and splits html code before importing
    to a vector database.

    param dfs: A list of dataframes from downstream dynamic tasks
    type dfs: list[pd.DataFrame]

    Returned dataframe fields are:
    'docSource': ie. 'astro', 'learn', 'docs', etc.
    'sha': the github sha for the document
    'docLink': URL for the specific document in github.
    'content': Chunked content in markdown format.

    """
    separators = ["<h1", "<h2", "<code", " ", ""]

    df = pd.concat(dfs, axis=0, ignore_index=True)

    splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
        # cl100k_base is used for text ada 002 and later embedding models
        encoding_name="cl100k_base",
        chunk_size=TARGET_CHUNK_SIZE,
        chunk_overlap=200,
        separators=separators,
        is_separator_regex=True,
    )

    # Split by chunking first
    df["doc_chunks"] = df["content"].apply(lambda x: splitter.split_text(text=x))
    df = df.explode("doc_chunks", ignore_index=True)

    # Remove HTML tags and transform HTML formatting to text
    html2text = Html2TextTransformer()
    df["content"] = df["doc_chunks"].apply(lambda x: html2text.transform_documents([Document(page_content=x)]))
    df["content"] = df["content"].apply(lambda x: x[0].page_content)

    # Remove blank doc chunks
    df = df[~df["content"].apply(lambda x: x.isspace() or x == "")]

    df["content"] = df["content"].apply(enforce_max_token_len)
    df.drop(["doc_chunks"], inplace=True, axis=1)
    df.reset_index(inplace=True, drop=True)

    return df
