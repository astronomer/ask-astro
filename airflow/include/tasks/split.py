from __future__ import annotations

import pandas as pd
from langchain.schema import Document
from langchain.text_splitter import (
    Language,
    RecursiveCharacterTextSplitter,
)
from langchain_community.document_transformers import Html2TextTransformer


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

    splitter = RecursiveCharacterTextSplitter(chunk_size=4000, chunk_overlap=200, separators=["\n\n", "\n", " ", ""])

    df["doc_chunks"] = df["content"].apply(lambda x: splitter.split_documents([Document(page_content=x)]))
    df = df.explode("doc_chunks", ignore_index=True)
    df["content"] = df["doc_chunks"].apply(lambda x: x.page_content)
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

    splitter = RecursiveCharacterTextSplitter.from_language(
        language=Language.PYTHON,
        # chunk_size=50,
        chunk_overlap=0,
    )

    df["doc_chunks"] = df["content"].apply(lambda x: splitter.split_documents([Document(page_content=x)]))
    df = df.explode("doc_chunks", ignore_index=True)
    df["content"] = df["doc_chunks"].apply(lambda x: x.page_content)
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
        chunk_size=4000,
        chunk_overlap=200,
        separators=separators,
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

    df.drop(["doc_chunks"], inplace=True, axis=1)
    df.reset_index(inplace=True, drop=True)

    return df


def split_list(urls: list[str], chunk_size: int = 0) -> list[list[str]]:
    """
    split the list of string into chunk of list of string

    param urls: URL list we want to chunk
    param chunk_size: Max size of chunked list
    """
    return [urls[i : i + chunk_size] for i in range(0, len(urls), chunk_size)]
