from __future__ import annotations

import pandas as pd
from langchain.schema import Document
from langchain.text_splitter import (
    HTMLHeaderTextSplitter,
    Language,
    RecursiveCharacterTextSplitter,
)


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

    headers_to_split_on = [
        ("h2", "h2"),
    ]

    df = pd.concat(dfs, axis=0, ignore_index=True)

    splitter = HTMLHeaderTextSplitter(headers_to_split_on)

    df["doc_chunks"] = df["content"].apply(lambda x: splitter.split_text(text=x))
    df = df.explode("doc_chunks", ignore_index=True)
    df["content"] = df["doc_chunks"].apply(lambda x: x.page_content)

    df.drop(["doc_chunks"], inplace=True, axis=1)
    df.reset_index(inplace=True, drop=True)

    return df
