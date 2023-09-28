import pandas as pd
from typing import List

from airflow.decorators import task

from langchain.text_splitter import (
    RecursiveCharacterTextSplitter,
    Language,
)
from langchain.schema import Document

# @task
def split_markdown(df:pd.DataFrame):
    """
    This task concatenates multiple dataframes from upstream dynamic tasks and 
    splits the documents before import

    Dataframe fields are:
    'docSource': ie. 'astro', 'learn', 'docs', etc.
    'sha': the github sha for the document
    'docLink': URL for the specific document in github.
    'content': Chunked content in markdown format.

    """
    
    df = pd.concat(df, axis=0, ignore_index=True)

    splitter = RecursiveCharacterTextSplitter()
    
    df['doc_chunks'] = df['content'].apply(lambda x: splitter.split_documents([Document(page_content=x)]))
    df = df.explode('doc_chunks', ignore_index=True)
    df['content'] = df['doc_chunks'].apply(lambda x: x.page_content)
    df.drop(['doc_chunks'], inplace=True, axis=1)
    df.reset_index(inplace=True, drop=True)

    return df

# @task
def split_python(df:pd.DataFrame):
    """
    This task concatenates multiple dataframes from upstream dynamic tasks and 
    splits python code

    Dataframe fields are:
    'docSource': ie. 'astro', 'learn', 'docs', etc.
    'sha': the github sha for the document
    'docLink': URL for the specific document in github.
    'content': Chunked content in markdown format.

    """

    df = pd.concat(df, axis=0, ignore_index=True)

    splitter = RecursiveCharacterTextSplitter.from_language(language=Language.PYTHON, 
                                                            # chunk_size=50, 
                                                            chunk_overlap=0)
    
    df['doc_chunks'] = df['content'].apply(lambda x: splitter.split_documents([Document(page_content=x)]))
    df = df.explode('doc_chunks', ignore_index=True)
    df['content'] = df['doc_chunks'].apply(lambda x: x.page_content)
    df.drop(['doc_chunks'], inplace=True, axis=1)
    df.reset_index(inplace=True, drop=True)

    return df
