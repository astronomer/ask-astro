from __future__ import annotations

import pandas as pd
import requests
from weaviate.util import generate_uuid5


def import_upsert_data(dfs: list[pd.DataFrame], class_name: str, primary_key: str) -> list:
    """
    This task concatenates multiple dataframes from upstream dynamic tasks and vectorizes with import to weaviate.
    This function is used as a python_callable with the weaviate_import decorator.  The returned dictionary is passed
    to the WeaviateImportDataOperator for ingest.  The operator returns a list of any objects that failed to import.

    A 'uuid' is generated based on the content and metadata (the git sha, document url, the document source and a
    concatenation of the headers).

    Any existing documents with the same primary_key but differing UUID or sha will be deleted prior to import.

    param dfs: A list of dataframes from downstream dynamic tasks
    type dfs: list[pd.DataFrame]

    param class_name: The name of the class to import data.  Class should be created with weaviate schema.
    type class_name: str

    param primary_key: The name of a column to use as a primary key for upsert logic.
    type primary_key: str
    """

    df = pd.concat(dfs, ignore_index=True)

    df["uuid"] = df.apply(lambda x: generate_uuid5(identifier=x.to_dict(), namespace=class_name), axis=1)

    print(f"Passing {len(df)} objects for import.")

    return {
        "data": df,
        "class_name": class_name,
        "upsert": True,
        "primary_key": primary_key,
        "uuid_column": "uuid",
        "error_threshold": 0,
        "verbose": True,
    }


def import_data(dfs: list[pd.DataFrame], class_name: str) -> list:
    """
    This task concatenates multiple dataframes from upstream dynamic tasks and vectorizes with import to weaviate.
    This function is used as a python_callable with the weaviate_import decorator.  The returned dictionary is passed
    to the WeaviateImportDataOperator for ingest. The operator returns a list of any objects that failed to import.

    A 'uuid' is generated based on the content and metadata (the git sha, document url, the document source and a
    concatenation of the headers) and Weaviate will create the vectors.

    Any existing documents are not upserted.  The assumption is that this is a first
    import of data and skipping upsert checks will speed up import.

    param dfs: A list of dataframes from downstream dynamic tasks
    type dfs: list[pd.DataFrame]

    param class_name: The name of the class to import data.  Class should be created with weaviate schema.
    type class_name: str
    """

    df = pd.concat(dfs, ignore_index=True)

    df["uuid"] = df.apply(lambda x: generate_uuid5(identifier=x.to_dict(), namespace=class_name), axis=1)

    print(f"Passing {len(df)} objects for import.")

    return {
        "data": df,
        "class_name": class_name,
        "upsert": False,
        "uuid_column": "uuid",
        "error_threshold": 0,
        "batched_mode": True,
        "batch_size": 1000,
        "verbose": False,
    }


def import_baseline(class_name: str, seed_baseline_url: str) -> list:
    """
    This task ingests data from a baseline of pre-embedded data. This is useful for evaluation and baselining changes
    over time.  This function is used as a python_callable with the weaviate_import decorator.  The returned
    dictionary is passed to the WeaviateImportDataOperator for ingest. The operator returns a list of any objects
    that failed to import.

    seed_baseline_url is a URI for a parquet file of pre-embedded data.

    Any existing documents are not upserted.  The assumption is that this is a first import of data and skipping
    upsert checks will speed up import.

    param class_name: The name of the class to import data.  Class should be created with weaviate schema.
    type class_name: str

    param seed_baseline_url: The url of a parquet file containing baseline data to ingest.
    param seed_baseline_url: str
    """

    seed_filename = f"include/data/{seed_baseline_url.split('/')[-1]}"

    try:
        df = pd.read_parquet(seed_filename)

    except Exception:
        with open(seed_filename, "wb") as fh:
            response = requests.get(seed_baseline_url, stream=True)
            fh.writelines(response.iter_content(1024))

        df = pd.read_parquet(seed_filename)

    return {
        "data": df,
        "class_name": class_name,
        "upsert": False,
        "uuid_column": "id",
        "embedding_column": "vector",
        "error_threshold": 0,
        "batched_mode": True,
        "batch_size": 2000,
        "verbose": True,
    }
