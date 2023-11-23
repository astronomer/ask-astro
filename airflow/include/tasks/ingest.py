from __future__ import annotations

import pandas as pd
import requests


def import_baseline(class_name: str, seed_baseline_url: str) -> list:
    """
    This task ingests data from a baseline of pre-embedded data. This is useful for evaluation and baselining changes
    over time.  This function is used as a python_callable with the weaviate_import decorator.  The returned
    dictionary is passed to the WeaviateImportDataOperator for ingest. The operator returns a list of any objects
    that failed to import.

    seed_baseline_url is a URI for a parquet file of pre-embedded data.

    Any existing documents are replaced. The assumption is that this is a first import of data and older data
    should be removed.

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
        "existing": "replace",
        "uuid_column": "id",
        "embedding_column": "vector",
        "error_threshold": 0,
        "batched_mode": True,
        "batch_size": 2000,
        "verbose": True,
    }
