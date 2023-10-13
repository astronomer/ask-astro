import pandas as pd
from weaviate.util import generate_uuid5


def import_upsert_data(dfs: list[pd.DataFrame], class_name: str, primary_key: str):
    """
    This task concatenates multiple dataframes from upstream dynamic tasks and
    vectorizes with import to weaviate.

    A 'uuid' is generated based on the content and metadata (the git sha, document url,
    the document source (ie. astro) and a concatenation of the headers).

    Any existing documents with the same primary_key but differing UUID or sha will be
    deleted prior to import.
    """

    df = pd.concat(dfs, ignore_index=True)

    df["uuid"] = df.apply(lambda x: generate_uuid5(x.to_dict()), axis=1)

    print(f"Passing {len(df)} objects for import.")

    return {
        "data": df,
        "class_name": class_name,
        "upsert": True,
        "primary_key": primary_key,
        "uuid_column": "uuid",
        "error_threshold": 0,
    }


def import_data(dfs: list[pd.DataFrame], class_name: str):
    """
    This task concatenates multiple dataframes from upstream dynamic tasks and
    vectorizes with import to weaviate.

    A 'uuid' is generated based on the content and metadata (the git sha, document url,
    the document source (ie. astro) and a concatenation of the headers).

    Any existing documents are not upserted.  The assumption is that this is a first
    import of data and skipping upsert checks will speed up import.
    """

    df = pd.concat(dfs, ignore_index=True)

    df["uuid"] = df.apply(lambda x: generate_uuid5(x.to_dict()), axis=1)

    print(f"Passing {len(df)} objects for import.")

    return {"data": df, "class_name": class_name, "upsert": False, "uuid_column": "uuid", "error_threshold": 0}
