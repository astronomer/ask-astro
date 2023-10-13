"""
Tasks for interacting with Weaviate.
"""

import time
from itertools import chain
from logging import getLogger

import pandas as pd
import weaviate
from include.config import Connections
from weaviate.util import generate_uuid5
from weaviate_provider.hooks.weaviate import WeaviateHook

from airflow.decorators import task

logger = getLogger(__name__)


@task()
def weaviate_upsert(outputs: list[list[dict[str, str]]], weaviate_class: str):
    """
    This task takes in a list of documents and upserts them into Weaviate. Note that
    Weaviate doesn't officially support upserting, so this first deletes any existing
    documents with the same docLink, then inserts the new documents.
    """
    docs = chain(*outputs)

    weaviate_hook = WeaviateHook(Connections.weaviate)
    client = weaviate_hook.get_conn()

    # the weaviate_import decorator expects a dataframe, so we need to convert
    df = pd.DataFrame(docs)
    df["uuid"] = df.apply(generate_uuid5, axis=1)

    def upsert(uuid: str, doc: dict[str, str], delay: int = 0):
        if delay > 0:
            logger.info("Sleeping for %s seconds", delay)
            time.sleep(delay)
        if delay > 60:
            raise Exception("Rate limit reached")

        try:
            if client.data_object.exists(uuid=uuid, class_name=weaviate_class):
                logger.info("Object with uuid %s exists, replacing", uuid)

                client.data_object.replace(uuid=uuid, class_name=weaviate_class, data_object=doc)
            else:
                logger.info("Object with uuid %s does not exist, inserting", uuid)

                client.data_object.create(class_name=weaviate_class, uuid=uuid, data_object=doc)
        except weaviate.UnexpectedStatusCodeException as e:
            if "Rate limit reached" in str(e):
                new_delay = delay * 2 if delay > 0 else 1
                logger.info("Rate limit reached, sleeping for %s seconds", new_delay)
                upsert(uuid, doc, new_delay)
            else:
                logger.error("Failed to insert document %s: %s", uuid, e)
        except Exception as e:
            logger.error("Failed to insert document %s: %s", uuid, e)

    for _, row in df.iterrows():
        doc = row.to_dict()
        uuid = doc.pop("uuid")

        upsert(uuid, doc)
