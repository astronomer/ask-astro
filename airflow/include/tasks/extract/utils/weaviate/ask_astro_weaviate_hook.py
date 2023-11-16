from __future__ import annotations

import json
import logging
import os
from typing import Any

from weaviate.exceptions import UnexpectedStatusCodeException

from airflow.providers.weaviate.hooks.weaviate import WeaviateHook

WEAVIATE_CLASS = os.environ.get("WEAVIATE_CLASS", "DocsDevAnkit")


class AskAstroWeaviateHook(WeaviateHook):
    """Extends the WeaviateHook to include specific methods for handling Ask-Astro."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger("airflow.task")
        self.client = self.get_client()

    def get_schema(self, schema_file: str) -> list:
        """
        Reads and processes the schema from a JSON file.

        :param schema_file: path to the schema JSON file
        """
        try:
            with open(schema_file) as file:
                schema_data = json.load(file)
        except FileNotFoundError:
            self.logger.error(f"Schema file {schema_file} not found.")
            raise
        except json.JSONDecodeError:
            self.logger.error(f"Invalid JSON in the schema file {schema_file}.")
            raise

        classes = schema_data.get("classes", [schema_data])
        for class_object in classes:
            class_object.update({"class": WEAVIATE_CLASS})

        self.logger.info("Schema processing completed.")
        return classes

    def compare_schema_subset(self, class_object: Any, class_schema: Any) -> bool:
        """
        Recursively check if requested schema/object is a subset of the current schema.

        :param class_object: The class object to check against current schema
        :param class_schema: The current schema class object
        """

        # Direct equality check
        if class_object == class_schema:
            return True

        # Type mismatch early return
        if type(class_object) != type(class_schema):
            return False

        # Dictionary comparison
        if isinstance(class_object, dict):
            return all(
                k in class_schema and self.compare_schema_subset(v, class_schema[k]) for k, v in class_object.items()
            )

        # List or Tuple comparison
        if isinstance(class_object, (list, tuple)):
            return len(class_object) == len(class_schema) and all(
                self.compare_schema_subset(obj, sch) for obj, sch in zip(class_object, class_schema)
            )

        # Default case for non-matching types or unsupported types
        return False

    def is_class_missing(self, class_object: dict) -> bool:
        """
        Checks if a class is missing from the schema.

        :param class_object: Class object to be checked against the current schema.
        """
        try:
            class_schema = self.client.schema.get(class_object.get("class", ""))
            return not self.compare_schema_subset(class_object=class_object, class_schema=class_schema)
        except UnexpectedStatusCodeException as e:
            return e.status_code == 404 and "with response body: None." in e.message
        except Exception as e:
            self.logger.error(f"Error checking schema: {e}")
            raise ValueError(f"Error during schema check {e}")

    def check_schema(self, class_objects: list) -> list[str]:
        """
        Verifies if the current schema includes the requested schema.

        :param class_objects: Class objects to be checked against the current schema.
        """
        try:
            missing_objects = [obj["class"] for obj in class_objects if self.is_class_missing(obj)]

            if missing_objects:
                self.logger.warning(f"Classes {missing_objects} are not in the current schema.")
                return ["create_schema"]
            else:
                self.logger.info("All classes are present in the current schema.")
                return ["check_seed_baseline"]
        except Exception as e:
            self.logger.error(f"Error during schema check: {e}")
            raise ValueError(f"Error during schema check {e}")

    def create_schema(self, class_objects: list, existing: str = "ignore") -> None:
        """
        Creates or updates the schema in Weaviate based on the given class objects.

        :param class_objects: A list of class objects for schema creation or update.
        :param existing: Strategy to handle existing classes ('ignore' or 'replace'). Defaults to 'ignore'.
        """
        for class_object in class_objects:
            class_name = class_object.get("class", "")
            self.logger.info(f"Processing schema for class: {class_name}")

            try:
                current_class = self.client.schema.get(class_name=class_name)
            except Exception as e:
                self.logger.error(f"Error retrieving current class schema: {e}")
                current_class = None
            if current_class is not None and existing == "replace":
                self.logger.info(f"Replacing existing class {class_name}")
                self.client.schema.delete_class(class_name=class_name)

            if current_class is None or existing == "replace":
                self.client.schema.create_class(class_object)
                self.logger.info(f"Created/updated class {class_name}")
