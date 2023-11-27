from __future__ import annotations

import logging
from typing import Any

import pandas as pd
import requests
from weaviate.exceptions import UnexpectedStatusCodeException
from weaviate.util import generate_uuid5

from airflow.exceptions import AirflowException
from airflow.providers.weaviate.hooks.weaviate import WeaviateHook


class AskAstroWeaviateHook(WeaviateHook):
    """
    Extends the WeaviateHook to include specific methods for handling Ask-Astro.

    This hook will be directly utilize the functionalities provided by Weaviate providers in
    upcoming releases of the `apache-airflow-providers-weaviate` package.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.batch_errors = []
        self.logger = logging.getLogger("airflow.task")
        self.client = self.get_client()

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
            error_msg = f"Error during schema check {e}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

    def check_schema(self, class_objects: list) -> bool:
        """
        Verifies if the current schema includes the requested schema.

        :param class_objects: Class objects to be checked against the current schema.
        """
        try:
            missing_objects = [obj["class"] for obj in class_objects if self.is_class_missing(obj)]

            if missing_objects:
                self.logger.warning(f"Classes {missing_objects} are not in the current schema.")
                return False
            else:
                self.logger.info("All classes are present in the current schema.")
                return True
        except Exception as e:
            error_msg = f"Error during schema check {e}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

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

    def generate_uuids(
        self,
        df: pd.DataFrame,
        class_name: str,
        column_subset: list[str] | None = None,
        vector_column: str | None = None,
        uuid_column: str | None = None,
    ) -> tuple[pd.DataFrame, str]:
        """
        Adds UUIDs to a DataFrame, useful for upsert operations where UUIDs must be known before ingestion.
        By default, UUIDs are generated using a custom function if 'uuid_column' is not specified.
        The function can potentially ingest the same data multiple times with different UUIDs.

        :param df: A dataframe with data to generate a UUID from.
        :param class_name: The name of the class use as part of the uuid namespace.
        :param uuid_column: Name of the column to create. Default is 'id'.
        :param column_subset: A list of columns to use for UUID generation. By default, all columns except
            vector_column will be used.
        :param vector_column: Name of the column containing the vector data.  If specified the vector will be
            removed prior to generating the uuid.
        """
        column_names = df.columns.to_list()

        column_subset = column_subset or column_names
        column_subset.sort()

        if uuid_column is None:
            self.logger.info(f"No uuid_column provided. Generating UUIDs as column name {uuid_column}.")
            df = df[column_names]
            if "id" in column_names:
                raise AirflowException("Property 'id' already in dataset. Consider renaming or specify 'uuid_column'.")
            else:
                uuid_column = "id"

        if uuid_column in column_names:
            raise AirflowException(
                f"Property {uuid_column} already in dataset. Consider renaming or specify a different 'uuid_column'."
            )

        df[uuid_column] = (
            df[column_subset]
            .drop(columns=[vector_column], inplace=False, errors="ignore")
            .apply(lambda row: generate_uuid5(identifier=row.to_dict(), namespace=class_name), axis=1)
        )

        return df, uuid_column

    def identify_upsert_targets(
        self, df: pd.DataFrame, class_name: str, doc_key: str, uuid_column: str
    ) -> pd.DataFrame:
        """
        Handles the 'upsert' operation for data ingestion.

        :param df: The DataFrame containing the data to be upserted.
        :param class_name: The name of the class to import data.
        :param doc_key: The document key used for upsert operation. This is a property of the data that
            uniquely identifies all chunks associated with one document.
        :param uuid_column: The column name containing the UUID.
        """
        if doc_key is None or doc_key not in df.columns:
            raise AirflowException("Specified doc_key is not specified or not in the dataset.")

        if uuid_column is None or uuid_column not in df.columns:
            raise AirflowException("Specified uuid_column is not specified or not in the dataset.")

        df = df.drop_duplicates(subset=[doc_key, uuid_column], keep="first")

        current_schema = self.client.schema.get(class_name=class_name)
        doc_key_schema = [prop for prop in current_schema["properties"] if prop["name"] == doc_key]

        if not doc_key_schema:
            raise AirflowException("doc_key does not exist in current schema.")
        elif doc_key_schema[0]["tokenization"] != "field":
            raise AirflowException("Tokenization for provided doc_key is not set to 'field'. Cannot upsert safely.")

        ids_df = df.groupby(doc_key)[uuid_column].apply(set).reset_index(name="new_ids")
        ids_df["existing_ids"] = ids_df[doc_key].apply(
            lambda x: self._query_objects(value=x, doc_key=doc_key, uuid_column=uuid_column, class_name=class_name)
        )

        ids_df["objects_to_insert"] = ids_df.apply(lambda x: list(x.new_ids.difference(x.existing_ids)), axis=1)
        ids_df["objects_to_delete"] = ids_df.apply(lambda x: list(x.existing_ids.difference(x.new_ids)), axis=1)
        ids_df["unchanged_objects"] = ids_df.apply(lambda x: x.new_ids.intersection(x.existing_ids), axis=1)

        return ids_df[[doc_key, "objects_to_insert", "objects_to_delete", "unchanged_objects"]]

    def batch_ingest(
        self,
        df: pd.DataFrame,
        class_name: str,
        uuid_column: str,
        existing: str,
        vector_column: str | None = None,
        batch_params: dict = {},
        verbose: bool = False,
        tenant: str | None = None,
    ) -> (list, Any):
        """
        Processes the DataFrame and batches the data for ingestion into Weaviate.

        :param df: DataFrame containing the data to be ingested.
        :param class_name: The name of the class in Weaviate to which data will be ingested.
        :param uuid_column: Name of the column containing the UUID.
        :param vector_column: Name of the column containing the vector data.
        :param batch_params: Parameters for batch configuration.
        :param existing: Strategy to handle existing data ('skip', 'replace', 'upsert' or 'error').
        :param verbose: Whether to log verbose output.
        :param tenant: The tenant to which the object will be added.
        """

        # configuration for context manager for __exit__ method to callback on errors for weaviate batch ingestion.
        if not batch_params.get("callback"):
            batch_params.update({"callback": self.process_batch_errors})

        self.client.batch.configure(**batch_params)

        with self.client.batch as batch:
            for row_id, row in df.iterrows():
                data_object = row.to_dict()
                uuid = data_object.pop(uuid_column)
                vector = data_object.pop(vector_column, None)

                try:
                    if self.client.data_object.exists(uuid=uuid, class_name=class_name):
                        if existing == "error":
                            raise AirflowException(f"Ingest of UUID {uuid} failed.  Object exists.")
                        if existing == "skip":
                            if verbose is True:
                                self.logger.warning(f"UUID {uuid} exists.  Skipping.")
                            continue
                        elif existing == "replace":
                            # Default for weaviate is replace existing
                            if verbose is True:
                                self.logger.warning(f"UUID {uuid} exists.  Overwriting.")

                except Exception as e:
                    if isinstance(e, AirflowException):
                        self.logger.error(f"Failed to add row {row_id} with UUID {uuid}. Error: {e}")
                        self.batch_errors.append({"uuid": uuid, "result": {"errors": str(e)}})
                        break
                    else:
                        if verbose:
                            self.logger.error(f"Failed to add row {row_id} with UUID {uuid}. Error: {e}")
                        self.batch_errors.append({"uuid": uuid, "result": {"errors": str(e)}})
                        continue

                try:
                    added_row = batch.add_data_object(
                        class_name=class_name, uuid=uuid, data_object=data_object, vector=vector, tenant=tenant
                    )
                    if verbose is True:
                        self.logger.info(f"Added row {row_id} with UUID {added_row} for batch import.")

                except Exception as e:
                    if verbose:
                        self.logger.error(f"Failed to add row {row_id} with UUID {uuid}. Error: {e}")
                    self.batch_errors.append({"uuid": uuid, "result": {"errors": str(e)}})

        return self.batch_errors

    def process_batch_errors(self, results: list, verbose: bool = True) -> None:
        """
        Processes the results from batch operation and collects any errors.

        :param results: Results from the batch operation.
        :param verbose: Flag to enable verbose logging.
        """
        for item in results:
            if "errors" in item["result"]:
                item_error = {"uuid": item["id"], "errors": item["result"]["errors"]}
                if verbose:
                    self.logger.info(
                        f"Error occurred in batch process for {item['id']} with error {item['result']['errors']}"
                    )
                self.batch_errors.append(item_error)

    def handle_upsert_rollback(
        self, objects_to_upsert: pd.DataFrame, class_name: str, verbose: bool, tenant: str | None = None
    ) -> tuple[list, set]:
        """
        Handles rollback of inserts in case of errors during upsert operation.

        :param objects_to_upsert: Dictionary of objects to upsert.
        :param class_name: Name of the class in Weaviate.
        :param verbose: Flag to enable verbose logging.
        :param tenant: The tenant to which the object will be added.
        """
        rollback_errors = []

        error_uuids = {error["uuid"] for error in self.batch_errors}

        objects_to_upsert["rollback_doc"] = objects_to_upsert.objects_to_insert.apply(
            lambda x: any(error_uuids.intersection(x))
        )

        objects_to_upsert["successful_doc"] = objects_to_upsert.objects_to_insert.apply(
            lambda x: error_uuids.isdisjoint(x)
        )

        rollback_objects = objects_to_upsert[objects_to_upsert.rollback_doc].objects_to_insert.to_list()
        rollback_objects = {item for sublist in rollback_objects for item in sublist}

        delete_objects = objects_to_upsert[objects_to_upsert.successful_doc].objects_to_delete.to_list()
        delete_objects = {item for sublist in delete_objects for item in sublist}

        for uuid in rollback_objects:
            try:
                if self.client.data_object.exists(uuid=uuid, class_name=class_name, tenant=tenant):
                    self.logger.info(f"Removing id {uuid} for rollback.")
                    self.client.data_object.delete(
                        uuid=uuid, class_name=class_name, tenant=tenant, consistency_level="ALL"
                    )
                elif verbose:
                    self.logger.info(f"UUID {uuid} does not exist. Skipping deletion during rollback.")
            except Exception as e:
                rollback_errors.append({"uuid": uuid, "result": {"errors": str(e)}})
                if verbose:
                    self.logger.info(f"Error in rolling back id {uuid}. Error: {str(e)}")

        return rollback_errors, delete_objects

    def handle_successful_upsert(
        self, objects_to_remove: list, class_name: str, verbose: bool, tenant: str | None = None
    ) -> list:
        """
        Handles removal of previous objects after successful upsert.

        :param class_name: Name of the class in Weaviate.
        :param objects_to_remove: If there were errors rollback will generate a list of successfully inserted objects.
         If not set, assume all objects inserted successfully and delete all objects_to_upsert['objects_to_delete']
        :param tenant:  The tenant to which the object will be added.
        :param verbose: Flag to enable verbose logging.
        """
        deletion_errors = []
        for uuid in objects_to_remove:
            try:
                if self.client.data_object.exists(uuid=uuid, class_name=class_name, tenant=tenant):
                    if verbose:
                        self.logger.info(f"Deleting id {uuid} for successful upsert.")
                    self.client.data_object.delete(
                        uuid=uuid, class_name=class_name, tenant=tenant, consistency_level="ALL"
                    )
                elif verbose:
                    self.logger.info(f"UUID {uuid} does not exist. Skipping deletion.")
            except Exception as e:
                deletion_errors.append({"uuid": uuid, "result": {"errors": str(e)}})
                if verbose:
                    self.logger.info(f"Error in rolling back id {uuid}. Error: {str(e)}")
        return deletion_errors

    def ingest_data(
        self,
        dfs: list[pd.DataFrame] | pd.DataFrame,
        class_name: str,
        existing: str = "skip",
        doc_key: str = None,
        uuid_column: str = None,
        vector_column: str = None,
        batch_params: dict = None,
        verbose: bool = True,
        tenant: str | None = None,
    ) -> list:
        """
        Ingests data into Weaviate, handling upserts and rollbacks, and returns a list of objects that failed to import.
        This function ingests data from pandas DataFrame(s) into a specified class in Weaviate. It supports various
        modes of handling existing data (upsert, skip, replace). Upsert logic uses 'doc_key' as a unique document
        identifier, enabling document-level atomicity during ingestion. Rollback is performed for any document
        encountering errors during ingest. The function returns a list of objects that failed to import for further
        handling.

        :param dfs: A single pandas DataFrame or a list of pandas DataFrames to be ingested.
        :param class_name: Name of the class in Weaviate schema where data is to be ingested.
        :param existing: Strategy for handling existing data: 'upsert', 'skip', or 'replace'. Default is 'skip'.
        :param doc_key: Column in DataFrame uniquely identifying each document, required for 'upsert' operations.
        :param uuid_column: Column with pre-generated UUIDs. If not provided, UUIDs will be generated.
        :param vector_column: Column with embedding vectors for pre-embedded data.
        :param batch_params: Additional parameters for Weaviate batch configuration.
        :param verbose: Flag to enable verbose output during the ingestion process.
        :param tenant: The tenant to which the object will be added.
        """

        global objects_to_upsert
        if existing not in ["skip", "replace", "upsert", "error"]:
            raise AirflowException(
                "Invalid parameter for 'existing'. Choices are 'skip', 'replace', 'upsert', 'error'."
            )

        df = pd.concat(dfs, ignore_index=True)

        if uuid_column is None:
            df, uuid_column = self.generate_uuids(
                df=df, class_name=class_name, vector_column=vector_column, uuid_column=uuid_column
            )

        if existing == "upsert" or existing == "skip":
            objects_to_upsert = self.identify_upsert_targets(
                df=df, class_name=class_name, doc_key=doc_key, uuid_column=uuid_column
            )

            objects_to_insert = {item for sublist in objects_to_upsert.objects_to_insert.tolist() for item in sublist}

            # subset df with only objects that need to be inserted
            df = df[df[uuid_column].isin(objects_to_insert)]

        self.logger.info(f"Passing {len(df)} objects for ingest.")

        self.batch_ingest(
            df=df,
            class_name=class_name,
            uuid_column=uuid_column,
            vector_column=vector_column,
            batch_params=batch_params,
            existing=existing,
            verbose=verbose,
            tenant=tenant,
        )

        if existing == "upsert":
            if len(self.batch_errors) > 0:
                self.logger.warning("Error during upsert. Rolling back all inserts for docs with errors.")
                rollback_errors, objects_to_remove = self.handle_upsert_rollback(
                    objects_to_upsert=objects_to_upsert, class_name=class_name, verbose=verbose
                )

                deletion_errors = self.handle_successful_upsert(
                    objects_to_remove=objects_to_remove, class_name=class_name, verbose=verbose
                )

                rollback_errors += deletion_errors

                if len(rollback_errors) > 0:
                    self.logger.error("Errors encountered during rollback.")
                    [self.logger.error(f"{rollback_error}" for rollback_error in rollback_errors)]
                    raise AirflowException("Errors encountered during rollback.")
        else:
            removal_errors = self.handle_successful_upsert(
                objects_to_remove={item for sublist in objects_to_upsert.objects_to_delete for item in sublist},
                class_name=class_name,
                verbose=verbose,
                tenant=tenant,
            )
            if removal_errors:
                self.logger.error("Errors encountered during removal.")
                [self.logger.error(f"{removal_error}" for removal_error in removal_errors)]
                raise AirflowException("Errors encountered during removal.")

        if self.batch_errors:
            self.logger.error("Errors encountered during ingest.")
            [self.logger.error(f"{batch_error}" for batch_error in self.batch_errors)]
            raise AirflowException("Errors encountered during ingest.")

    def _query_objects(self, value: Any, doc_key: str, class_name: str, uuid_column: str) -> set:
        """
        Check for existence of a data_object as a property of a data class and return all object ids.

        :param value: The value of the property to query.
        :param doc_key: The name of the property to query.
        :param class_name: The name of the class to query.
        :param uuid_column: The name of the column containing the UUID.
        """
        existing_uuids = (
            self.client.query.get(properties=[doc_key], class_name=class_name)
            .with_additional([uuid_column])
            .with_where({"path": doc_key, "operator": "Equal", "valueText": value})
            .do()["data"]["Get"][class_name]
        )

        return {additional["_additional"]["id"] for additional in existing_uuids}

    def import_baseline(
        self,
        seed_baseline_url: str,
        class_name: str,
        existing: str = "skip",
        doc_key: str = None,
        uuid_column: str = None,
        vector_column: str = None,
        batch_params: dict = None,
        verbose: bool = True,
    ) -> list:
        """
        This task ingests data from a baseline of pre-embedded data. This is useful for evaluation and baselining changes
        over time.  This function is used as a python_callable with the weaviate_import decorator.  The returned
        dictionary is passed to the WeaviateImportDataOperator for ingest. The operator returns a list of any objects
        that failed to import. seed_baseline_url is a URI for a parquet file of pre-embedded data. Any existing
        documents are replaced.  The assumption is that this is a first import of data and older data
        should be removed.

        :param class_name: The name of the class to import data.  Class should be created with weaviate schema.
        :param seed_baseline_url: The url of a parquet file containing baseline data to ingest.
        :param vector_column: For pre-embedded data specify the name of the column containing the embedding vector
        :param uuid_column: For data with pre-generated UUID specify the name of the column containing the UUID
        :param batch_params: Additional parameters to pass to the weaviate batch configuration
        :param verbose: Whether to print verbose output
        :param existing: Whether to 'upsert', 'skip' or 'replace' any existing documents.  Default is 'skip'.
        :param doc_key: If using upsert you must specify a doc_key which uniquely identifies a document which may or may
         not include multiple (unique) chunks.
        """

        seed_filename = f"include/data/{seed_baseline_url.split('/')[-1]}"

        try:
            df = pd.read_parquet(seed_filename)

        except Exception:
            with open(seed_filename, "wb") as fh:
                response = requests.get(seed_baseline_url, stream=True)
                fh.writelines(response.iter_content(1024))

            df = pd.read_parquet(seed_filename)

        return self.ingest_data(
            dfs=df,
            class_name=class_name,
            existing=existing,
            doc_key=doc_key,
            uuid_column=uuid_column,
            vector_column=vector_column,
            verbose=verbose,
            batch_params=batch_params,
        )
