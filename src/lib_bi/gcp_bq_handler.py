import io
import re
import polars as pl
import logging
from datetime import datetime
from google.cloud import bigquery
from google.auth.credentials import Credentials
from google.cloud.bigquery.table import RowIterator
from google.cloud.bigquery.job.base import _AsyncJob
from concurrent.futures import ThreadPoolExecutor


class GCPBigQueryHandler:
    def __init__(
        self, project: str | None = None, credentials: Credentials | None = None
    ) -> None:
        self.client = bigquery.Client(project=project, credentials=credentials)

    def execute_query(
        self,
        sql: str,
        timeout: int = 30,
        job_config: bigquery.QueryJobConfig | None = None,
        page_size: int | None = None,
    ) -> RowIterator:
        try:
            query_job = self.client.query(sql, job_config=job_config)
            return query_job.result(timeout=timeout, page_size=page_size)
        except Exception as error:
            logging.error(
                f"Executing query: {sql}\n The error is: {error}", stack_info=True
            )
            raise error

    def execute_query_to_df(
        self,
        sql: str,
        timeout: int = 30,
        job_config: bigquery.QueryJobConfig | None = None,
    ):
        try:
            result = self.execute_query(sql, timeout, job_config)
            return result.to_dataframe()
        except Exception as error:
            logging.error(
                f"Failed converting query result to pandas dataframe: {sql}\n The error is: {error}",
                stack_info=True,
            )
            raise error

    def execute_query_log_errs(
        self,
        sql: str,
        rows_to_insert: dict,
        key_error_name: str,
        key_datetime_name: str,
        dataset_id: str,
        table_id: str,
        timeout: int = 30,
        job_config: bigquery.QueryJobConfig | None = None,
        page_size: int | None = None,
    ) -> None:
        """
        Executes a SQL query and logs any errors that occur during execution to a BigQuery table.

        Args:
            sql (str): The SQL query to be executed.
            rows_to_insert (dict): The dictionary containing data to be inserted into the BigQuery table. This dictionary
                                will be updated with the error information and current timestamp if an error occurs.
            key_error_name (str): The key in `rows_to_insert` where the error message should be stored.
            key_datetime_name (str): The key in `rows_to_insert` where the current timestamp should be stored.
            dataset_id (str): The ID of the BigQuery dataset where the error log table is located.
            table_id (str): The ID of the BigQuery table where error information should be inserted.
            timeout (int, optional): The maximum amount of time in seconds the query is allowed to run before timing out.
                                    Default is 30 seconds.
            job_config (bigquery.QueryJobConfig, optional): Configuration options for the query job.
            page_size (int | None, optional): The number of rows to fetch at a time during iteration.

        Raises:
            Exception: If an error occurs during the execution of the SQL query, the error is caught and logged to the
                    specified BigQuery table.

        """
        try:
            self.execute_query(sql, timeout, job_config, page_size)
        except Exception as error:
            current_time = datetime.now()
            rows_to_insert[key_error_name] = error
            rows_to_insert[key_datetime_name] = current_time
            table_ref = self.client.dataset(dataset_id).table(table_id)
            self.client.insert_rows_json(table_ref, [rows_to_insert])

            raise error

    def export_table_to_storage(
        self,
        project: str,
        dataset_id: str,
        table_id: str,
        gcs_path: str,
        location="southamerica-west1",
        format_table=bigquery.DestinationFormat.CSV,
        compression=bigquery.Compression.GZIP,
    )-> _AsyncJob:
        """
        Exports a BigQuery table to a specified Cloud Storage destination.

        Args:
            project (str): GCP project ID.
            dataset_id (str): BigQuery dataset ID.
            table_id (str): BigQuery table ID.
            gcs_path (str): Cloud Storage destination URI (e.g., 'gs://my-bucket/my-file').
            location (str, optional): Location of the BigQuery table. Defaults to 'southamerica-west1'.
            compression (str, optional): Compression type for the exported file. Defaults to GZIP.

        Returns:
            ExtractJob.result: The result of the extract job.
        """
        dataset_ref = bigquery.DatasetReference(project, dataset_id)
        table_ref = dataset_ref.table(table_id)
        try:
            # Job configuration
            job_config = bigquery.ExtractJobConfig()
            job_config.destination_format = format_table
            job_config.compression = compression

            # API request to export table
            extract_job = self.client.extract_table(
                table_ref,
                gcs_path,
                location=location,
                job_config=job_config,
                timeout=10000,
            )

            # Wait for job completion
            result = extract_job.result()

            return result

        except Exception as error:
            logging.error(
                f"Failed to export table {table_ref} to {gcs_path}: {error}",
                stack_info=True,
            )
            raise error

    def load_df_to_table(self, path_table_name: str, df: pl.DataFrame):
        # Write DataFrame to stream as parquet file; does not hit disk
        with io.BytesIO() as stream:
            df.write_parquet(stream)
            stream.seek(0)
            job = self.client.load_table_from_file(
                stream,
                destination=path_table_name,
                project="mi-casino",
                job_config=bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.PARQUET,
                ),
            )
        try:
            job.result()
        except Exception as error:
            logging.error(f"Could not load the df to {path_table_name}: {error}")
            raise error

    def execute_query_to_df_polars(
        self,
        sql: str,
        timeout: int = 30,
        job_config: bigquery.QueryJobConfig | None = None,
    ) -> pl.DataFrame | None:
        try:
            results = self.execute_query(
                sql,
                timeout,
                job_config,
            ).to_dataframe()

            df = pl.from_pandas(results)

            return df
        except Exception as error:
            logging.error(
                f"Failed converting query result to polars dataframe: {sql}\n The error is: {error}",
                stack_info=True,
            )
            raise error

    def get_tables_name_with_regex(self, proyect_id, dataset_id, regex: re.Pattern):
        """
        Retrieves the full paths of tables in a BigQuery dataset that start with 'get_'.

        Args:
            proyecto_id (str): The ID of the Google Cloud project.
            dataset_id (str): The ID of the BigQuery dataset.

        Returns:
            list: A list of full paths for tables that start with 'get_'.
        """

        # Get the dataset reference
        dataset_ref = self.client.dataset(dataset_id, proyect_id)
        tables = self.client.list_tables(dataset_ref)

        # Filter tables that start with 'get_'
        get_tables = [
            table.table_id for table in tables if re.match(regex, table.table_id)
        ]

        # Get full paths
        table_names = [table for table in get_tables]

        return table_names

    def load_to_gcs_in_parallel(
        self,
        tables_name: str,
        uri_path: str,
        proyect_id: str,
        dataset_id: str,
    ) -> None:
        with ThreadPoolExecutor() as pool:
            pool.map(
                lambda table_name: self.export_table_to_storage(
                    proyect_id,
                    dataset_id,
                    table_name,
                    uri_path.format(table_name=table_name),
                ),
                tables_name,
            )

    def check_and_create_table(
        self,
        dataset_id: str,
        table_id: str,
        schema: list[bigquery.SchemaField],
        rows_to_insert: dict,
    ) -> None:
        table_ref = self.client.dataset(dataset_id).table(table_id)

        try:
            self.client.get_table(table_ref)
        except Exception:
            table = bigquery.Table(table_ref, schema=schema)
            table = self.client.create_table(table)
            logging.info(f"The table {table_ref} was created")

        try:
            self.client.insert_rows_json(table_ref, [rows_to_insert])
        except Exception as error:
            logging.error(
                f"There were errors inserting the data into the table {table_ref}. {error}"
            )
            raise error
