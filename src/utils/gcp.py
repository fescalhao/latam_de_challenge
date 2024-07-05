import logging

from google.api_core.exceptions import ClientError
from google.cloud import storage, bigquery
from pandas import DataFrame

from src.utils.utils import check_gcp_env_variable


def download_file(bucket_name: str, source_blob_name: str, destination_file_name: str) -> None:
    """
    Function to download files from Google Cloud Storage
    :param bucket_name: Bucket name
    :param source_blob_name: Path to the file inside the bucket
    :param destination_file_name: Location (file name included) where the file will be written
    """
    storage_client = None

    # Checks if the GCP Environment Variable is present
    check_gcp_env_variable()

    try:
        # Instantiate a GCP Storage Client
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)

        # Downloads the file from the object storage
        blob.download_to_filename(destination_file_name)

        print(f"Blob {source_blob_name} downloaded to {destination_file_name}.")
    except ClientError as e:
        logging.error(f"Error while downloading the object {bucket_name}/{source_blob_name} \n{e}")
    except Exception as e:
        logging.error(f"An error occurred during the download of the object {bucket_name}/{source_blob_name}. \n{e}")
    finally:
        if storage_client:
            storage_client.close()


def create_bigquery_table(project_id: str, dataset: str, table_name: str):
    client = None
    table_id = f"{project_id}.{dataset}.{table_name}"

    # Checks if the GCP Environment Variable is present
    check_gcp_env_variable()

    try:
        # Instantiate the Bigquery client
        client = bigquery.Client()

        # Instantiate a Table object
        table = bigquery.Table(table_id)

        # Make the API request to create the table
        client.create_table(table)

        print(f"Created {table_id}.")
    except ClientError as e:
        logging.error(f"Error while creating the table {table_id} on BigQuery. \n{e}")
    except Exception as e:
        logging.error(f"An error occurred during the creation of the table {table_id} on BigQuery. \n{e}")
    finally:
        if client:
            client.close()


def insert_bigquery_data_from_pandas(project_id: str, dataset: str, table_name: str, dataframe: DataFrame, data_schema: list):
    client = None
    table_id = f"{project_id}.{dataset}.{table_name}"

    # Checks if the GCP Environment Variable is present
    check_gcp_env_variable()

    try:
        # Instantiate the Bigquery client
        client = bigquery.Client()

        # Define the job configuration
        job_config = bigquery.LoadJobConfig(
            schema=data_schema,
            write_disposition='WRITE_TRUNCATE'
        )

        # Make the API request
        job = client.load_table_from_dataframe(
            dataframe, table_id, job_config=job_config
        )

        # Wait for the job to complete.
        job.result()

        # Gets information about the table
        table = client.get_table(table_id)

        print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")
    except ClientError as e:
        logging.error(f"Error while inserting data to the table {table_id} on BigQuery. \n{e}")
    except Exception as e:
        logging.error(f"An error occurred during the insertion of data to the table {table_id} on BigQuery. \n{e}")
    finally:
        if client:
            client.close()


def query_bigquery(query: str):
    client = None

    # Checks if the GCP Environment Variable is present
    check_gcp_env_variable()

    try:
        # Instantiate a Bigquery client
        client = bigquery.Client()

        # Define a job configuration
        job_config = bigquery.QueryJobConfig()
        job_config.use_query_cache = False

        # Submit the query job
        query_job = client.query(query, job_config=job_config)

        # Wait for the query to complete
        query_job.result()  # Waits for the query to finish

        # Fetch results
        results = query_job.result()

        return results
    except ClientError as e:
        logging.error(f"Error while running query on BigQuery. Query:\n\n{query}\n\n{e}")
    except Exception as e:
        logging.error(f"An error occurred while running the query on BigQuery. Query:\n\n{query}\n\n{e}")
    finally:
        if client:
            client.close()
