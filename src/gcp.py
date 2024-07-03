from google.cloud import storage, bigquery
from pandas import DataFrame


def download_file(bucket_name, source_blob_name, destination_file_name):
    """Downloads a file from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    blob.download_to_filename(destination_file_name)

    print(f"Blob {source_blob_name} downloaded to {destination_file_name}.")


def create_bigquery_table(project_id: str, dataset: str, table_name: str):
    client = None
    table_id = f"{project_id}.{dataset}.{table_name}"
    try:
        # Instantiate a Bigquery client
        client = bigquery.Client()

        # Instantiate a Table object
        table = bigquery.Table(table_id)

        # Make the API request to create the table
        client.create_table(table)

        print(f"Created {table_id}.")
    except Exception as e:
        print(e)
    finally:
        if client:
            client.close()


def insert_bigquery_data_from_pandas(project_id: str, dataset: str, table_name: str, dataframe: DataFrame, data_schema: list):
    client = None
    table_id = f"{project_id}.{dataset}.{table_name}"

    try:
        # Instantiate a Bigquery client
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
    except Exception as e:
        print(e)
    finally:
        if client:
            client.close()


def query_bigquery(query: str):
    client = None

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
    except Exception as e:
        print(e)
    finally:
        if client:
            client.close()

