from google.cloud import storage


def download_file(bucket_name: str, source_blob_name: str, destination_file_name: str) -> None:
    """
    Function to download files from Google Cloud Storage
    :param bucket_name: Bucket name
    :param source_blob_name: Path to the file inside the bucket
    :param destination_file_name: Location (file name included) where the file will be written
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    blob.download_to_filename(destination_file_name)

    print(f"Blob {source_blob_name} downloaded to {destination_file_name}.")
