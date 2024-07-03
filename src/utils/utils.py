import pandas as pd


def tweets_json_to_parquet(file_path: str, destination: str) -> None:
    """
    Simple function to convert a json to parquet file using pandas
    :param file_path: Source file path
    :param destination: File destination
    """
    df = pd.read_json(file_path, lines=True)
    df.to_parquet(destination)
