import json
import logging
import os

import pandas as pd
from pandas import DataFrame


def tweets_json_to_parquet(file_path: str, destination: str) -> None:
    """
    Simple function to convert a json to parquet file using pandas
    :param file_path: Source file path
    :param destination: File destination
    """
    try:
        df = pd.read_json(file_path, lines=True)
        df.to_parquet(destination)
    except FileNotFoundError as e:
        logging.error(f"File {file_path} not found. \n{e}")
    except Exception as e:
        logging.error(f"Error while converting the file {file_path} from JSON to Parquet. \n{e}")


def read_columns_from_json(file_path: str, column_list: list[str]) -> DataFrame:
    """
    Function to read only the specified columns from a json file
    :param file_path: File containing all the tweets data
    :param column_list: List of columns to be extracted from the json
    :return:
    """
    data = []
    try:
        with open(file_path) as f:
            for line in f:
                doc = json.loads(line)
                lst = [doc[col] for col in column_list]
                data.append(lst)

        return pd.DataFrame(data=data, columns=column_list)
    except FileNotFoundError as e:
        logging.error(f"File {file_path} not found. \n{e}")
    except Exception as e:
        logging.error(f"Error while extracting data for columns {column_list} from file {file_path}. \n{e}")


def check_gcp_env_variable() -> None:
    """
    Checks if the environment variable containing the path to the GCP key_file exists
    :return:
    """
    var = 'GOOGLE_APPLICATION_CREDENTIALS'
    if var in os.environ:
        logging.info(f"Environment variable {var} present. Proceeding...")
    else:
        raise Exception(f"Environment variable {var} not present. It's necessary to access the Google Cloud services")
