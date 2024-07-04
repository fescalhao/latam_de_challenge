import json

import pandas as pd
from pandas import DataFrame


def tweets_json_to_parquet(file_path: str, destination: str) -> None:
    """
    Simple function to convert a json to parquet file using pandas
    :param file_path: Source file path
    :param destination: File destination
    """
    df = pd.read_json(file_path, lines=True)
    df.to_parquet(destination)


def read_columns_from_json(file_path: str, column_list: list[str]) -> DataFrame:
    """
    Function to read only the specified columns from a json file
    :param file_path: File containing all the tweets data
    :param column_list: List of columns to be extracted from the json
    :return:
    """
    data = []
    with open(file_path) as f:
        for line in f:
            doc = json.loads(line)
            lst = [doc[col] for col in column_list]
            data.append(lst)

    return pd.DataFrame(data=data, columns=column_list)

