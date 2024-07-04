from typing import List, Tuple

import emoji
import pandas as pd
import numpy as np


def q2_memory(file_path: str) -> List[Tuple[str, int]]:
    """
    Function responsible to retrieve the top 10 most used emojis and how many times each one
    of them were used.
    :param file_path: File containing all the tweets data.
    :return: A list of tuples containing the top 10 emojis and their count
    """
    # Define the regex to contemplate all possible emojis
    r = r'([\U0001F600-\U0001F64F\U0001F680-\U0001F6FF\U0001F300-\U0001F5FF\U0001F900-\U0001F9FF\U0001F1E0-\U0001F1FF])'

    # Reads the json file containing the data
    df = pd.read_json(file_path, lines=True)

    # Selects only the needed column to reduce the size of the object in memory
    df = df[['content']]

    # Extract only the emojis from the content message grouping them in a comma-separated string
    df['content'] = df['content'].str.extractall(r).groupby(level=0).agg(','.join)

    # Split the string into a list
    df['content'] = df['content'].str.split(',')

    # Dropping NaN values and exploding the list to rows
    df = df.dropna().explode('content')

    # Count the number of each emoji
    df = df.value_counts().sort_index().rename_axis('emoji').reset_index(name='emoji_count')

    # Sort the emojis by how many times they were used in descending order and take the top 10
    df_final = df.sort_values(by='emoji_count', ascending=False).head(10)

    # Returns the results in the expected format to the caller
    return [(row[1], row[2]) for row in df_final.itertuples()]
