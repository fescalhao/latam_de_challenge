import logging
from typing import List, Tuple

from pandas.errors import InvalidColumnName

from src.utils.utils import read_columns_from_json


def q2_memory(file_path: str) -> List[Tuple[str, int]]:
    """
    Function responsible to retrieve the top 10 most used emojis and how many times each one
    of them were used.
    :param file_path: File containing all the tweets data.
    :return: A list of tuples containing the top 10 emojis and their count
    """
    # Define the regex to contemplate all possible emojis
    r = r'([\U0001F600-\U0001F64F\U0001F680-\U0001F6FF\U0001F300-\U0001F5FF\U0001F900-\U0001F9FF\U0001F1E0-\U0001F1FF])'

    # Define the desired columns
    cols = ['content']

    # Gets the Panda's Dataframe with the specified columns
    df = read_columns_from_json(file_path, cols)

    try:
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

    # This is just a broad error handling. A way to improve this part of the
    # code is to implement more specific error handling
    except InvalidColumnName as e:
        logging.warning(f"Could not find the specified column in the Dataframe.\n{e}")
    except ValueError as e:
        logging.error(f"A ValueError occurred while executing the function q2_memory.\n{e}")
    except Exception as e:
        logging.error(f"An error occurred while execution the function q2_memory.\n{e}")
