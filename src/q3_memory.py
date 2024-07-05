import logging
from typing import List, Tuple

from pandas.errors import InvalidColumnName

from src.utils.utils import read_columns_from_json


def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    def _get_mentioned_user(mentions: list) -> list[str]:
        """
        Function to extract the username from all mentions in the tweet
        :param mentions: List of mentions from a specific tweet
        :return: List of usernames
        """
        user_list = []
        if mentions:
            for user in mentions:
                user_list.append(user['username'])

        return user_list

    # Define the desired columns
    cols = ['mentionedUsers']

    # Gets the Panda's Dataframe with the specified columns
    df = read_columns_from_json(file_path, cols)

    try:
        # Extract only the username from all mentions in the tweet
        df = df['mentionedUsers'].apply(_get_mentioned_user)

        # Explode the list into rows and drops the NaN values
        df = df.explode('mentionedUsers').dropna()

        # Count the number of each username
        df = df.value_counts().sort_index().rename_axis('user').reset_index(name='mentions_count')

        # Sort the usernames by the amount of mentions in descending order and take the top 10
        df_final = df.sort_values(by='mentions_count', ascending=False).head(10)

        # Returns the results in the expected format to the caller
        return [(row[1], row[2]) for row in df_final.itertuples()]

    # This is just a broad error handling. A way to improve this part of the
    # code is to implement more specific error handling
    except InvalidColumnName as e:
        logging.warning(f"Could not find the specified column in the Dataframe.\n{e}")
    except ValueError as e:
        logging.error(f"A ValueError occurred while executing the function q1_memory.\n{e}")
    except Exception as e:
        logging.error(f"An error occurred while execution the function q1_memory.\n{e}")
