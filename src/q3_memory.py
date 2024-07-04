from typing import List, Tuple
import pandas as pd


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

    # Reads the json file containing the data
    df = pd.read_json(file_path, lines=True)

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
