from typing import List, Tuple
from datetime import datetime

from memory_profiler import profile

from src.utils.utils import read_columns_from_json


@profile
def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    """
    Function responsible to retrieve the top 10 dates with the most tweets and also showing the user that had the most
    tweets that day.
    :param file_path: File containing all the tweets data.
    :return: A list of tuples containing the specific date and the user with most tweets on that day.
    """

    # Define the desired columns
    cols = ['date', 'user']

    # Gets the Panda's Dataframe with the specified columns
    df = read_columns_from_json(file_path, cols)

    # Converts the date column type
    df['date'] = df['date'].astype('datetime64[ns, UTC]')

    # Gets only the date part of the timestamp value
    df['date'] = df['date'].dt.date

    # Extracts the username from the user object
    df['user'] = df['user'].apply(lambda u: u['username'])

    # Counts the number of tweets by day and user
    df['total_tweets'] = df.groupby(by=['date', 'user'])['user'].transform('count')

    # Groups the data by the highest number of tweets on each day
    df = df.groupby(by=['date', 'user']).max().reset_index()

    # Sorts it by number of tweets to filter the days with the most tweets
    df_final = df.sort_values(by='total_tweets', ascending=False).drop_duplicates(subset=['date']).head(10)

    # Returns the results in the expected format to the caller
    return [(row[1], row[2]) for row in df_final.itertuples()]

