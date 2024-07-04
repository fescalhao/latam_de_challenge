from typing import List, Tuple
from datetime import datetime

from src.utils.gcp import query_bigquery


def q1_time(project_id: str, dataset: str, table: str) -> List[Tuple[datetime.date, str]]:
    """
    Function responsible to retrieve the top 10 dates with the most tweets and also showing the user that had the most
    tweets that day from the BigQuery table "tweets".
    :param project_id: GCP Account Project ID
    :type dataset: The specific dataset from BigQuery
    :param table: The table containing the tweets data
    :return: A list of tuples containing the specific date and the user with most tweets on that day.
    """
    # Defining the query
    query = f"""
    WITH tweets as (
      SELECT DATE(TIMESTAMP_MILLIS(CAST(date/1e6 as INTEGER))) as date, 
             user.username as user
        FROM `{project_id}.{dataset}.{table}`
    ),
    count_tweets as (
      SELECT date, user, COUNT(user) as num_tweets,
             dense_rank() OVER (PARTITION BY date ORDER BY COUNT(user) DESC) as rank
        FROM tweets
       GROUP BY date, user 
    )
    
    SELECT date, user
      FROM count_tweets
     WHERE rank = 1
    ORDER BY num_tweets DESC
    LIMIT 10;
    """

    # Calling the function
    result = query_bigquery(query)

    # Returning the results in the expected format to the caller
    return [(row[0], row[1]) for row in result]
