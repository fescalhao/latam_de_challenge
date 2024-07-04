from typing import List, Tuple

from src.utils.gcp import query_bigquery


def q3_time(project_id: str, dataset: str, table: str) -> List[Tuple[str, int]]:
    """
        Function responsible to retrieve the top 10 users with the most mentions in tweets and also showing the
        amount of mentions for each user from the BigQuery table "tweets".
        :param project_id: GCP Account Project ID
        :type dataset: The specific dataset from BigQuery
        :param table: The table containing the tweets data
        :return: A list of tuples containing the top 10 user and the amount of mentions.
        """
    # Defining the query
    query = f"""
    WITH mentions as (
      select mu.element.username, count(mu.element.username) as mentions_count
        from `{project_id}.{dataset}.{table}`,
        unnest (mentionedUsers.list) as mu
      group by mu.element.username
    )
    
    SELECT *
      FROM mentions
    ORDER BY mentions_count DESC
    limit 10;
    """

    # Calling the function
    result = query_bigquery(query)

    # Returning the results in the expected format to the caller
    return [(row[0], row[1]) for row in result]
