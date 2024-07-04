from typing import List, Tuple

from src.utils.gcp import query_bigquery


def q2_time(project_id: str, dataset: str, table: str) -> List[Tuple[str, int]]:
    """
    Function responsible to retrieve the top 10 most used emojis and how many times each one
    of them were used from BigQuery.
    :param project_id: GCP Account Project ID
    :type dataset: The specific dataset from BigQuery
    :param table: The table containing the tweets data
    :return: A list of tuples containing the specific date and the user with most tweets on that day.
    """
    # Define the query
    query = f"""
    WITH emoji_list_tb as (
      SELECT REGEXP_EXTRACT_ALL(content, '[\U0001F600-\U0001F64F\U0001F680-\U0001F6FF\U0001F300-\U0001F5FF\U0001F900-\U0001F9FF\U0001F1E0-\U0001F1FF]') as emoji_list
        FROM `{project_id}.{dataset}.{table}`
    ),
    emojis_tb as (
      SELECT emojis
        FROM emoji_list_tb,
         UNNEST (emoji_list) as emojis
    )
    
    SELECT emojis, count(*) as emoji_count
      FROM emojis_tb
     GROUP BY emojis
    ORDER BY count(*) DESC
    LIMIT 10;
    """

    # Calling the function
    result = query_bigquery(query)

    # Returning the results in the expected format to the caller
    return [(row[0], row[1]) for row in result]
