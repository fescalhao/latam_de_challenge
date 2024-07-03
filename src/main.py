from src.q1_time import q1_time
from src.utils.gcp import download_file
from os.path import exists
import pandas as pd
from src.q1_memory import q1_memory


def main():
    file_path = '/tmp/tweets_data.json'
    bucket_name = 'latam-de-challenge'
    source_blob = 'source-file/farmers-protest-tweets-2021-2-4.json'

    if not exists(file_path):
        download_file(bucket_name, source_blob, file_path)

    # ------------------------------ q1_memory ------------------------------
    result = q1_memory(file_path)
    for row in result:
        print(row)

    # ------------------------------ q1_time ------------------------------
    project_id = 'latam-de-428219'
    dataset = 'challenge_data'
    table = 'tweets_tb'

    result = q1_time(project_id, dataset, table)
    for row in result:
        print(row)


if __name__ == '__main__':
    main()
