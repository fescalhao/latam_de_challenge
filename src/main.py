from src.gcp import download_file
from os.path import exists

from src.q1_memory import q1_memory


def main():
    file_path = '/tmp/tweets_data.json'
    bucket_name = 'latam-de-challenge'
    source_blob = 'source-file/farmers-protest-tweets-2021-2-4.json'

    if not exists(file_path):
        download_file(bucket_name, source_blob, file_path)

    q1_memory_result = q1_memory(file_path)

    print(q1_memory_result)

if __name__ == '__main__':
    main()
