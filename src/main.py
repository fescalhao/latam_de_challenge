from src.gcp import download_file


def main():
    bucket_name = 'latam-de-challenge'
    source_blob = 'source-file/farmers-protest-tweets-2021-2-4.json'
    destination = '/tmp/challenge_DE/data/tweets_data.json'

    download_file(bucket_name, source_blob, destination)


if __name__ == '__main__':
    main()
