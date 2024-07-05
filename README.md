# Data Engineer Challenge

## Introduction
The purpose of the challenge is to process a file containing tweets to return three different results as following:
1. Return the top 10 dates with the most tweets with the user that also tweeted the most in the specific date.
2. Return the top 10 emojis most used in tweets and their respective count
3. Return the top 10 users most mentioned in tweets and their respective count

Each result will be process using 2 different functions. One considering the execution time 
and the other considering the memory usage.

## Tools
To achieve the objective the following tools and libraries were used:
* Pandas
* Google BigQuery
* Memory-profiler
* cProfiler

## Steps to Run the Code
To run the project you will need a Google Cloud account. The project assumes that the file to be processed will be in
a Google Cloud Storage folder and also present in a BigQuery table.

The file is present in the bucket in JSON and Parquet formats. There is a helper function **tweets_json_to_parquet** to
convert the json to parquet file. The upload for both files were made manually.

You will also need a Service Account inside your Google Cloud account with permission to access files in
Google Cloud Storage and also create and read tables from BigQuery. The key_file from this Service Account should be
downloaded and an environment variable **GOOGLE_APPLICATION_CREDENTIALS** has to be created pointing to its location. 

After that it's possible to execute the **src/main.py** file from the challenge_DE folder.