# Fabric notebook source

import requests
import json
import pandas as pd

# function to call api

def fetch_news_data():
    url = "https://real-time-news-data.p.rapidapi.com/topic-news-by-section"

    querystring = {"topic":"TECHNOLOGY","section":"CAQiW0NCQVNQZ29JTDIwdk1EZGpNWFlTQW1WdUdnSlZVeUlQQ0FRYUN3b0pMMjB2TURKdFpqRnVLaGtLRndvVFIwRkVSMFZVWDFORlExUkpUMDVmVGtGTlJTQUJLQUEqKggAKiYICiIgQ0JBU0Vnb0lMMjB2TURkak1YWVNBbVZ1R2dKVlV5Z0FQAVAB","limit":"500","country":"US","lang":"en"}

    headers = {
	"x-rapidapi-key": "aab7a93b45mshe85e616ca4c3e3fp14ed9ajsn72effb73e8b3",
	"x-rapidapi-host": "real-time-news-data.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)
    
    # Check if the request was successful
    if response.status_code == 200:
        return response.json()  # Return the JSON data
    else:
        print(f"Error: {response.status_code}")
        return None


# function to clean the json data 

def clean_news_data(json_data):

    # create a list to hold the cleaned data
    cleaned_data = []
    
    # check if data exists in the JSON
    if 'data' in json_data:
        articles = json_data['data']
        
        # iterate over each article 
        for article in articles:
           
            cleaned_article = {
                'title': article.get('title'),
                'link': article.get('link'),
                'snippet': article.get('snippet'),
                'thumbnail_url': article.get('thumbnail_url'),
                'published_datetime': article.get('published_datetime_utc'),
                'source_name': article.get('source_name'),
                'source_url': article.get('source_url'),
                'source_logo_url': article.get('source_logo_url')
            }
            # append to the list
            cleaned_data.append(cleaned_article)
    
    # convert the list to dataframe
    df = pd.DataFrame(cleaned_data)
    
    # convert datetime to a more readable format 
    df['published_datetime'] = pd.to_datetime(df['published_datetime'], errors='coerce')
    
    return df

# fetch news data from the API
news_json = fetch_news_data()

# if data was fetched successfully, clean and display 
if news_json:
    cleaned_df = clean_news_data(news_json)
    print(cleaned_df)


# save as delta table

from pyspark.sql.utils import AnalysisException

# analysis exception - dealing with duplicates (if there are any)

try:
    table_name = 'news_lake_db.news_load'

    spark_df = spark.createDataFrame(cleaned_df)
    spark_df.write.format("delta").saveAsTable(table_name)

except AnalysisException:

    print('Table already exists')

    spark_df.createOrReplaceTempView('vw_spark_df')

    # type 1 merge

    spark.sql(f"""  MERGE INTO {table_name} target_table
                    USING vw_spark_df source_view
                   
                    ON source_view.link = target_table.link

                    WHEN MATCHED AND
                    source_view.title <> target_table.title OR
                    source_view.snippet <> target_table.snippet OR
                    source_view.thumbnail_url <> target_table.thumbnail_url OR
                    source_view.published_datetime <> target_table.published_datetime OR
                    source_view.source_name <> target_table.source_name OR
                    source_view.source_url <> target_table.source_url OR
                    source_view.source_logo_url <> target_table.source_logo_url

                    THEN UPDATE SET *

                    WHEN NOT MATCHED THEN INSERT *

                """)

