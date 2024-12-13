# Fabric notebook source

### Performing sentiment analysis on news headlines 
# - seeing whether a news story is positive, negative or neutral 
# - using machine learning workflows - through synapse machine learning

df = spark.sql("SELECT * FROM news_lake_db.news_load")
display(df)

import synapse.ml.core
from synapse.ml.services import AnalyzeText

# configure model
model = (AnalyzeText()
        .setTextCol('snippet')
        .setKind('SentimentAnalysis')
        .setOutputCol('response')
        .setErrorCol('error')
        )

# apply model to dataframe

result = model.transform(df)

display(result)

from pyspark.sql.functions import col

# create sentiment column

sentiment_df = result.withColumn('sentiment', col('response.documents.sentiment'))
display(sentiment_df)

# remove error and response col

sentiment_df_final = sentiment_df.drop('error', 'response')

display(sentiment_df_final)

from pyspark.sql.utils import AnalysisException

try:
    table_name = 'news_lake_db.news_sentiment_analysis'
    sentiment_df_final.write.format("delta").saveAsTable(table_name)

except AnalysisException:

    print('Table already exists')

    sentiment_df_final.createOrReplaceTempView('vw_sentiment_df_final')

    # type 1 merge

    spark.sql(f"""  MERGE INTO {table_name} target_table
                    USING vw_sentiment_df_final source_view
                   
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
