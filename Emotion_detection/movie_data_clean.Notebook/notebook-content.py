# Fabric notebook source

import pandas as pd

df = spark.sql("SELECT * FROM MovieData.movie_list LIMIT 1000")
display(df)

pdf = df.toPandas()
pdf.head()

# remove year and extra text from title
pdf['title'] = pdf['title'].str.replace(r"\(.*\)", "", regex=True).str.strip()

# split genres into a list
pdf['genres'] = pdf['genres'].str.split('|')

pdf.head()

df_cleaned = spark.createDataFrame(pdf)

df_cleaned.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("movie_list")

tag_df = spark.sql("SELECT * FROM MovieData.user_tags")
display(tag_df)

tag_pdf = tag_df.toPandas()
tag_pdf.head()

genome_df = spark.sql("SELECT * FROM MovieData.genome_tags")
display(genome_df)

genome_pdf = genome_df.toPandas()
genome_pdf.head()

relevance_df = spark.sql("SELECT * FROM MovieData.genome_relevance_scores LIMIT 1000")
display(relevance_df)

relevance_pdf = relevance_df.toPandas()
relevance_pdf.head()

