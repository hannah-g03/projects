# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7028769d-5193-4c93-974a-a1bd0a93905e",
# META       "default_lakehouse_name": "MovieData",
# META       "default_lakehouse_workspace_id": "346e1b64-7681-4e36-aaf6-454ec69177f7"
# META     }
# META   }
# META }

# CELL ********************

import pandas as pd


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM MovieData.movie_list LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pdf = df.toPandas()
pdf.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# remove year and extra text from title
pdf['title'] = pdf['title'].str.replace(r"\(.*\)", "", regex=True).str.strip()

# split genres into a list
pdf['genres'] = pdf['genres'].str.split('|')

pdf.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_cleaned = spark.createDataFrame(pdf)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_cleaned.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("movie_list")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tag_df = spark.sql("SELECT * FROM MovieData.user_tags")
display(tag_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tag_pdf = tag_df.toPandas()
tag_pdf.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

genome_df = spark.sql("SELECT * FROM MovieData.genome_tags")
display(genome_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

genome_pdf = genome_df.toPandas()
genome_pdf.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

relevance_df = spark.sql("SELECT * FROM MovieData.genome_relevance_scores LIMIT 1000")
display(relevance_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

relevance_pdf = relevance_df.toPandas()
relevance_pdf.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
