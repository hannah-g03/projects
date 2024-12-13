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

import os
import json
from delta import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

movie_df = spark.read.format("csv").option("header","true").load("Files/movie.csv")
display(movie_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rating_df = spark.read.format("csv").option("header","true").load("Files/rating.csv")
display(rating_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tag_df = spark.read.format("csv").option("header","true").load("Files/tag.csv")
display(tag_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

link_df = spark.read.format("csv").option("header","true").load("Files/link.csv")
display(link_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

genome_df = spark.read.format("csv").option("header","true").load("Files/genome_tags.csv")
display(genome_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

genome_scores_df = spark.read.format("csv").option("header","true").load("Files/genome_scores.csv")
display(genome_scores_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

movie_df.write.format("delta").option("mergeSchema", "true") \
    .mode("overwrite").saveAsTable("movie_list")

rating_df.write.format("delta").option("mergeSchema", "true") \
    .mode("overwrite").saveAsTable("user_ratings")

tag_df.write.format("delta").option("mergeSchema", "true") \
    .mode("overwrite").saveAsTable("user_tags")

link_df.write.format("delta").option("mergeSchema", "true") \
    .mode("overwrite").saveAsTable("IMDB_links")

genome_df.write.format("delta").option("mergeSchema", "true") \
    .mode("overwrite").saveAsTable("genome_tags")

genome_scores_df.write.format("delta").option("mergeSchema", "true") \
    .mode("overwrite").saveAsTable("genome_relevance_scores")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
