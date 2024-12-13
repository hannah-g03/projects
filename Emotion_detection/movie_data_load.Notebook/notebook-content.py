# Fabric notebook source
import os
import json
from delta import *

movie_df = spark.read.format("csv").option("header","true").load("Files/movie.csv")
display(movie_df)

rating_df = spark.read.format("csv").option("header","true").load("Files/rating.csv")
display(rating_df)

tag_df = spark.read.format("csv").option("header","true").load("Files/tag.csv")
display(tag_df)

link_df = spark.read.format("csv").option("header","true").load("Files/link.csv")
display(link_df)

genome_df = spark.read.format("csv").option("header","true").load("Files/genome_tags.csv")
display(genome_df)

genome_scores_df = spark.read.format("csv").option("header","true").load("Files/genome_scores.csv")
display(genome_scores_df)

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

