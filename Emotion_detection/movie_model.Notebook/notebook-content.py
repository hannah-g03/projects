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
from pyspark.sql.functions import col, array_contains, expr

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

emotion = "joy"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# load tables
movie_list_df = spark.sql("SELECT * FROM MovieData.movie_list")
genome_tags_df = spark.sql("SELECT * FROM MovieData.genome_tags")
genome_relevance_scores_df = spark.sql("SELECT * FROM MovieData.genome_relevance_scores")
user_tags_df = spark.sql("SELECT * FROM MovieData.user_tags")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Mapping emotions -> genre (based on emotions in detection model)
emotion_map = {
    "joy": {
        "genres": ["Comedy", "Adventure", "Romance", "Family"],
        "tags": ["funny", "feel-good", "lighthearted"]
    },
    "sadness": {
        "genres": ["Drama", "Slice of Life", "Biography"],
        "tags": ["heartwarming", "tragic", "emotional"]
    },
    "fear": {
        "genres": ["Horror", "Thriller", "Mystery"],
        "tags": ["dark", "suspenseful", "supernatural"]
    },
    "surprise": {
        "genres": ["Mystery", "Sci-Fi", "Thriller"],
        "tags": ["mind-bending", "plot twist", "intriguing"]
    },
    "love": {
        "genres": ["Romance", "Drama", "Family"],
        "tags": ["romantic", "passionate", "connection"]
    },
    "anger": {
        "genres": ["Action", "Crime", "Drama"],
        "tags": ["revenge", "justice", "intense"]
    }
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

relevant_genres = emotion_map[emotion]["genres"]
relevant_tags = emotion_map[emotion]["tags"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# filter genome_tags to get tagIds for relevant tags

filtered_genome_tags_df = genome_tags_df.filter(col("tag").isin(relevant_tags))
tag_ids = [row.tagId for row in filtered_genome_tags_df.collect()]  # Collect tag IDs as a list

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# filter movies by relevant genres and join with genome_relevance_scores 
filtered_movies_df = (
    movie_list_df
    .filter(expr(f"array_contains(genres, '{relevant_genres[0]}')") |  
            expr(f"array_contains(genres, '{relevant_genres[1]}')") |
            expr(f"array_contains(genres, '{relevant_genres[2]}')") |
            expr(f"array_contains(genres, '{relevant_genres[3]}')"))
    .join(genome_relevance_scores_df, on="movieId", how="inner")
    .filter(col("tagId").isin(tag_ids))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# sort by relevance and remove duplicates
final_recommendations_df = filtered_movies_df.orderBy(col("relevance").desc()).dropDuplicates(["movieId"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# show recommendation
final_recommendations_df.select("title", "relevance").show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
