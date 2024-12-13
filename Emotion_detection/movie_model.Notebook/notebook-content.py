# Fabric notebook source

import pandas as pd
from pyspark.sql.functions import col, array_contains, expr


emotion = "joy"


# load tables
movie_list_df = spark.sql("SELECT * FROM MovieData.movie_list")
genome_tags_df = spark.sql("SELECT * FROM MovieData.genome_tags")
genome_relevance_scores_df = spark.sql("SELECT * FROM MovieData.genome_relevance_scores")
user_tags_df = spark.sql("SELECT * FROM MovieData.user_tags")


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


relevant_genres = emotion_map[emotion]["genres"]
relevant_tags = emotion_map[emotion]["tags"]

# filter genome_tags to get tagIds for relevant tags

filtered_genome_tags_df = genome_tags_df.filter(col("tag").isin(relevant_tags))
tag_ids = [row.tagId for row in filtered_genome_tags_df.collect()]  # Collect tag IDs as a list

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


# sort by relevance and remove duplicates
final_recommendations_df = filtered_movies_df.orderBy(col("relevance").desc()).dropDuplicates(["movieId"])

# show recommendation
final_recommendations_df.select("title", "relevance").show(10)

