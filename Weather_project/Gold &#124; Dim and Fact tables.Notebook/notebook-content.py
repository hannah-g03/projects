# Fabric notebook source

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Load cleaned data from silver
df_cleaned = spark.table("current_weather_data")

# Create dimension tables
dim_city = df_cleaned.select('city').distinct().withColumn("city_id", col("city").cast("integer"))
dim_date = df_cleaned.select('dt').distinct().withColumn("date_id", col("dt").cast("integer"))

# Create fact table
fact_weather = df_cleaned \
    .join(dim_city, 'city', 'inner') \
    .join(dim_date, 'dt', 'inner') \
    .drop('city', 'dt')

# Save as Delta tables 
dim_city.write.format("delta").mode("append").saveAsTable("dim_city")
dim_date.write.format("delta").mode("append").saveAsTable("dim_date")
fact_weather.write.format("delta").mode("append").saveAsTable("fact_weather")
