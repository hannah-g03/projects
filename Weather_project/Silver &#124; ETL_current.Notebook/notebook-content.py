# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a40bc079-4900-4630-97c0-902c8c1e2328",
# META       "default_lakehouse_name": "Weather_lakehouse",
# META       "default_lakehouse_workspace_id": "346e1b64-7681-4e36-aaf6-454ec69177f7"
# META     }
# META   }
# META }

# CELL ********************

import os
import json
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import col, explode_outer, when, lit
from pyspark.sql.types import *
from delta import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run Bronze | Functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Path for data 
data_path = "/lakehouse/default/Files/weather_data"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load the data - newest folder
data = load_and_clean_newest_data(data_path)

# Convert list of dictionaries to a Spark DataFrame
df = spark.read.json(spark.sparkContext.parallelize(data))

# Flatten the DataFrame
flattened_df = flatten(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Enforce datatypes
flattened_df = flattened_df.withColumn("coord_lon", col("coord_lon").cast(FloatType())) \
       .withColumn("coord_lat", col("coord_lat").cast(FloatType())) \
       .withColumn("main_temp", col("main_temp").cast(FloatType())) \
       .withColumn("main_feels_like", col("main_feels_like").cast(FloatType())) \
       .withColumn("main_temp_min", col("main_temp_min").cast(FloatType())) \
       .withColumn("main_temp_max", col("main_temp_max").cast(FloatType())) \
       .withColumn("main_pressure", col("main_pressure").cast(IntegerType())) \
       .withColumn("main_humidity", col("main_humidity").cast(IntegerType())) \
       .withColumn("visibility", col("visibility").cast(IntegerType())) \
       .withColumn("wind_speed", col("wind_speed").cast(FloatType())) \
       .withColumn("wind_deg", col("wind_deg").cast(IntegerType())) \
       .withColumn("wind_gust", col("wind_gust").cast(FloatType())) \
       .withColumn("clouds_all", col("clouds_all").cast(IntegerType())) \
       .withColumn("dt", col("dt").cast(TimestampType())) \
       .withColumn("sys_sunrise", col("sys_sunrise").cast(TimestampType())) \
       .withColumn("sys_sunset", col("sys_sunset").cast(TimestampType())) \
       .withColumn("timezone", col("timezone").cast(IntegerType())) \
       .withColumn("id", col("id").cast(IntegerType())) \
       .withColumn("rain_1h", col("rain_1h").cast(DoubleType())) \
       .withColumn("cod", col("cod").cast(IntegerType()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add ID column
flattened_df_with_id = flattened_df.withColumn("city_id",
    when(flattened_df.city == "birmingham", lit(1)).
    when(flattened_df.city == "bristol", lit(2)).
    when(flattened_df.city == "cambridge", lit(3)).
    when(flattened_df.city == "coventry", lit(4)).
    when(flattened_df.city == "derby", lit(5)).
    when(flattened_df.city == "kingston_upon_hull", lit(6)).
    when(flattened_df.city == "leeds", lit(7)).
    when(flattened_df.city == "leicester", lit(8)).
    when(flattened_df.city == "liverpool", lit(9)).
    when(flattened_df.city == "london", lit(10)).
    when(flattened_df.city == "manchester", lit(11)).
    when(flattened_df.city == "newcastle_upon_tyne", lit(12)).
    when(flattened_df.city == "nottingham", lit(13)).
    when(flattened_df.city == "oxford", lit(14)).
    when(flattened_df.city == "plymouth", lit(15)).
    when(flattened_df.city == "portsmouth", lit(16)).
    when(flattened_df.city == "reading", lit(17)).
    when(flattened_df.city == "sheffield", lit(18)).
    when(flattened_df.city == "southampton", lit(19)).
    when(flattened_df.city == "york", lit(20)).
    otherwise(lit(-1))  
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Creating tables:**

# MARKDOWN ********************

# - saving as 'overwrite' as the cities will stay the same every refresh

# CELL ********************

# Create dim table - dim_city
# Drop duplicates based on city_id to get unique city records
df_unique_city = flattened_df_with_id.dropDuplicates(subset=['city_id'])

dim_city = df_unique_city.select('city_id', 'city', 'coord_lat', 'coord_lon')

# Save as a Delta table
dim_city.write.format("delta").mode("overwrite").saveAsTable("dim_city")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create dim - dim_weather
# Create unique df based on weather_id (drop duplicates)
df_unique_weather = flattened_df_with_id.drop_duplicates(subset=['weather_id'])

dim_weather = df_unique_weather.select('weather_id', 'weather_main', 'weather_description')

# Create delta table object
delta_table = DeltaTable.forName(spark, "dim_weather")

# Merge - to prevent duplication
delta_table.alias("tgt").merge(
    dim_weather.alias("src"),
    "tgt.weather_id = src.weather_id AND tgt.weather_main = src.weather_main AND tgt.weather_description = src.weather_description"
).whenNotMatchedInsertAll().execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Create fact table - fact_temp
fact_temp = flattened_df_with_id.select('city_id', 'weather_id', 'dt', 'main_temp', 'main_feels_like', 'main_temp_max', 'main_temp_min')


# Create delta table object
delta_table_fact = DeltaTable.forName(spark, "fact_temp")

# Merge - to prevent duplication
delta_table_fact.alias("tgt").merge(
    fact_temp.alias("src"),
    "tgt.city_id = src.city_id AND tgt.dt = src.dt"
).whenNotMatchedInsertAll().execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create fact table - fact_wind
fact_wind = flattened_df_with_id.select('city_id', 'weather_id', 'dt', 'wind_speed', 'wind_deg', 'wind_gust')

# Create delta table object
delta_table_wind = DeltaTable.forName(spark, "fact_wind")

# Merge - to prevent duplication
delta_table_wind.alias("tgt").merge(
    fact_wind.alias("src"),
    "tgt.city_id = src.city_id AND tgt.dt = src.dt"
).whenNotMatchedInsertAll().execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save as deltaTable
flattened_df_with_id.write.format("delta").option("mergeSchema", "true") \
    .mode("append").saveAsTable("current_weather_data")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

flattened_df_with_id.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
