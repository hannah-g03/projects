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
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql.functions import col, explode_outer, when, lit, explode
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType, StructType, StructField

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

cities = {
'london' : {'lat' : 51.5074,'lon' : 0.1278},
'birmingham' : {'lat' : 52.4862,'lon' : 1.8904},
'manchester' : {'lat' : 53.4808,'lon' : 2.2426},
'liverpool' : {'lat' : 53.4084,'lon' : 2.9916},
'leeds' : {'lat' : 53.8008,'lon' : 1.5491},
'sheffield' : {'lat' : 53.3811,'lon' : 1.4701},
'bristol' : {'lat' : 51.4545,'lon' : 2.5879},
'newcastle_upon_tyne' : {'lat' : 54.9783,'lon' : 1.6174},
'nottingham' : {'lat' : 52.9548,'lon' : 1.1581},
'southampton' : {'lat' : 50.9097,'lon' : 1.4044},
'portsmouth' : {'lat' : 50.8198,'lon' : 1.0880},
'leicester' : {'lat' : 52.6369,'lon' : 1.1398},
'coventry' : {'lat' : 52.4068,'lon' : 1.5197},
'reading' : {'lat' : 51.4543,'lon' : 0.9781},
'kingston_upon_hull' : {'lat' : 53.7676,'lon' : 0.3274},
'york' : {'lat' : 53.9590,'lon' : 1.0815},
'oxford' : {'lat' : 51.7520,'lon' : 1.2577},
'cambridge' : {'lat' : 52.2053,'lon' : 0.1218},
'plymouth' : {'lat' : 50.3755,'lon' : 4.1427},
'derby' : {'lat' : 52.9225,'lon' : 1.4746}
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Path for data 
data_path = "/lakehouse/default/Files/historical_weather_data"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load the data 
data = load_and_clean_data(data_path)

# Convert list of dictionaries to a Spark DataFrame
df = spark.read.json(spark.sparkContext.parallelize(data))

# Normalise DataFrame
normalized_df = normalize_historical_data(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Flatten weather arrays
normalized_df = normalized_df.withColumn("weather_id", explode("weather_id")) \
    .withColumn("weather_main", explode("weather_main")) \
    .withColumn("weather_description", explode("weather_description")) \
    .withColumn("weather_icon", explode("weather_icon"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

normalized_df.printSchema()
normalized_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add coordinates

normalized_df = normalized_df.withColumn('coord_lat',
    when((normalized_df.city == 'london'), lit(51.5074)).
    when((normalized_df.city == 'birmingham'), lit(52.4862)).
    when((normalized_df.city == 'manchester'), lit(53.4808)).
    when((normalized_df.city == 'liverpool'), lit(53.4084)).
    when((normalized_df.city == 'leeds'), lit(53.8008)).
    when((normalized_df.city == 'sheffield'), lit(53.3811)).
    when((normalized_df.city == 'bristol'), lit(51.4545)).
    when((normalized_df.city == 'newcastle_upon_tyne'), lit(54.9783)).
    when((normalized_df.city == 'nottingham'), lit(52.9548)).
    when((normalized_df.city == 'southampton'), lit(50.9097)).
    when((normalized_df.city == 'portsmouth'), lit(50.8198)).
    when((normalized_df.city == 'leicester'), lit(52.6369)).
    when((normalized_df.city == 'coventry'), lit(52.4068)).
    when((normalized_df.city == 'reading'), lit(51.4543)).
    when((normalized_df.city == 'kingston_upon_hull'), lit(53.7676)).
    when((normalized_df.city == 'york'), lit(53.9590)).
    when((normalized_df.city == 'oxford'), lit(51.7520)).
    when((normalized_df.city == 'cambridge'), lit(52.2053)).
    when((normalized_df.city == 'plymouth'), lit(50.3755)).
    when((normalized_df.city == 'derby'), lit(52.9225)).
    otherwise(lit(-1))
)

normalized_df = normalized_df.withColumn('coord_lon',
    when((normalized_df.city == 'london'), lit(0.1278)).
    when((normalized_df.city == 'birmingham'), lit(1.8904)).
    when((normalized_df.city == 'manchester'), lit(2.2426)).
    when((normalized_df.city == 'liverpool'), lit(2.99164)).
    when((normalized_df.city == 'leeds'), lit(1.5491)).
    when((normalized_df.city == 'sheffield'), lit(1.4701)).
    when((normalized_df.city == 'bristol'), lit(2.5879)).
    when((normalized_df.city == 'newcastle_upon_tyne'), lit(1.6174)).
    when((normalized_df.city == 'nottingham'), lit(1.1581)).
    when((normalized_df.city == 'southampton'), lit(1.4044)).
    when((normalized_df.city == 'portsmouth'), lit(1.0880)).
    when((normalized_df.city == 'leicester'), lit(1.1398)).
    when((normalized_df.city == 'coventry'), lit(1.5197)).
    when((normalized_df.city == 'reading'), lit(0.9781)).
    when((normalized_df.city == 'kingston_upon_hull'), lit(0.3274)).
    when((normalized_df.city == 'york'), lit(1.0815)).
    when((normalized_df.city == 'oxford'), lit(1.2577)).
    when((normalized_df.city == 'cambridge'), lit(0.1218)).
    when((normalized_df.city == 'plymouth'), lit(4.1427)).
    when((normalized_df.city == 'derby'), lit(1.4746)).
    otherwise(lit(-1))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Select and cast columns to match the current schema
normalized_df = normalized_df.select(
    col("city_id").cast("long").alias("city_id"),
    col("dt").alias("dt"),  
    col("main_temp").cast("float"),
    col("main_feels_like").cast("float"),
    col("main_temp_min").cast("float"),
    col("main_temp_max").cast("float"),
    col("main_pressure").cast("integer"),
    col("main_humidity").cast("integer"),
    col("wind_speed").cast("float"),
    col("wind_deg").cast("integer"),
    col("clouds_all").cast("integer"),
    col("weather_id").cast("long"),
    col("weather_main").cast("string"),
    col("weather_description").cast("string"),
    col("weather_icon").cast("string"),
    col("coord_lat").cast("float"),
    col("coord_lon").cast("float"),
    col('city').cast('string')
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

normalized_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# city_id coming up as -1 , need to fix ^^^


# CELL ********************

# Add ID column
df_with_id = normalized_df.withColumn("city_id",
    when(normalized_df.city == "birmingham", lit(1)).
    when(normalized_df.city == "bristol", lit(2)).
    when(normalized_df.city == "cambridge", lit(3)).
    when(normalized_df.city == "coventry", lit(4)).
    when(normalized_df.city == "derby", lit(5)).
    when(normalized_df.city == "kingston_upon_hull", lit(6)).
    when(normalized_df.city == "leeds", lit(7)).
    when(normalized_df.city == "leicester", lit(8)).
    when(normalized_df.city == "liverpool", lit(9)).
    when(normalized_df.city == "london", lit(10)).
    when(normalized_df.city == "manchester", lit(11)).
    when(normalized_df.city == "newcastle_upon_tyne", lit(12)).
    when(normalized_df.city == "nottingham", lit(13)).
    when(normalized_df.city == "oxford", lit(14)).
    when(normalized_df.city == "plymouth", lit(15)).
    when(normalized_df.city == "portsmouth", lit(16)).
    when(normalized_df.city == "reading", lit(17)).
    when(normalized_df.city == "sheffield", lit(18)).
    when(normalized_df.city == "southampton", lit(19)).
    when(normalized_df.city == "york", lit(20)).
    otherwise(lit(-1))  
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_with_id.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create dim - dim_weather
# Create unique df based on weather_id (drop duplicates)
df_unique_weather = df_with_id.drop_duplicates(subset=['weather_id'])

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

df_with_id.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_unique_temp = df_with_id.toPandas().sort_values('dt').groupby('city').apply(lambda x: x.drop_duplicates(subset='dt', keep='first')).reset_index(drop=True)

fact_temp = df_unique_temp[['city_id', 'weather_id', 'dt', 'main_temp', 'main_feels_like', 'main_temp_min', 'main_temp_max']]

fact_temp = spark.createDataFrame(fact_temp)

# Save as a Delta table
fact_temp.write.format("delta").mode("overwrite").saveAsTable("fact_temp")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_temp.display()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create fact table - fact_temp

df_unique_temp = df_with_id.dropDuplicates(subset=['dt'])

fact_temp = df_unique_temp.select('weather_id', 'city_id', 'dt', 'main_temp', 'main_feels_like', 'main_temp_min', 'main_temp_max' )

# Create delta table object
delta_table_temp = DeltaTable.forName(spark, "fact_temp")

# Merge - to prevent duplication
delta_table_temp.alias("tgt").merge(
    fact_temp.alias("src"),
    "tgt.dt = src.dt AND tgt.city_id = src.city_id"
).whenNotMatchedInsertAll().execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



##################################################

# Create fact table - fact_wind
fact_wind = df_with_id.select('city_id', 'weather_id', 'dt', 'wind_speed', 'wind_deg')

# Create delta table object
delta_table_wind = DeltaTable.forName(spark, "fact_wind")

# Merge - to prevent duplication
delta_table_wind.alias("tgt").merge(
    fact_wind.alias("src"),
    "tgt.city_id = src.city_id AND tgt.dt = src.dt"
).whenNotMatchedInsertAll().execute()

###################################################

# Create fact table - atmospheric metrics
fact_metrics = df_with_id.select('city_id', 'weather_id', 'dt', 'main_pressure', 'main_humidity', 'clouds_all')

# Create delta table object
delta_table = DeltaTable.forName(spark, "fact_metrics")

# Merge - to prevent duplication
delta_table.alias("tgt").merge(
    fact_metrics.alias("src"),
    "tgt.city_id = src.city_id AND tgt.dt = src.dt"
).whenNotMatchedInsertAll().execute()

#################################################

# Append to current_weather_data 
df_with_id.write.format("delta").option("mergeSchema", "true") \
    .mode("append").saveAsTable("current_weather_data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
