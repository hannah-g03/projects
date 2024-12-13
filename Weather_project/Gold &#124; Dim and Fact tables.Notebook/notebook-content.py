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

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load cleaned data from silver
df_cleaned = spark.table("current_weather_data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create dimension tables
dim_city = df_cleaned.select('city').distinct().withColumn("city_id", col("city").cast("integer"))
dim_date = df_cleaned.select('dt').distinct().withColumn("date_id", col("dt").cast("integer"))

# Create fact table
fact_weather = df_cleaned \
    .join(dim_city, 'city', 'inner') \
    .join(dim_date, 'dt', 'inner') \
    .drop('city', 'dt')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save as Delta tables 
dim_city.write.format("delta").mode("append").saveAsTable("dim_city")
dim_date.write.format("delta").mode("append").saveAsTable("dim_date")
fact_weather.write.format("delta").mode("append").saveAsTable("fact_weather")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }