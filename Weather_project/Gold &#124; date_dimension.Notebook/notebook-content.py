# Fabric notebook source

import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format
from pyspark.sql.types import *
import datetime as dt


###### **Creating dimension table for date**

# Define the date range
start_date = dt.date(2024, 7, 24)
end_date = dt.date(2030, 12, 31)

# Generate the list of dates
date_list = [start_date + dt.timedelta(days=x) for x in range((end_date - start_date).days + 1)]

# DataFrame from the date list
date_df = spark.createDataFrame([(d,) for d in date_list], ["date"])

date_df = date_df.withColumn("year", date_format(col("date"), "yyyy").cast("int")) \
    .withColumn("month", date_format(col("date"), "MM").cast("int")) \
    .withColumn("day", date_format(col("date"), "dd").cast("int")) \
    .withColumn("day_of_week", date_format(col("date"), "E")) 

# save as delta table
date_df.write.format("delta").mode("overwrite").saveAsTable("dim_date")

