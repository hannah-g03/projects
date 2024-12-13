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
from pyspark.sql.functions import col, date_format
from pyspark.sql.types import *
import datetime as dt

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **Creating dimension table for date**

# CELL ********************

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# save as delta table
date_df.write.format("delta").mode("overwrite").saveAsTable("dim_date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
