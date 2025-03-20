# Databricks notebook source
import json
import time
from datetime import datetime
import os

data_path = "/dbfs/tmp/streaming_data/"

dbutils.fs.mkdirs("dbfs:/tmp/streaming_data/")  # Create directory

def generate_streaming_data():
    for i in range(10):
        data = [{"event": f"event_{i%3}", "timestamp": datetime.now().isoformat()}]
        file_path = f"{data_path}/data_{i}.json"
        with open(file_path, "w") as f:
            json.dump(data, f)
        time.sleep(5)

generate_streaming_data()



# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window
from pyspark.sql.types import StructType, StringType, TimestampType

spark = SparkSession.builder.appName("DatabricksStreaming").getOrCreate()

schema = StructType().add("event", StringType()).add("timestamp", TimestampType())

stream_df = (
    spark.readStream
    .schema(schema)
    .option("maxFilesPerTrigger", 1)
    .json("dbfs:/tmp/streaming_data/")  # Corrected Databricks FileStore path
)

stream_df = stream_df.withColumn("timestamp", col("timestamp").cast("timestamp"))

agg_df = (
    stream_df
    .withWatermark("timestamp", "2 minutes")
    .groupBy(window(col("timestamp"), "1 minute"), col("event"))
    .count()
)

query = (
    agg_df
    .writeStream
    .outputMode("append")
    .format("console")
    .trigger(processingTime="30 seconds")
    .option("truncate", "false")
    .start()
)

import time
time.sleep(120)  # Run for 2 minutes, then stop
query.stop()



# COMMAND ----------

import json
import time
from datetime import datetime

data_path = "dbfs:/tmp/structured_streaming/"

dbutils.fs.mkdirs(data_path)  # Create directory if not exists

def generate_streaming_data():
    for i in range(10):
        data = [{"event": f"event_{i%3}", "timestamp": datetime.now().isoformat()}]
        file_path = f"{data_path}/data_{i}.json"
        dbutils.fs.put(file_path, json.dumps(data), True)  # Write JSON file
        time.sleep(5)  # Simulate real-time streaming

generate_streaming_data()



# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window
from pyspark.sql.types import StructType, StringType, TimestampType

spark = SparkSession.builder.appName("StructuredStreamingPipeline").getOrCreate()

schema = StructType().add("event", StringType()).add("timestamp", TimestampType())

stream_df = (
    spark.readStream
    .schema(schema)
    .option("maxFilesPerTrigger", 1)
    .json("dbfs:/tmp/structured_streaming/")  # Corrected Databricks FileStore path
)

stream_df = stream_df.withColumn("timestamp", col("timestamp").cast("timestamp"))

agg_df = (
    stream_df
    .withWatermark("timestamp", "2 minutes")  # Allow late data up to 2 minutes
    .groupBy(window(col("timestamp"), "1 minute"), col("event"))
    .count()
)

import time

query = (
    agg_df
    .writeStream
    .outputMode("append")
    .format("delta")
    .option("path", "dbfs:/tmp/structured_streaming_output/")
    .option("checkpointLocation", "dbfs:/tmp/structured_streaming_checkpoint/")
    .trigger(processingTime="30 seconds")
    .start()
)

time.sleep(120)  # Run for 2 minutes, then stop
query.stop()  # Stop the query gracefully


# COMMAND ----------

df = spark.read.format("delta").load("dbfs:/tmp/structured_streaming_output/")
display(df)


# COMMAND ----------

display(dbutils.fs.ls("dbfs:/tmp/structured_streaming/"))


# COMMAND ----------

for stream in spark.streams.active:
    print(stream)


# COMMAND ----------

import time

query = (
    agg_df
    .writeStream
    .outputMode("append")
    .format("console")
    .trigger(processingTime="30 seconds")
    .start()
)

time.sleep(60)  # Run for 2 minutes
query.stop()  # Stop gracefully after 2 minutes

