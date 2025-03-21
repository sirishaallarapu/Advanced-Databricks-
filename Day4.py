# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

spark = SparkSession.builder.appName("DailyETLJob").getOrCreate()

# COMMAND ----------

file_path = "/databricks-datasets/iot/iot_devices.json"
raw_data_df = spark.read.format("json").load(file_path)

# COMMAND ----------

transformed_df = (raw_data_df
    .filter(col("device_id").isNotNull())  # Remove rows with null device_id
    .select(
        col("device_id"),
        col("device_name"),
        col("temp").cast("float"),  # Ensure temperature is float
        col("humidity").cast("float"),    # Ensure humidity is float
        current_timestamp().alias("processed_time")  # Add processing timestamp
    )
    .dropDuplicates(["device_id"])  # Remove duplicate devices
)

# COMMAND ----------

output_table = "daily_etl_output"
transformed_df.write.format("delta").mode("overwrite").saveAsTable(output_table)

# COMMAND ----------

spark.sql(f"SELECT * FROM {output_table} LIMIT 10").show()
