# Databricks notebook source
import requests

csv_url = "https://raw.githubusercontent.com/prasertcbs/basic-dataset/master/Restaurant%20customer%20data.csv"
local_path = "/tmp/restaurant_data.csv"

response = requests.get(csv_url)
with open(local_path, "wb") as f:
    f.write(response.content)

dbutils.fs.mv(f"file://{local_path}", "dbfs:/FileStore/tables/restaurant_data.csv")


# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/"))


# COMMAND ----------

df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("dbfs:/FileStore/tables/restaurant_data.csv")
display(df)


# COMMAND ----------

df.write.format("delta").mode("overwrite").save("dbfs:/mnt/bronze/restaurant_data")


# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_restaurant_data
USING DELTA
LOCATION 'dbfs:/mnt/bronze/restaurant_data'
""")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_restaurant_data LIMIT 10;
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, trim

# Load Bronze data
bronze_df = spark.read.format("delta").load("dbfs:/mnt/bronze/restaurant_data")

# Check available columns
bronze_df.printSchema()

# Data Cleaning (modify based on actual column names)
silver_df = (bronze_df
    .dropDuplicates()
    .na.fill("Unknown")  # Fill missing values
    .withColumnRenamed("drink_level", "drinking_preference")  # Rename a column
    .withColumn("drinking_preference", trim(col("drinking_preference")))  # Trim spaces
)

# Save as Silver Table
silver_df.write.format("delta").mode("overwrite").save("dbfs:/mnt/silver/restaurant_data")

print("✅ Data successfully transformed and saved in Silver layer!")



# COMMAND ----------

silver_df = spark.read.format("delta").load("dbfs:/mnt/silver/restaurant_data")
silver_df.show(5)
silver_df.printSchema()


# COMMAND ----------

from pyspark.sql.functions import count, avg

gold_df = (silver_df
    .groupBy("userID")
    .agg(
        count("*").alias("total_visits"),
        avg("budget").alias("avg_budget")
    )
)

# Save Gold Layer
gold_df.write.format("delta").mode("overwrite").save("dbfs:/mnt/gold/user_preferences")

print("✅ Gold Layer created successfully!")


# COMMAND ----------

gold_df = spark.read.format("delta").load("dbfs:/mnt/gold/user_preferences")
gold_df.show(5)


# COMMAND ----------

from pyspark.sql.functions import col

silver_df = spark.read.format("delta").load("dbfs:/mnt/silver/restaurant_data")

# Check if budget column contains nulls
silver_df.select("budget").show(10, truncate=False)

# Check distinct values in the budget column
silver_df.select("budget").distinct().show()


# COMMAND ----------

from pyspark.sql.functions import when
from pyspark.sql.types import DoubleType

# Convert budget categories to numerical values and cast to Double
silver_cleaned_df = silver_df.withColumn(
    "budget",
    when(col("budget") == "low", 30)
    .when(col("budget") == "medium", 60)
    .when(col("budget") == "high", 100)
    .otherwise(None)  # Convert "?" to null
    .cast(DoubleType())  # Ensure consistent schema
)

silver_cleaned_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("dbfs:/mnt/silver/restaurant_data")




# COMMAND ----------

silver_cleaned_df = spark.read.format("delta").load("dbfs:/mnt/silver/restaurant_data")
silver_cleaned_df.show(5)


# COMMAND ----------

from pyspark.sql.functions import avg, count

gold_df = silver_cleaned_df.groupBy("userID").agg(
    count("*").alias("total_visits"),
    avg("budget").alias("avg_budget")  # Now `budget` is numeric
)

gold_df.show(10)


# COMMAND ----------

gold_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("dbfs:/mnt/gold/user_preferences")


# COMMAND ----------

gold_df = spark.read.format("delta").load("dbfs:/mnt/gold/user_preferences")
gold_df.show(10)


# COMMAND ----------

gold_df = spark.read.format("delta").load("dbfs:/mnt/gold/user_preferences")
gold_df.show(10)

