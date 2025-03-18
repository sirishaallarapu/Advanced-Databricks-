# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("DeltaDemo").getOrCreate()

data = [
    (1, "Siri", 25),
    (2, "Nani", 30),
    (3, "Cherry", 35)
]

df = spark.createDataFrame(data, ["id", "name", "age"])
df.write.format("delta").mode("overwrite").save("/mnt/delta_table")

spark.sql("DROP TABLE IF EXISTS people_delta")
spark.sql("""
    CREATE TABLE people_delta 
    USING DELTA 
    LOCATION '/mnt/delta_table'
""")

print("Delta Table Created Successfully!")

# COMMAND ----------

spark.sql("INSERT INTO people_delta VALUES (4, 'Dolly', 40)")


# COMMAND ----------

spark.sql("UPDATE people_delta SET age = 32 WHERE name = 'Nani'")


# COMMAND ----------

spark.sql("DELETE FROM people_delta WHERE name = 'Cherry'")


# COMMAND ----------

spark.sql("DESCRIBE HISTORY people_delta").show(truncate=False)


# COMMAND ----------

df_previous = spark.read.format("delta").option("versionAsOf", 1).load("/mnt/delta_table")
df_previous.show()


# COMMAND ----------

spark.sql("OPTIMIZE people_delta ZORDER BY (id)")


# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DeltaLakeDemo").getOrCreate()

data = [
    (1, "John", "Finance", 50000),
    (2, "Emma", "HR", 60000),
    (3, "Liam", "IT", 75000)
]

df = spark.createDataFrame(data, ["id", "name", "department", "salary"])
df.write.format("delta").mode("overwrite").save("/mnt/employee_delta")

spark.sql("DROP TABLE IF EXISTS employee_delta")
spark.sql("""
    CREATE TABLE employee_delta
    USING DELTA
    LOCATION '/mnt/employee_delta'
""")


# COMMAND ----------

spark.sql("INSERT INTO employee_delta VALUES (4, 'Sophia', 'IT', 80000)")
spark.sql("INSERT INTO employee_delta VALUES (5, 'Daniel', 'Finance', 72000)")


# COMMAND ----------

spark.sql("UPDATE employee_delta SET salary = 85000 WHERE name = 'Sophia'")


# COMMAND ----------

spark.sql("DELETE FROM employee_delta WHERE name = 'Liam'")


# COMMAND ----------

spark.sql("DESCRIBE HISTORY employee_delta").show(truncate=False)


# COMMAND ----------

df_old = spark.read.format("delta").option("versionAsOf", 1).load("/mnt/employee_delta")
df_old.show()


# COMMAND ----------

new_data = [
    (6, "Olivia", "Marketing", 67000, "Remote"),
    (7, "Noah", "HR", 62000, "Onsite")
]

df_new = spark.createDataFrame(new_data, ["id", "name", "department", "salary", "work_mode"])
df_new.write.format("delta").mode("append").option("mergeSchema", "true").save("/mnt/employee_delta")



# COMMAND ----------

from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "/mnt/employee_delta")

updates = [
    (4, "Sophia", "IT", 90000),
    (8, "Ethan", "Finance", 71000)
]

df_updates = spark.createDataFrame(updates, ["id", "name", "department", "salary"])

delta_table.alias("target").merge(
    df_updates.alias("source"),
    "target.id = source.id"
).whenMatchedUpdate(set={"salary": "source.salary"}) \
 .whenNotMatchedInsert(values={"id": "source.id", "name": "source.name", "department": "source.department", "salary": "source.salary"}) \
 .execute()


# COMMAND ----------

spark.sql("OPTIMIZE employee_delta ZORDER BY (department)")


# COMMAND ----------

df.write.format("delta").mode("overwrite").partitionBy("department").save("/mnt/partitioned_employee_delta")

