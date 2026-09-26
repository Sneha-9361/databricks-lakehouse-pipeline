# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Incremental Ingestion from Bronze
# MAGIC Reads newly arrived call records from `workspace.default.bronze_call_records`
# MAGIC using Delta Change Data Feed / timestamp watermarking for incremental processing.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_date

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Determine the High Watermark from Silver
# MAGIC Fetch the latest ingestion timestamp already processed into Silver to read incrementally.

# COMMAND ----------

silver_table_name = "workspace.default.silver_call_records"
bronze_table_name = "workspace.default.bronze_call_records"

# Check if target silver table exists to set watermark
silver_exists = spark.catalog.tableExists(silver_table_name)

if silver_exists:
    last_processed_timestamp = (
        spark.table(silver_table_name)
        .selectExpr("max(_ingestion_timestamp) as max_ts")
        .collect()[0]["max_ts"]
    )
else:
    last_processed_timestamp = None

print(f"Incremental watermark (last_processed_timestamp): {last_processed_timestamp}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Read Incremental Slice from Bronze
# MAGIC Filter Bronze records strictly newer than the watermark. If first run, read all.

# COMMAND ----------

bronze_df = spark.table(bronze_table_name)

if last_processed_timestamp is not None:
    incremental_bronze_df = bronze_df.filter(
        col("_ingestion_timestamp") > last_processed_timestamp
    )
else:
    incremental_bronze_df = bronze_df

print(f"Total incremental records fetched: {incremental_bronze_df.count()}")
display(incremental_bronze_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Temporary Staging View for Validation / Merge
# MAGIC Register the incremental slice as a temporary view to be consumed downstream.

# COMMAND ----------

incremental_bronze_df.createOrReplaceTempView("tv_bronze_incremental_slice")



# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Apply Business Cleaning & Transformations
# MAGIC - Clean phone numbers to standard 10 digits
# MAGIC - Calculate call duration in minutes
# MAGIC - Attach processing timestamp

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, round as spark_round

cleaned_silver_df = (
    incremental_bronze_df
    # Remove any non-digit characters from phone numbers
    .withColumn("caller_number", regexp_replace(col("caller_number"), r"[^0-9]", ""))
    .withColumn("receiver_number", regexp_replace(col("receiver_number"), r"[^0-9]", ""))
    # Convert duration seconds to minutes rounded to 2 decimals
    .withColumn("call_duration_minutes", spark_round(col("call_duration") / 60.0, 2))
    # Audit column for Silver layer processing
    .withColumn("_silver_processed_timestamp", current_timestamp())
)

print(f"Cleaned records count: {cleaned_silver_df.count()}")
display(cleaned_silver_df)