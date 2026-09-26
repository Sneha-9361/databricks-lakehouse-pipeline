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