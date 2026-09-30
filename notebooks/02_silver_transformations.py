# Databricks notebook source
# MAGIC %md
# MAGIC # Day 12: Silver Incremental Transformation & Idempotent Delta MERGE INTO (Upsert)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_date, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Read Bronze Incremental Data
# Fetch incoming bronze data
bronze_df = spark.table("workspace.default.bronze_call_records")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Transform & Deduplicate Source Batch
# Ensure incoming batch does not contain internal duplicate primary keys
window_spec = Window.partitionBy("call_id").orderBy(col("_ingestion_timestamp").desc())

clean_incoming_silver_df = (
    bronze_df
    .filter(col("call_id").isNotNull())
    .filter(col("call_duration") > 0)
    .withColumn("call_duration_minutes", (col("call_duration") / 60.0))
    .withColumn("call_date", to_date(col("_ingestion_timestamp")))
    .withColumn("_silver_processed_timestamp", current_timestamp())
    .withColumn("_row_num", row_number().over(window_spec))
    .filter(col("_row_num") == 1)
    .drop("_row_num")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Idempotent Upsert via Delta MERGE INTO

target_table_name = "workspace.default.silver_call_records"

# Check if target silver table exists; if not, initialize it
if not spark.catalog.tableExists(target_table_name):
    (
        clean_incoming_silver_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table_name)
    )
    print(f"Initialized target table: {target_table_name}")
else:
    # Target Delta Table instance
    target_delta_table = DeltaTable.forName(spark, target_table_name)

    # Perform Upsert: update matching call_ids, insert new call_ids
    (
        target_delta_table.alias("target")
        .merge(
            source=clean_incoming_silver_df.alias("source"),
            condition="target.call_id = source.call_id"
        )
        .whenMatchedUpdate(
            condition="source._ingestion_timestamp >= target._ingestion_timestamp",
            set={
                "caller_number": col("source.caller_number"),
                "receiver_number": col("source.receiver_number"),
                "call_duration": col("source.call_duration"),
                "call_duration_minutes": col("source.call_duration_minutes"),
                "call_date": col("source.call_date"),
                "_ingestion_timestamp": col("source._ingestion_timestamp"),
                "_silver_processed_timestamp": col("source._silver_processed_timestamp")
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"Successfully merged incoming records into {target_table_name}")