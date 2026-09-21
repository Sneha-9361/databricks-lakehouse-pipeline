# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Deduplication & Merge Upsert
# MAGIC Performs Delta Lake MERGE on `silver_call_records`:
# MAGIC - Deduplicates incoming records based on `call_id`
# MAGIC - Updates matching existing rows (matched)
# MAGIC - Inserts new rows (not matched)

# COMMAND ----------

from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable

CATALOG_NAME = "workspace"
SCHEMA_NAME = "default"
SILVER_TABLE_NAME = f"{CATALOG_NAME}.{SCHEMA_NAME}.silver_call_records"

# COMMAND ----------

# 1. Read incoming Silver records that need deduplication
df_silver_source = spark.read.table(SILVER_TABLE_NAME)

# 2. Window specification to keep only the latest record per call_id
window_spec = Window.partitionBy("call_id").orderBy(col("_silver_processed_at").desc())

df_deduped = (
    df_silver_source
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
)

# COMMAND ----------

# 3. Perform Delta MERGE INTO target table
target_table = DeltaTable.forName(spark, SILVER_TABLE_NAME)

(
    target_table.alias("target")
    .merge(
        source=df_deduped.alias("source"),
        condition="target.call_id = source.call_id"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print("Delta MERGE completed: silver table deduplicated and upserted successfully.")