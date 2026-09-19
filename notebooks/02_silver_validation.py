# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Data Quality & Schema Validation
# MAGIC Validates Bronze records:
# MAGIC - `call_id` is NOT NULL
# MAGIC - `call_duration` > 0
# MAGIC 
# MAGIC Routes valid records to `silver_call_records` and failing records to `quarantine_call_records`.

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

CATALOG_NAME = "workspace"
SCHEMA_NAME = "default"
BRONZE_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.bronze_call_records"
SILVER_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.silver_call_records"
QUARANTINE_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.quarantine_call_records"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Read Bronze Layer Data

# COMMAND ----------

df_bronze = spark.read.table(BRONZE_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Define Quality Conditions

# COMMAND ----------

# Validation rule 1: call_id must not be null
# Validation rule 2: call_duration must be positive (> 0)
valid_condition = (col("call_id").isNotNull()) & (col("call_duration") > 0)

# Filter valid records
df_valid = (
    df_bronze
    .filter(valid_condition)
    .withColumn("_silver_processed_at", current_timestamp())
)

# Filter invalid records (Quarantine)
df_quarantine = (
    df_bronze
    .filter(~valid_condition)
    .withColumn("_quarantined_at", current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Write Valid Data to Silver

# COMMAND ----------

(
    df_valid.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

print(f"Appended {df_valid.count()} valid records to {SILVER_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Write Faulty Data to Quarantine

# COMMAND ----------

(
    df_quarantine.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(QUARANTINE_TABLE)
)

print(f"Routed {df_quarantine.count()} invalid records to {QUARANTINE_TABLE}")