# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: Raw Data Ingestion
# MAGIC Ingests raw JSON call logs from the landing volume and appends them into a managed Delta table.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name

# Configure catalog, schema, and paths
CATALOG_NAME = "workspace"
SCHEMA_NAME = "default"
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/raw_landing"
TARGET_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.bronze_call_records"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Read Raw JSON Files

# COMMAND ----------

raw_json_path = f"{VOLUME_PATH}/mock_call_logs.json"

df_raw = (
    spark.read
    .format("json")
    .option("inferSchema", "true")
    .load(raw_json_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Add Ingestion Metadata

# COMMAND ----------

# Bronze standard: preserve raw data as-is, add audit metadata
df_bronze = (
    df_raw
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source_file", input_file_name())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Append to Managed Delta Table

# COMMAND ----------

(
    df_bronze.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

print(f"Successfully appended records to {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Verification

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.default.bronze_call_records LIMIT 10;