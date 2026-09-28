# Databricks notebook source
# MAGIC %md
# MAGIC # Day 9: Silver Transformations — Schema Standardization & Transcript Tokenization

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    lower,
    regexp_replace,
    split,
    array_remove,
    trim,
    current_timestamp,
    lit
)
from pyspark.sql.types import (
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
    DateType
)

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Read Base Silver Data
silver_base_df = spark.table("workspace.default.silver_call_records")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Standardize Timestamps & Cast Datatypes
typed_silver_df = (
    silver_base_df
    .withColumn("call_id", col("call_id").cast(StringType()))
    .withColumn("caller_number", col("caller_number").cast(StringType()))
    .withColumn("receiver_number", col("receiver_number").cast(StringType()))
    .withColumn("call_duration", col("call_duration").cast(IntegerType()))
    .withColumn("call_duration_minutes", col("call_duration_minutes").cast(DoubleType()))
    .withColumn("call_date", col("call_date").cast(DateType()))
    .withColumn("call_timestamp", to_timestamp(col("_ingestion_timestamp"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("_ingestion_timestamp", col("_ingestion_timestamp").cast(TimestampType()))
    .withColumn("_silver_processed_timestamp", current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Transcript Cleaning & Tokenization
# Add sample transcript text if the column does not exist yet
if "call_transcript" not in typed_silver_df.columns:
    typed_silver_df = typed_silver_df.withColumn(
        "call_transcript",
        lit("Customer reached out regarding unexpected account fee! Issue resolved after billing adjustment.")
    )

tokenized_silver_df = (
    typed_silver_df
    .withColumn("clean_text", lower(col("call_transcript")))
    .withColumn("clean_text", regexp_replace(col("clean_text"), r"[^a-zA-Z\s]", " "))
    .withColumn("clean_text", trim(regexp_replace(col("clean_text"), r"\s+", " ")))
    .withColumn("transcript_tokens", split(col("clean_text"), " "))
    .withColumn("transcript_tokens", array_remove(col("transcript_tokens"), ""))
    .drop("clean_text")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Write Enriched Data to Delta Table
(
    tokenized_silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.default.silver_call_transcripts_enriched")
)