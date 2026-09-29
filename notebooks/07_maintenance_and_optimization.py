# Databricks notebook source
# MAGIC %md
# MAGIC # Day 15: Lakehouse Maintenance — Compaction, Z-Ordering, and File Retention (VACUUM)
# MAGIC Routine maintenance scripts executed to:
# MAGIC 1. Compact small Delta files using OPTIMIZE
# MAGIC 2. Enable data skipping on high-cardinality query columns using Z-ORDER
# MAGIC 3. Prune historical files outside safety retention thresholds using VACUUM

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Optimize Silver Layer Tables
# MAGIC Silver tables experience frequent MERGE INTO upserts, generating small files.
# MAGIC Z-Ordering by call_date improves incremental watermarking and range queries.

# COMMAND ----------

print("Optimizing silver_call_records...")
spark.sql("""
    OPTIMIZE workspace.default.silver_call_records
    ZORDER BY (call_date, call_id)
""")

print("Optimizing silver_call_transcripts_enriched...")
spark.sql("""
    OPTIMIZE workspace.default.silver_call_transcripts_enriched
    ZORDER BY (call_date)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Optimize Gold KPI Tables
# MAGIC Optimizes business summary tables frequently queried by BI dashboards and analysts.

# COMMAND ----------

print("Optimizing gold_daily_call_summary...")
spark.sql("""
    OPTIMIZE workspace.default.gold_daily_call_summary
    ZORDER BY (call_date)
""")

print("Optimizing gold_call_sentiment_metrics...")
spark.sql("""
    OPTIMIZE workspace.default.gold_call_sentiment_metrics
    ZORDER BY (sentiment_classification, call_date)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Reclaim Storage with VACUUM
# MAGIC Cleans up stale commit files older than retention policy (default: 168 hours / 7 days).

# COMMAND ----------

# Maintain a 7-day retention safety window for time travel queries and concurrent transactions
spark.sql("""
    VACUUM workspace.default.silver_call_records RETAIN 168 HOURS
""")

spark.sql("""
    VACUUM workspace.default.gold_call_sentiment_metrics RETAIN 168 HOURS
""")

print("Delta maintenance routines completed successfully.")