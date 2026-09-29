# Databricks notebook source
# MAGIC %md
# MAGIC # Day 12: End-to-End Medallion Pipeline Integration Tests
# MAGIC Automated assertions validating schema contracts, data reconciliation, 
# MAGIC and sentiment classification boundaries across the Lakehouse.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 1: Validate Table Availability Across All Layers
# MAGIC Verifies that all expected Delta tables exist and can be queried.

# COMMAND ----------

expected_tables = [
    "workspace.default.bronze_call_records",
    "workspace.default.silver_call_records",
    "workspace.default.silver_call_transcripts_enriched",
    "workspace.default.gold_daily_call_summary",
    "workspace.default.gold_transcript_keyword_frequency",
    "workspace.default.gold_call_sentiment_metrics",
    "workspace.default.gold_escalation_alerts"
]

for table_name in expected_tables:
    table_exists = spark.catalog.tableExists(table_name)
    assert table_exists, f"Integration Failure: Table {table_name} does not exist in catalog!"
    print(f"PASS: Table verified -> {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 2: Validate Schema Contract for Gold Sentiment Table
# MAGIC Checks that downstream BI requirements are met with exact column names.

# COMMAND ----------

gold_sentiment_df = spark.table("workspace.default.gold_call_sentiment_metrics")

required_gold_columns = {
    "call_id",
    "caller_number",
    "receiver_number",
    "call_date",
    "call_duration_minutes",
    "sentiment_classification",
    "_gold_calculated_timestamp"
}

actual_gold_columns = set(gold_sentiment_df.columns)
missing_columns = required_gold_columns - actual_gold_columns

assert len(missing_columns) == 0, f"Schema Contract Failure: Missing columns {missing_columns}"
print("PASS: Gold Sentiment schema contract satisfied.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 3: Sentiment Classification Domain Integrity
# MAGIC Guarantees no unexpected or null category labels were generated during NLP scoring.

# COMMAND ----------

allowed_sentiments = {"Positive", "Negative", "Neutral", "Escalated"}

distinct_sentiments = {
    row["sentiment_classification"]
    for row in gold_sentiment_df.select("sentiment_classification").distinct().collect()
}

invalid_sentiments = distinct_sentiments - allowed_sentiments
assert len(invalid_sentiments) == 0, f"Domain Violation: Found unexpected sentiment classes: {invalid_sentiments}"
print(f"PASS: All sentiment classifications are valid: {distinct_sentiments}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 4: Verify Zero Negative Call Durations in Gold
# MAGIC Asserts that data quality constraints applied in Silver propagated cleanly to Gold.

# COMMAND ----------

invalid_duration_count = (
    spark.table("workspace.default.gold_daily_call_summary")
    .filter((col("total_duration_minutes") < 0) | (col("avg_duration_minutes") < 0))
    .count()
)

assert invalid_duration_count == 0, f"Integrity Failure: Found {invalid_duration_count} rows with negative durations!"
print("PASS: Gold call duration metrics are non-negative.")

# COMMAND ----------

print("\n--- ALL PIPELINE INTEGRATION TESTS PASSED SUCCESSFULLY ---")