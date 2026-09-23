# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: Compaction and Z-Ordering Maintenance
# MAGIC Compacts small files and creates multi-dimensional clustering on `call_date`.

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE workspace.default.bronze_call_records
# MAGIC ZORDER BY (call_date);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY workspace.default.bronze_call_records
# MAGIC LIMIT 1;