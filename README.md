# databricks-lakehouse-pipeline
Updated upstream


# Databricks Lakehouse Pipeline (Medallion Architecture)

A 3-week end-to-end data engineering project built with PySpark, Delta Lake, Unity Catalog, and Databricks Workflows.

## Architecture
- **Bronze:** Raw data ingestion using Auto Loader (`cloudFiles`).
- **Silver:** Cleaned, deduplicated, and schema-enforced Delta tables (`MERGE INTO`).
- **Gold:** Curated aggregations and business-level KPI tables.

## Structure
- `notebooks/`: Databricks exploration and transformation notebooks.
- `src/`: Reusable Python modules and ingestion logic.
- `tests/`: Automated unit and schema validation tests.
- `workflows/`: Orchestration and multi-task job definitions.

Tasks:
Day 1: Create your GitHub repo. Link it inside your Databricks workspace via Settings → Linked accounts / Git credentials. Clone it under Databricks Workspace / Git Folders.  

Day 2: Write notebooks/00_setup_and_generator.py. Write a pure Python script to generate mock call logs (Call ID, Timestamp, Customer ID, Agent ID, Call Duration, Call Reason, Customer Transcript).

Day 3: Commit test batches of raw data as JSON/Parquet files into Databricks DBFS or Unity Catalog Volumes.

Day 4: Write notebooks/01_bronze_ingestion.py using PySpark to read the raw files and append them to a managed Delta table: bronze_call_records.

Day 5: Add schema validation checks in PySpark (ensure non-null call_id and positive call_duration).

