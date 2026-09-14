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

