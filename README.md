# Databricks Medallion Architecture Data Pipeline

An enterprise-grade, automated data pipeline implementing the Medallion Architecture (Bronze, Silver, Gold) on Azure Databricks, PySpark, and Delta Lake. It features automated schema enforcement, quarantine validation routing, engine-level Delta CHECK constraints, NLP transcript tokenization, multi-task workflow orchestration, and CI/CD automation.

Platform: Azure Databricks (Unity Catalog)
Compute Engine: Apache Spark (PySpark) & Delta Lake
Languages: Python, PySpark, SQL, Bash
DevOps & Orchestration: Git, GitHub Actions, Databricks Workflows, PyTest, Flake8

---

## Architecture Overview & Data Flow

1. Bronze Layer (workspace.default.bronze_call_records)
- Raw batch ingestion from cloud storage volumes (/Volumes/workspace/default/raw_landing).
- Append-only immutability preserving original landing state.
- Automated audit columns tracking data lineage: _source_file, _ingestion_timestamp, and call_date.

2. Silver Layer (workspace.default.silver_call_records)
- Incremental watermark filtering consuming only net-new incoming Bronze records.
- Explicit schema type casting and UTC timestamp standardization.
- Data Quality Validation: Separates valid records from anomalies into a dedicated quarantine table (quarantine_call_records) for NULL call IDs and non-positive call durations.
- Idempotent upserts using Delta MERGE INTO matching on call_id to prevent duplicate rows on pipeline retries.
- Engine-level Delta CHECK constraints enforcing data integrity at the storage layer:
  - valid_call_id: CHECK (call_id IS NOT NULL)
  - positive_duration: CHECK (call_duration > 0)

3. Silver Enriched NLP Layer (workspace.default.silver_call_transcripts_enriched)
- Regex-based text cleaning: strips punctuation, special characters, and digits, converting text to lowercase.
- PySpark array operations: tokenizes conversation transcripts into clean word arrays (transcript_tokens) for topic and sentiment extraction.

4. Gold Analytics Layer (workspace.default.gold_*)
- gold_daily_call_summary: Aggregates daily call counts, total volumes, and average talk durations.
- gold_call_sentiment_metrics: Rule-based sentiment analysis classifying conversations into Positive, Neutral, Negative, and Escalated categories.
- gold_escalation_alerts: Identifies and flags high-risk cancellation, supervisor request, or refund keywords for operational alerting.

5. Orchestration & Maintenance
- Databricks Workflows: Multi-task DAG orchestrating end-to-end execution across Bronze, Silver, Quality, NLP, Gold, and Maintenance tasks.
- Storage Optimization: Scheduled OPTIMIZE jobs with Z-ORDER clustering on high-cardinality filter keys (call_date, call_id).
- Storage Reclaim: VACUUM pruning historical commit files outside the 7-day (168-hour) Delta Time Travel retention window.

---

## Daily Implementation Roadmap

- Days 1 to 3: Environment Setup & Data Generation
Configured Unity Catalog workspace, provisioned Spark cluster, and generated synthetic batch call transcripts and call log files.

- Day 4: Bronze Ingestion Layer
Created notebooks/01_bronze_ingestion.py to ingest raw JSON call logs and append lineage tracking columns (_source_file, _ingestion_timestamp).

- Day 5: Quality Assurance & Quarantine Routing
Created notebooks/04_silver_quality_checks.py to validate incoming records against business rules and isolate defective records into quarantine storage.

- Day 6: Bronze Compaction & Z-Ordering
Created notebooks/03_bronze_maintenance.py to compact small files via OPTIMIZE and cluster data along call_date using Z-ORDER.

- Day 7: Architecture Review & Baseline Documentation
Documented Bronze ingestion guarantees, lineage tracking, and Lakehouse transaction log design.

- Day 8: Silver Incremental Ingestion
Implemented watermark-based incremental filtering to read and transform only newly landed Bronze records.

- Day 9: Schema Casting & Transcript Tokenization
Created notebooks/05_silver_nlp_transformations.py to enforce explicit schema datatypes, UTC timestamps, and regex-based NLP text tokenization.

- Day 10: Gold KPI Aggregations
Created notebooks/06_gold_call_kpis.py to calculate daily volume rollups, average call duration, and keyword frequencies.

- Day 11: Sentiment Classification & Escalations
Built deterministic rule-based sentiment routing and supervisor escalation alerting logic.

- Day 12: Delta Lake MERGE INTO Upserts
Created notebooks/02_silver_transformations.py implementing idempotent MERGE INTO upserts on call_id to eliminate duplicate records during pipeline retries.

- Day 13: Automated Testing Suite
Created tests/test_end_to_end_pipeline.py containing pytest contract and schema assertion tests.

- Day 14: CI/CD Pipeline Automation
Configured .github/workflows/pipeline_ci.yml to run automated flake8 linting and pytest test suites on every pull request and push to the main branch.

- Day 15: Lakehouse Storage Maintenance & Safe Optimization
Created notebooks/07_maintenance_and_optimization.py to safely execute OPTIMIZE, Z-ORDER, and VACUUM routines across active catalog tables.

- Day 16: Engine-Level Delta Constraints
Configured ALTER TABLE ADD CONSTRAINT rules directly in Delta Lake to enforce non-null keys and positive durations at the storage engine level.

- Day 17: Production Orchestration DAG
Configured workflows/pipeline_job_config.json to run the multi-task Medallion Lakehouse ETL pipeline end-to-end with automated retry and dependency management.

---

## Repository Structure

- .github/workflows/pipeline_ci.yml: GitHub Actions CI pipeline for linting and automated tests
- notebooks/01_bronze_ingestion.py: Raw ingestion with lineage metadata
- notebooks/02_silver_transformations.py: Watermarking, cleaning, and Delta MERGE upserts
- notebooks/04_silver_quality_checks.py: Data validation and quarantine isolation
- notebooks/05_silver_nlp_transformations.py: Datatype standardization and transcript tokenization
- notebooks/06_gold_call_kpis.py: Business aggregations, sentiment metrics, and escalation alerts
- notebooks/07_maintenance_and_optimization.py: Safe Delta OPTIMIZE, Z-ORDER, and VACUUM routines
- tests/test_end_to_end_pipeline.py: Automated pipeline schema and contract test suite
- workflows/pipeline_job_config.json: Databricks Workflows multi-task DAG definition
- README.md: Comprehensive project documentation
