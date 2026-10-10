# End-to-End Databricks Medallion Lakehouse Pipeline

An enterprise-grade, automated data pipeline implementing the **Medallion Architecture** (Bronze, Silver, Gold) on Azure Databricks, PySpark, and Delta Lake. It features automated schema enforcement, quarantine routing, engine-level Delta CHECK constraints, NLP transcript tokenization, multi-task workflow orchestration, and CI/CD automation.

---

## 🏛️ Architecture & Data Flow

```text
====================================================================================================
                        MEDALLION LAKEHOUSE ARCHITECTURE & DATA FLOW
====================================================================================================

      +-----------------------------------------+
      |  External Sources (CSV / JSON Files)    |
      +-----------------------------------------+
                           |
                           | Auto Loader / Batch Ingestion
                           v
      +-----------------------------------------+
      |              BRONZE LAYER               |
      |       (workspace.default.bronze)        |
      |-----------------------------------------|
      | * Raw, unparsed data ingestion          |
      | * Audit metadata added:                 |
      |   - _source_file                        |
      |   - _ingestion_timestamp                |
      +-----------------------------------------+
                           |
                           | Incremental Watermark Filter
                           v
      +-----------------------------------------+
      |              SILVER LAYER               |
      |       (workspace.default.silver)        |
      |-----------------------------------------|
      | * Enforce strict schema & cast types    |
      | * Standardize timestamps to UTC         |
      | * Cleaning & Deduplication              |
      | * Idempotent Delta MERGE INTO (Upsert)  |
      | * Tokenize customer call transcripts    |
      +--------------------+--------------------+
                           |
            +--------------+--------------+
            |                             |
            v Quality Split               v Passed Records
+-----------------------+   +------------------------------------------+
|   QUARANTINE TABLE    |   |         SILVER ENRICHED TABLES           |
| (quarantine_records)  |   | (silver_call_records, silver_nlp_tokens) |
|-----------------------|   +---------------------+--------------------+
| * Null Call IDs       |                         |
| * Negative durations  |                         | Aggregations & Sentiment NLP
+-----------------------+                         v
                            +------------------------------------------+
                            |                GOLD LAYER                |
                            |         (workspace.default.gold)         |
                            |------------------------------------------|
                            | * gold_daily_call_summary                |
                            |   - Total calls, volume, avg duration    |
                            | * gold_transcript_keyword_frequency      |
                            |   - Unrolled tokens (explode)            |
                            | * gold_call_sentiment_metrics            |
                            |   - Positive, Neutral, Negative          |
                            | * gold_escalation_alerts                 |
                            |   - Flagged supervisor/refund/cancel     |
                            +---------------------+--------------------+
                                                  |
                                                  v
                            +------------------------------------------+
                            |       ANALYTICS & CONSUMPTION            |
                            |------------------------------------------|
                            | * Power BI / Tableau Dashboards          |
                            | * Customer Success Escalation Alerts     |
                            | * Executive KPI Reports                  |
                            +------------------------------------------+

====================================================================================================
                      ORCHESTRATION & CI/CD INFRASTRUCTURE
====================================================================================================

    +-------------------+      git push       +----------------------+
    | Local Development | ------------------> |   GitHub Repository  |
    |  (VS Code / Git)  |                     | (origin/main branch) |
    +-------------------+                     +----------+-----------+
                                                         |
                                                         | Triggers
                                                         v
                                              +----------------------+
                                              |    GitHub Actions    |
                                              | (pipeline_ci.yml)    |
                                              |----------------------|
                                              | 1. Lint with flake8  |
                                              | 2. Run pytest suite  |
                                              +----------+-----------+
                                                         |
                                                         | Deploys Config
                                                         v
                                              +----------------------+
                                              | Databricks Workflows |
                                              | (Multi-task DAG Job) |
                                              |----------------------|
                                              | Bronze -> Silver ->  |
                                              | NLP/Quality -> Gold  |
                                              +----------------------+
```

---

## 📅 Pipeline Implementation Roadmap

| Stage | Focus Area | Deliverables / Modules |
|---|---|---|
| **Days 1–3** | Environment & Setup | Unity Catalog initialization (`workspace.default`), cluster provisioning, synthetic call log generation. |
| **Day 4** | Bronze Ingestion | `notebooks/01_bronze_ingestion.py`: Raw JSON ingestion with audit metadata (`_source_file`, `_ingestion_timestamp`). |
| **Day 5** | Quarantine Validation | `notebooks/04_silver_quality_checks.py`: Routes anomalies (`call_id IS NULL`, `call_duration <= 0`) to quarantine tables. |
| **Day 6** | Maintenance & Z-Order | `notebooks/03_bronze_maintenance.py`: Compacts small Parquet files via `OPTIMIZE` and clusters along `call_date` with `ZORDER`. |
| **Day 7** | Architecture Review | Documentation of raw ingestion guarantees and Lakehouse storage patterns. |
| **Day 8** | Incremental Silver Reading | `notebooks/02_silver_transformations.py`: Timestamp watermarking to consume only net-new Bronze records. |
| **Day 9** | Schema & NLP Tokenization | `notebooks/05_silver_nlp_transformations.py`: Enforces explicit schema casting and tokenizes transcripts using regex. |
| **Day 10** | Gold Layer KPI Aggregations | `notebooks/06_gold_call_kpis.py`: Generates `gold_daily_call_summary` and customer complaint keyword metrics via `explode()`. |
| **Day 11** | Sentiment Heuristic | Implements deterministic rule-based sentiment routing (`Positive`, `Negative`, `Neutral`, `Escalated`). |
| **Day 12** | Delta Lake Upsert (`MERGE`) | Implements idempotent `MERGE INTO` deduplication on primary key `call_id`. |
| **Day 13** | Automated Testing Suite | `tests/test_end_to_end_pipeline.py`: Validates table availability, schema contracts, and domain constraints. |
| **Day 14** | CI/CD Automation | `.github/workflows/pipeline_ci.yml`: Automated GitHub Actions runner for `flake8` linting and `pytest`. |
| **Day 15** | Storage Optimization | `notebooks/07_maintenance_and_optimization.py`: Table optimization (`OPTIMIZE`, `ZORDER`) and vacuuming historical logs (`VACUUM ... RETAIN 168 HOURS`). |
| **Day 16** | Engine Table Constraints | Enforces Delta Lake table-level `CHECK` constraints (`valid_call_id`, `positive_duration`) directly in Unity Catalog. |
| **Day 17** | Production Orchestration | `workflows/pipeline_job_config.json`: Multi-task DAG workflow with task dependencies, timeouts, and email alerting. |

---

## 🔍 Core Engineering Patterns

### 1. Bronze Layer Ingestion
- **Append-Only Immutability:** Preserves raw landing records as-is for auditable history.
- **Audit Lineage:** Every ingested row is tracked with `_source_file` and `_ingestion_timestamp`.

### 2. Silver Quality, Deduplication & Storage Constraints
- **Quarantine Routing:** Anomaly rows are isolated into quarantine tables rather than dropped, maintaining count reconciliation ($\text{Bronze} = \text{Silver} + \text{Quarantine}$).
- **Idempotent Upsert (`MERGE INTO`):** Prevents duplicate entries on notebook re-runs by matching on `call_id`.
- **In-Engine CHECK Constraints:** Unity Catalog/Delta metadata rules block non-compliant writes at the storage layer:
  - `valid_call_id`: `CHECK (call_id IS NOT NULL)`
  - `positive_duration`: `CHECK (call_duration > 0)`

### 3. Gold Reporting & Text Analytics
- **Native PySpark NLP:** Regex cleaning and array operations (`array_intersect`, `explode`) avoid JVM Python serialization overhead.
- **Atomic Overwrites:** Uses Delta `.mode("overwrite")` with schema evolution to guarantee clean reads for BI dashboards without downtime.

### 4. Storage Maintenance & Compaction
- **OPTIMIZE & Z-ORDER:** Solves the small file problem by compacting Parquet files to ~1 GB and clustering high-cardinality query keys (`call_date`, `call_id`) for file skipping.
- **VACUUM:** Safely reclaims storage space while retaining a 7-day (168-hour) window for Delta Time Travel and ACID isolation.

---

## 📂 Repository Layout

```text
├── .github/
│   └── workflows/
│       └── pipeline_ci.yml              # GitHub Actions CI for linting & tests
├── notebooks/
│   ├── 01_bronze_ingestion.py           # Ingestion from landing volume with lineage
│   ├── 02_silver_transformations.py      # Watermarking, cleaning & Delta MERGE upsert
│   ├── 04_silver_quality_checks.py      # Quarantine validation rules
│   ├── 05_silver_nlp_transformations.py # Schema casting & transcript tokenization
│   ├── 06_gold_call_kpis.py             # KPI summaries, sentiment scoring & alerts
│   └── 07_maintenance_and_optimization.py # OPTIMIZE, ZORDER, and VACUUM routines
├── tests/
│   └── test_end_to_end_pipeline.py      # End-to-end integration and contract test suite
├── workflows/
│   └── pipeline_job_config.json         # Databricks Workflows multi-task DAG definition
└── README.md
```

---

## 🛠️ Tech Stack
- **Compute Engine:** Apache Spark (PySpark), Delta Lake
- **Cloud Platform:** Azure Databricks, Unity Catalog
- **Languages:** Python, SQL, Bash
- **DevOps & Quality:** Git, GitHub Actions, PyTest
