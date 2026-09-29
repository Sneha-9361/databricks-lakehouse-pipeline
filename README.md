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