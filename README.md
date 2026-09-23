# Databricks Medallion Architecture Pipeline

A robust end-to-end Lakehouse batch ingestion and processing pipeline built on Azure Databricks, PySpark, and Delta Lake.

---

## 🏗️ Architecture Overview

The pipeline implements the standard multi-hop Medallion Architecture:

```text
Raw Sources (CSV / JSON / Mock)
              │
              ▼
┌─────────────────────────────────────────┐
│              Bronze Layer               │
│  - Raw append-only historical records   │
│  - Lineage: _ingestion_timestamp,       │
│             _source_file, call_date     │
│  - Maintenance: OPTIMIZE & Z-ORDER      │
└──────────────────┬──────────────────────┘
                   │
          ┌────────┴────────┐
          ▼                 ▼
┌──────────────────┐ ┌────────────────────┐
│   Silver Layer   │ │  Quarantine Layer  │
│ - Schema checks  │ │ - Null IDs         │
│ - Delta MERGE    │ │ - Non-positive     │
│   (Deduplication)│ │   durations        │
└──────────────────┘ └────────────────────┘
          │
          ▼
┌─────────────────────────────────────────┐
│               Gold Layer                │
│  - Aggregated KPIs & Business Metrics   │
└─────────────────────────────────────────┘