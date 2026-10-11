# Databricks Lakehouse Maintenance Script

tables = [
    "workspace.default.bronze_call_records",
    "workspace.default.silver_call_records",
    "workspace.default.silver_call_transcripts_enriched",
    "workspace.default.gold_daily_call_summary",
    "workspace.default.gold_call_sentiment_metrics"
]

for table_name in tables:
    if spark.catalog.tableExists(table_name):
        print(f"Running maintenance on {table_name}...")
        spark.sql(f"OPTIMIZE {table_name}")
        print(f"Successfully optimized {table_name}")
    else:
        print(f"Skipping {table_name}: not created yet")

print("All table maintenance completed successfully!")