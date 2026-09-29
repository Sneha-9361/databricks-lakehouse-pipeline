# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. KPI 3: Escalation Risk Flagging (Python Function)
# MAGIC Uses a reusable Python function and PySpark native array operations to identify 
# MAGIC high-risk calls requesting cancellations, refunds, or supervisors.

# COMMAND ----------

from pyspark.sql.functions import array_intersect, array, lit, size, when
from pyspark.sql import DataFrame

def flag_escalation_calls(df: DataFrame, token_column: str) -> DataFrame:
    """
    Analyzes an array of transcript tokens and flags the row if it contains 
    any critical escalation keywords.
    """
    # Define our list of high-risk business keywords
    escalation_keywords = ["cancel", "refund", "supervisor", "manager", "attorney"]
    
    # Convert the standard Python list into a PySpark array of Literal values
    spark_keyword_array = array(*[lit(word) for word in escalation_keywords])
    
    # Apply transformation rules
    processed_df = (
        df
        # array_intersect returns matching words found in both the transcript and our keyword list
        .withColumn("escalation_keywords_found", array_intersect(col(token_column), spark_keyword_array))
        # If the size of the intersected array is greater than 0, flag as True
        .withColumn("requires_escalation", when(size(col("escalation_keywords_found")) > 0, True).otherwise(False))
    )
    
    return processed_df

# COMMAND ----------

# Apply the function to our enriched silver data
gold_escalation_alerts_df = flag_escalation_calls(silver_transcripts_df, "transcript_tokens")

# Save the escalation alerts as a distinct Gold table for the customer success team
(
    gold_escalation_alerts_df
    .filter(col("requires_escalation") == True)
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.default.gold_escalation_alerts")
)

print("Escalation alerts table successfully written to Gold.")