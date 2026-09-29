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












# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. KPI 4: Customer Sentiment Classification
# MAGIC Computes a sentiment score across customer transcripts:
# MAGIC - Escalated: Any high-risk escalation keyword present
# MAGIC - Positive: Positive tokens exceed negative tokens
# MAGIC - Neutral: Equal count or no explicit sentiment tokens detected

# COMMAND ----------

from pyspark.sql.functions import array_intersect, array, lit, size, when, col

def apply_sentiment_scoring(df, token_col: str):
    """
    Classifies transcripts into Positive, Neutral, or Escalated using set-based word counts.
    """
    positive_words = ["happy", "resolved", "great", "thank", "thanks", "helpful", "satisfied", "excellent"]
    negative_words = ["angry", "bad", "slow", "broken", "issue", "delay", "poor", "unhelpful"]
    escalation_words = ["cancel", "refund", "supervisor", "manager", "attorney"]

    spark_pos = array(*[lit(w) for w in positive_words])
    spark_neg = array(*[lit(w) for w in negative_words])
    spark_esc = array(*[lit(w) for w in escalation_words])

    return (
        df
        # 1. Count matching tokens for each sentiment dictionary
        .withColumn("pos_matches", size(array_intersect(col(token_col), spark_pos)))
        .withColumn("neg_matches", size(array_intersect(col(token_col), spark_neg)))
        .withColumn("esc_matches", size(array_intersect(col(token_col), spark_esc)))
        # 2. Rule hierarchy: Escalated takes precedence, then Positive vs Negative
        .withColumn(
            "sentiment_classification",
            when(col("esc_matches") > 0, "Escalated")
            .when(col("pos_matches") > col("neg_matches"), "Positive")
            .when(col("neg_matches") > col("pos_matches"), "Negative")
            .otherwise("Neutral")
        )
        .drop("pos_matches", "neg_matches", "esc_matches")
    )

# COMMAND ----------

# Apply sentiment scoring on the enriched Silver transcript table
gold_sentiment_df = apply_sentiment_scoring(silver_transcripts_df, "transcript_tokens")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Write Sentiment KPI Table to Delta Lake

# COMMAND ----------

(
    gold_sentiment_df
    .select(
        "call_id",
        "caller_number",
        "receiver_number",
        "call_date",
        "call_duration_minutes",
        "sentiment_classification",
        "_silver_processed_timestamp"
    )
    .withColumn("_gold_calculated_timestamp", current_timestamp())
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.default.gold_call_sentiment_metrics")
)

print("Gold sentiment KPI table successfully written.")

