# =============================================================================
# Zadanie zaliczeniowe M8 — przykładowe rozwiązanie
# Plik: 02_bronze_to_silver_returns.py
# Cel: Auto Loader bronze Parquet → Delta silver.returns (MERGE INTO)
# =============================================================================

import dbutils
from pyspark.sql import functions as F

dbutils.widgets.text("table_name", "returns")
table_name = dbutils.widgets.get("table_name")

STORAGE      = "shopmastersa"
BRONZE_PATH  = f"abfss://bronze@{STORAGE}.dfs.core.windows.net/{table_name}/"
SILVER_TABLE = f"silver.{table_name}"
CHECKPOINT   = f"abfss://checkpoints@{STORAGE}.dfs.core.windows.net/{table_name}_silver/"
SCHEMA_PATH  = f"abfss://checkpoints@{STORAGE}.dfs.core.windows.net/{table_name}_schema/"

VALID_REASONS = ["damaged", "wrong_item", "changed_mind", "other"]

df_stream = (
    spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "parquet")
      .option("cloudFiles.schemaLocation", SCHEMA_PATH)
      .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
      .load(BRONZE_PATH)
      # Normalizacja powodu zwrotu — nieznane → 'other'
      .withColumn(
          "return_reason",
          F.when(F.col("return_reason").isin(VALID_REASONS), F.col("return_reason"))
           .otherwise(F.lit("other"))
      )
      .withColumn("returned_at", F.to_timestamp("returned_at"))
      .withColumn("refund_amount_pln", F.col("refund_amount_pln").cast("decimal(12,2)"))
      .withColumn("_processed_at", F.current_timestamp())
)

def merge_returns(batch_df, batch_id):
    batch_df.createOrReplaceTempView("batch_returns")
    if spark.catalog.tableExists(SILVER_TABLE):
        spark.sql(f"""
            MERGE INTO {SILVER_TABLE} AS t
            USING batch_returns AS s
            ON t.return_id = s.return_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    else:
        batch_df.write.format("delta").saveAsTable(SILVER_TABLE)

(
    df_stream.writeStream
      .foreachBatch(merge_returns)
      .option("checkpointLocation", CHECKPOINT)
      .trigger(availableNow=True)
      .start()
      .awaitTermination()
)

print(f"✅ silver.{table_name}: {spark.table(SILVER_TABLE).count():,} wierszy")

spark.sql(f"""
    SELECT return_reason, COUNT(*) as cnt, ROUND(SUM(refund_amount_pln),2) as total_refund
    FROM {SILVER_TABLE}
    GROUP BY return_reason ORDER BY cnt DESC
""").show()
