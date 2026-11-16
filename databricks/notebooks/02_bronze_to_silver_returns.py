# =============================================================================
# Zadanie zaliczeniowe M8 — przykładowe rozwiązanie
# Plik: 02_bronze_to_silver_returns.py
# Cel: Przyrostowe ładowanie bronze Parquet → Delta silver.returns (MERGE INTO)
#      z watermarkiem przekazywanym z pipeline ADF.
# =============================================================================

import dbutils
from pyspark.sql import functions as F

dbutils.widgets.text("table_name", "returns")
dbutils.widgets.text("storage_account", "shopmastersa")
dbutils.widgets.text("last_watermark", "1900-01-01 00:00:00")

table_name      = dbutils.widgets.get("table_name")
storage_account = dbutils.widgets.get("storage_account")
last_watermark  = dbutils.widgets.get("last_watermark")

BRONZE_PATH  = f"abfss://bronze@{storage_account}.dfs.core.windows.net/{table_name}/"
SILVER_TABLE = f"silver.{table_name}"

VALID_REASONS = ["damaged", "wrong_item", "changed_mind", "other"]

print(f"Processing: {BRONZE_PATH} → {SILVER_TABLE}")
print(f"Last watermark: {last_watermark}")

df = (
    spark.read
      .format("parquet")
      .load(BRONZE_PATH)
      .withColumn("_ingested_at", F.current_timestamp())
      .filter(F.col("_ingested_at") > F.lit(last_watermark).cast("timestamp"))
      .withColumn(
          "return_reason",
          F.when(F.col("return_reason").isin(VALID_REASONS), F.col("return_reason"))
           .otherwise(F.lit("other"))
      )
      .withColumn("returned_at", F.to_timestamp("returned_at"))
      .withColumn("refund_amount_pln", F.col("refund_amount_pln").cast("decimal(12,2)"))
      .withColumn("_processed_at", F.current_timestamp())
)

row_count = df.count()
print(f"  Wierszy > watermark: {row_count:,}")

if row_count == 0:
    print("  Brak nowych danych — watermark bez zmian")
    dbutils.notebook.exit(last_watermark)

new_watermark = df.agg(F.max("_processed_at")).first()[0]

df.createOrReplaceTempView("batch_returns")

if spark.catalog.tableExists(SILVER_TABLE):
    spark.sql(f"""
        MERGE INTO {SILVER_TABLE} AS t
        USING batch_returns AS s
        ON t.return_id = s.return_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
else:
    df.write.format("delta").saveAsTable(SILVER_TABLE)

total = spark.table(SILVER_TABLE).count()
print(f"OK silver.{table_name}: {total:,} wierszy (nowy watermark: {new_watermark})")

spark.sql(f"""
    SELECT return_reason, COUNT(*) as cnt, ROUND(SUM(refund_amount_pln),2) as total_refund
    FROM {SILVER_TABLE}
    GROUP BY return_reason ORDER BY cnt DESC
""").show()

dbutils.notebook.exit(str(new_watermark))
