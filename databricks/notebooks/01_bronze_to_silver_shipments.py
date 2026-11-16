# =============================================================================
# Zadanie zaliczeniowe M8 — przykładowe rozwiązanie
# Plik: 01_bronze_to_silver_shipments.py
# Cel: Przyrostowe ładowanie bronze Parquet → Delta silver.shipments (MERGE INTO)
#      z watermarkiem przekazywanym z pipeline ADF (etl_watermarks.last_loaded_at).
# Wymagania: Databricks Runtime 13.3+, ADLS Gen2 dostępny
#
# Idempotentność: notebook czyta tylko wiersze gdzie _ingested_at > last_watermark
# i zwraca max(_ingested_at) przez dbutils.notebook.exit() — pipeline zapisuje tę
# wartość do etl_watermarks przez stored procedure update_watermark.
# =============================================================================

import dbutils
from pyspark.sql import functions as F

dbutils.widgets.text("table_name", "shipments")
dbutils.widgets.text("storage_account", "shopmastersa")
dbutils.widgets.text("last_watermark", "1900-01-01 00:00:00")

table_name      = dbutils.widgets.get("table_name")
storage_account = dbutils.widgets.get("storage_account")
last_watermark  = dbutils.widgets.get("last_watermark")

BRONZE_PATH  = f"abfss://bronze@{storage_account}.dfs.core.windows.net/{table_name}/"
SILVER_TABLE = f"silver.{table_name}"

print(f"Processing: {BRONZE_PATH} → {SILVER_TABLE}")
print(f"Last watermark: {last_watermark}")

df = (
    spark.read
      .format("parquet")
      .load(BRONZE_PATH)
      .withColumn("_ingested_at", F.current_timestamp())
      .filter(F.col("_ingested_at") > F.lit(last_watermark).cast("timestamp"))
      .withColumn("carrier", F.upper(F.trim(F.col("carrier"))))
      .withColumn("status", F.lower(F.trim(F.col("status"))))
      .withColumn("_processed_at", F.current_timestamp())
)

row_count = df.count()
print(f"  Wierszy > watermark: {row_count:,}")

if row_count == 0:
    print("  Brak nowych danych — watermark bez zmian")
    dbutils.notebook.exit(last_watermark)

new_watermark = df.agg(F.max("_processed_at")).first()[0]

df.createOrReplaceTempView("batch_shipments")

if spark.catalog.tableExists(SILVER_TABLE):
    spark.sql(f"""
        MERGE INTO {SILVER_TABLE} AS target
        USING batch_shipments AS source
        ON target.shipment_id = source.shipment_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
else:
    df.write.format("delta").saveAsTable(SILVER_TABLE)

total = spark.table(SILVER_TABLE).count()
print(f"OK silver.{table_name}: {total:,} wierszy (nowy watermark: {new_watermark})")

spark.sql(f"""
    SELECT carrier, status, COUNT(*) as cnt, ROUND(SUM(cost_pln),2) as total_cost
    FROM {SILVER_TABLE}
    GROUP BY carrier, status
    ORDER BY carrier, cnt DESC
""").show()

dbutils.notebook.exit(str(new_watermark))
