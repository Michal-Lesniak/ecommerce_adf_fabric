# =============================================================================
# Zadanie zaliczeniowe M8 — przykładowe rozwiązanie
# Plik: 01_bronze_to_silver_shipments.py
# Cel: Auto Loader bronze Parquet → Delta silver.shipments (MERGE INTO)
# Wymagania: Databricks Runtime 13.3+, ADLS Gen2 dostępny
# =============================================================================

import dbutils
from pyspark.sql import functions as F
from delta.tables import DeltaTable

dbutils.widgets.text("file_path", "")
dbutils.widgets.text("table_name", "shipments")

table_name = dbutils.widgets.get("table_name")

STORAGE       = "shopmastersa"
BRONZE_PATH   = f"abfss://bronze@{STORAGE}.dfs.core.windows.net/{table_name}/"
SILVER_TABLE  = f"silver.{table_name}"
CHECKPOINT    = f"abfss://checkpoints@{STORAGE}.dfs.core.windows.net/{table_name}_silver/"
SCHEMA_PATH   = f"abfss://checkpoints@{STORAGE}.dfs.core.windows.net/{table_name}_schema/"

print(f"Processing: {BRONZE_PATH} → {SILVER_TABLE}")

# Auto Loader — incremental read z bronze
df_stream = (
    spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "parquet")
      .option("cloudFiles.schemaLocation", SCHEMA_PATH)
      .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
      .option("cloudFiles.inferColumnTypes", "true")
      .load(BRONZE_PATH)
      .withColumn("carrier", F.upper(F.trim(F.col("carrier"))))
      .withColumn("status", F.lower(F.trim(F.col("status"))))
      .withColumn("_processed_at", F.current_timestamp())
)

# MERGE INTO silver — deduplication na shipment_id
def merge_shipments(batch_df, batch_id):
    batch_df.createOrReplaceTempView("batch_shipments")

    if spark.catalog.tableExists(SILVER_TABLE):
        spark.sql(f"""
            MERGE INTO {SILVER_TABLE} AS target
            USING batch_shipments AS source
            ON target.shipment_id = source.shipment_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    else:
        batch_df.write.format("delta").saveAsTable(SILVER_TABLE)

    count = spark.table(SILVER_TABLE).count()
    print(f"  Batch {batch_id}: silver.{table_name} = {count:,} wierszy")

(
    df_stream.writeStream
      .foreachBatch(merge_shipments)
      .option("checkpointLocation", CHECKPOINT)
      .trigger(availableNow=True)
      .start()
      .awaitTermination()
)

print(f"✅ silver.{table_name}: {spark.table(SILVER_TABLE).count():,} wierszy")

# Weryfikacja: statystyki
spark.sql(f"""
    SELECT carrier, status, COUNT(*) as cnt, ROUND(SUM(cost_pln),2) as total_cost
    FROM {SILVER_TABLE}
    GROUP BY carrier, status
    ORDER BY carrier, cnt DESC
""").show()
