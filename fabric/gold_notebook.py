# =============================================================================
# Zadanie zaliczeniowe M8 — przykładowe rozwiązanie
# Plik: gold_notebook.py (uruchomić jako Fabric Notebook)
# Cel: 3 gold tables z silver.shipments i silver.returns
# Wymagania: Fabric Lakehouse z Shortcutem do ADLS silver
# =============================================================================

from pyspark.sql import functions as F

# =============================================================================
# GOLD TABLE 1: shipment_metrics_daily
# Dzienne metryki per carrier: liczba wysyłek, avg czas dostawy, koszty, on-time rate
# SLA breach = delivered_at - shipped_at > 72h
# =============================================================================

silver_shipments = spark.read.table("silver.shipments")

gold_shipment_metrics = (
    silver_shipments
    .filter(F.col("shipped_at").isNotNull())
    .withColumn(
        "delivery_hours",
        F.when(
            F.col("delivered_at").isNotNull(),
            (F.unix_timestamp("delivered_at") - F.unix_timestamp("shipped_at")) / 3600
        ).otherwise(F.lit(None))
    )
    .withColumn("date", F.to_date("shipped_at"))
    .groupBy("date", "carrier")
    .agg(
        F.count("shipment_id").alias("total_shipments"),
        F.round(F.avg("delivery_hours"), 2).alias("avg_delivery_time_hours"),
        F.round(F.sum("cost_pln"), 2).alias("total_cost_pln"),
        F.round(
            F.sum(F.when(F.col("delivery_hours") <= 72, 1).otherwise(0)) /
            F.count(F.col("delivered_at")) * 100,
            1
        ).alias("on_time_rate_pct")
    )
    .orderBy("date", "carrier")
)

gold_shipment_metrics.write.mode("overwrite").format("delta").saveAsTable("gold.shipment_metrics_daily")
print(f"✅ gold.shipment_metrics_daily: {gold_shipment_metrics.count()} wierszy")

# =============================================================================
# GOLD TABLE 2: returns_by_category
# Zwroty per kategoria produktu i powód — wymaga join z products
# Jeśli brak tabeli products — używamy product_id jako zastępnik kategorii
# =============================================================================

silver_returns = spark.read.table("silver.returns")

if spark.catalog.tableExists("silver.products"):
    silver_products = spark.read.table("silver.products").select("product_id", "product_category")
    returns_with_category = silver_returns.join(silver_products, on="product_id", how="left")
else:
    returns_with_category = silver_returns.withColumn("product_category", F.lit("uncategorized"))

total_orders = spark.read.table("silver.orders").count() if spark.catalog.tableExists("silver.orders") else 1

gold_returns = (
    returns_with_category
    .groupBy("product_category", "return_reason")
    .agg(
        F.count("return_id").alias("return_count"),
        F.round(F.sum("refund_amount_pln"), 2).alias("total_refund_pln"),
        F.round(F.count("return_id") / total_orders * 100, 2).alias("return_rate_pct")
    )
    .orderBy(F.col("return_count").desc())
)

gold_returns.write.mode("overwrite").format("delta").saveAsTable("gold.returns_by_category")
print(f"✅ gold.returns_by_category: {gold_returns.count()} wierszy")

# =============================================================================
# GOLD TABLE 3: sla_breach_count
# Naruszenia SLA (>72h) per carrier per miesiąc
# =============================================================================

gold_sla = (
    silver_shipments
    .filter(F.col("shipped_at").isNotNull())
    .filter(F.col("delivered_at").isNotNull())
    .withColumn(
        "delivery_hours",
        (F.unix_timestamp("delivered_at") - F.unix_timestamp("shipped_at")) / 3600
    )
    .withColumn("month", F.date_trunc("month", F.col("shipped_at")).cast("date"))
    .groupBy("carrier", "month")
    .agg(
        F.count("shipment_id").alias("total_shipments"),
        F.sum(F.when(F.col("delivery_hours") > 72, 1).otherwise(0)).alias("breached_shipments")
    )
    .withColumn(
        "breach_rate_pct",
        F.round(F.col("breached_shipments") / F.col("total_shipments") * 100, 1)
    )
    .orderBy("month", "carrier")
)

gold_sla.write.mode("overwrite").format("delta").saveAsTable("gold.sla_breach_count")
print(f"✅ gold.sla_breach_count: {gold_sla.count()} wierszy")

# =============================================================================
# PODSUMOWANIE
# =============================================================================

print("\n=== GOLD LAYER KOMPLETNY ===")
for tbl in ["gold.shipment_metrics_daily", "gold.returns_by_category", "gold.sla_breach_count"]:
    cnt = spark.table(tbl).count()
    print(f"  {tbl:<40} {cnt:>6,} wierszy")
