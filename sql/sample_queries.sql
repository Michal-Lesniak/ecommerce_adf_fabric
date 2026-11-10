-- =============================================================================
-- Zadanie zaliczeniowe M8 — przykładowe rozwiązanie
-- Plik: sample_queries.sql
-- Cel: Weryfikacja poprawności danych w warstwach silver i gold
-- Wymagania: Databricks SQL lub Fabric SQL Analytics Endpoint
-- =============================================================================

-- =========================================================
-- WERYFIKACJA: silver.shipments
-- =========================================================

-- Liczba wierszy i unikalność shipment_id (brak duplikatów)
SELECT
    COUNT(*)                                    AS total_rows,
    COUNT(DISTINCT shipment_id)                 AS unique_shipments,
    COUNT(*) - COUNT(DISTINCT shipment_id)      AS duplicates
FROM silver.shipments;
-- Oczekiwane: duplicates = 0

-- Rozkład statusów
SELECT status, COUNT(*) AS cnt
FROM silver.shipments
GROUP BY status
ORDER BY cnt DESC;
-- Oczekiwane: delivered, in_transit, shipped, returned

-- Rozkład carrier (po normalizacji — tylko UPPERCASE)
SELECT carrier, COUNT(*) AS cnt, ROUND(SUM(cost_pln), 2) AS total_cost
FROM silver.shipments
GROUP BY carrier
ORDER BY cnt DESC;
-- Oczekiwane: DHL, GLS, InPost, DPD (uppercase)

-- Przesyłki z podejrzanie długim czasem dostawy (> 7 dni = SLA breach ryzyko)
SELECT
    shipment_id,
    carrier,
    shipped_at,
    delivered_at,
    ROUND((UNIX_TIMESTAMP(delivered_at) - UNIX_TIMESTAMP(shipped_at)) / 3600, 1) AS delivery_hours
FROM silver.shipments
WHERE delivered_at IS NOT NULL
  AND (UNIX_TIMESTAMP(delivered_at) - UNIX_TIMESTAMP(shipped_at)) / 3600 > 72
ORDER BY delivery_hours DESC;

-- =========================================================
-- WERYFIKACJA: silver.returns
-- =========================================================

-- Rozkład powodów zwrotu
SELECT return_reason, COUNT(*) AS cnt, ROUND(SUM(refund_amount_pln), 2) AS total_refund
FROM silver.returns
GROUP BY return_reason
ORDER BY cnt DESC;
-- Oczekiwane: damaged, wrong_item, changed_mind, other (bez wartości spoza listy)

-- Sprawdź czy brak wartości NULL w kluczowych kolumnach
SELECT
    COUNT(*)                                AS total,
    SUM(CASE WHEN return_id IS NULL THEN 1 ELSE 0 END)          AS null_return_id,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END)           AS null_order_id,
    SUM(CASE WHEN return_reason IS NULL THEN 1 ELSE 0 END)      AS null_reason,
    SUM(CASE WHEN refund_amount_pln IS NULL THEN 1 ELSE 0 END)  AS null_refund
FROM silver.returns;

-- =========================================================
-- WERYFIKACJA: gold.shipment_metrics_daily
-- =========================================================

SELECT
    date,
    carrier,
    total_shipments,
    avg_delivery_time_hours,
    total_cost_pln,
    on_time_rate_pct
FROM gold.shipment_metrics_daily
ORDER BY date DESC, carrier
LIMIT 20;

-- Sprawdź czy on_time_rate_pct mieści się w 0-100
SELECT
    MIN(on_time_rate_pct)   AS min_rate,
    MAX(on_time_rate_pct)   AS max_rate,
    AVG(on_time_rate_pct)   AS avg_rate
FROM gold.shipment_metrics_daily;
-- Oczekiwane: min >= 0, max <= 100

-- =========================================================
-- WERYFIKACJA: gold.returns_by_category
-- =========================================================

SELECT
    product_category,
    return_reason,
    return_count,
    total_refund_pln,
    return_rate_pct
FROM gold.returns_by_category
ORDER BY return_count DESC
LIMIT 20;

-- =========================================================
-- WERYFIKACJA: gold.sla_breach_count
-- =========================================================

SELECT
    carrier,
    month,
    total_shipments,
    breached_shipments,
    breach_rate_pct
FROM gold.sla_breach_count
ORDER BY month DESC, breach_rate_pct DESC;

-- Sprawdź logikę: breached_shipments <= total_shipments
SELECT
    COUNT(*) AS rows_with_breach_gt_total
FROM gold.sla_breach_count
WHERE breached_shipments > total_shipments;
-- Oczekiwane: 0

-- =========================================================
-- CROSS-CHECK: watermark table (PostgreSQL)
-- =========================================================

-- Uruchom na PostgreSQL (psql):
-- SELECT table_name, last_loaded_at, updated_at FROM etl_watermarks ORDER BY table_name;
-- Po pierwszym runie: last_loaded_at powinien być > 2024-01-01 (inicjalny)
-- Po ponownym runie z tymi samymi danymi: last_loaded_at się nie zmieni (watermark działa)
