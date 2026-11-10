-- =============================================================================
-- Zadanie zaliczeniowe M8 — przykładowe rozwiązanie
-- Plik: watermark_table.sql
-- Cel: Tabela kontrolna i stored procedure dla shipments i returns pipeline
-- Wymagania: PostgreSQL 13+
-- =============================================================================

CREATE TABLE IF NOT EXISTS etl_watermarks (
    table_name      VARCHAR(100)  PRIMARY KEY,
    last_loaded_at  TIMESTAMP     NOT NULL,
    updated_at      TIMESTAMP     DEFAULT NOW()
);

INSERT INTO etl_watermarks (table_name, last_loaded_at) VALUES
    ('shipments', '2024-01-01 00:00:00'),
    ('returns',   '2024-01-01 00:00:00')
ON CONFLICT (table_name) DO NOTHING;

CREATE OR REPLACE PROCEDURE update_watermark(
    p_table_name    TEXT,
    p_new_watermark TIMESTAMP
)
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE etl_watermarks
    SET    last_loaded_at = p_new_watermark,
           updated_at     = NOW()
    WHERE  table_name     = p_table_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Tabela % nie istnieje w etl_watermarks', p_table_name;
    END IF;
END;
$$;

SELECT table_name, last_loaded_at FROM etl_watermarks ORDER BY table_name;
