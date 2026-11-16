# ShopMaster — ADF + Fabric E2E Pipeline

Zadanie zaliczeniowe M8 — przyrostowe ładowanie danych e-commerce (shipments, returns) z landing CSV przez bronze Parquet do silver Delta w Databricks, z warstwą gold w Microsoft Fabric.

---

## Architektura

```
Landing (CSV)                 Bronze (Parquet)            Silver (Delta)             Gold (Fabric)
┌──────────────┐  Event      ┌──────────────┐  Databricks ┌──────────────┐  Mirror  ┌──────────────┐
│ landing/     │ ───────────▶│ bronze/      │ ───────────▶│ silver.*     │ ────────▶│ gold.*       │
│ *.csv        │  Trigger    │ table=X/     │  Notebook   │ Delta tables │  Fabric  │ aggregates   │
│              │             │ yyyy/MM/dd/  │  (MERGE)    │              │          │ (SQL views)  │
└──────────────┘             └──────────────┘             └──────────────┘          └──────────────┘
       │                            ▲                            │                         │
       │  BlobCreated event         │                            │                         │
       ▼                            │                            │                         │
┌──────────────────────────────────┴──────────────────────────────┐                        │
│  ADF: pl_event_listener                                          │                        │
│   └─▶ pl_validate_and_move  (walidacja + landing→bronze Copy)    │                        │
│   └─▶ pl_incremental_load   (Lookup watermark → notebook         │                        │
│                               → Script update_watermark)         │                        │
└─────────────────────────────┬────────────────────────────────────┘                        │
                              │                                                             │
                     ┌────────▼─────────┐                                                   │
                     │ PostgreSQL       │     monitoring/alerts                             │
                     │ etl_watermarks   │ ◀─────────────────────────────────────────────────┘
                     │ (last_loaded_at) │
                     └──────────────────┘
```

## Pipeline'y ADF

| Pipeline | Trigger | Odpowiedzialność |
| --- | --- | --- |
| `pl_event_listener` | Storage Event Trigger (`tr_landing_blob_created`) | Orchestrator — wyciąga `table_name` z nazwy pliku, deleguje do `pl_validate_and_move`, potem `pl_incremental_load` dla tej tabeli |
| `pl_validate_and_move` | child (ExecutePipeline) | Walidacja pliku (exists, size > 0, nazwa zgodna z `table_name`), Copy CSV → Parquet bronze z partycjonowaniem `yyyy/MM/dd` |
| `pl_incremental_load` | child + scheduled fallback | `ForEach(tables)` → `Lookup` na `etl_watermarks` → Databricks notebook (bronze > watermark → silver MERGE) → `Script` z `CALL update_watermark(...)` na Postgres |

### Idempotentność przez watermark

Każdy notebook otrzymuje `last_watermark` z tabeli `etl_watermarks` (PostgreSQL) i filtruje bronze po `_ingested_at > last_watermark`. Po sukcesie zwraca nowy watermark przez `dbutils.notebook.exit()`, a pipeline zapisuje go stored procedurą `update_watermark`. Drugi run z tymi samymi danymi nie zdupluje wierszy ani nie zmieni watermarku.

## Global Parameters

Wszystkie wartości środowiskowe wyniesione do `adf/global_parameters.json`:

| Parametr | Dev value | Opis |
| --- | --- | --- |
| `storageAccountName` | `shopmastersa` | ADLS Gen2 account (landing/bronze/silver/checkpoints) |
| `landingContainer` / `bronzeContainer` / `silverContainer` / `checkpointsContainer` | `landing` / `bronze` / `silver` / `checkpoints` | Kontenery ADLS |
| `databricksNotebookBasePath` | `/Shared/shopmaster` | Katalog notebooków w workspace |
| `watermarkTable` | `etl_watermarks` | Tabela kontrolna w Postgres |

Deploy do stg/prod — podmień wartości w ARM parametrization podczas CI/CD, kod pipeline'ów nie wymaga zmian.

## Struktura repo

```
ecommerce_adf_fabric/
├── adf/
│   ├── global_parameters.json         # środowiskowe (storage account, kontenery, ścieżki)
│   ├── linked_services/               # ADLS, Databricks, Postgres, Key Vault
│   ├── datasets/                      # landing CSV, bronze Parquet, etl_watermarks
│   ├── pipelines/                     # pl_event_listener, pl_validate_and_move, pl_incremental_load
│   └── triggers/                      # tr_landing_blob_created (BlobCreated event)
├── databricks/
│   ├── job_definition.json            # Databricks Workflow definition
│   └── notebooks/                     # 01_bronze_to_silver_shipments.py, 02_bronze_to_silver_returns.py
├── fabric/                            # Fabric notebook + lakehouse config dla warstwy gold
├── sql/
│   ├── watermark_table.sql            # DDL + stored proc update_watermark
│   └── sample_queries.sql             # weryfikacja silver/gold
├── sample_data/                       # testowe CSV (shipments, returns)
├── monitoring/                        # alerty, KQL queries
├── docs/                              # dokumentacja i decyzje architektoniczne
└── architecture_diagram.html          # interaktywny diagram E2E
```

## Deploy

**1. PostgreSQL — tabela watermark i stored procedure:**
```bash
psql -h <host> -U <user> -d shopmaster -f sql/watermark_table.sql
```

**2. Databricks — upload notebooków:**
```bash
databricks workspace import-dir databricks/notebooks /Shared/shopmaster
```
Następnie uzupełnij `adf/linked_services/ls_databricks_shopmaster.json`:
- `domain` — URL workspace
- `existingClusterId` — ID clustra (lub zamień na `newClusterConfig`)

**3. Key Vault — sekrety:**
- `adls-account-key` — ADLS account key
- `pg-connection-string` — PostgreSQL connection string
- `databricks-token` — Databricks PAT

**4. ADF — deploy artefaktów:**

W kolejności (zależności między obiektami):
1. `linked_services/*.json`
2. `datasets/*.json`
3. `global_parameters.json` (zakładka Manage → Global parameters)
4. `pipelines/pl_validate_and_move.json` → `pl_incremental_load.json` → `pl_event_listener.json`
5. `triggers/tr_landing_blob_created.json`

Managed Identity ADF musi mieć role:
- `Storage Blob Data Contributor` na `shopmastersa`
- `Key Vault Secrets User` na vault
- `EventGrid EventSubscription Contributor` na storage account (wymagane dla Storage Event Triggers)

## Testowanie end-to-end

1. Wgraj `sample_data/shipments_20240115.csv` do `landing/`:
   ```bash
   az storage blob upload --account-name shopmastersa \
       --container-name landing --file sample_data/shipments_20240115.csv \
       --name shipments_20240115.csv --auth-mode login
   ```
2. `tr_landing_blob_created` powinien odpalić `pl_event_listener` w ciągu 1-2 minut.
3. Zweryfikuj w Databricks SQL: `SELECT COUNT(*) FROM silver.shipments;`
4. Zweryfikuj watermark: `SELECT table_name, last_loaded_at FROM etl_watermarks;`
5. Wgraj plik ponownie — drugi run powinien zakończyć się `Brak nowych danych — watermark bez zmian`.

Pełne zapytania weryfikacyjne w `sql/sample_queries.sql`.

## Monitoring

- `monitoring/` — KQL queries dla Log Analytics (failed pipeline runs, latency P95, SLA breach).
- `pl_incremental_load` zapisuje `UpdateWatermark` tylko przy `dependsOn: Succeeded` z notebooka — fail notebooka = watermark bez zmian = retry przetwarza te same dane.

## Convention — nazewnictwo plików landing

Pipeline wyciąga `table_name` z pierwszego segmentu nazwy pliku: `<table>_<yyyyMMdd>.csv`:
- `shipments_20240115.csv` → `table_name=shipments`
- `returns_20240115.csv`   → `table_name=returns`

Pliki o innym schemacie nazwy trafiają do gałęzi `ifFalse` w `pl_validate_and_move` i są graceful-skippowane (`SKIPPED:<filename>` w zmiennej `validation_result`).
