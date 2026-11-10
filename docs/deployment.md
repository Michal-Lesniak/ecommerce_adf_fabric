# Deployment Guide — Przykładowe rozwiązanie M8

Dokument opisuje jak wdrożyć przykładowe rozwiązanie od zera. Zakłada że masz działające środowisko z `00_setup/` (konto Azure, Resource Group, ADLS Gen2, ADF, Fabric Trial).

## Wymagania wstępne

```bash
# Sprawdź Azure CLI
az --version           # >= 2.50
az account show        # zalogowany

# Sprawdź Python
python --version       # >= 3.8
pip install azure-storage-blob pandas

# Zmienne środowiskowe (ustaw przed uruchomieniem)
export RG="rg-shopmaster-dev-weu"
export ADF_NAME="adf-shopmaster-dev-weu"
export STORAGE="shopmastersa"
export KV="kv-shopmaster-dev"
export SUBSCRIPTION=$(az account show --query id -o tsv)
```

## Krok 1 — Infrastruktura (jeśli nie zrobione w 00_setup/)

```bash
az group create --name $RG --location westeurope

az storage account create \
  --name $STORAGE --resource-group $RG \
  --location westeurope --sku Standard_LRS \
  --kind StorageV2 --hierarchical-namespace true

for c in landing bronze silver gold checkpoints; do
  az storage container create --name $c --account-name $STORAGE --auth-mode login
done

az keyvault create --name $KV --resource-group $RG --location westeurope

az datafactory create \
  --resource-group $RG --factory-name $ADF_NAME --location westeurope
```

## Krok 2 — Key Vault secrets

```bash
# Connection string PostgreSQL (zastąp wartościami z M5/M6)
az keyvault secret set \
  --vault-name $KV --name pg-connection-string \
  --value "Server=HOST;Database=shopmaster_db;Port=5432;User Id=USER;Password=PASS;"

# Databricks PAT token (z Databricks → User Settings → Tokens)
az keyvault secret set \
  --vault-name $KV --name databricks-token \
  --value "dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

# ADLS account key
ADLS_KEY=$(az storage account keys list \
  --account-name $STORAGE --resource-group $RG \
  --query "[0].value" -o tsv)
az keyvault secret set \
  --vault-name $KV --name adls-account-key --value "$ADLS_KEY"

# Nadaj ADF dostęp do Key Vault przez Managed Identity
ADF_MI=$(az datafactory show \
  --resource-group $RG --factory-name $ADF_NAME \
  --query "identity.principalId" -o tsv)
az keyvault set-policy --name $KV \
  --object-id $ADF_MI --secret-permissions get list
```

## Krok 3 — Import ADF (Linked Services, Datasets, Pipelines, Triggers)

Import ręczny przez ADF Studio:
1. Otwórz `adf.azure.com` → wybierz `adf-shopmaster-dev-weu`
2. Manage → Linked Services → New → zaimportuj każdy plik z `adf/linked_services/` (kolejność: KV → postgres → adls → databricks)
3. Author → Datasets → importuj pliki z `adf/datasets/`
4. Author → Pipelines → importuj `adf/pipelines/pl_event_listener.json`
5. Manage → Triggers → importuj `adf/triggers/tr_landing_blob_created.json`
   - Pamiętaj zaktualizować `YOUR_SUBSCRIPTION_ID` w triggerze!
6. Publish All (niebieski przycisk u góry)

Alternatywnie — ARM template (jeśli tworzysz z ARM export):
```bash
az deployment group create \
  --resource-group $RG \
  --template-file adf/arm_template.json \
  --parameters factoryName=$ADF_NAME
```

## Krok 4 — PostgreSQL watermark table

```bash
# Zakładając że masz lokalny PostgreSQL z shopmaster_db z M1
psql -U shopmaster -d shopmaster_db -f sql/watermark_table.sql

# Weryfikacja
psql -U shopmaster -d shopmaster_db \
  -c "SELECT table_name, last_loaded_at FROM etl_watermarks;"
```

## Krok 5 — Databricks notebooks

1. W Databricks workspace: otwórz `/Shared/` → stwórz folder `shopmaster`
2. Zaimportuj oba notebooki (File → Import → Python file):
   - `databricks/notebooks/01_bronze_to_silver_shipments.py`
   - `databricks/notebooks/02_bronze_to_silver_returns.py`
3. Uruchom ręcznie każdy notebook z domyślnymi parametrami (test połączenia z ADLS)
4. Sprawdź że `silver.shipments` i `silver.returns` istnieją w Delta catalog

## Krok 6 — Microsoft Fabric Lakehouse

1. Zaloguj się do `app.fabric.microsoft.com`
2. Stwórz Workspace `ws_shopmaster_dev` (lub użyj istniejącego)
3. New → Lakehouse → wpisz `lh_shopmaster_dev`
4. W Lakehouse → Files → New shortcut → External sources → Azure Data Lake Storage Gen2
   - URL: `https://shopmastersa.dfs.core.windows.net/`
   - Auth: Account key (z KV lub skopiuj ręcznie)
   - Folder: `/silver`
5. Wgraj `fabric/gold_notebook.py` do Fabric (New Notebook → wklej kod lub zaimportuj)
6. Uruchom notebook — powinien stworzyć 3 tabele w `gold.*`

## Krok 7 — Azure Monitor alert

```bash
# Pobierz Resource ID ADF
ADF_RESOURCE_ID=$(az datafactory show \
  --resource-group $RG --factory-name $ADF_NAME \
  --query id -o tsv)

# Wdróż alert rule
az deployment group create \
  --resource-group $RG \
  --template-file monitoring/alert_rule.json \
  --parameters alertEmailAddress="twoj@email.com" adfResourceId="$ADF_RESOURCE_ID"
```

## Krok 8 — Test end-to-end

```bash
# Wgraj przykładowy plik shipments do landing/
az storage blob upload \
  --account-name $STORAGE \
  --container-name landing \
  --name "shipments/shipments_20240115.csv" \
  --file "sample_data/shipments_20240115.csv" \
  --auth-mode login

# Poczekaj 2-3 minuty (Event Grid delay ~1 min + pipeline run)
echo "Sprawdź: https://adf.azure.com → Monitor → Pipeline runs"

# Sprawdź czy plik pojawił się w bronze/
az storage blob list \
  --account-name $STORAGE --container-name bronze \
  --prefix "shipments/" --auth-mode login \
  --query "[].name" -o tsv

# Sprawdź silver table w Databricks
# Uruchom w Databricks notebook: spark.table("silver.shipments").count()
```

## Typowe problemy

| Problem | Przyczyna | Rozwiązanie |
|---|---|---|
| Event trigger nie startuje | Event Grid nie zarejestrowany | `az provider register --namespace Microsoft.EventGrid` |
| ADF nie ma dostępu do KV | MI nie ma uprawnień | Powtórz Krok 2 (set-policy) |
| Databricks Activity timeout | Cold start klastra | Zwiększ timeout do `0.02:00:00` lub użyj warm cluster |
| Fabric Shortcut 403 | Wygasły account key lub firewall | Storage → Networking → Allow all networks |
| silver.shipments duplikaty | MERGE key problem | Sprawdź `shipment_id` unikalność w źródle |
