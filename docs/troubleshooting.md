# Troubleshooting Guide — M8 Pipeline

## ADF — najczęstsze błędy

### Pipeline nie startuje po wgraniu pliku do landing/

**Objaw:** Plik pojawia się w ADLS landing/ ale brak nowego pipeline run w Monitor.

**Diagnoza:**
1. Manage → Triggers → sprawdź czy `tr_landing_blob_created` ma status `Started` (nie `Stopped`)
2. Sprawdź Event Grid registration:
   ```bash
   az provider show --namespace Microsoft.EventGrid --query registrationState
   # Oczekiwane: "Registered"
   ```
3. Sprawdź subskrypcję w triggerze — `scope` musi pasować do Twojego Subscription ID
4. Event Grid ma delay ~1-3 min — poczekaj i odśwież Monitor

**Fix:** Jeśli Event Grid nie zarejestrowany:
```bash
az provider register --namespace Microsoft.EventGrid
# Poczekaj 2-5 minut, sprawdź status ponownie
```

---

### Copy Activity — "Access to the path is denied"

**Objaw:** Activity kończy się błędem `AuthorizationPermissionMismatch` lub `403 Forbidden`.

**Diagnoza:** ADF Managed Identity nie ma uprawnień do Storage Account.

**Fix:**
```bash
ADF_MI=$(az datafactory show --resource-group $RG --factory-name $ADF_NAME \
  --query "identity.principalId" -o tsv)

# Nadaj Storage Blob Data Contributor
az role assignment create \
  --assignee $ADF_MI \
  --role "Storage Blob Data Contributor" \
  --scope "/subscriptions/SUBSCRIPTION_ID/resourceGroups/$RG/providers/Microsoft.Storage/storageAccounts/shopmastersa"
```

---

### Databricks Activity — "Cluster not found" lub timeout

**Objaw:** Activity timeout po kilku minutach lub `ClusterNotFound`.

**Diagnoza:** `existingClusterId` w Linked Service jest przestarzały lub klaster jest zatrzymany.

**Fix:**
1. W Databricks → Compute → skopiuj aktualny Cluster ID (format: `XXXX-XXXXXX-XXXXXXXX`)
2. Zaktualizuj `ls_databricks_shopmaster.json`, re-importuj do ADF
3. Jeśli klaster jest w trybie idle (auto-terminate): poczekaj ~5 min na warm-up lub wyłącz auto-terminate
4. Zwiększ timeout Activity do `0.02:00:00` (2 godziny) w Properties → Settings

---

### Key Vault — "SecretNotFound" lub "Forbidden"

**Objaw:** Linked Service nie może pobrać connection string z KV.

**Diagnoza:** Brak access policy dla ADF Managed Identity.

**Fix:**
```bash
ADF_MI=$(az datafactory show --resource-group $RG --factory-name $ADF_NAME \
  --query "identity.principalId" -o tsv)
az keyvault set-policy --name kv-shopmaster-dev \
  --object-id $ADF_MI --secret-permissions get list
```

---

## Databricks — najczęstsze błędy

### Auto Loader — "Schema location already in use"

**Objaw:** Notebook kończy się błędem o checkpointLocation.

**Fix:** Usuń checkpoint i schema folder, uruchom ponownie:
```python
dbutils.fs.rm("abfss://checkpoints@shopmastersa.dfs.core.windows.net/shipments_silver/", True)
dbutils.fs.rm("abfss://checkpoints@shopmastersa.dfs.core.windows.net/shipments_schema/", True)
```

---

### MERGE INTO — duplikaty po ponownym runie

**Objaw:** `silver.shipments` ma więcej wierszy niż oczekiwano po drugim runie.

**Diagnoza:** MERGE key `shipment_id` nie jest unikatowy w source batch lub typy danych się różnią.

**Fix:**
```python
# Sprawdź duplikaty w batch
batch_df.groupBy("shipment_id").count().filter("count > 1").show()

# Jeśli są — deduplikuj przed MERGE
batch_df = batch_df.dropDuplicates(["shipment_id"])
```

---

### Notebook nie widzi silver.shipments po stworzeniu

**Objaw:** `spark.table("silver.shipments")` rzuca `AnalysisException: Table not found`.

**Diagnoza:** Tabela jest w innej bazie/katalogu lub Unity Catalog jest wyłączony.

**Fix:**
```python
# Sprawdź dostępne bazy
spark.sql("SHOW DATABASES").show()

# Użyj pełnej ścieżki jeśli Unity Catalog
spark.sql("USE CATALOG hive_metastore")
spark.sql("USE DATABASE silver")
```

---

## MS Fabric — najczęstsze błędy

### Shortcut — tabele nie są widoczne jako Delta

**Objaw:** Shortcut widzi pliki ale nie tworzy "Tables" automatycznie.

**Diagnoza:** Folder w ADLS nie ma `_delta_log/` (nie jest Delta table).

**Fix:** Upewnij się że Databricks zapisuje do silver jako Delta:
```python
# Zły sposób (Parquet):
df.write.parquet("abfss://silver@.../shipments/")

# Dobry sposób (Delta):
df.write.format("delta").saveAsTable("silver.shipments")
# lub
df.write.format("delta").save("abfss://silver@.../shipments/")
```

---

### Fabric Notebook — "SparkSession not available"

**Objaw:** Import PySpark rzuca błąd w Fabric Notebook.

**Fix:** W Fabric Notebook `spark` jest predefiniowany — nie importuj ani nie twórz nowej sesji:
```python
# Źle:
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Dobrze:
from pyspark.sql import functions as F
# spark jest już dostępny globalnie
df = spark.read.table("silver.shipments")
```

---

## Azure Monitor

### Alert nie wysyła emaila

**Diagnoza:** Action Group nie ma potwierdzonego emaila lub alert jest w `Fired` state (autoMitigate=true wygasił go).

**Fix:**
1. Portal → Monitor → Action Groups → `ag-shopmaster-pipeline-alerts` → Test action group → Send test email
2. Sprawdź spam folder
3. Jeśli alert fire/resolve za szybko: zwiększ `windowSize` do `PT15M`

---

### Jak sprawdzić historię pipeline runs w Log Analytics

```kusto
// Uruchom w Log Analytics Workspace
AzureDiagnostics
| where ResourceProvider == "MICROSOFT.DATAFACTORY"
| where OperationName == "PipelineRunEnd"
| where ResultType == "Failed"
| project TimeGenerated, pipelineName_s, status_s, message_s
| order by TimeGenerated desc
| take 50
```
