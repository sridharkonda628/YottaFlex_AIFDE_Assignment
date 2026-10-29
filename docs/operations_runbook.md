# Operations Runbook

This runbook guides data engineers, forward-deployed engineers, and SREs in operating, monitoring, diagnosing, and recovering the Daily Metrics Pipeline.

---

## 1. Standard Operational Commands

All commands assume the Python virtual environment is activated:
```powershell
.venv\Scripts\Activate.ps1
```

### 1.1 Generating Synthetic Test Data
Generate a small reproducible sample dataset:
```powershell
python -m daily_metrics.generator --output-dir data/sample --count 100 --seed 42
```

Generate a large benchmark dataset (100,000+ records):
```powershell
python -m daily_metrics.generator --output-dir data/generated --count 100000 --seed 42
```

### 1.2 Running the Pipeline
Ingest and process a transaction event file:
```powershell
python -m daily_metrics.cli run \
    --input-file data/sample/transactions_sample.csv \
    --db-path data/metrics.duckdb \
    --reference-dir data/sample \
    --output-dir outputs/sample
```

### 1.3 Running Standalone Reconciliation
Execute an on-demand reconciliation audit against the persistent database:
```powershell
python -m daily_metrics.cli reconcile \
    --db-path data/metrics.duckdb \
    --output-dir outputs/sample
```

### 1.4 Inspecting Operational Audit Logs
Query the latest execution runs directly:
```powershell
python -m daily_metrics.cli audit-history --db-path data/metrics.duckdb --limit 10
```

---

## 2. Diagnosing Failures

### 2.1 File-Level Rejections (Fatal Schema Errors)
* **Symptom**: CLI exits with code 1; output message says `FatalSchemaError: Missing required column(s)...` or `Unexpected column(s)...`.
* **Root Cause**: The upstream file violated the schema contract (e.g., column renamed, extra debugging column added, or missing mandatory field).
* **Diagnosis**:
  1. Inspect the error log in `outputs/<run>/run_summary.json`.
  2. Inspect the file header:
     ```powershell
     Get-Content data/sample/transactions_sample.csv -Head 1
     ```
* **Resolution**: If the upstream schema change is legitimate, update `src/daily_metrics/validation.py` and document the schema version bump. If erroneous, reject the source file back to the producer.

### 2.2 Quarantined Records
* **Symptom**: Run completes with status `SUCCESS` or `WARNING`, but `rejected_row_count > 0`.
* **Root Cause**: Specific rows failed business rules (e.g., negative price, unknown `product_id`, conflicting payload).
* **Triage Steps**:
  1. Inspect `outputs/<run>/rejected_records.csv` or query the database:
     ```sql
     SELECT rejection_reason, COUNT(*) 
     FROM quarantine_records 
     WHERE run_id = '<run_id>' 
     GROUP BY rejection_reason;
     ```
  2. For `UNKNOWN_PRODUCT_ID` or `UNKNOWN_CUSTOMER_ID`:
     Verify whether a new product/customer was released before the reference catalog was updated. Update reference data and execute a backfill.
  3. For `CONFLICTING_EVENT_PAYLOAD`:
     Two distinct payloads arrived with the exact same `event_id`. Contact the upstream event producer to resolve the idempotency key collision.

### 2.3 Reconciliation Mismatches
* **Symptom**: CLI exits with `ReconciliationError` and run status is marked `RECONCILIATION_FAILED`.
* **Root Cause**: Discrepancy between calculated aggregate in `daily_product_metrics` and raw event ledger.
* **Diagnosis**:
  1. Inspect `outputs/<run>/reconciliation_report.json`.
  2. Identify the failing check (e.g., `gross_revenue_mismatch` or `partition_count_mismatch`).
  3. Inspect the specific `(metric_date, product_id)` variance.
* **Resolution**: Do NOT publish downstream reports. Run a targeted partition rebuild or investigate concurrent ledger modifications.

---

## 3. Retries and Recovery Procedures

### 3.1 Retrying an Interrupted or Failed Batch
Because writes are transactional:
1. An interrupted run aborts automatically via DuckDB rollback.
2. Simply re-execute the exact same command:
   ```powershell
   python -m daily_metrics.cli run --input-file <same_file.csv> --db-path <db_path>
   ```
3. The pipeline will process the batch cleanly without partial-state corruption.

### 3.2 Reprocessing a Batch (Exact Rerun)
If an operator runs a batch that was already successfully processed:
* The pipeline detects all events as exact duplicates via SHA-256 payload fingerprinting.
* `new_event_count` will be `0`.
* `exact_duplicate_count` will equal the valid event count.
* Existing analytical metrics remain completely untouched.
* Audit log confirms idempotent skip.

### 3.3 Historical Backfill Procedure
When historical corrections or late-arriving events are delivered:
1. Place the historical file in the ingestion queue.
2. Run the pipeline normally:
   ```powershell
   python -m daily_metrics.cli run --input-file data/backfill_2026_09.csv --db-path data/metrics.duckdb
   ```
3. The affected-partition detector will automatically identify historical dates, recompute only those dates from the ledger, and update `daily_product_metrics`.

---

## 4. Maintenance and Reset

### 4.1 Resetting Local Database (Development Only)
To wipe and reinitialize the local environment:
```powershell
Remove-Item -Force data/metrics.duckdb*
python -m daily_metrics.cli init-db --db-path data/metrics.duckdb --reference-dir data/sample
```
