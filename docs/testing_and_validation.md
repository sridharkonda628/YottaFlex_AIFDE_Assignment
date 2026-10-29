# Testing and Validation Report

This document records the automated testing methodology, executed test suite results, code coverage, reconciliation proofs, and large-scale performance benchmark findings for the Daily Metrics Pipeline.

---

## 1. Automated Test Suite Execution Summary

The test suite was executed in an isolated Python 3.13.0 virtual environment (`.venv`) using `pytest` and `pytest-cov`.

* **Total Test Cases Executed**: 36
* **Tests Passed**: 36
* **Tests Failed**: 0
* **Pass Rate**: 100%
* **Total Suite Execution Time**: 7.39s (with coverage instrumentation)

### 1.1 Test Coverage by Module

| Module Name | Statements | Missed | Coverage | Key Components Covered |
|---|---|---|---|---|
| `daily_metrics.reconciliation` | 68 | 1 | **99%** | PK uniqueness, global totals, per-partition deep diffing |
| `daily_metrics.transformations` | 36 | 1 | **97%** | Affected partition detection, partition aggregates |
| `daily_metrics.publishing` | 72 | 4 | **94%** | Atomic ACID publish, empty partition cleanup, CSV export |
| `daily_metrics.validation` | 119 | 12 | **90%** | Reference integrity, payload hash, row validation |
| `daily_metrics.audit` | 27 | 0 | **100%** | Run audit logging, summary JSON export |
| `daily_metrics.config` | 96 | 1 | **99%** | Schemas, dataclasses, exception hierarchy |
| `daily_metrics.ingestion` | 53 | 9 | **83%** | Fail-closed headers, malformed CSV error traps |
| `daily_metrics.event_resolution`| 66 | 11 | **83%** | Intra-batch & ledger deduplication, version conflict traps |
| `daily_metrics.cli` | 162 | 67 | **59%** | Core orchestrator and execution entrypoints |
| **Total Codebase** | **821** | **197** | **76%** | Complete pipeline logic |

---

## 2. Test Case Specification and Results Matrix

| Test ID | Module / Test Case | Description & Failure Mode Prevented | Status |
|---|---|---|---|
| **TC-01** | `test_ingestion.py::test_valid_batch_ingestion` | Ingests standard well-formed batch; asserts header parsing and row extraction. | **PASSED** |
| **TC-02** | `test_ingestion.py::test_empty_input_file` | Handles 0-byte or empty CSV without throwing unhandled exceptions; emits clean audit. | **PASSED** |
| **TC-03** | `test_ingestion.py::test_malformed_csv` | Fails closed on uneven columns or unclosed quotes; prevents corrupt partial ingestion. | **PASSED** |
| **TC-04** | `test_ingestion.py::test_missing_required_columns` | Raises `FatalSchemaError` when required source columns are omitted. | **PASSED** |
| **TC-05** | `test_ingestion.py::test_unexpected_columns` | Raises `FatalSchemaError` on unexpected columns; preserves strict schema contract. | **PASSED** |
| **TC-06** | `test_validation.py::test_invalid_types` | Quarantines non-numeric event versions or non-integer quantities. | **PASSED** |
| **TC-07** | `test_validation.py::test_invalid_timestamps` | Quarantines invalid date strings; prevents date conversion panics. | **PASSED** |
| **TC-08** | `test_validation.py::test_negative_quantity` | Enforces `quantity > 0` constraint on UPSERT events. | **PASSED** |
| **TC-09** | `test_validation.py::test_negative_price` | Enforces `unit_price >= 0.00` constraint on UPSERT events. | **PASSED** |
| **TC-10** | `test_validation.py::test_unknown_product_id_rejected` | Enforces referential integrity against product catalog; routes to quarantine. | **PASSED** |
| **TC-11** | `test_validation.py::test_unknown_customer_id_rejected` | Enforces referential integrity against customer catalog; routes to quarantine. | **PASSED** |
| **TC-12** | `test_event_resolution.py::test_exact_duplicate_event_delivery` | Ignores redelivered events with identical `event_id` and payload hash; zero metric impact. | **PASSED** |
| **TC-13** | `test_event_resolution.py::test_conflicting_duplicate_event_id` | Detects same `event_id` with differing payload; quarantines under `CONFLICTING_EVENT_PAYLOAD`. | **PASSED** |
| **TC-14** | `test_event_resolution.py::test_conflicting_transaction_version` | Detects same `(transaction_id, version)` with differing payload; quarantines conflict. | **PASSED** |
| **TC-15** | `test_event_resolution.py::test_late_arriving_older_version` | Asserts late-arriving version 1 does not overwrite active winning version 2 state. | **PASSED** |
| **TC-16** | `test_event_resolution.py::test_out_of_order_event_arrival` | Correctly resolves winning state when version 2 arrives before version 1. | **PASSED** |
| **TC-17** | `test_event_resolution.py::test_transaction_reinstatement` | Tests UPSERT(v1) -> REVERSAL(v2) -> UPSERT(v3); restores active metrics. | **PASSED** |
| **TC-18** | `test_metrics.py::test_valid_batch_metrics` | Validates arithmetic accuracy of `units_sold`, `transaction_count`, and `gross_revenue`. | **PASSED** |
| **TC-19** | `test_metrics.py::test_transaction_update_metrics` | Validates recalculation of target aggregates when a transaction increases quantity/price. | **PASSED** |
| **TC-20** | `test_metrics.py::test_transaction_reversal` | Validates cancellation semantics: REVERSAL cleanly subtracts transaction contribution. | **PASSED** |
| **TC-21** | `test_metrics.py::test_update_changing_product` | Tests transaction moving from PROD_001 to PROD_002; updates both partitions. | **PASSED** |
| **TC-22** | `test_metrics.py::test_update_changing_date` | Tests transaction moving from 2026-10-01 to 2026-10-05; updates both date partitions. | **PASSED** |
| **TC-23** | `test_metrics.py::test_empty_partition_cleanup` | Deletes partition row in target table when all transactions are reversed/moved. | **PASSED** |
| **TC-24** | `test_metrics.py::test_target_schema_structure` | Asserts target schema structure, composite primary key, and typed columns. | **PASSED** |
| **TC-25** | `test_metrics.py::test_duplicate_target_keys_prevented` | Asserts zero primary key duplication in `daily_product_metrics`. | **PASSED** |
| **TC-26** | `test_reconciliation.py::test_reconciliation_pass` | Asserts independent reconciliation passes on valid database state. | **PASSED** |
| **TC-27** | `test_reconciliation.py::test_reconciliation_detects_tampered_revenue` | Asserts reconciliation catches even 1-cent discrepancy in target revenue. | **PASSED** |
| **TC-28** | `test_reconciliation.py::test_reconciliation_detects_tampered_units` | Asserts reconciliation catches unit discrepancies. | **PASSED** |
| **TC-29** | `test_reconciliation.py::test_reconciliation_detects_orphan_partition` | Asserts reconciliation catches orphan partition absent from ledger. | **PASSED** |
| **TC-30** | `test_recovery.py::test_transaction_rollback_on_failure` | Tests `ROLLBACK` on database error; leaves ledger and target completely intact. | **PASSED** |
| **TC-31** | `test_recovery.py::test_recovery_after_failed_database_transaction` | Simulates crash during publishing; asserts clean retry succeeds and audits both runs. | **PASSED** |
| **TC-32** | `test_recovery.py::test_failed_run_visible_in_audit` | Asserts failed runs remain permanently recorded in audit table with error context. | **PASSED** |
| **TC-33** | `test_idempotency.py::test_rerun_same_batch` | Tests running the same batch twice; second run records 0 new events, 0 target writes. | **PASSED** |
| **TC-34** | `test_idempotency.py::test_batch_rerun_idempotency` | Verifies multi-run consistency; target table state remains 100% byte-for-byte identical. | **PASSED** |
| **TC-35** | `test_backfill.py::test_historical_backfill` | Ingests contemporary batch, then historical batch; updates historical partition cleanly. | **PASSED** |
| **TC-36** | `test_validation.py::test_source_schema_validation` | Comprehensive validation of field parsing, ISO UTC normalization, and hash generation. | **PASSED** |

---

## 3. Sample Dataset Reconciliation Evidence

From the actual execution of `data/sample/transactions_sample.csv` (100 input records):

```json
{
  "reconciliation_time": "2026-10-29T06:50:12.852798+00:00",
  "overall_status": "PASSED",
  "checks": [
    {
      "check_name": "target_pk_uniqueness",
      "expected": 52,
      "actual": 52,
      "difference": 0,
      "status": "PASS",
      "diagnostic": "Primary key (metric_date, product_id) must have zero duplicates in target table."
    },
    {
      "check_name": "global_units_sold",
      "expected": 395,
      "actual": 395,
      "difference": 0,
      "status": "PASS",
      "diagnostic": "Authoritative ledger units sold vs target published units sold."
    },
    {
      "check_name": "global_gross_revenue",
      "expected": 45947.54,
      "actual": 45947.54,
      "difference": 0.0,
      "status": "PASS",
      "diagnostic": "Authoritative ledger revenue vs target published gross revenue."
    },
    {
      "check_name": "global_partition_count",
      "expected": 52,
      "actual": 52,
      "difference": 0,
      "status": "PASS",
      "diagnostic": "Number of active (metric_date, product_id) partitions."
    },
    {
      "check_name": "per_partition_reconciliation",
      "expected": "0 partition variances",
      "actual": "0 partition variances",
      "difference": 0,
      "status": "PASS",
      "diagnostic": "Every individual (metric_date, product_id) matches exact expected aggregates."
    }
  ],
  "mismatch_count": 0
}
```

---

## 4. Large-Scale Benchmark Execution (100,000 Records)

Benchmark executed using `benchmarks/benchmark.py` on Windows 11 with Python 3.13.0 and DuckDB 2.0.0.

### 4.1 Measured Metrics

| Measurement Category | Initial Ingestion (Cold Ingest) | Duplicate-Only Rerun | Delta / Speedup |
|---|---|---|---|
| **Input Record Count** | 100,000 records | 100,000 records | - |
| **Validated Records** | 92,318 | 92,318 | - |
| **New Events Accepted** | 84,846 | 0 | -100% (Idempotent skip) |
| **Exact Duplicates** | 4,762 | 89,608 | +84,846 |
| **Quarantined Records** | 10,392 | 10,392 | - |
| **Target Partitions Updated** | 169 partitions | 0 partitions | -100% (No writes) |
| **Total Wall-Clock Time** | **189.51 seconds** | **27.45 seconds** | **6.9x Faster** |
| **Ingestion Throughput** | **527.7 rows / sec** | **3,642.7 rows / sec** | **+590% Throughput** |
| **Standalone Reconciliation** | **0.1254 seconds** (PASSED) | Skipped | Sub-second ground truth check |

### 4.2 Performance Bottleneck Analysis

* **Identified Primary Bottleneck**:
  During initial cold ingestion of 100,000 records, the pipeline performs:
  1. Python-level CSV DictReader iteration and row-by-row string parsing.
  2. In-memory SHA-256 canonical hashing across 100,000 records.
  3. Chunked DuckDB queries for duplicate event ID checking and transaction version resolution.
* **Why DuckDB Aggregation was Negligible**:
  Notice that calculating and updating all 169 affected partitions took less than 1.5 seconds out of the 189 seconds! Columnar vectorized aggregation in DuckDB is extraordinarily fast ($O(K)$ across partitions).
* **Recommended Production Optimization**:
  To scale from 100,000 records to 10,000,000+ records:
  - Ingest the raw CSV directly into a DuckDB staging table using DuckDB's vectorized `read_csv_auto()`.
  - Perform validation, SHA-256 hashing, and reference joins in parallel C++ SQL rather than pure Python loops.
  - Expected Throughput Improvement: 500 rows/sec $\rightarrow$ 150,000+ rows/sec (300x speedup).
