# Design Decisions and Trade-Off Analysis

This document details the architectural, technological, and data engineering decisions made in designing the Daily Metrics Pipeline for YottaFlex.ai. Every decision is evaluated against business correctness, data integrity, idempotency, operational simplicity, and production evolution paths.

---

## 1. Technology Selection and Justification

### 1.1 Storage & Analytical Engine: DuckDB
* **What it does**: In-process SQL OLAP database engine supporting ACID transactions, vectorized columnar execution, rich SQL dialect, and direct CSV/Parquet/Pandas querying.
* **Why it was selected**: It provides lightning-fast columnar execution for analytical windowing and aggregations with zero external server dependencies, full ACID transaction semantics (enabling atomic target table swaps/updates and rollbacks), and native decimal precision (`DECIMAL(18,2)`).
* **Requirements satisfied**: REQ-01, REQ-09, REQ-10, REQ-13, REQ-14, REQ-17, REQ-21.
* **Alternatives considered**:
  * *SQLite*: Standard embedded database, but row-oriented, weak typing (no native fixed-point decimal type, uses floating point/integer hacks), slower on large analytical aggregations and window functions.
  * *PostgreSQL / MySQL*: Full relational server, but requires running external Docker containers or background daemon services, violating the requirement for self-contained local zero-credential execution.
  * *Apache Spark / DuckDB in Cloud*: Excessive overhead for local vertical slice; cluster orchestration complexity without benefit for local tests.
* **Why alternatives were not selected**: SQLite lacks native columnar performance and strict decimal arithmetic. External RDBMS violates the zero-credential local reproducibility rule.
* **Limitations introduced**: Single-writer concurrency model. While concurrent readers are supported, concurrent pipelines writing to the exact same DuckDB file must serialize.
* **Production evolution triggers**: When raw event volume exceeds terabytes across distributed nodes, migrate the storage layer to Delta Lake / Apache Iceberg on Amazon S3 / Google Cloud Storage, queried via DuckDB or Trino / Snowflake / Databricks.

### 1.2 Pipeline Language & Orchestration: Python 3.11+
* **What it does**: Powers ingestion parsing, business rule validation, hashing, reconciliation assertions, and CLI orchestration.
* **Why it was selected**: Universal standard for modern data engineering, strong typing via `typing` / `dataclasses`, and rich standard library.
* **Requirements satisfied**: REQ-01 through REQ-23.
* **Alternatives considered**: Go, Rust, Java.
* **Why alternatives were not selected**: Python provides maximum velocity, native interoperability with DuckDB and Pandas, and immediate reviewer reproducibility without separate toolchains.
* **Limitations introduced**: GIL (Global Interpreter Lock) for pure Python loops.
* **Mitigation**: Offload heavy row-by-row processing and windowed deduplication to DuckDB's vectorized C++ engine while Python handles schema routing, validation, and pipeline coordination.

### 1.3 Testing Framework: pytest
* **What it does**: Automated test discovery, parametrization, fixture isolation, and failure reporting.
* **Why it was selected**: Standard Python testing framework with exceptional support for isolated fixtures (temporary in-memory DuckDB databases and mock CSV files).
* **Requirements satisfied**: REQ-20 (Automated Test Suite).

---

## 2. Core Architectural Design Decisions

### 2.1 Why an Immutable Event Ledger is Necessary
* **Context**: Analytical systems frequently need to handle transaction updates, reversals, backfills, and audit requests.
* **Selected Approach**: Maintain an append-only `event_ledger` containing every accepted, valid event with payload fingerprint and metadata.
* **Trade-offs**: Requires additional storage compared to a destructive in-place update model.
* **Justification**:
  * Complete auditability: We can trace the exact historical progression of any transaction.
  * Recovery & Re-computation: If business metric logic evolves, or if bad code runs, we can replay from the ledger without losing source truth.
  * Zero Data Loss: Overwriting raw transactions destroys lineage and makes forensic auditing impossible.

### 2.2 Why Source Event Versions Determine State (Version Precedence)
* **Context**: Network delays, distributed message queues, and upstream retries cause events to arrive out of order.
* **Selected Approach**: Every transaction event carries a monotonically increasing `event_version` assigned by the upstream source of truth. The winning state for a transaction is strictly defined as `MAX(event_version)`.
* **Alternatives Considered**: Ingestion timestamp ordering, event timestamp ordering (`event_ts`).
* **Why Alternatives Failed**:
  * *Ingestion Timestamp*: Flawed because network latency or backfill pipelines would cause a late-arriving old event (e.g., version 1 arriving at 12:05) to overwrite an already-ingested newer event (version 2 arriving at 12:01).
  * *Event Timestamp (`event_ts`)*: Clock drift, microsecond collisions, and clock resets make wall-clock timestamps unreliable for discrete sequential state transitions.
* **Rule**: Arrival time NEVER determines business state precedence.

### 2.3 Why Ingestion Timestamps Do Not Determine Business Precedence
* **Context**: Determining when a transaction occurred vs. when it was received.
* **Selected Approach**: `ingested_at` is strictly metadata for lineage, latency monitoring, and SLA auditing. It has zero bearing on state resolution or `metric_date`.
* **Justification**: Using arrival time for analytical partitioning creates non-deterministic results: the same historical event processed on two different days would land in different reporting buckets.

### 2.4 Cancellation Reversal Semantics vs. Compensating Entries
* **Context**: When a transaction is cancelled or reversed, how should it impact daily metrics?
* **Selected Approach**: Transaction Cancellation Semantics. A `REVERSAL` event removes the transaction from active analytical metrics (`units_sold`, `transaction_count`, `gross_revenue`).
* **Financial Accounting Distinction**:
  * In double-entry ledger systems, a refund is often a separate financial transaction with a negative value on the refund date.
  * In the operational sales metric context assigned (`units_sold` and `gross_revenue` for products on the date of sale), a cancelled order means the original sale did not materialize.
  * Therefore, cancelling the original transaction's contribution cleanly removes it from that sale date's metrics. If a later `UPSERT` with version 3 arrives, the transaction is reinstated.

### 2.5 Aggregation Grain: `(metric_date, product_id)`
* **Context**: Reporting requirement to provide daily product-level metrics.
* **Selected Approach**: Primary key of `daily_product_metrics` is strictly composite `(metric_date, product_id)`.
* **Justification**: `metric_date` is normalized to UTC Date (`CAST(event_ts AS DATE)`). This provides a crisp, queryable grain that eliminates multi-timezone ambiguity while allowing straightforward rollups by week, month, or product category.

### 2.6 Affected-Partition Incremental Recomputation
* **Context**: Recomputing daily metrics when new batches arrive.
* **Selected Approach**: Affected-Partition Recomputation with Fallback Capability.
  1. For every incoming transaction in a batch, compare its previous winning state in the ledger with its new winning state.
  2. Identify all affected `(metric_date, product_id)` pairs (including old partitions if date/product changed or if reversed).
  3. Recompute aggregate values only for those affected partitions from the authoritative `current_transaction_state`.
  4. Atomically delete and replace only those partition rows in `daily_product_metrics`.
  5. If an entire partition has all its transactions reversed, delete the empty partition so no ghost records remain.
* **Why Not Full Refresh Every Time**: Full refresh scales as $O(N)$ with total historical ledger size. In contrast, affected-partition recomputation scales as $O(K)$ where $K$ is the volume of modified partitions, making it exponentially faster for high-volume historical datasets.

### 2.7 Idempotency Key & Payload Fingerprinting
* **Context**: Preventing duplicate event ingestion and detecting payload tampering.
* **Selected Approach**:
  * Primary Idempotency Key: `event_id`.
  * Business Payload Hash: SHA-256 digest of canonical business fields `(transaction_id, event_version, event_type, product_id, customer_id, event_ts, quantity, unit_price)`.
* **Resolution Matrix**:
  | Condition | Action | Audit Metric Impact |
  |---|---|---|
  | `event_id` exists, `payload_hash` matches | Exact duplicate delivery: Silently ignore / skip | `exact_duplicate_count += 1` |
  | `event_id` exists, `payload_hash` differs | Conflicting duplicate event: Quarantine record | `conflicting_duplicate_count += 1`, `rejected_row_count += 1` |
  | Same `(transaction_id, event_version)`, different `event_id` | Version conflict: Quarantine record | `conflicting_duplicate_count += 1`, `rejected_row_count += 1` |
  | New `event_id`, valid version | Insert into `event_ledger` | `new_event_count += 1` |

### 2.8 ACID Database Transactions for Publishing
* **Context**: Ensuring pipeline failures never leave corrupt or half-written analytical tables.
* **Selected Approach**: All database operations for a batch (ledger insert, partition updates, quarantine writes, audit logging) execute inside an atomic DuckDB transaction (`BEGIN TRANSACTION` ... `COMMIT`).
* **Rollback Protocol**: If any fatal error, unhandled exception, or constraint violation occurs before commit, `ROLLBACK` is immediately executed. The run status is recorded as `FAILED` in the audit log in a separate isolated audit transaction.

### 2.9 Separation of Quarantined Records from Analytical State
* **Context**: Handling dirty, malformed, or unreferenced records.
* **Selected Approach**: Invalid rows are quarantined into `quarantine_records` with raw JSON and machine-readable error reasons. They are never inserted into `event_ledger`.
* **Justification**: Allowing invalid records into the analytical ledger with null or default values (e.g., price = 0, product = "UNKNOWN") distorts business KPIs. Quarantining preserves full lineage without polluting analytics.

### 2.10 Independent Reconciliation
* **Context**: Validating pipeline correctness without self-referential bias.
* **Selected Approach**: A dedicated `Reconciler` component executes an independent, declarative SQL aggregation directly from `event_ledger` (evaluating current state from first principles) and compares every partition, active transaction count, units sold, and gross revenue against `daily_product_metrics`.
* **Precision**: Exact fixed-point decimal comparison (`DECIMAL(18,2)`), not floating-point approximation. Any difference `!= 0.00` causes an immediate `RECONCILIATION_FAILED` status.

### 2.11 Zero Cloud Dependencies for Local Execution
* **Context**: Requirement to be fully self-contained and reproducible.
* **Selected Approach**: Single embedded DuckDB file or in-memory instance, standard Python CSV libraries, pytest, zero external server daemons or cloud credentials.
* **Production Path**: The code is structured with clean abstraction boundaries: swapping DuckDB for Snowflake or S3-backed Iceberg requires modifying only the `publishing.py` connection layer.
