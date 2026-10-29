# Data Dictionary

This document provides formal specifications for all source, reference, intermediate, target, quarantine, and audit entities used within the Daily Metrics Data Pipeline.

---

## 1. Source Entity: Transaction Event (`source_csv`)

Source files are comma-separated values (CSV) containing transaction lifecycle events.

| Column Name | Physical Type | Logical Type | Nullable | Primary Key | Business Validation Rules | Description |
|---|---|---|---|---|---|---|
| `event_id` | VARCHAR | UUID / String | NO | Yes (Source Event Key) | Non-empty, alphanumeric / UUID format. Unique per source event. | Stable identifier for the event. Used as the idempotency key. |
| `transaction_id` | VARCHAR | String | NO | Business Key | Non-empty, stable business entity identifier. | Identifies the business transaction across versions. |
| `event_version` | BIGINT | Integer | NO | Version | Strictly positive integer (`> 0`). Monotonically increments with transaction state changes. | Source state sequence number. Highest version dictates current state. |
| `event_type` | VARCHAR | Enum | NO | Discriminator | Must be either `'UPSERT'` or `'REVERSAL'`. Case-sensitive. | Event category. UPSERT conveys complete transaction state; REVERSAL indicates cancellation. |
| `product_id` | VARCHAR | String | Conditional | Foreign Key | Required for UPSERT (`NOT NULL`). Must exist in active `reference_products`. Optional for REVERSAL. | Reference to product sold. |
| `customer_id` | VARCHAR | String | Conditional | Foreign Key | Required for UPSERT (`NOT NULL`). Must exist in active `reference_customers`. Optional for REVERSAL. | Reference to purchasing customer. |
| `event_ts` | VARCHAR / TIMESTAMP | ISO-8601 UTC | NO | Dimension | Valid ISO-8601 timestamp string with timezone (or UTC implied `YYYY-MM-DDTHH:MM:SSZ`). | Business time of the transaction event. Used to derive `metric_date`. |
| `quantity` | BIGINT | Integer | Conditional | Measure | Required and strictly positive (`> 0`) for UPSERT. Nullable for REVERSAL. | Number of product units transacted. |
| `unit_price` | DECIMAL(18,2) | Fixed Decimal | Conditional | Measure | Required and non-negative (`>= 0.00`) for UPSERT. Nullable for REVERSAL. | Per-unit transaction price in standard currency. |
| `ingested_at` | VARCHAR / TIMESTAMP | ISO-8601 UTC | YES | Lineage | Valid ISO-8601 timestamp if provided; pipeline populates current UTC timestamp if absent. | Timestamp when the record reached ingestion. |
| `source_file` | VARCHAR | String | YES | Lineage | Non-empty filename if present; populated by pipeline from input path. | Name of source CSV file for end-to-end traceability. |

---

## 2. Reference Entities

Reference data is maintained to guarantee referential integrity and prevent orphan transactions from polluting analytical metrics.

### 2.1 Reference Products (`reference_products`)

| Column Name | Physical Type | Nullable | Primary Key | Description |
|---|---|---|---|---|
| `product_id` | VARCHAR | NO | YES | Unique product identifier (e.g., `PROD_001`). |
| `product_name`| VARCHAR | NO | NO | Descriptive product name. |
| `category` | VARCHAR | NO | NO | Product category (e.g., `Electronics`, `Apparel`). |
| `is_active` | BOOLEAN | NO | NO | Soft-delete flag; only active products can accept transactions. |

### 2.2 Reference Customers (`reference_customers`)

| Column Name | Physical Type | Nullable | Primary Key | Description |
|---|---|---|---|---|
| `customer_id` | VARCHAR | NO | YES | Unique customer identifier (e.g., `CUST_0001`). |
| `customer_name`| VARCHAR | NO | NO | Customer full name or enterprise entity. |
| `email` | VARCHAR | NO | NO | Customer contact email. |
| `tier` | VARCHAR | NO | NO | Customer loyalty tier (`Standard`, `Premium`, `Enterprise`). |
| `is_active` | BOOLEAN | NO | NO | Soft-delete flag. |

---

## 3. Storage Model Entities

### 3.1 Event Ledger (`event_ledger`)

Stores the complete, immutable historical audit log of all accepted events.

| Column Name | Physical Type | Constraints | Description |
|---|---|---|---|
| `event_id` | VARCHAR | PRIMARY KEY | Unique event identifier. |
| `transaction_id` | VARCHAR | NOT NULL | Business transaction key. |
| `event_version` | BIGINT | NOT NULL | Source version sequence number. |
| `event_type` | VARCHAR | NOT NULL | `'UPSERT'` or `'REVERSAL'`. |
| `product_id` | VARCHAR | NULLABLE | Product ID (mandatory for UPSERT). |
| `customer_id` | VARCHAR | NULLABLE | Customer ID (mandatory for UPSERT). |
| `event_ts` | TIMESTAMP WITH TIME ZONE | NOT NULL | Business timestamp (UTC). |
| `metric_date` | DATE | NOT NULL | Partition date (`CAST(event_ts AS DATE)`). |
| `quantity` | BIGINT | NULLABLE | Item quantity. |
| `unit_price` | DECIMAL(18,2) | NULLABLE | Price per unit. |
| `gross_amount` | DECIMAL(18,2) | NULLABLE | `quantity * unit_price`. |
| `payload_hash` | VARCHAR(64) | NOT NULL | SHA-256 fingerprint of business payload. |
| `ingested_at` | TIMESTAMP WITH TIME ZONE | NOT NULL | Arrival time at ingestion. |
| `source_file` | VARCHAR | NOT NULL | File lineage origin. |
| `run_id` | VARCHAR | NOT NULL | Pipeline run identifier that committed this record. |
| `created_at` | TIMESTAMP WITH TIME ZONE | DEFAULT CURRENT_TIMESTAMP | Database insertion timestamp. |

**Indexes & Constraints**:
- `PRIMARY KEY (event_id)`
- `UNIQUE (transaction_id, event_version)`: Enforces version uniqueness per transaction.
- `INDEX idx_ledger_txn (transaction_id)`
- `INDEX idx_ledger_partition (metric_date, product_id)`

---

### 3.2 Current Transaction State (`current_transaction_state`)

Represents the authoritative, latest winning business state for each transaction. Can be accessed as an authoritative materialized table or an indexed deterministic view over `event_ledger`.

```sql
CREATE VIEW current_transaction_state AS
WITH ranked_events AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id 
            ORDER BY event_version DESC, event_ts DESC
        ) AS rank_num
    FROM event_ledger
)
SELECT
    transaction_id,
    event_id,
    event_version,
    event_type,
    product_id,
    customer_id,
    event_ts,
    metric_date,
    quantity,
    unit_price,
    gross_amount,
    CASE WHEN event_type = 'UPSERT' THEN TRUE ELSE FALSE END AS is_active,
    source_file,
    run_id,
    created_at AS last_updated_at
FROM ranked_events
WHERE rank_num = 1;
```

---

### 3.3 Target Daily Product Metrics (`daily_product_metrics`)

The analytical target table aggregated at the grain of `(metric_date, product_id)`.

| Column Name | Physical Type | Nullable | Constraints | Business Definition |
|---|---|---|---|---|
| `metric_date` | DATE | NO | COMPOSITE PK (Part 1) | Date derived from transaction `event_ts` in UTC (`CAST(event_ts AS DATE)`). |
| `product_id` | VARCHAR | NO | COMPOSITE PK (Part 2) | Product business key from active reference catalog. |
| `units_sold` | BIGINT | NO | `>= 0` | Total units sold: `SUM(quantity)` across all active transactions for that date and product. |
| `transaction_count` | BIGINT | NO | `>= 0` | Total distinct active transactions: `COUNT(DISTINCT transaction_id)`. |
| `gross_revenue` | DECIMAL(18,2) | NO | `>= 0.00` | Total gross revenue: `SUM(CAST(quantity * unit_price AS DECIMAL(18,2)))`. Decimal-safe. |
| `last_updated_at` | TIMESTAMP WITH TIME ZONE | NO | - | Timestamp of the most recent pipeline run updating this partition. |
| `last_run_id` | VARCHAR | NO | - | Pipeline `run_id` responsible for the most recent update. |

---

### 3.4 Quarantined / Rejected Records (`quarantine_records`)

Captures all invalid, malformed, or conflicting records without losing lineage or halting clean records.

| Column Name | Physical Type | Nullable | Description |
|---|---|---|---|
| `quarantine_id` | VARCHAR | NO | UUID primary key. |
| `run_id` | VARCHAR | NO | Active pipeline run ID. |
| `source_file` | VARCHAR | NO | Lineage file path. |
| `source_row_num`| BIGINT | YES | Line number in the source file. |
| `raw_record_json`| VARCHAR | NO | Sanitized JSON representation of raw input line. |
| `rejection_reason`| VARCHAR | NO | Machine-readable error code (e.g. `UNKNOWN_PRODUCT_ID`, `NEGATIVE_QUANTITY`, `CONFLICTING_EVENT_PAYLOAD`). |
| `rejection_category`| VARCHAR | NO | Classification (`FATAL_SCHEMA`, `VALIDATION_RULE`, `CONFLICT_DUPLICATE`, `REFERENTIAL_INTEGRITY`). |
| `created_at` | TIMESTAMP WITH TIME ZONE | NO | Quarantined timestamp. |

---

### 3.5 Run Audit Table (`run_audit`)

Complete operational audit record of every pipeline execution.

| Column Name | Physical Type | Nullable | Description |
|---|---|---|---|
| `run_id` | VARCHAR | NO | UUID primary key. |
| `source_file` | VARCHAR | NO | Input file path. |
| `start_time` | TIMESTAMP WITH TIME ZONE | NO | Execution start timestamp. |
| `end_time` | TIMESTAMP WITH TIME ZONE | YES | Execution end timestamp. |
| `run_status` | VARCHAR | NO | Status: `'SUCCESS'`, `'FAILED'`, `'RECONCILIATION_FAILED'`. |
| `input_row_count` | BIGINT | NO | Count of raw input lines read. |
| `validated_row_count`| BIGINT | NO | Count of rows passing syntactic & reference checks. |
| `new_event_count` | BIGINT | NO | Count of newly inserted events into ledger. |
| `exact_duplicate_count` | BIGINT | NO | Count of identical duplicate events deduplicated. |
| `conflicting_duplicate_count` | BIGINT | NO | Count of conflicting duplicate events quarantined. |
| `rejected_row_count`| BIGINT | NO | Count of malformed/invalid rows rejected. |
| `target_row_count` | BIGINT | NO | Count of rows affected / published in `daily_product_metrics`. |
| `duration_seconds` | DOUBLE | YES | Total wall-clock execution time. |
| `failure_reason` | VARCHAR | YES | Diagnostic stack trace or error message if failed. |
| `reconciliation_status` | VARCHAR | YES | `'PASSED'`, `'FAILED'`, or `'SKIPPED'`. |
