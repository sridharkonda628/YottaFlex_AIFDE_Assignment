"""
Database schema management, atomic transactional publishing, and export generation.
"""

from __future__ import annotations

import csv
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Set, Tuple

import duckdb

from daily_metrics.config import (
    DatabaseTransactionError,
    QuarantineRecord,
    TransactionEvent,
)
from daily_metrics.transformations import (
    compute_partition_aggregates,
    identify_affected_partitions,
)

logger = logging.getLogger(__name__)


DDL_SCHEMA = """
-- 1. Immutable Event Ledger
CREATE TABLE IF NOT EXISTS event_ledger (
    event_id VARCHAR PRIMARY KEY,
    transaction_id VARCHAR NOT NULL,
    event_version BIGINT NOT NULL,
    event_type VARCHAR NOT NULL,
    product_id VARCHAR,
    customer_id VARCHAR,
    event_ts TIMESTAMP WITH TIME ZONE NOT NULL,
    metric_date DATE NOT NULL,
    quantity BIGINT,
    unit_price DECIMAL(18,2),
    gross_amount DECIMAL(18,2),
    payload_hash VARCHAR(64) NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE NOT NULL,
    source_file VARCHAR NOT NULL,
    run_id VARCHAR NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_txn_version UNIQUE (transaction_id, event_version)
);

-- 2. Current Transaction State View (Authoritative Winning State)
CREATE OR REPLACE VIEW current_transaction_state AS
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

-- 3. Analytical Target Table: Daily Product Metrics
CREATE TABLE IF NOT EXISTS daily_product_metrics (
    metric_date DATE NOT NULL,
    product_id VARCHAR NOT NULL,
    units_sold BIGINT NOT NULL,
    transaction_count BIGINT NOT NULL,
    gross_revenue DECIMAL(18,2) NOT NULL,
    last_updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    last_run_id VARCHAR NOT NULL,
    PRIMARY KEY (metric_date, product_id)
);

-- 4. Quarantine Store
CREATE TABLE IF NOT EXISTS quarantine_records (
    quarantine_id VARCHAR PRIMARY KEY,
    run_id VARCHAR NOT NULL,
    source_file VARCHAR NOT NULL,
    source_row_num BIGINT,
    raw_record_json VARCHAR NOT NULL,
    rejection_reason VARCHAR NOT NULL,
    rejection_category VARCHAR NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 5. Operational Run Audit Store
CREATE TABLE IF NOT EXISTS run_audit (
    run_id VARCHAR PRIMARY KEY,
    source_file VARCHAR NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time TIMESTAMP WITH TIME ZONE,
    status VARCHAR NOT NULL,
    input_row_count BIGINT NOT NULL,
    validated_row_count BIGINT NOT NULL,
    new_event_count BIGINT NOT NULL,
    exact_duplicate_count BIGINT NOT NULL,
    conflicting_duplicate_count BIGINT NOT NULL,
    rejected_row_count BIGINT NOT NULL,
    target_row_count BIGINT NOT NULL,
    duration_seconds DOUBLE,
    failure_reason VARCHAR,
    reconciliation_status VARCHAR
);

-- Reference Tables
CREATE TABLE IF NOT EXISTS reference_products (
    product_id VARCHAR PRIMARY KEY,
    product_name VARCHAR NOT NULL,
    category VARCHAR NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS reference_customers (
    customer_id VARCHAR PRIMARY KEY,
    customer_name VARCHAR NOT NULL,
    email VARCHAR NOT NULL,
    tier VARCHAR NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE
);
"""


def init_database_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Initializes tables, views, and indexes in DuckDB."""
    conn.execute(DDL_SCHEMA)


def populate_reference_tables(
    conn: duckdb.DuckDBPyConnection,
    reference_dir: Path,
) -> None:
    """Populates reference tables in DuckDB from reference CSV files."""
    prod_file = reference_dir / "reference_products.csv"
    cust_file = reference_dir / "reference_customers.csv"

    if prod_file.exists():
        conn.execute(f"""
            INSERT OR REPLACE INTO reference_products
            SELECT 
                product_id, 
                product_name, 
                category, 
                is_active::BOOLEAN
            FROM read_csv_auto('{prod_file.as_posix()}')
        """)

    if cust_file.exists():
        conn.execute(f"""
            INSERT OR REPLACE INTO reference_customers
            SELECT 
                customer_id, 
                customer_name, 
                email, 
                tier, 
                is_active::BOOLEAN
            FROM read_csv_auto('{cust_file.as_posix()}')
        """)


def publish_batch_atomically(
    conn: duckdb.DuckDBPyConnection,
    new_events: List[TransactionEvent],
    quarantined_records: List[QuarantineRecord],
    run_id: str,
) -> int:
    """
    Publishes newly accepted events and updates affected analytical partitions atomically.
    Uses strict ACID database transaction boundaries.
    
    Returns:
        int: Number of partition rows modified/inserted in daily_product_metrics.
    """
    # 1. Identify affected partitions before and after inserting new events
    affected_partitions = identify_affected_partitions(conn, new_events)

    now_utc = datetime.now(timezone.utc).isoformat()
    target_rows_written = 0

    try:
        conn.execute("BEGIN TRANSACTION")

        # 2. Insert new valid events into immutable ledger
        if new_events:
            event_tuples = [e.to_tuple() for e in new_events]
            conn.executemany(
                """
                INSERT INTO event_ledger (
                    event_id, transaction_id, event_version, event_type,
                    product_id, customer_id, event_ts, metric_date,
                    quantity, unit_price, gross_amount, payload_hash,
                    ingested_at, source_file, run_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                event_tuples,
            )

        # 3. Insert quarantine records
        if quarantined_records:
            q_tuples = [q.to_tuple() for q in quarantined_records]
            conn.executemany(
                """
                INSERT INTO quarantine_records (
                    quarantine_id, run_id, source_file, source_row_num,
                    raw_record_json, rejection_reason, rejection_category, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                q_tuples,
            )

        # 4. If affected partitions exist, recalculate and replace them atomically
        if affected_partitions:
            # Delete old partition values for affected partitions
            # Using temporary table for safe set deletion
            conn.execute("CREATE TEMP TABLE IF NOT EXISTS temp_delete_partitions (metric_date DATE, product_id VARCHAR)")
            conn.execute("DELETE FROM temp_delete_partitions")
            conn.executemany(
                "INSERT INTO temp_delete_partitions VALUES (?, ?)",
                list(affected_partitions),
            )

            conn.execute("""
                DELETE FROM daily_product_metrics
                WHERE (metric_date, product_id) IN (
                    SELECT metric_date, product_id FROM temp_delete_partitions
                )
            """)
            conn.execute("DROP TABLE IF EXISTS temp_delete_partitions")

            # Compute updated aggregates
            updated_aggregates = compute_partition_aggregates(conn, affected_partitions)

            if updated_aggregates:
                insert_tuples = [
                    (
                        m_date,
                        p_id,
                        u_sold,
                        tx_cnt,
                        round(g_rev, 2),
                        now_utc,
                        run_id,
                    )
                    for m_date, p_id, u_sold, tx_cnt, g_rev in updated_aggregates
                ]

                conn.executemany(
                    """
                    INSERT INTO daily_product_metrics (
                        metric_date, product_id, units_sold, transaction_count,
                        gross_revenue, last_updated_at, last_run_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    insert_tuples,
                )
                target_rows_written = len(insert_tuples)

        conn.execute("COMMIT")
        logger.info(
            "Batch committed successfully. %d new events, %d rejects, %d metric partitions updated.",
            len(new_events),
            len(quarantined_records),
            target_rows_written,
        )
        return target_rows_written

    except Exception as e:
        logger.error("Database transaction failed. Executing ROLLBACK. Error: %s", e)
        try:
            conn.execute("ROLLBACK")
        except Exception as rb_err:
            logger.critical("Rollback failed: %s", rb_err)
        raise DatabaseTransactionError(f"Atomic publish failed and rolled back: {e}") from e


def export_published_metrics(
    conn: duckdb.DuckDBPyConnection,
    output_dir: Path,
    run_id: str,
) -> Path:
    """Exports current daily_product_metrics to CSV for external consumption."""
    output_dir.mkdir(parents=True, exist_ok=True)
    export_path = output_dir / "daily_product_metrics.csv"

    query = """
        SELECT
            metric_date::VARCHAR AS metric_date,
            product_id,
            units_sold,
            transaction_count,
            gross_revenue::DECIMAL(18,2) AS gross_revenue,
            last_updated_at::VARCHAR AS last_updated_at,
            last_run_id
        FROM daily_product_metrics
        ORDER BY metric_date, product_id;
    """
    df = conn.execute(query).df()
    df.to_csv(export_path, index=False)
    logger.info("Exported daily_product_metrics to %s (%d rows)", export_path, len(df))
    return export_path


def export_quarantine_records(
    conn: duckdb.DuckDBPyConnection,
    output_dir: Path,
    run_id: Optional[str] = None,
) -> Path:
    """Exports quarantined records to CSV for operational inspection."""
    output_dir.mkdir(parents=True, exist_ok=True)
    export_path = output_dir / "rejected_records.csv"

    query = """
        SELECT
            quarantine_id,
            run_id,
            source_file,
            source_row_num,
            raw_record_json,
            rejection_reason,
            rejection_category,
            created_at::VARCHAR AS created_at
        FROM quarantine_records
    """
    if run_id:
        query += f" WHERE run_id = '{run_id}'"
    query += " ORDER BY created_at DESC;"

    df = conn.execute(query).df()
    df.to_csv(export_path, index=False)
    logger.info("Exported quarantine records to %s (%d rows)", export_path, len(df))
    return export_path
