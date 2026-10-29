"""
Metrics transformation, affected-partition identification, and aggregate computation.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional, Set, Tuple

import duckdb

from daily_metrics.config import TransactionEvent

logger = logging.getLogger(__name__)


def identify_affected_partitions(
    conn: duckdb.DuckDBPyConnection,
    new_events: List[TransactionEvent],
) -> Set[Tuple[str, str]]:
    """
    Identifies all (metric_date, product_id) partitions affected by incoming events.
    Checks both the prior winning state and prospective state for each touched transaction.
    """
    if not new_events:
        return set()

    affected_partitions: Set[Tuple[str, str]] = set()

    # Add prospective partitions directly from incoming UPSERT events
    for event in new_events:
        if event.event_type == "UPSERT" and event.product_id and event.metric_date:
            affected_partitions.add((event.metric_date, event.product_id))

    # Check prior state in database for touched transaction_ids
    unique_txns = list(set([e.transaction_id for e in new_events]))
    table_check = conn.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = 'event_ledger'"
    ).fetchone()[0]

    if table_check > 0 and unique_txns:
        chunk_size = 5000
        for i in range(0, len(unique_txns), chunk_size):
            chunk = unique_txns[i:i + chunk_size]
            placeholders = ",".join(["?"] * len(chunk))
            
            # Query prior winning state from view or ledger
            query = f"""
                WITH ranked AS (
                    SELECT 
                        transaction_id, 
                        metric_date, 
                        product_id, 
                        event_type,
                        ROW_NUMBER() OVER (
                            PARTITION BY transaction_id 
                            ORDER BY event_version DESC, event_ts DESC
                        ) as rn
                    FROM event_ledger
                    WHERE transaction_id IN ({placeholders})
                )
                SELECT metric_date, product_id 
                FROM ranked 
                WHERE rn = 1 AND event_type = 'UPSERT' AND product_id IS NOT NULL;
            """
            rows = conn.execute(query, chunk).fetchall()
            for m_date, p_id in rows:
                if m_date and p_id:
                    affected_partitions.add((str(m_date), str(p_id)))

    return affected_partitions


def compute_partition_aggregates(
    conn: duckdb.DuckDBPyConnection,
    affected_partitions: Set[Tuple[str, str]],
) -> List[Tuple[str, str, int, int, float]]:
    """
    Computes exact aggregates for specified partitions directly from current winning transaction state.
    
    Returns list of tuples: (metric_date, product_id, units_sold, transaction_count, gross_revenue)
    """
    if not affected_partitions:
        return []

    # Create temporary table for filtering affected partitions
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS temp_affected_partitions (metric_date DATE, product_id VARCHAR)")
    conn.execute("DELETE FROM temp_affected_partitions")
    
    conn.executemany(
        "INSERT INTO temp_affected_partitions VALUES (?, ?)",
        list(affected_partitions),
    )

    query = """
        WITH ranked_events AS (
            SELECT
                transaction_id,
                event_version,
                event_type,
                product_id,
                metric_date,
                quantity,
                gross_amount,
                ROW_NUMBER() OVER (
                    PARTITION BY transaction_id 
                    ORDER BY event_version DESC, event_ts DESC
                ) AS rank_num
            FROM event_ledger
        ),
        current_state AS (
            SELECT
                transaction_id,
                product_id,
                metric_date,
                quantity,
                gross_amount
            FROM ranked_events
            WHERE rank_num = 1 AND event_type = 'UPSERT'
        )
        SELECT
            cs.metric_date::VARCHAR,
            cs.product_id,
            CAST(COALESCE(SUM(cs.quantity), 0) AS BIGINT) AS units_sold,
            CAST(COUNT(DISTINCT cs.transaction_id) AS BIGINT) AS transaction_count,
            CAST(ROUND(COALESCE(SUM(cs.gross_amount), 0.00), 2) AS DOUBLE) AS gross_revenue
        FROM current_state cs
        INNER JOIN temp_affected_partitions tap
            ON cs.metric_date = tap.metric_date
            AND cs.product_id = tap.product_id
        GROUP BY cs.metric_date, cs.product_id
        HAVING COUNT(DISTINCT cs.transaction_id) > 0;
    """

    results = conn.execute(query).fetchall()
    conn.execute("DROP TABLE IF EXISTS temp_affected_partitions")
    return results
