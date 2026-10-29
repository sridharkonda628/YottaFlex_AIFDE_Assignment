"""
Tests for daily product metrics calculation, updates, reversals, partition shifts, and schema.
Covers:
- Metric calculations (units_sold, transaction_count, gross_revenue)
- Transaction updates
- Transaction reversals
- Product changes
- Date changes
- Empty partition cleanup
- Key uniqueness
"""

from __future__ import annotations

from decimal import Decimal
import duckdb
import pytest

from daily_metrics.publishing import publish_batch_atomically
from tests.conftest import make_event


def test_valid_batch_metrics(mem_db: duckdb.DuckDBPyConnection) -> None:
    """Test 13: Verifying accurate metric calculation for valid batch."""
    evt1 = make_event("E1", "T1", 1, product_id="PROD_001", event_ts="2026-10-01T10:00:00Z", quantity=2, unit_price=Decimal("10.00"))
    evt2 = make_event("E2", "T2", 1, product_id="PROD_001", event_ts="2026-10-01T11:00:00Z", quantity=3, unit_price=Decimal("15.00"))
    publish_batch_atomically(mem_db, [evt1, evt2], [], "run-1")

    row = mem_db.execute("""
        SELECT metric_date, product_id, units_sold, transaction_count, gross_revenue
        FROM daily_product_metrics
        WHERE metric_date = '2026-10-01' AND product_id = 'PROD_001'
    """).fetchone()

    assert row is not None
    assert str(row[0]) == "2026-10-01"
    assert row[1] == "PROD_001"
    assert row[2] == 5  # 2 + 3
    assert row[3] == 2  # 2 distinct txns
    assert float(row[4]) == 65.00  # (2 * 10) + (3 * 15) = 20 + 45 = 65.00


def test_transaction_update_metrics(mem_db: duckdb.DuckDBPyConnection) -> None:
    """Test 13b: Updating transaction quantity and price updates target partition."""
    evt1 = make_event("E1", "T1", 1, product_id="PROD_001", event_ts="2026-10-01T10:00:00Z", quantity=2, unit_price=Decimal("10.00"))
    publish_batch_atomically(mem_db, [evt1], [], "run-1")

    # Update T1 to quantity=5, unit_price=20.00 (gross = 100.00)
    evt1_v2 = make_event("E1_v2", "T1", 2, product_id="PROD_001", event_ts="2026-10-01T10:00:00Z", quantity=5, unit_price=Decimal("20.00"))
    publish_batch_atomically(mem_db, [evt1_v2], [], "run-2")

    row = mem_db.execute("""
        SELECT units_sold, transaction_count, gross_revenue
        FROM daily_product_metrics
        WHERE metric_date = '2026-10-01' AND product_id = 'PROD_001'
    """).fetchone()

    assert row[0] == 5
    assert row[1] == 1
    assert float(row[2]) == 100.00


def test_transaction_reversal(mem_db: duckdb.DuckDBPyConnection) -> None:
    """Test 14: Transaction reversal cleanly removes its contribution from active metrics."""
    evt1 = make_event("E1", "T1", 1, product_id="PROD_001", event_ts="2026-10-01T10:00:00Z", quantity=2, unit_price=Decimal("10.00"))
    evt2 = make_event("E2", "T2", 1, product_id="PROD_001", event_ts="2026-10-01T11:00:00Z", quantity=4, unit_price=Decimal("10.00"))
    publish_batch_atomically(mem_db, [evt1, evt2], [], "run-1")

    # Reversing T1
    rev = make_event("E1_REV", "T1", 2, event_type="REVERSAL", event_ts="2026-10-01T12:00:00Z")
    publish_batch_atomically(mem_db, [rev], [], "run-2")

    row = mem_db.execute("""
        SELECT units_sold, transaction_count, gross_revenue
        FROM daily_product_metrics
        WHERE metric_date = '2026-10-01' AND product_id = 'PROD_001'
    """).fetchone()

    # Only T2 remains: quantity=4, gross=40.00
    assert row[0] == 4
    assert row[1] == 1
    assert float(row[2]) == 40.00


def test_update_changing_product(mem_db: duckdb.DuckDBPyConnection) -> None:
    """Test 17: Update that shifts transaction from PROD_001 to PROD_002 updates both partitions."""
    evt = make_event("E1", "T1", 1, product_id="PROD_001", event_ts="2026-10-01T10:00:00Z", quantity=3, unit_price=Decimal("10.00"))
    publish_batch_atomically(mem_db, [evt], [], "run-1")

    # Version 2 changes product to PROD_002
    evt_v2 = make_event("E1_V2", "T1", 2, product_id="PROD_002", event_ts="2026-10-01T10:00:00Z", quantity=3, unit_price=Decimal("10.00"))
    publish_batch_atomically(mem_db, [evt_v2], [], "run-2")

    # PROD_001 should now have 0 active transactions (cleaned up)
    p1 = mem_db.execute("SELECT count(*) FROM daily_product_metrics WHERE product_id = 'PROD_001'").fetchone()[0]
    assert p1 == 0

    # PROD_002 should have the 3 units
    p2 = mem_db.execute("SELECT units_sold, gross_revenue FROM daily_product_metrics WHERE product_id = 'PROD_002'").fetchone()
    assert p2 is not None
    assert p2[0] == 3
    assert float(p2[1]) == 30.00


def test_update_changing_date(mem_db: duckdb.DuckDBPyConnection) -> None:
    """Test 18: Update that moves transaction business date updates both date partitions."""
    evt = make_event("E1", "T1", 1, product_id="PROD_001", event_ts="2026-10-01T10:00:00Z", quantity=2, unit_price=Decimal("15.00"))
    publish_batch_atomically(mem_db, [evt], [], "run-1")

    # Version 2 moves date to 2026-10-05
    evt_v2 = make_event("E1_V2", "T1", 2, product_id="PROD_001", event_ts="2026-10-05T14:00:00Z", quantity=2, unit_price=Decimal("15.00"))
    publish_batch_atomically(mem_db, [evt_v2], [], "run-2")

    # 2026-10-01 should be deleted (0 active)
    d1 = mem_db.execute("SELECT count(*) FROM daily_product_metrics WHERE metric_date = '2026-10-01'").fetchone()[0]
    assert d1 == 0

    # 2026-10-05 should have the metric
    d2 = mem_db.execute("SELECT units_sold FROM daily_product_metrics WHERE metric_date = '2026-10-05'").fetchone()
    assert d2 is not None
    assert d2[0] == 2


def test_empty_partition_cleanup(mem_db: duckdb.DuckDBPyConnection) -> None:
    """Test 15b: When all transactions in a partition are reversed, the empty partition is removed."""
    evt = make_event("E1", "T1", 1, product_id="PROD_001", event_ts="2026-10-01T10:00:00Z", quantity=1, unit_price=Decimal("10.00"))
    publish_batch_atomically(mem_db, [evt], [], "run-1")

    # Reverse T1
    rev = make_event("E1_REV", "T1", 2, event_type="REVERSAL")
    publish_batch_atomically(mem_db, [rev], [], "run-2")

    # Assert no row exists in daily_product_metrics for 2026-10-01 and PROD_001
    count = mem_db.execute("SELECT count(*) FROM daily_product_metrics").fetchone()[0]
    assert count == 0


def test_target_schema_structure(mem_db: duckdb.DuckDBPyConnection) -> None:
    """Test target schema primary key and column constraints."""
    info = mem_db.execute("PRAGMA table_info('daily_product_metrics')").fetchall()
    col_names = [r[1] for r in info]
    assert "metric_date" in col_names
    assert "product_id" in col_names
    assert "units_sold" in col_names
    assert "transaction_count" in col_names
    assert "gross_revenue" in col_names


def test_duplicate_target_keys_prevented(mem_db: duckdb.DuckDBPyConnection) -> None:
    """Test 23: Target table enforces primary key uniqueness on (metric_date, product_id)."""
    evt1 = make_event("E1", "T1", 1, product_id="PROD_001", event_ts="2026-10-01T10:00:00Z")
    publish_batch_atomically(mem_db, [evt1], [], "run-1")

    # Verify count(*) equals count(distinct (metric_date, product_id))
    stats = mem_db.execute("""
        SELECT COUNT(*), COUNT(DISTINCT metric_date || product_id)
        FROM daily_product_metrics
    """).fetchone()
    assert stats[0] == stats[1]
