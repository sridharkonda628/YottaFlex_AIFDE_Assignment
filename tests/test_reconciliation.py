"""
Tests for independent reconciliation and correctness proof.
Covers:
- Reconciliation passing on synchronized data
- Detection of revenue discrepancies
- Detection of units sold discrepancies
- Detection of partition count mismatches
- Error raising on failed reconciliation
"""

from __future__ import annotations

from decimal import Decimal
import duckdb
import pytest

from daily_metrics.config import ReconciliationError
from daily_metrics.publishing import publish_batch_atomically
from daily_metrics.reconciliation import Reconciler
from tests.conftest import make_event


def test_reconciliation_pass(mem_db: duckdb.DuckDBPyConnection) -> None:
    """Test 17a: Independent reconciliation succeeds when target metrics match ledger ground truth."""
    evt1 = make_event("E1", "T1", 1, product_id="PROD_001", quantity=3, unit_price=Decimal("10.00"))
    evt2 = make_event("E2", "T2", 1, product_id="PROD_002", quantity=2, unit_price=Decimal("25.00"))
    publish_batch_atomically(mem_db, [evt1, evt2], [], "run-1")

    reconciler = Reconciler(mem_db)
    report = reconciler.verify_or_raise()

    assert report["overall_status"] == "PASSED"
    assert report["mismatch_count"] == 0


def test_reconciliation_detects_tampered_revenue(mem_db: duckdb.DuckDBPyConnection) -> None:
    """Test 22: Reconciliation detects an unauthorized or corrupted revenue discrepancy."""
    evt = make_event("E1", "T1", 1, product_id="PROD_001", quantity=1, unit_price=Decimal("50.00"))
    publish_batch_atomically(mem_db, [evt], [], "run-1")

    # Manually tamper with daily_product_metrics to simulate a silent bug or data corruption
    mem_db.execute("UPDATE daily_product_metrics SET gross_revenue = 49.99 WHERE product_id = 'PROD_001'")

    reconciler = Reconciler(mem_db)
    report = reconciler.run_reconciliation()

    assert report["overall_status"] == "FAILED"
    assert report["mismatch_count"] > 0
    with pytest.raises(ReconciliationError):
        reconciler.verify_or_raise()


def test_reconciliation_detects_tampered_units(mem_db: duckdb.DuckDBPyConnection) -> None:
    """Reconciliation detects a discrepancy in units sold."""
    evt = make_event("E1", "T1", 1, product_id="PROD_001", quantity=10, unit_price=Decimal("5.00"))
    publish_batch_atomically(mem_db, [evt], [], "run-1")

    # Tamper with units_sold
    mem_db.execute("UPDATE daily_product_metrics SET units_sold = 9 WHERE product_id = 'PROD_001'")

    reconciler = Reconciler(mem_db)
    report = reconciler.run_reconciliation()

    assert report["overall_status"] == "FAILED"
    units_check = next(c for c in report["checks"] if c["check_name"] == "global_units_sold")
    assert units_check["status"] == "FAIL"


def test_reconciliation_detects_orphan_partition(mem_db: duckdb.DuckDBPyConnection) -> None:
    """Reconciliation detects an orphan partition that has no ledger events."""
    evt = make_event("E1", "T1", 1, product_id="PROD_001", quantity=1, unit_price=Decimal("10.00"))
    publish_batch_atomically(mem_db, [evt], [], "run-1")

    # Inject ghost partition
    mem_db.execute("""
        INSERT INTO daily_product_metrics VALUES (
            '2026-10-25', 'PROD_GHOST', 1, 1, 10.00, CURRENT_TIMESTAMP, 'run-fake'
        )
    """)

    reconciler = Reconciler(mem_db)
    report = reconciler.run_reconciliation()

    assert report["overall_status"] == "FAILED"
    partition_check = next(c for c in report["checks"] if c["check_name"] == "per_partition_reconciliation")
    assert partition_check["status"] == "FAIL"
