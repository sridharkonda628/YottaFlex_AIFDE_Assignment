"""
Tests for historical backfills and late-arriving reporting period recomputations.
Covers:
- Ingestion of past-dated batches
- Recalculation of historical partitions without corrupting current partitions
- Full reconciliation across backfilled and contemporary periods
"""

from __future__ import annotations

from pathlib import Path
import duckdb
import pytest

from daily_metrics.cli import run_pipeline
from daily_metrics.config import PipelineConfig, RunStatus
from daily_metrics.reconciliation import Reconciler


def test_historical_backfill(
    temp_dir: Path, temp_db_path: Path, temp_reference_dir: Path
) -> None:
    """Test 21: Ingesting contemporary batch followed by a historical backfill batch."""
    # 1. Contemporary batch: October 2026
    current_batch = temp_dir / "batch_current.csv"
    current_batch.write_text(
        "event_id,transaction_id,event_version,event_type,product_id,customer_id,event_ts,quantity,unit_price\n"
        "EVT_OCT_1,TXN_OCT_1,1,UPSERT,PROD_001,CUST_0001,2026-10-15T10:00:00Z,2,50.00\n",
        encoding="utf-8",
    )
    cfg_current = PipelineConfig.from_paths(
        db_path=temp_db_path,
        input_file=current_batch,
        reference_dir=temp_reference_dir,
        output_dir=temp_dir / "out_oct",
    )
    audit_oct = run_pipeline(cfg_current)
    assert audit_oct.status == RunStatus.SUCCESS.value

    # 2. Historical backfill batch: September 2026 (1 month earlier)
    backfill_batch = temp_dir / "batch_backfill_sept.csv"
    backfill_batch.write_text(
        "event_id,transaction_id,event_version,event_type,product_id,customer_id,event_ts,quantity,unit_price\n"
        "EVT_SEPT_1,TXN_SEPT_1,1,UPSERT,PROD_001,CUST_0001,2026-09-01T08:00:00Z,10,25.00\n"
        "EVT_SEPT_2,TXN_SEPT_2,1,UPSERT,PROD_002,CUST_0002,2026-09-01T09:00:00Z,4,15.00\n",
        encoding="utf-8",
    )
    cfg_backfill = PipelineConfig.from_paths(
        db_path=temp_db_path,
        input_file=backfill_batch,
        reference_dir=temp_reference_dir,
        output_dir=temp_dir / "out_sept",
    )
    audit_sept = run_pipeline(cfg_backfill)
    assert audit_sept.status == RunStatus.SUCCESS.value
    assert audit_sept.new_event_count == 2

    # 3. Verify target daily_product_metrics contains both September and October partitions
    conn = duckdb.connect(str(temp_db_path))
    rows = conn.execute("""
        SELECT metric_date, product_id, units_sold, gross_revenue
        FROM daily_product_metrics
        ORDER BY metric_date, product_id
    """).fetchall()

    assert len(rows) == 3
    # Row 1: 2026-09-01, PROD_001
    assert str(rows[0][0]) == "2026-09-01"
    assert rows[0][1] == "PROD_001"
    assert rows[0][2] == 10
    assert float(rows[0][3]) == 250.00

    # Row 2: 2026-09-01, PROD_002
    assert str(rows[1][0]) == "2026-09-01"
    assert rows[1][1] == "PROD_002"
    assert rows[1][2] == 4
    assert float(rows[1][3]) == 60.00

    # Row 3: 2026-10-15, PROD_001
    assert str(rows[2][0]) == "2026-10-15"
    assert rows[2][1] == "PROD_001"
    assert rows[2][2] == 2
    assert float(rows[2][3]) == 100.00

    # 4. Independent reconciliation passes across the combined historical and contemporary dataset
    reconciler = Reconciler(conn)
    report = reconciler.verify_or_raise()
    assert report["overall_status"] == "PASSED"
    conn.close()
