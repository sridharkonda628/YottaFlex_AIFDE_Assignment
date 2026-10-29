"""
Tests for pipeline idempotency and safe reruns.
Covers:
- Rerunning the exact same batch
- Exact duplicate delivery handling
- Batch rerun idempotency
"""

from __future__ import annotations

from pathlib import Path
import duckdb
import pytest

from daily_metrics.cli import run_pipeline
from daily_metrics.config import PipelineConfig, RunStatus


def test_rerun_same_batch(temp_dir: Path, temp_db_path: Path, temp_reference_dir: Path) -> None:
    """Test 19: Rerunning the same batch produces 0 new events and does not alter metrics."""
    batch_file = temp_dir / "batch_idempotent.csv"
    batch_file.write_text(
        "event_id,transaction_id,event_version,event_type,product_id,customer_id,event_ts,quantity,unit_price\n"
        "EVT_IDEM_1,TXN_IDEM_1,1,UPSERT,PROD_001,CUST_0001,2026-10-01T10:00:00Z,5,20.00\n"
        "EVT_IDEM_2,TXN_IDEM_2,1,UPSERT,PROD_002,CUST_0002,2026-10-01T11:00:00Z,3,15.00\n",
        encoding="utf-8",
    )

    cfg = PipelineConfig.from_paths(
        db_path=temp_db_path,
        input_file=batch_file,
        reference_dir=temp_reference_dir,
        output_dir=temp_dir / "out_1",
        run_id="run-initial",
    )
    audit1 = run_pipeline(cfg)
    assert audit1.status == RunStatus.SUCCESS.value
    assert audit1.new_event_count == 2
    assert audit1.exact_duplicate_count == 0

    # Capture metrics state
    conn = duckdb.connect(str(temp_db_path))
    metrics_run1 = conn.execute("SELECT * FROM daily_product_metrics ORDER BY metric_date, product_id").fetchall()
    conn.close()

    # Rerun identical batch
    cfg2 = PipelineConfig.from_paths(
        db_path=temp_db_path,
        input_file=batch_file,
        reference_dir=temp_reference_dir,
        output_dir=temp_dir / "out_2",
        run_id="run-rerun",
    )
    audit2 = run_pipeline(cfg2)
    assert audit2.status == RunStatus.SUCCESS.value
    assert audit2.new_event_count == 0
    assert audit2.exact_duplicate_count == 2

    # Verify metrics state is completely unchanged
    conn = duckdb.connect(str(temp_db_path))
    metrics_run2 = conn.execute("SELECT * FROM daily_product_metrics ORDER BY metric_date, product_id").fetchall()
    conn.close()

    assert metrics_run1 == metrics_run2


def test_batch_rerun_idempotency(temp_dir: Path, temp_db_path: Path, temp_reference_dir: Path) -> None:
    """Test idempotency over multiple repeated reruns."""
    batch_file = temp_dir / "batch_multi.csv"
    batch_file.write_text(
        "event_id,transaction_id,event_version,event_type,product_id,customer_id,event_ts,quantity,unit_price\n"
        "EVT_M1,TXN_M1,1,UPSERT,PROD_001,CUST_0001,2026-10-02T10:00:00Z,1,100.00\n",
        encoding="utf-8",
    )
    cfg = PipelineConfig.from_paths(
        db_path=temp_db_path,
        input_file=batch_file,
        reference_dir=temp_reference_dir,
        output_dir=temp_dir / "out_multi",
    )

    run_pipeline(cfg)
    run_pipeline(cfg)
    audit3 = run_pipeline(cfg)

    assert audit3.new_event_count == 0
    assert audit3.exact_duplicate_count == 1

    conn = duckdb.connect(str(temp_db_path))
    total_ledger = conn.execute("SELECT count(*) FROM event_ledger").fetchone()[0]
    total_metrics = conn.execute("SELECT units_sold, gross_revenue FROM daily_product_metrics").fetchone()
    conn.close()

    assert total_ledger == 1
    assert total_metrics[0] == 1
    assert float(total_metrics[1]) == 100.00
