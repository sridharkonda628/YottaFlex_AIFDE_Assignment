"""
Tests for failure handling, transaction rollback, and recovery.
Covers:
- Database transaction rollback on error
- Recovery after failed transaction
- Visibility of failed runs in audit
- Partial failure and retry behavior
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch
import duckdb
import pytest

from daily_metrics.cli import run_pipeline
from daily_metrics.config import DatabaseTransactionError, PipelineConfig, RunStatus
from daily_metrics.publishing import publish_batch_atomically
from tests.conftest import make_event


def test_transaction_rollback_on_failure(mem_db: duckdb.DuckDBPyConnection) -> None:
    """Test 24: Unhandled error during publishing triggers ROLLBACK leaving database state pristine."""
    evt1 = make_event("E_GOOD", "T_GOOD", 1, quantity=1)
    publish_batch_atomically(mem_db, [evt1], [], "run-1")

    # Ingest a second event, but mock compute_partition_aggregates to raise an unhandled exception
    evt2 = make_event("E_FAIL", "T_FAIL", 1, quantity=5)

    with patch("daily_metrics.publishing.compute_partition_aggregates", side_effect=RuntimeError("Simulated Disk Full")):
        with pytest.raises(DatabaseTransactionError):
            publish_batch_atomically(mem_db, [evt2], [], "run-2")

    # Verify event E_FAIL was NOT committed to event_ledger
    ledger_entries = mem_db.execute("SELECT event_id FROM event_ledger").fetchall()
    assert len(ledger_entries) == 1
    assert ledger_entries[0][0] == "E_GOOD"


def test_recovery_after_failed_database_transaction(
    temp_dir: Path, temp_db_path: Path, temp_reference_dir: Path
) -> None:
    """Test 20: Recovery after a simulated failure, retrying successfully."""
    batch_file = temp_dir / "batch_recover.csv"
    batch_file.write_text(
        "event_id,transaction_id,event_version,event_type,product_id,customer_id,event_ts,quantity,unit_price\n"
        "EVT_REC_1,TXN_REC_1,1,UPSERT,PROD_001,CUST_0001,2026-10-01T10:00:00Z,2,50.00\n",
        encoding="utf-8",
    )
    cfg_fail = PipelineConfig.from_paths(
        db_path=temp_db_path,
        input_file=batch_file,
        reference_dir=temp_reference_dir,
        output_dir=temp_dir / "out_fail",
        run_id="run-fail-1",
    )

    # First attempt fails due to simulated error
    with patch("daily_metrics.cli.publish_batch_atomically", side_effect=RuntimeError("Simulated crash")):
        audit_fail = run_pipeline(cfg_fail)
        assert audit_fail.status == RunStatus.FAILED.value

    # Second attempt (retry) without simulated error
    cfg_retry = PipelineConfig.from_paths(
        db_path=temp_db_path,
        input_file=batch_file,
        reference_dir=temp_reference_dir,
        output_dir=temp_dir / "out_retry",
        run_id="run-retry-2",
    )
    audit_retry = run_pipeline(cfg_retry)
    assert audit_retry.status == RunStatus.SUCCESS.value
    assert audit_retry.new_event_count == 1

    # Verify audit table captures both the failure and the subsequent success
    conn = duckdb.connect(str(temp_db_path))
    runs = conn.execute("SELECT run_id, status FROM run_audit ORDER BY start_time ASC").fetchall()
    conn.close()

    assert len(runs) == 2
    assert runs[0] == ("run-fail-1", "FAILED")
    assert runs[1] == ("run-retry-2", "SUCCESS")


def test_failed_run_visible_in_audit(
    temp_dir: Path, temp_db_path: Path, temp_reference_dir: Path
) -> None:
    """Test that failed runs remain visible in the persistent audit log."""
    bad_csv = temp_dir / "bad_schema.csv"
    bad_csv.write_text("invalid_header\nfoo\n", encoding="utf-8")

    cfg = PipelineConfig.from_paths(
        db_path=temp_db_path,
        input_file=bad_csv,
        reference_dir=temp_reference_dir,
        output_dir=temp_dir / "out_bad",
        run_id="run-schema-fail",
    )
    audit = run_pipeline(cfg)
    assert audit.status == RunStatus.FAILED.value

    conn = duckdb.connect(str(temp_db_path))
    audit_row = conn.execute("SELECT run_id, status, failure_reason FROM run_audit WHERE run_id = 'run-schema-fail'").fetchone()
    conn.close()

    assert audit_row is not None
    assert audit_row[1] == "FAILED"
    assert "Missing required column" in audit_row[2]
