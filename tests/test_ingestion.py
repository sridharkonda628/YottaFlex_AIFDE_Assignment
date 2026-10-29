"""
Tests for source CSV ingestion, schema validation, and fail-closed behavior.
Covers:
- Valid batch ingestion
- Empty input file
- Malformed CSV
- Missing required fields
- Unexpected columns
"""

from __future__ import annotations

from pathlib import Path
import pytest

from daily_metrics.config import FatalSchemaError, PipelineConfig, RunStatus
from daily_metrics.cli import run_pipeline
from daily_metrics.ingestion import read_source_csv


def test_valid_batch_ingestion(temp_dir: Path, temp_db_path: Path, temp_reference_dir: Path) -> None:
    """Test 1: Ingesting a standard valid CSV batch."""
    csv_file = temp_dir / "valid_batch.csv"
    csv_file.write_text(
        "event_id,transaction_id,event_version,event_type,product_id,customer_id,event_ts,quantity,unit_price\n"
        "EVT_001,TXN_001,1,UPSERT,PROD_001,CUST_0001,2026-10-01T10:00:00Z,2,49.99\n",
        encoding="utf-8",
    )
    header, rows = read_source_csv(csv_file)
    assert "event_id" in header
    row_list = list(rows)
    assert len(row_list) == 1
    assert row_list[0][1]["transaction_id"] == "TXN_001"


def test_empty_input_file(temp_dir: Path, temp_db_path: Path, temp_reference_dir: Path) -> None:
    """Test 2: Ingesting an empty input file (0 bytes)."""
    empty_csv = temp_dir / "empty.csv"
    empty_csv.write_text("", encoding="utf-8")

    header, rows = read_source_csv(empty_csv)
    assert header == []
    assert list(rows) == []

    # Run through full pipeline orchestrator
    cfg = PipelineConfig.from_paths(
        db_path=temp_db_path,
        input_file=empty_csv,
        reference_dir=temp_reference_dir,
        output_dir=temp_dir / "out_empty",
    )
    audit = run_pipeline(cfg)
    assert audit.status == RunStatus.SUCCESS.value
    assert audit.input_row_count == 0
    assert audit.new_event_count == 0


def test_malformed_csv(temp_dir: Path, temp_db_path: Path, temp_reference_dir: Path) -> None:
    """Test 3: Ingesting a structurally broken CSV (unclosed quotes/irregular delimiters)."""
    malformed_csv = temp_dir / "malformed.csv"
    malformed_csv.write_text(
        'event_id,transaction_id,event_version,event_type,product_id,customer_id,event_ts,quantity,unit_price\n'
        'EVT_BAD,"TXN_BROKEN,1,UPSERT,PROD_001,CUST_0001,2026-10-01T10:00:00Z\n',
        encoding="utf-8",
    )
    with pytest.raises(FatalSchemaError):
        header, rows = read_source_csv(malformed_csv)
        list(rows)


def test_missing_required_columns(temp_dir: Path) -> None:
    """Test 4: Failing closed when mandatory schema columns are omitted."""
    broken_csv = temp_dir / "missing_cols.csv"
    # Missing unit_price and customer_id
    broken_csv.write_text(
        "event_id,transaction_id,event_version,event_type,product_id,event_ts,quantity\n"
        "EVT_001,TXN_001,1,UPSERT,PROD_001,2026-10-01T10:00:00Z,2\n",
        encoding="utf-8",
    )
    with pytest.raises(FatalSchemaError) as exc_info:
        read_source_csv(broken_csv)
    assert "Missing required column(s)" in str(exc_info.value)


def test_unexpected_columns(temp_dir: Path) -> None:
    """Test 5: Failing closed when unexpected/unknown columns are injected."""
    rogue_csv = temp_dir / "rogue_cols.csv"
    rogue_csv.write_text(
        "event_id,transaction_id,event_version,event_type,product_id,customer_id,event_ts,quantity,unit_price,rogue_debug_field\n"
        "EVT_001,TXN_001,1,UPSERT,PROD_001,CUST_0001,2026-10-01T10:00:00Z,2,49.99,hacked\n",
        encoding="utf-8",
    )
    with pytest.raises(FatalSchemaError) as exc_info:
        read_source_csv(rogue_csv)
    assert "Unexpected column(s) detected" in str(exc_info.value)
