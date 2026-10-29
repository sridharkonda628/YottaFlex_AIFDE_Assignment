"""
Shared pytest fixtures and test environment configuration.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Generator
import pytest
import duckdb

from daily_metrics.generator import write_reference_data
from daily_metrics.publishing import init_database_schema, populate_reference_tables


@pytest.fixture
def temp_dir(tmp_path: Path) -> Path:
    """Provides an isolated temporary directory for file-based tests."""
    return tmp_path


@pytest.fixture
def temp_reference_dir(tmp_path: Path) -> Path:
    """Creates isolated reference products and customers CSV files."""
    ref_dir = tmp_path / "reference"
    write_reference_data(ref_dir)
    return ref_dir


@pytest.fixture
def temp_db_path(tmp_path: Path) -> Path:
    """Provides path to an isolated temporary DuckDB file."""
    return tmp_path / "test_metrics.duckdb"


@pytest.fixture
def mem_db(temp_reference_dir: Path) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Provides an initialized in-memory DuckDB connection with schemas and reference tables."""
    conn = duckdb.connect(":memory:")
    init_database_schema(conn)
    populate_reference_tables(conn, temp_reference_dir)
    yield conn
    conn.close()


def make_event(
    event_id: str,
    transaction_id: str,
    event_version: int,
    event_type: str = "UPSERT",
    product_id: str = "PROD_001",
    customer_id: str = "CUST_0001",
    event_ts: str = "2026-10-01T10:00:00Z",
    quantity: int = 1,
    unit_price: Any = None,
    run_id: str = "run-test",
) -> Any:
    from datetime import datetime
    from decimal import Decimal
    from daily_metrics.config import TransactionEvent
    from daily_metrics.validation import compute_payload_hash

    if unit_price is None:
        unit_price = Decimal("50.00")
    elif not isinstance(unit_price, Decimal):
        unit_price = Decimal(str(unit_price))

    dt = datetime.fromisoformat(event_ts.replace("Z", "+00:00"))
    phash = compute_payload_hash(
        transaction_id=transaction_id,
        event_version=event_version,
        event_type=event_type,
        product_id=product_id if event_type == "UPSERT" else None,
        customer_id=customer_id if event_type == "UPSERT" else None,
        event_ts_str=dt.isoformat(),
        quantity=quantity if event_type == "UPSERT" else None,
        unit_price=unit_price if event_type == "UPSERT" else None,
    )
    gross = (Decimal(quantity) * unit_price).quantize(Decimal("0.01")) if event_type == "UPSERT" else None
    return TransactionEvent(
        event_id=event_id,
        transaction_id=transaction_id,
        event_version=event_version,
        event_type=event_type,
        product_id=product_id if event_type == "UPSERT" else None,
        customer_id=customer_id if event_type == "UPSERT" else None,
        event_ts=dt,
        metric_date=dt.date().isoformat(),
        quantity=quantity if event_type == "UPSERT" else None,
        unit_price=unit_price if event_type == "UPSERT" else None,
        gross_amount=gross,
        payload_hash=phash,
        ingested_at=dt,
        source_file="test.csv",
        run_id=run_id,
    )

