"""
Tests for business rule validation, data types, and referential integrity.
Covers:
- Invalid data types
- Invalid timestamps
- Non-positive quantities
- Negative unit prices
- Unknown product ID (referential integrity)
- Unknown customer ID (referential integrity)
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import pytest

from daily_metrics.validation import ReferenceStore, validate_row


@pytest.fixture
def sample_ref_store() -> ReferenceStore:
    return ReferenceStore(
        active_products={"PROD_001", "PROD_002"},
        active_customers={"CUST_0001", "CUST_0002"},
    )


def test_source_schema_validation(sample_ref_store: ReferenceStore) -> None:
    """Test standard valid UPSERT row."""
    row = {
        "event_id": "EVT_VALID_01",
        "transaction_id": "TXN_VALID_01",
        "event_version": "1",
        "event_type": "UPSERT",
        "product_id": "PROD_001",
        "customer_id": "CUST_0001",
        "event_ts": "2026-10-01T12:00:00Z",
        "quantity": "3",
        "unit_price": "25.50",
    }
    event, quarantine = validate_row(
        row=row,
        row_num=2,
        source_file="test.csv",
        run_id="run-1",
        ref_store=sample_ref_store,
        arrival_time=datetime.now(timezone.utc),
    )
    assert quarantine is None
    assert event is not None
    assert event.event_id == "EVT_VALID_01"
    assert event.quantity == 3
    assert event.unit_price == Decimal("25.50")
    assert event.gross_amount == Decimal("76.50")
    assert event.metric_date == "2026-10-01"


def test_invalid_types(sample_ref_store: ReferenceStore) -> None:
    """Test 6: Rejecting rows with non-numeric event versions or non-integer quantities."""
    # Bad version
    row_bad_ver = {
        "event_id": "EVT_001",
        "transaction_id": "TXN_001",
        "event_version": "V_ONE",
        "event_type": "UPSERT",
        "product_id": "PROD_001",
        "customer_id": "CUST_0001",
        "event_ts": "2026-10-01T12:00:00Z",
        "quantity": "1",
        "unit_price": "10.00",
    }
    event, q = validate_row(
        row=row_bad_ver,
        row_num=2,
        source_file="test.csv",
        run_id="run-1",
        ref_store=sample_ref_store,
        arrival_time=datetime.now(timezone.utc),
    )
    assert event is None
    assert q is not None
    assert q.rejection_reason == "INVALID_EVENT_VERSION_NOT_INTEGER"

    # Bad quantity
    row_bad_qty = {
        "event_id": "EVT_002",
        "transaction_id": "TXN_002",
        "event_version": "1",
        "event_type": "UPSERT",
        "product_id": "PROD_001",
        "customer_id": "CUST_0001",
        "event_ts": "2026-10-01T12:00:00Z",
        "quantity": "FIVE",
        "unit_price": "10.00",
    }
    event, q = validate_row(
        row=row_bad_qty,
        row_num=3,
        source_file="test.csv",
        run_id="run-1",
        ref_store=sample_ref_store,
        arrival_time=datetime.now(timezone.utc),
    )
    assert event is None
    assert q is not None
    assert q.rejection_reason == "INVALID_QUANTITY_NOT_INTEGER"


def test_invalid_timestamps(sample_ref_store: ReferenceStore) -> None:
    """Test 7: Rejecting unparseable or malformed timestamp strings."""
    row = {
        "event_id": "EVT_003",
        "transaction_id": "TXN_003",
        "event_version": "1",
        "event_type": "UPSERT",
        "product_id": "PROD_001",
        "customer_id": "CUST_0001",
        "event_ts": "NOT_AN_ISO_TIMESTAMP",
        "quantity": "1",
        "unit_price": "10.00",
    }
    event, q = validate_row(
        row=row,
        row_num=4,
        source_file="test.csv",
        run_id="run-1",
        ref_store=sample_ref_store,
        arrival_time=datetime.now(timezone.utc),
    )
    assert event is None
    assert q is not None
    assert q.rejection_reason == "INVALID_EVENT_TIMESTAMP"


def test_negative_quantity(sample_ref_store: ReferenceStore) -> None:
    """Test rejection when quantity is zero or negative for an UPSERT."""
    row = {
        "event_id": "EVT_004",
        "transaction_id": "TXN_004",
        "event_version": "1",
        "event_type": "UPSERT",
        "product_id": "PROD_001",
        "customer_id": "CUST_0001",
        "event_ts": "2026-10-01T12:00:00Z",
        "quantity": "-4",
        "unit_price": "10.00",
    }
    event, q = validate_row(
        row=row,
        row_num=5,
        source_file="test.csv",
        run_id="run-1",
        ref_store=sample_ref_store,
        arrival_time=datetime.now(timezone.utc),
    )
    assert event is None
    assert q is not None
    assert q.rejection_reason == "NEGATIVE_OR_ZERO_QUANTITY"


def test_negative_price(sample_ref_store: ReferenceStore) -> None:
    """Test rejection when unit price is negative."""
    row = {
        "event_id": "EVT_005",
        "transaction_id": "TXN_005",
        "event_version": "1",
        "event_type": "UPSERT",
        "product_id": "PROD_001",
        "customer_id": "CUST_0001",
        "event_ts": "2026-10-01T12:00:00Z",
        "quantity": "2",
        "unit_price": "-15.99",
    }
    event, q = validate_row(
        row=row,
        row_num=6,
        source_file="test.csv",
        run_id="run-1",
        ref_store=sample_ref_store,
        arrival_time=datetime.now(timezone.utc),
    )
    assert event is None
    assert q is not None
    assert q.rejection_reason == "NEGATIVE_UNIT_PRICE"


def test_unknown_product_id_rejected(sample_ref_store: ReferenceStore) -> None:
    """Test 8: Rejecting transaction with unregistered product_id."""
    row = {
        "event_id": "EVT_006",
        "transaction_id": "TXN_006",
        "event_version": "1",
        "event_type": "UPSERT",
        "product_id": "PROD_DOES_NOT_EXIST_999",
        "customer_id": "CUST_0001",
        "event_ts": "2026-10-01T12:00:00Z",
        "quantity": "1",
        "unit_price": "10.00",
    }
    event, q = validate_row(
        row=row,
        row_num=7,
        source_file="test.csv",
        run_id="run-1",
        ref_store=sample_ref_store,
        arrival_time=datetime.now(timezone.utc),
    )
    assert event is None
    assert q is not None
    assert q.rejection_reason == "UNKNOWN_PRODUCT_ID"


def test_unknown_customer_id_rejected(sample_ref_store: ReferenceStore) -> None:
    """Test 9: Rejecting transaction with unregistered customer_id."""
    row = {
        "event_id": "EVT_007",
        "transaction_id": "TXN_007",
        "event_version": "1",
        "event_type": "UPSERT",
        "product_id": "PROD_001",
        "customer_id": "CUST_DOES_NOT_EXIST_999",
        "event_ts": "2026-10-01T12:00:00Z",
        "quantity": "1",
        "unit_price": "10.00",
    }
    event, q = validate_row(
        row=row,
        row_num=8,
        source_file="test.csv",
        run_id="run-1",
        ref_store=sample_ref_store,
        arrival_time=datetime.now(timezone.utc),
    )
    assert event is None
    assert q is not None
    assert q.rejection_reason == "UNKNOWN_CUSTOMER_ID"
