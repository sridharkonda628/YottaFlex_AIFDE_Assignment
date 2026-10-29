"""
Tests for event deduplication, conflict resolution, version precedence, and out-of-order handling.
Covers:
- Exact duplicate event delivery
- Conflicting duplicate event ID
- Conflicting transaction versions
- Late-arriving older versions
- Out-of-order arrival
- Transaction reinstatement
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import List
import duckdb
import pytest

from daily_metrics.config import TransactionEvent
from daily_metrics.event_resolution import resolve_batch_duplicates_and_conflicts
from daily_metrics.publishing import publish_batch_atomically
from daily_metrics.validation import compute_payload_hash


def make_event(
    event_id: str,
    transaction_id: str,
    event_version: int,
    event_type: str = "UPSERT",
    product_id: str = "PROD_001",
    customer_id: str = "CUST_0001",
    event_ts: str = "2026-10-01T10:00:00Z",
    quantity: int = 1,
    unit_price: Decimal = Decimal("50.00"),
    run_id: str = "run-test",
) -> TransactionEvent:
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


def test_exact_duplicate_event_delivery(mem_db: duckdb.DuckDBPyConnection) -> None:
    """Test 10: Ingesting an identical duplicate event delivery."""
    evt1 = make_event("EVT_DUP_1", "TXN_DUP_1", 1)
    # First ingestion
    new_evts, ex_dups, confs = resolve_batch_duplicates_and_conflicts([evt1], mem_db, "run-1", "test.csv")
    assert len(new_evts) == 1
    publish_batch_atomically(mem_db, new_evts, confs, "run-1")

    # Second delivery: identical event_id and payload
    evt2 = make_event("EVT_DUP_1", "TXN_DUP_1", 1)
    new_evts2, ex_dups2, confs2 = resolve_batch_duplicates_and_conflicts([evt2], mem_db, "run-2", "test.csv")
    assert len(new_evts2) == 0
    assert len(ex_dups2) == 1
    assert len(confs2) == 0


def test_conflicting_duplicate_event_id(mem_db: duckdb.DuckDBPyConnection) -> None:
    """Test 11: Same event ID arrives with a conflicting payload."""
    evt1 = make_event("EVT_CONF_1", "TXN_CONF_1", 1, quantity=2, unit_price=Decimal("10.00"))
    publish_batch_atomically(mem_db, [evt1], [], "run-1")

    # Conflicting delivery with same event_id but altered quantity
    evt2 = make_event("EVT_CONF_1", "TXN_CONF_1", 1, quantity=99, unit_price=Decimal("10.00"))
    new_evts, ex_dups, confs = resolve_batch_duplicates_and_conflicts([evt2], mem_db, "run-2", "test.csv")
    assert len(new_evts) == 0
    assert len(confs) == 1
    assert confs[0].rejection_reason == "CONFLICTING_EVENT_PAYLOAD"


def test_conflicting_transaction_version(mem_db: duckdb.DuckDBPyConnection) -> None:
    """Test 12: Distinct events have the same (transaction_id, event_version) but different payload."""
    evt1 = make_event("EVT_VER_A", "TXN_SAME_VER", 1, quantity=1)
    publish_batch_atomically(mem_db, [evt1], [], "run-1")

    # Different event_id attempting to overwrite version 1 with different data
    evt2 = make_event("EVT_VER_B", "TXN_SAME_VER", 1, quantity=5)
    new_evts, ex_dups, confs = resolve_batch_duplicates_and_conflicts([evt2], mem_db, "run-2", "test.csv")
    assert len(new_evts) == 0
    assert len(confs) == 1
    assert confs[0].rejection_reason == "CONFLICTING_TRANSACTION_VERSION"


def test_late_arriving_older_version(mem_db: duckdb.DuckDBPyConnection) -> None:
    """Test 15: Version 1 arrives late after Version 2 has already won."""
    # Version 2 is ingested first
    evt_v2 = make_event("EVT_V2", "TXN_LATE_1", 2, quantity=10, unit_price=Decimal("20.00"))
    publish_batch_atomically(mem_db, [evt_v2], [], "run-1")

    # Check winning state
    state1 = mem_db.execute(
        "SELECT event_version, quantity FROM current_transaction_state WHERE transaction_id = 'TXN_LATE_1'"
    ).fetchone()
    assert state1[0] == 2
    assert state1[1] == 10

    # Now Version 1 arrives late
    evt_v1 = make_event("EVT_V1", "TXN_LATE_1", 1, quantity=2, unit_price=Decimal("20.00"))
    new_evts, _, _ = resolve_batch_duplicates_and_conflicts([evt_v1], mem_db, "run-2", "test.csv")
    publish_batch_atomically(mem_db, new_evts, [], "run-2")

    # Winning state must remain Version 2!
    state2 = mem_db.execute(
        "SELECT event_version, quantity FROM current_transaction_state WHERE transaction_id = 'TXN_LATE_1'"
    ).fetchone()
    assert state2[0] == 2
    assert state2[1] == 10

    # But event ledger must contain both records for complete historical lineage
    ledger_count = mem_db.execute(
        "SELECT count(*) FROM event_ledger WHERE transaction_id = 'TXN_LATE_1'"
    ).fetchone()[0]
    assert ledger_count == 2


def test_out_of_order_event_arrival(mem_db: duckdb.DuckDBPyConnection) -> None:
    """Test 16: Out-of-order events within the same batch or across batches."""
    evt_v2 = make_event("EVT_OUT_V2", "TXN_OUT_1", 2, quantity=5)
    evt_v1 = make_event("EVT_OUT_V1", "TXN_OUT_1", 1, quantity=1)

    # Ingest v2 first, then v1
    publish_batch_atomically(mem_db, [evt_v2], [], "run-1")
    publish_batch_atomically(mem_db, [evt_v1], [], "run-2")

    winning = mem_db.execute(
        "SELECT event_version, quantity FROM current_transaction_state WHERE transaction_id = 'TXN_OUT_1'"
    ).fetchone()
    assert winning[0] == 2
    assert winning[1] == 5


def test_transaction_reinstatement(mem_db: duckdb.DuckDBPyConnection) -> None:
    """Test transaction reinstatement: UPSERT(v1) -> REVERSAL(v2) -> UPSERT(v3)."""
    v1 = make_event("EVT_REIN_1", "TXN_REIN", 1, event_type="UPSERT", quantity=3)
    publish_batch_atomically(mem_db, [v1], [], "run-1")

    # Reverse transaction
    v2 = make_event("EVT_REIN_2", "TXN_REIN", 2, event_type="REVERSAL")
    publish_batch_atomically(mem_db, [v2], [], "run-2")

    state_rev = mem_db.execute(
        "SELECT is_active FROM current_transaction_state WHERE transaction_id = 'TXN_REIN'"
    ).fetchone()
    assert state_rev[0] is False

    # Reinstate with Version 3
    v3 = make_event("EVT_REIN_3", "TXN_REIN", 3, event_type="UPSERT", quantity=4)
    publish_batch_atomically(mem_db, [v3], [], "run-3")

    state_rein = mem_db.execute(
        "SELECT is_active, event_version, quantity FROM current_transaction_state WHERE transaction_id = 'TXN_REIN'"
    ).fetchone()
    assert state_rein[0] is True
    assert state_rein[1] == 3
    assert state_rein[2] == 4
