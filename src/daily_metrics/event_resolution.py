"""
Event deduplication, conflict resolution, and version precedence state resolution.
"""

from __future__ import annotations

import json
import logging
from typing import Dict, List, Set, Tuple
import uuid

import duckdb

from daily_metrics.config import (
    QuarantineRecord,
    RejectionCategory,
    TransactionEvent,
)

logger = logging.getLogger(__name__)


def resolve_batch_duplicates_and_conflicts(
    events: List[TransactionEvent],
    conn: duckdb.DuckDBPyConnection,
    run_id: str,
    source_file: str,
) -> Tuple[List[TransactionEvent], List[TransactionEvent], List[QuarantineRecord]]:
    """
    Evaluates incoming validated events against existing database ledger and within-batch duplicates.
    
    Returns:
        (new_valid_events, exact_duplicate_events, conflict_quarantine_records)
    """
    if not events:
        return [], [], []

    # 1. Fetch existing ledger records for matching event_ids or (transaction_id, event_version)
    event_ids = [e.event_id for e in events]
    txn_keys = [(e.transaction_id, e.event_version) for e in events]

    existing_events_by_id: Dict[str, Tuple[str, str, int]] = {}  # event_id -> (payload_hash, txn_id, version)
    existing_versions: Dict[Tuple[str, int], Tuple[str, str]] = {}  # (txn_id, version) -> (event_id, payload_hash)

    # Check if event_ledger table exists
    table_check = conn.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = 'event_ledger'"
    ).fetchone()[0]

    if table_check > 0 and event_ids:
        # Query existing event_ids
        # Chunking if large list
        chunk_size = 5000
        for i in range(0, len(event_ids), chunk_size):
            chunk = event_ids[i:i + chunk_size]
            placeholders = ",".join(["?"] * len(chunk))
            rows = conn.execute(
                f"SELECT event_id, payload_hash, transaction_id, event_version FROM event_ledger WHERE event_id IN ({placeholders})",
                chunk,
            ).fetchall()
            for eid, phash, tid, ever in rows:
                existing_events_by_id[eid] = (phash, tid, ever)

        # Query existing (transaction_id, event_version)
        unique_txns = list(set([t[0] for t in txn_keys]))
        for i in range(0, len(unique_txns), chunk_size):
            chunk_txns = unique_txns[i:i + chunk_size]
            placeholders = ",".join(["?"] * len(chunk_txns))
            rows = conn.execute(
                f"SELECT transaction_id, event_version, event_id, payload_hash FROM event_ledger WHERE transaction_id IN ({placeholders})",
                chunk_txns,
            ).fetchall()
            for tid, ever, eid, phash in rows:
                existing_versions[(tid, ever)] = (eid, phash)

    new_valid_events: List[TransactionEvent] = []
    exact_duplicates: List[TransactionEvent] = []
    quarantined_conflicts: List[QuarantineRecord] = []

    # Tracking sets within this current batch
    seen_batch_event_ids: Dict[str, str] = {}  # event_id -> payload_hash
    seen_batch_versions: Dict[Tuple[str, int], Tuple[str, str]] = {}  # (txn_id, version) -> (event_id, payload_hash)

    for event in events:
        # Check against existing ledger by event_id
        if event.event_id in existing_events_by_id:
            existing_hash, ex_tid, ex_ver = existing_events_by_id[event.event_id]
            if existing_hash == event.payload_hash:
                # Exact duplicate already in ledger
                exact_duplicates.append(event)
                continue
            else:
                # Conflicting duplicate: same event_id, different payload
                quarantined_conflicts.append(
                    QuarantineRecord(
                        quarantine_id=str(uuid.uuid4()),
                        run_id=run_id,
                        source_file=source_file,
                        source_row_num=None,
                        raw_record_json=json.dumps({
                            "event_id": event.event_id,
                            "transaction_id": event.transaction_id,
                            "event_version": event.event_version,
                            "payload_hash": event.payload_hash,
                            "existing_hash": existing_hash,
                        }),
                        rejection_reason="CONFLICTING_EVENT_PAYLOAD",
                        rejection_category=RejectionCategory.CONFLICT_DUPLICATE.value,
                    )
                )
                continue

        # Check against existing ledger by (transaction_id, event_version)
        txn_ver_key = (event.transaction_id, event.event_version)
        if txn_ver_key in existing_versions:
            ex_eid, ex_hash = existing_versions[txn_ver_key]
            if ex_eid != event.event_id or ex_hash != event.payload_hash:
                quarantined_conflicts.append(
                    QuarantineRecord(
                        quarantine_id=str(uuid.uuid4()),
                        run_id=run_id,
                        source_file=source_file,
                        source_row_num=None,
                        raw_record_json=json.dumps({
                            "event_id": event.event_id,
                            "transaction_id": event.transaction_id,
                            "event_version": event.event_version,
                            "existing_event_id": ex_eid,
                        }),
                        rejection_reason="CONFLICTING_TRANSACTION_VERSION",
                        rejection_category=RejectionCategory.CONFLICT_DUPLICATE.value,
                    )
                )
                continue

        # Check against within-batch duplicates
        if event.event_id in seen_batch_event_ids:
            batch_hash = seen_batch_event_ids[event.event_id]
            if batch_hash == event.payload_hash:
                exact_duplicates.append(event)
                continue
            else:
                quarantined_conflicts.append(
                    QuarantineRecord(
                        quarantine_id=str(uuid.uuid4()),
                        run_id=run_id,
                        source_file=source_file,
                        source_row_num=None,
                        raw_record_json=json.dumps({
                            "event_id": event.event_id,
                            "transaction_id": event.transaction_id,
                            "event_version": event.event_version,
                            "reason": "Intra-batch conflicting event_id",
                        }),
                        rejection_reason="CONFLICTING_EVENT_PAYLOAD",
                        rejection_category=RejectionCategory.CONFLICT_DUPLICATE.value,
                    )
                )
                continue

        if txn_ver_key in seen_batch_versions:
            b_eid, b_hash = seen_batch_versions[txn_ver_key]
            if b_eid != event.event_id or b_hash != event.payload_hash:
                quarantined_conflicts.append(
                    QuarantineRecord(
                        quarantine_id=str(uuid.uuid4()),
                        run_id=run_id,
                        source_file=source_file,
                        source_row_num=None,
                        raw_record_json=json.dumps({
                            "event_id": event.event_id,
                            "transaction_id": event.transaction_id,
                            "event_version": event.event_version,
                            "conflicting_batch_event_id": b_eid,
                        }),
                        rejection_reason="CONFLICTING_TRANSACTION_VERSION",
                        rejection_category=RejectionCategory.CONFLICT_DUPLICATE.value,
                    )
                )
                continue

        # Valid new event
        seen_batch_event_ids[event.event_id] = event.payload_hash
        seen_batch_versions[txn_ver_key] = (event.event_id, event.payload_hash)
        new_valid_events.append(event)

    return new_valid_events, exact_duplicates, quarantined_conflicts
