"""
Operational auditing, metrics recording, and structured diagnostic reporting.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb

from daily_metrics.config import QuarantineRecord, RunAuditRecord, RunStatus

logger = logging.getLogger(__name__)


def record_run_audit(
    conn: duckdb.DuckDBPyConnection,
    audit_record: RunAuditRecord,
) -> None:
    """Persists a run audit record into DuckDB run_audit table."""
    conn.execute("""
        INSERT OR REPLACE INTO run_audit (
            run_id, source_file, start_time, end_time, status,
            input_row_count, validated_row_count, new_event_count,
            exact_duplicate_count, conflicting_duplicate_count,
            rejected_row_count, target_row_count, duration_seconds,
            failure_reason, reconciliation_status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, audit_record.to_tuple())
    logger.info("Recorded run audit for run_id=%s with status=%s", audit_record.run_id, audit_record.status)


def build_rejection_summary(quarantined_records: List[QuarantineRecord]) -> Dict[str, int]:
    """Summarizes quarantine records by rejection reason."""
    summary: Dict[str, int] = {}
    for r in quarantined_records:
        summary[r.rejection_reason] = summary.get(r.rejection_reason, 0) + 1
    return summary


def export_run_summary(
    audit_record: RunAuditRecord,
    quarantined_records: List[QuarantineRecord],
    reconciliation_report: Optional[Dict[str, Any]],
    output_dir: Path,
) -> Path:
    """Exports structured run summary JSON to output directory."""
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = output_dir / "run_summary.json"

    summary_data = audit_record.to_dict()
    summary_data["rejection_breakdown"] = build_rejection_summary(quarantined_records)
    summary_data["reconciliation_report"] = reconciliation_report

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary_data, f, indent=2)

    logger.info("Exported run summary to %s", summary_path)
    return summary_path
