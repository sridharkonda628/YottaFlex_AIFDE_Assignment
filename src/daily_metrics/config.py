"""
Configuration, schemas, data models, and exception classes for the Daily Metrics Pipeline.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
import uuid


class EventType(str, Enum):
    UPSERT = "UPSERT"
    REVERSAL = "REVERSAL"


class RunStatus(str, Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    RECONCILIATION_FAILED = "RECONCILIATION_FAILED"


class RejectionCategory(str, Enum):
    FATAL_SCHEMA = "FATAL_SCHEMA"
    VALIDATION_RULE = "VALIDATION_RULE"
    CONFLICT_DUPLICATE = "CONFLICT_DUPLICATE"
    REFERENTIAL_INTEGRITY = "REFERENTIAL_INTEGRITY"


# Authoritative Source Schema
# These columns are the canonical contract for source CSV files.
CANONICAL_SOURCE_COLUMNS: List[str] = [
    "event_id",
    "transaction_id",
    "event_version",
    "event_type",
    "product_id",
    "customer_id",
    "event_ts",
    "quantity",
    "unit_price",
]

# Optional metadata columns allowed in source CSVs without failing schema check
OPTIONAL_METADATA_COLUMNS: List[str] = [
    "ingested_at",
    "source_file",
]

ALL_ALLOWED_COLUMNS: Set[str] = set(CANONICAL_SOURCE_COLUMNS).union(OPTIONAL_METADATA_COLUMNS)


# Exceptions
class PipelineError(Exception):
    """Base exception for pipeline errors."""
    pass


class FatalSchemaError(PipelineError):
    """Raised when source file violates schema contract (missing/unexpected columns)."""
    pass


class DatabaseTransactionError(PipelineError):
    """Raised when database commit or rollback fails."""
    pass


class ReconciliationError(PipelineError):
    """Raised when published metrics do not match ledger ground truth."""
    pass


@dataclass
class TransactionEvent:
    """Represents a validated transaction event."""
    event_id: str
    transaction_id: str
    event_version: int
    event_type: str
    product_id: Optional[str]
    customer_id: Optional[str]
    event_ts: datetime
    metric_date: str  # YYYY-MM-DD
    quantity: Optional[int]
    unit_price: Optional[Decimal]
    gross_amount: Optional[Decimal]
    payload_hash: str
    ingested_at: datetime
    source_file: str
    run_id: str

    def to_tuple(self) -> tuple:
        return (
            self.event_id,
            self.transaction_id,
            self.event_version,
            self.event_type,
            self.product_id,
            self.customer_id,
            self.event_ts.isoformat(),
            self.metric_date,
            self.quantity,
            float(self.unit_price) if self.unit_price is not None else None,
            float(self.gross_amount) if self.gross_amount is not None else None,
            self.payload_hash,
            self.ingested_at.isoformat(),
            self.source_file,
            self.run_id,
        )


@dataclass
class QuarantineRecord:
    """Represents a rejected or quarantined record."""
    quarantine_id: str
    run_id: str
    source_file: str
    source_row_num: Optional[int]
    raw_record_json: str
    rejection_reason: str
    rejection_category: str
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_tuple(self) -> tuple:
        return (
            self.quarantine_id,
            self.run_id,
            self.source_file,
            self.source_row_num,
            self.raw_record_json,
            self.rejection_reason,
            self.rejection_category,
            self.created_at.isoformat(),
        )


@dataclass
class RunAuditRecord:
    """Represents operational metrics for a single pipeline execution."""
    run_id: str
    source_file: str
    start_time: datetime
    end_time: Optional[datetime] = None
    status: str = RunStatus.SUCCESS.value
    input_row_count: int = 0
    validated_row_count: int = 0
    new_event_count: int = 0
    exact_duplicate_count: int = 0
    conflicting_duplicate_count: int = 0
    rejected_row_count: int = 0
    target_row_count: int = 0
    duration_seconds: Optional[float] = None
    failure_reason: Optional[str] = None
    reconciliation_status: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "source_file": self.source_file,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "status": self.status,
            "input_row_count": self.input_row_count,
            "validated_row_count": self.validated_row_count,
            "new_event_count": self.new_event_count,
            "exact_duplicate_count": self.exact_duplicate_count,
            "conflicting_duplicate_count": self.conflicting_duplicate_count,
            "rejected_row_count": self.rejected_row_count,
            "target_row_count": self.target_row_count,
            "duration_seconds": self.duration_seconds,
            "failure_reason": self.failure_reason,
            "reconciliation_status": self.reconciliation_status,
        }

    def to_tuple(self) -> tuple:
        return (
            self.run_id,
            self.source_file,
            self.start_time.isoformat(),
            self.end_time.isoformat() if self.end_time else None,
            self.status,
            self.input_row_count,
            self.validated_row_count,
            self.new_event_count,
            self.exact_duplicate_count,
            self.conflicting_duplicate_count,
            self.rejected_row_count,
            self.target_row_count,
            self.duration_seconds,
            self.failure_reason,
            self.reconciliation_status,
        )


@dataclass
class PipelineConfig:
    """Pipeline configuration parameters."""
    db_path: Path
    input_file: Path
    reference_dir: Path
    output_dir: Path
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timezone_str: str = "UTC"
    auto_reconcile: bool = True

    @classmethod
    def from_paths(
        cls,
        db_path: str | Path,
        input_file: str | Path,
        reference_dir: str | Path,
        output_dir: str | Path,
        run_id: Optional[str] = None,
        auto_reconcile: bool = True,
    ) -> PipelineConfig:
        return cls(
            db_path=Path(db_path).resolve(),
            input_file=Path(input_file).resolve(),
            reference_dir=Path(reference_dir).resolve(),
            output_dir=Path(output_dir).resolve(),
            run_id=run_id or str(uuid.uuid4()),
            auto_reconcile=auto_reconcile,
        )
