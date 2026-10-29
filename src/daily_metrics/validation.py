"""
Business validation, referential integrity verification, and quarantine routing.
"""

from __future__ import annotations

import csv
import hashlib
import json
import logging
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
import uuid

from daily_metrics.config import (
    EventType,
    QuarantineRecord,
    RejectionCategory,
    TransactionEvent,
)

logger = logging.getLogger(__name__)


def compute_payload_hash(
    transaction_id: str,
    event_version: int,
    event_type: str,
    product_id: Optional[str],
    customer_id: Optional[str],
    event_ts_str: str,
    quantity: Optional[int],
    unit_price: Optional[Decimal],
) -> str:
    """
    Computes a deterministic SHA-256 fingerprint over normalized business payload attributes.
    Used for duplicate conflict detection and idempotency verification.
    """
    norm_product = (product_id or "").strip()
    norm_customer = (customer_id or "").strip()
    norm_qty = str(quantity) if quantity is not None else ""
    norm_price = f"{unit_price:.2f}" if unit_price is not None else ""
    
    # Canonical canonicalized string representation
    canonical_payload = "|".join([
        transaction_id.strip(),
        str(event_version),
        event_type.strip(),
        norm_product,
        norm_customer,
        event_ts_str.strip(),
        norm_qty,
        norm_price,
    ])
    return hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()


class ReferenceStore:
    """Maintains active reference entities for referential integrity enforcement."""

    def __init__(self, active_products: Set[str], active_customers: Set[str]) -> None:
        self.active_products = active_products
        self.active_customers = active_customers

    @classmethod
    def load_from_dir(cls, reference_dir: Path) -> ReferenceStore:
        """Loads reference products and customers from CSV files in reference_dir."""
        products_file = reference_dir / "reference_products.csv"
        customers_file = reference_dir / "reference_customers.csv"

        active_products: Set[str] = set()
        active_customers: Set[str] = set()

        if products_file.exists():
            with open(products_file, mode="r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    prod_id = row.get("product_id", "").strip()
                    is_active = row.get("is_active", "true").strip().lower() in ("true", "1", "t", "yes")
                    if prod_id and is_active:
                        active_products.add(prod_id)
        else:
            logger.warning("Products reference file not found at %s. Reference checks may fail.", products_file)

        if customers_file.exists():
            with open(customers_file, mode="r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    cust_id = row.get("customer_id", "").strip()
                    is_active = row.get("is_active", "true").strip().lower() in ("true", "1", "t", "yes")
                    if cust_id and is_active:
                        active_customers.add(cust_id)
        else:
            logger.warning("Customers reference file not found at %s. Reference checks may fail.", customers_file)

        return cls(active_products=active_products, active_customers=active_customers)


def parse_iso_timestamp(ts_raw: str) -> Optional[datetime]:
    """Parses ISO-8601 timestamp string into UTC datetime object."""
    if not ts_raw:
        return None
    try:
        # Handles standard ISO formats including 'Z' or offset
        norm_ts = ts_raw.strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(norm_ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None


def validate_row(
    row: Dict[str, str],
    row_num: int,
    source_file: str,
    run_id: str,
    ref_store: ReferenceStore,
    arrival_time: datetime,
) -> Tuple[Optional[TransactionEvent], Optional[QuarantineRecord]]:
    """
    Validates a single source row against business rules, data types, and reference keys.
    Returns (TransactionEvent, None) if valid, or (None, QuarantineRecord) if rejected.
    """
    raw_json = json.dumps(row, default=str)

    # 1. Mandatory Identifiers
    event_id = row.get("event_id", "").strip()
    if not event_id:
        return None, QuarantineRecord(
            quarantine_id=str(uuid.uuid4()),
            run_id=run_id,
            source_file=source_file,
            source_row_num=row_num,
            raw_record_json=raw_json,
            rejection_reason="MISSING_EVENT_ID",
            rejection_category=RejectionCategory.VALIDATION_RULE.value,
        )

    transaction_id = row.get("transaction_id", "").strip()
    if not transaction_id:
        return None, QuarantineRecord(
            quarantine_id=str(uuid.uuid4()),
            run_id=run_id,
            source_file=source_file,
            source_row_num=row_num,
            raw_record_json=raw_json,
            rejection_reason="MISSING_TRANSACTION_ID",
            rejection_category=RejectionCategory.VALIDATION_RULE.value,
        )

    # 2. Event Version (Must be strictly positive integer)
    raw_version = row.get("event_version", "").strip()
    try:
        event_version = int(raw_version)
        if event_version <= 0:
            return None, QuarantineRecord(
                quarantine_id=str(uuid.uuid4()),
                run_id=run_id,
                source_file=source_file,
                source_row_num=row_num,
                raw_record_json=raw_json,
                rejection_reason="INVALID_EVENT_VERSION_NON_POSITIVE",
                rejection_category=RejectionCategory.VALIDATION_RULE.value,
            )
    except (ValueError, TypeError):
        return None, QuarantineRecord(
            quarantine_id=str(uuid.uuid4()),
            run_id=run_id,
            source_file=source_file,
            source_row_num=row_num,
            raw_record_json=raw_json,
            rejection_reason="INVALID_EVENT_VERSION_NOT_INTEGER",
            rejection_category=RejectionCategory.VALIDATION_RULE.value,
        )

    # 3. Event Type
    event_type = row.get("event_type", "").strip().upper()
    if event_type not in (EventType.UPSERT.value, EventType.REVERSAL.value):
        return None, QuarantineRecord(
            quarantine_id=str(uuid.uuid4()),
            run_id=run_id,
            source_file=source_file,
            source_row_num=row_num,
            raw_record_json=raw_json,
            rejection_reason=f"INVALID_EVENT_TYPE_{event_type or 'EMPTY'}",
            rejection_category=RejectionCategory.VALIDATION_RULE.value,
        )

    # 4. Event Timestamp
    raw_ts = row.get("event_ts", "").strip()
    event_ts = parse_iso_timestamp(raw_ts)
    if event_ts is None:
        return None, QuarantineRecord(
            quarantine_id=str(uuid.uuid4()),
            run_id=run_id,
            source_file=source_file,
            source_row_num=row_num,
            raw_record_json=raw_json,
            rejection_reason="INVALID_EVENT_TIMESTAMP",
            rejection_category=RejectionCategory.VALIDATION_RULE.value,
        )

    metric_date = event_ts.date().isoformat()

    # 5. Type-Specific Validation (UPSERT vs REVERSAL)
    product_id: Optional[str] = row.get("product_id", "").strip() or None
    customer_id: Optional[str] = row.get("customer_id", "").strip() or None
    quantity: Optional[int] = None
    unit_price: Optional[Decimal] = None
    gross_amount: Optional[Decimal] = None

    if event_type == EventType.UPSERT.value:
        # UPSERT requires valid product_id, customer_id, quantity, unit_price
        if not product_id:
            return None, QuarantineRecord(
                quarantine_id=str(uuid.uuid4()),
                run_id=run_id,
                source_file=source_file,
                source_row_num=row_num,
                raw_record_json=raw_json,
                rejection_reason="MISSING_PRODUCT_ID_FOR_UPSERT",
                rejection_category=RejectionCategory.VALIDATION_RULE.value,
            )

        if ref_store.active_products and product_id not in ref_store.active_products:
            return None, QuarantineRecord(
                quarantine_id=str(uuid.uuid4()),
                run_id=run_id,
                source_file=source_file,
                source_row_num=row_num,
                raw_record_json=raw_json,
                rejection_reason="UNKNOWN_PRODUCT_ID",
                rejection_category=RejectionCategory.REFERENTIAL_INTEGRITY.value,
            )

        if not customer_id:
            return None, QuarantineRecord(
                quarantine_id=str(uuid.uuid4()),
                run_id=run_id,
                source_file=source_file,
                source_row_num=row_num,
                raw_record_json=raw_json,
                rejection_reason="MISSING_CUSTOMER_ID_FOR_UPSERT",
                rejection_category=RejectionCategory.VALIDATION_RULE.value,
            )

        if ref_store.active_customers and customer_id not in ref_store.active_customers:
            return None, QuarantineRecord(
                quarantine_id=str(uuid.uuid4()),
                run_id=run_id,
                source_file=source_file,
                source_row_num=row_num,
                raw_record_json=raw_json,
                rejection_reason="UNKNOWN_CUSTOMER_ID",
                rejection_category=RejectionCategory.REFERENTIAL_INTEGRITY.value,
            )

        # Quantity: strictly positive integer
        raw_qty = row.get("quantity", "").strip()
        try:
            quantity = int(raw_qty)
            if quantity <= 0:
                return None, QuarantineRecord(
                    quarantine_id=str(uuid.uuid4()),
                    run_id=run_id,
                    source_file=source_file,
                    source_row_num=row_num,
                    raw_record_json=raw_json,
                    rejection_reason="NEGATIVE_OR_ZERO_QUANTITY",
                    rejection_category=RejectionCategory.VALIDATION_RULE.value,
                )
        except (ValueError, TypeError):
            return None, QuarantineRecord(
                quarantine_id=str(uuid.uuid4()),
                run_id=run_id,
                source_file=source_file,
                source_row_num=row_num,
                raw_record_json=raw_json,
                rejection_reason="INVALID_QUANTITY_NOT_INTEGER",
                rejection_category=RejectionCategory.VALIDATION_RULE.value,
            )

        # Unit Price: non-negative decimal
        raw_price = row.get("unit_price", "").strip()
        try:
            unit_price = Decimal(raw_price).quantize(Decimal("0.01"))
            if unit_price < Decimal("0.00"):
                return None, QuarantineRecord(
                    quarantine_id=str(uuid.uuid4()),
                    run_id=run_id,
                    source_file=source_file,
                    source_row_num=row_num,
                    raw_record_json=raw_json,
                    rejection_reason="NEGATIVE_UNIT_PRICE",
                    rejection_category=RejectionCategory.VALIDATION_RULE.value,
                )
        except (InvalidOperation, TypeError):
            return None, QuarantineRecord(
                quarantine_id=str(uuid.uuid4()),
                run_id=run_id,
                source_file=source_file,
                source_row_num=row_num,
                raw_record_json=raw_json,
                rejection_reason="INVALID_UNIT_PRICE_DECIMAL",
                rejection_category=RejectionCategory.VALIDATION_RULE.value,
            )

        gross_amount = (Decimal(quantity) * unit_price).quantize(Decimal("0.01"))

    else:
        # REVERSAL: product, customer, quantity, unit_price are optional
        # If product_id or customer_id are provided, we don't strictly require reference match for a cancellation,
        # but we preserve what was given for audit
        pass

    # Lineage metadata
    raw_ingested_at = row.get("ingested_at", "").strip()
    parsed_ingested_at = parse_iso_timestamp(raw_ingested_at) or arrival_time
    effective_source_file = row.get("source_file", "").strip() or source_file

    payload_hash = compute_payload_hash(
        transaction_id=transaction_id,
        event_version=event_version,
        event_type=event_type,
        product_id=product_id,
        customer_id=customer_id,
        event_ts_str=event_ts.isoformat(),
        quantity=quantity,
        unit_price=unit_price,
    )

    valid_event = TransactionEvent(
        event_id=event_id,
        transaction_id=transaction_id,
        event_version=event_version,
        event_type=event_type,
        product_id=product_id,
        customer_id=customer_id,
        event_ts=event_ts,
        metric_date=metric_date,
        quantity=quantity,
        unit_price=unit_price,
        gross_amount=gross_amount,
        payload_hash=payload_hash,
        ingested_at=parsed_ingested_at,
        source_file=effective_source_file,
        run_id=run_id,
    )

    return valid_event, None
