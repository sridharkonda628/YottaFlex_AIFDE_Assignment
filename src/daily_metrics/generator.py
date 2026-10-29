"""
Deterministic synthetic dataset and reference data generator.
Generates comprehensive scenarios: normal upserts, updates, reversals,
reinstatements, exact duplicates, conflicting duplicates, out-of-order versions,
late arrivals, invalid references, and corrupt rows.
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import random
from typing import Any, Dict, List, Optional, Tuple
import uuid


SAMPLE_PRODUCTS = [
    ("PROD_001", "Ultra Wireless Headphones", "Electronics", "true"),
    ("PROD_002", "Ergonomic Mechanical Keyboard", "Electronics", "true"),
    ("PROD_003", "Organic Fair-Trade Coffee Beans", "Grocery", "true"),
    ("PROD_004", "Stainless Steel Water Bottle", "Home & Kitchen", "true"),
    ("PROD_005", "Noise-Cancelling Earbuds", "Electronics", "true"),
    ("PROD_006", "Smart Fitness Tracker Watch", "Electronics", "true"),
    ("PROD_007", "Merino Wool Winter Beanie", "Apparel", "true"),
    ("PROD_008", "Ceramic Pour-Over Dripper", "Home & Kitchen", "true"),
    ("PROD_009", "Recycled Canvas Tote Bag", "Apparel", "true"),
    ("PROD_010", "LED Desk Lamp with Wireless Charger", "Home & Kitchen", "true"),
]

SAMPLE_CUSTOMERS = [
    ("CUST_0001", "Alice Zhang", "alice.zhang@example.com", "Premium", "true"),
    ("CUST_0002", "Bob Smith", "bob.smith@example.com", "Standard", "true"),
    ("CUST_0003", "Carlos Mendes", "carlos.m@example.com", "Enterprise", "true"),
    ("CUST_0004", "Diana Prince", "diana.p@example.com", "Premium", "true"),
    ("CUST_0005", "Evan Wright", "evan.w@example.com", "Standard", "true"),
    ("CUST_0006", "Fatima Al-Mansoor", "fatima.m@example.com", "Enterprise", "true"),
    ("CUST_0007", "George Clark", "george.c@example.com", "Standard", "true"),
    ("CUST_0008", "Hannah Abbott", "hannah.a@example.com", "Premium", "true"),
]


def write_reference_data(reference_dir: Path) -> Tuple[Path, Path]:
    """Writes reference_products.csv and reference_customers.csv."""
    reference_dir.mkdir(parents=True, exist_ok=True)
    products_path = reference_dir / "reference_products.csv"
    customers_path = reference_dir / "reference_customers.csv"

    with open(products_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["product_id", "product_name", "category", "is_active"])
        for prod in SAMPLE_PRODUCTS:
            writer.writerow(prod)

    with open(customers_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["customer_id", "customer_name", "email", "tier", "is_active"])
        for cust in SAMPLE_CUSTOMERS:
            writer.writerow(cust)

    return products_path, customers_path


def generate_synthetic_transactions(
    count: int = 100,
    seed: int = 42,
    base_date: Optional[datetime] = None,
) -> List[Dict[str, str]]:
    """
    Generates a deterministic list of transaction event dictionaries.
    Includes normal transactions, updates, reversals, duplicates, and edge cases.
    """
    random.seed(seed)
    if base_date is None:
        base_date = datetime(2026, 10, 1, 10, 0, 0, tzinfo=timezone.utc)

    rows: List[Dict[str, str]] = []

    # Keep track of generated transactions to create realistic updates, reversals, and duplicates
    active_txns: List[Dict[str, Any]] = []

    # Probabilistic distribution of edge cases
    # 70% normal new transactions, 10% updates, 5% reversals, 5% exact duplicates,
    # 2% conflicting duplicates, 3% invalid reference, 5% malformed/corrupt
    
    target_count = count
    i = 0

    while len(rows) < target_count:
        dice = random.random()
        event_id = f"EVT_{seed}_{len(rows):07d}"
        ingested_at = (base_date + timedelta(seconds=len(rows))).isoformat()
        source_file = "transactions_synthetic.csv"

        if dice < 0.65 or not active_txns:
            # Normal new transaction (UPSERT, version 1)
            txn_id = f"TXN_{seed}_{len(active_txns):06d}"
            prod_id = random.choice(SAMPLE_PRODUCTS)[0]
            cust_id = random.choice(SAMPLE_CUSTOMERS)[0]
            day_offset = random.randint(0, 14)
            event_ts = base_date + timedelta(days=day_offset, minutes=random.randint(0, 1400))
            quantity = random.randint(1, 10)
            unit_price = f"{Decimal(random.randint(500, 25000)) / Decimal(100):.2f}"

            row = {
                "event_id": event_id,
                "transaction_id": txn_id,
                "event_version": "1",
                "event_type": "UPSERT",
                "product_id": prod_id,
                "customer_id": cust_id,
                "event_ts": event_ts.isoformat(),
                "quantity": str(quantity),
                "unit_price": unit_price,
                "ingested_at": ingested_at,
                "source_file": source_file,
            }
            rows.append(row)
            active_txns.append({
                "txn_id": txn_id,
                "version": 1,
                "product_id": prod_id,
                "customer_id": cust_id,
                "event_ts": event_ts,
                "quantity": quantity,
                "unit_price": unit_price,
                "is_active": True,
            })

        elif dice < 0.78:
            # Transaction Update (higher event_version)
            parent = random.choice(active_txns)
            parent["version"] += 1
            new_version = parent["version"]
            
            # Maybe change quantity, price, or product
            new_qty = random.randint(1, 15)
            new_prod = random.choice(SAMPLE_PRODUCTS)[0] if random.random() < 0.3 else parent["product_id"]
            new_price = f"{Decimal(random.randint(500, 25000)) / Decimal(100):.2f}"
            new_ts = parent["event_ts"] + timedelta(hours=random.randint(1, 24))

            row = {
                "event_id": event_id,
                "transaction_id": parent["txn_id"],
                "event_version": str(new_version),
                "event_type": "UPSERT",
                "product_id": new_prod,
                "customer_id": parent["customer_id"],
                "event_ts": new_ts.isoformat(),
                "quantity": str(new_qty),
                "unit_price": new_price,
                "ingested_at": ingested_at,
                "source_file": source_file,
            }
            rows.append(row)
            parent["product_id"] = new_prod
            parent["quantity"] = new_qty
            parent["unit_price"] = new_price
            parent["is_active"] = True

        elif dice < 0.85:
            # Transaction Reversal (REVERSAL event)
            parent = random.choice(active_txns)
            parent["version"] += 1
            rev_ts = parent["event_ts"] + timedelta(hours=random.randint(2, 48))

            row = {
                "event_id": event_id,
                "transaction_id": parent["txn_id"],
                "event_version": str(parent["version"]),
                "event_type": "REVERSAL",
                "product_id": "",
                "customer_id": "",
                "event_ts": rev_ts.isoformat(),
                "quantity": "",
                "unit_price": "",
                "ingested_at": ingested_at,
                "source_file": source_file,
            }
            rows.append(row)
            parent["is_active"] = False

        elif dice < 0.90:
            # Exact duplicate delivery of a previous row
            target_row = random.choice(rows)
            dup_row = dict(target_row)  # Exact same event_id and payload
            rows.append(dup_row)

        elif dice < 0.93:
            # Conflicting duplicate event_id (same event_id, different payload)
            target_row = random.choice(rows)
            conflict_row = dict(target_row)
            conflict_row["quantity"] = "999"  # Alter payload to cause conflict
            rows.append(conflict_row)

        elif dice < 0.96:
            # Invalid reference keys (unknown product or customer)
            txn_id = f"TXN_{seed}_BADREF_{len(rows):06d}"
            bad_prod = "PROD_UNKNOWN_999" if random.random() < 0.5 else random.choice(SAMPLE_PRODUCTS)[0]
            bad_cust = "CUST_UNKNOWN_999" if bad_prod != "PROD_UNKNOWN_999" else random.choice(SAMPLE_CUSTOMERS)[0]

            row = {
                "event_id": event_id,
                "transaction_id": txn_id,
                "event_version": "1",
                "event_type": "UPSERT",
                "product_id": bad_prod,
                "customer_id": bad_cust,
                "event_ts": base_date.isoformat(),
                "quantity": "5",
                "unit_price": "29.99",
                "ingested_at": ingested_at,
                "source_file": source_file,
            }
            rows.append(row)

        else:
            # Malformed row / invalid business constraints
            err_type = random.choice([
                "bad_qty", "bad_price", "bad_version", "bad_ts", "missing_txn", "bad_type"
            ])
            txn_id = f"TXN_{seed}_ERR_{len(rows):06d}"
            prod_id = random.choice(SAMPLE_PRODUCTS)[0]
            cust_id = random.choice(SAMPLE_CUSTOMERS)[0]

            row = {
                "event_id": event_id,
                "transaction_id": txn_id if err_type != "missing_txn" else "",
                "event_version": "1" if err_type != "bad_version" else "-1",
                "event_type": "UPSERT" if err_type != "bad_type" else "UNKNOWN_TYPE",
                "product_id": prod_id,
                "customer_id": cust_id,
                "event_ts": base_date.isoformat() if err_type != "bad_ts" else "NOT_A_TIMESTAMP",
                "quantity": "2" if err_type != "bad_qty" else "-10",
                "unit_price": "19.99" if err_type != "bad_price" else "-5.00",
                "ingested_at": ingested_at,
                "source_file": source_file,
            }
            rows.append(row)

    return rows[:target_count]


def write_dataset_to_csv(rows: List[Dict[str, str]], output_file: Path) -> Path:
    """Writes row dictionaries to CSV file."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        # Write empty file with header
        with open(output_file, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "event_id", "transaction_id", "event_version", "event_type",
                "product_id", "customer_id", "event_ts", "quantity",
                "unit_price", "ingested_at", "source_file"
            ])
        return output_file

    fieldnames = list(rows[0].keys())
    with open(output_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return output_file


def main() -> None:
    parser = argparse.ArgumentParser(description="Deterministic synthetic transaction event generator")
    parser.add_argument("--output-dir", type=str, default="data/sample", help="Output directory")
    parser.add_argument("--count", type=int, default=100, help="Number of records to generate")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for determinism")
    parser.add_argument("--filename", type=str, default="transactions_sample.csv", help="CSV filename")

    args = parser.parse_args()
    out_dir = Path(args.output_dir)
    write_reference_data(out_dir)

    rows = generate_synthetic_transactions(count=args.count, seed=args.seed)
    csv_path = out_dir / args.filename
    write_dataset_to_csv(rows, csv_path)
    print(f"Generated {len(rows)} synthetic transaction events at {csv_path}")


if __name__ == "__main__":
    main()
