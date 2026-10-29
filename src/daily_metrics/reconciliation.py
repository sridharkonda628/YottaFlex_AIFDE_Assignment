"""
Independent reconciliation process comparing published metrics against authoritative ledger ground truth.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb

from daily_metrics.config import ReconciliationError

logger = logging.getLogger(__name__)


class Reconciler:
    """
    Executes independent validation comparing daily_product_metrics with
    authoritative ground truth directly derived from event_ledger.
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self.conn = conn

    def run_reconciliation(self) -> Dict[str, Any]:
        """
        Runs comprehensive suite of reconciliation checks.
        Returns a detailed report dictionary.
        """
        report: Dict[str, Any] = {
            "reconciliation_time": datetime.now(timezone.utc).isoformat(),
            "overall_status": "PASSED",
            "checks": [],
            "mismatch_count": 0,
        }

        # Check 1: Target Primary Key Uniqueness
        pk_check = self.conn.execute("""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(DISTINCT (metric_date || '::' || product_id)) as unique_keys
            FROM daily_product_metrics;
        """).fetchone()
        
        total_rows = pk_check[0] if pk_check else 0
        unique_keys = pk_check[1] if pk_check else 0
        pk_pass = (total_rows == unique_keys)

        report["checks"].append({
            "check_name": "target_pk_uniqueness",
            "expected": total_rows,
            "actual": unique_keys,
            "difference": total_rows - unique_keys,
            "status": "PASS" if pk_pass else "FAIL",
            "diagnostic": "Primary key (metric_date, product_id) must have zero duplicates in target table.",
        })
        if not pk_pass:
            report["mismatch_count"] += 1

        # Check 2: Global Grand Totals Comparison
        expected_totals = self.conn.execute("""
            SELECT
                CAST(COUNT(DISTINCT transaction_id) AS BIGINT) AS expected_txns,
                CAST(COALESCE(SUM(quantity), 0) AS BIGINT) AS expected_units,
                CAST(ROUND(COALESCE(SUM(gross_amount), 0.00), 2) AS DOUBLE) AS expected_revenue,
                CAST(COUNT(DISTINCT (metric_date || '::' || product_id)) AS BIGINT) AS expected_partitions
            FROM current_transaction_state
            WHERE is_active = TRUE AND product_id IS NOT NULL;
        """).fetchone()

        actual_totals = self.conn.execute("""
            SELECT
                CAST(COALESCE(SUM(transaction_count), 0) AS BIGINT) AS actual_txns,
                CAST(COALESCE(SUM(units_sold), 0) AS BIGINT) AS actual_units,
                CAST(ROUND(COALESCE(SUM(gross_revenue), 0.00), 2) AS DOUBLE) AS actual_revenue,
                CAST(COUNT(*) AS BIGINT) AS actual_partitions
            FROM daily_product_metrics;
        """).fetchone()

        exp_txns = expected_totals[0] if expected_totals else 0
        exp_units = expected_totals[1] if expected_totals else 0
        exp_rev = round(expected_totals[2], 2) if expected_totals else 0.0
        exp_parts = expected_totals[3] if expected_totals else 0

        act_txns = actual_totals[0] if actual_totals else 0
        act_units = actual_totals[1] if actual_totals else 0
        act_rev = round(actual_totals[2], 2) if actual_totals else 0.0
        act_parts = actual_totals[3] if actual_totals else 0

        # Units Sold Check
        units_diff = act_units - exp_units
        report["checks"].append({
            "check_name": "global_units_sold",
            "expected": exp_units,
            "actual": act_units,
            "difference": units_diff,
            "status": "PASS" if units_diff == 0 else "FAIL",
            "diagnostic": "Authoritative ledger units sold vs target published units sold.",
        })
        if units_diff != 0:
            report["mismatch_count"] += 1

        # Gross Revenue Check (Exact decimal)
        rev_diff = round(act_rev - exp_rev, 2)
        report["checks"].append({
            "check_name": "global_gross_revenue",
            "expected": exp_rev,
            "actual": act_rev,
            "difference": rev_diff,
            "status": "PASS" if abs(rev_diff) < 0.001 else "FAIL",
            "diagnostic": "Authoritative ledger revenue vs target published gross revenue.",
        })
        if abs(rev_diff) >= 0.001:
            report["mismatch_count"] += 1

        # Partition Count Check
        parts_diff = act_parts - exp_parts
        report["checks"].append({
            "check_name": "global_partition_count",
            "expected": exp_parts,
            "actual": act_parts,
            "difference": parts_diff,
            "status": "PASS" if parts_diff == 0 else "FAIL",
            "diagnostic": "Number of active (metric_date, product_id) partitions.",
        })
        if parts_diff != 0:
            report["mismatch_count"] += 1

        # Check 3: Detailed Per-Partition Deep Reconciliation
        partition_variances = self.conn.execute("""
            WITH expected_partitions AS (
                SELECT
                    metric_date,
                    product_id,
                    CAST(COALESCE(SUM(quantity), 0) AS BIGINT) AS exp_units,
                    CAST(COUNT(DISTINCT transaction_id) AS BIGINT) AS exp_txns,
                    CAST(ROUND(COALESCE(SUM(gross_amount), 0.00), 2) AS DOUBLE) AS exp_rev
                FROM current_transaction_state
                WHERE is_active = TRUE AND product_id IS NOT NULL
                GROUP BY metric_date, product_id
            ),
            actual_partitions AS (
                SELECT
                    metric_date,
                    product_id,
                    units_sold AS act_units,
                    transaction_count AS act_txns,
                    CAST(ROUND(gross_revenue, 2) AS DOUBLE) AS act_rev
                FROM daily_product_metrics
            )
            SELECT
                COALESCE(e.metric_date::VARCHAR, a.metric_date::VARCHAR) AS metric_date,
                COALESCE(e.product_id, a.product_id) AS product_id,
                COALESCE(e.exp_units, 0) AS exp_units,
                COALESCE(a.act_units, 0) AS act_units,
                COALESCE(e.exp_rev, 0.0) AS exp_rev,
                COALESCE(a.act_rev, 0.0) AS act_rev,
                COALESCE(e.exp_txns, 0) AS exp_txns,
                COALESCE(a.act_txns, 0) AS act_txns
            FROM expected_partitions e
            FULL OUTER JOIN actual_partitions a
                ON e.metric_date = a.metric_date
                AND e.product_id = a.product_id
            WHERE COALESCE(e.exp_units, 0) != COALESCE(a.act_units, 0)
               OR ABS(COALESCE(e.exp_rev, 0.0) - COALESCE(a.act_rev, 0.0)) > 0.001
               OR COALESCE(e.exp_txns, 0) != COALESCE(a.act_txns, 0);
        """).fetchall()

        if partition_variances:
            report["mismatch_count"] += len(partition_variances)
            variance_samples = [
                {
                    "metric_date": row[0],
                    "product_id": row[1],
                    "expected_units": row[2],
                    "actual_units": row[3],
                    "expected_revenue": row[4],
                    "actual_revenue": row[5],
                    "expected_txns": row[6],
                    "actual_txns": row[7],
                }
                for row in partition_variances[:10]  # top 10 discrepancies
            ]
            report["checks"].append({
                "check_name": "per_partition_reconciliation",
                "expected": "0 partition variances",
                "actual": f"{len(partition_variances)} partition variances detected",
                "difference": len(partition_variances),
                "status": "FAIL",
                "diagnostic": f"Sample variances: {variance_samples}",
            })
        else:
            report["checks"].append({
                "check_name": "per_partition_reconciliation",
                "expected": "0 partition variances",
                "actual": "0 partition variances",
                "difference": 0,
                "status": "PASS",
                "diagnostic": "Every individual (metric_date, product_id) matches exact expected aggregates.",
            })

        if report["mismatch_count"] > 0:
            report["overall_status"] = "FAILED"

        return report

    def verify_or_raise(self) -> Dict[str, Any]:
        """Runs reconciliation and raises ReconciliationError if any check fails."""
        report = self.run_reconciliation()
        if report["overall_status"] != "PASSED":
            err_msg = f"Independent reconciliation failed with {report['mismatch_count']} mismatch(es). Report: {json.dumps(report, indent=2)}"
            logger.error(err_msg)
            raise ReconciliationError(err_msg)
        logger.info("Independent reconciliation passed successfully.")
        return report


def export_reconciliation_report(report: Dict[str, Any], output_dir: Path) -> Path:
    """Exports reconciliation report as JSON for machine-readable audit inspection."""
    output_dir.mkdir(parents=True, exist_ok=True)
    report_file = output_dir / "reconciliation_report.json"
    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    logger.info("Exported reconciliation report to %s", report_file)
    return report_file
