"""
Command-line interface and orchestrator for the Daily Metrics Pipeline.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
import uuid

import duckdb

from daily_metrics.audit import export_run_summary, record_run_audit
from daily_metrics.config import (
    FatalSchemaError,
    PipelineConfig,
    QuarantineRecord,
    ReconciliationError,
    RejectionCategory,
    RunAuditRecord,
    RunStatus,
)
from daily_metrics.event_resolution import resolve_batch_duplicates_and_conflicts
from daily_metrics.generator import generate_synthetic_transactions, write_dataset_to_csv, write_reference_data
from daily_metrics.ingestion import read_source_csv
from daily_metrics.publishing import (
    export_published_metrics,
    export_quarantine_records,
    init_database_schema,
    populate_reference_tables,
    publish_batch_atomically,
)
from daily_metrics.reconciliation import Reconciler, export_reconciliation_report
from daily_metrics.validation import ReferenceStore, validate_row

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("daily_metrics")


def run_pipeline(config: PipelineConfig) -> RunAuditRecord:
    """
    Executes the end-to-end data pipeline according to configuration.
    Returns the final RunAuditRecord.
    """
    start_wall = datetime.now(timezone.utc)
    start_mono = time.monotonic()
    run_id = config.run_id
    source_file_name = config.input_file.name

    logger.info("Starting pipeline run_id=%s for file=%s", run_id, config.input_file)

    audit = RunAuditRecord(
        run_id=run_id,
        source_file=source_file_name,
        start_time=start_wall,
        status=RunStatus.SUCCESS.value,
    )

    all_quarantined: list[QuarantineRecord] = []
    recon_report = None

    # Connect to DuckDB
    config.db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(config.db_path))

    try:
        # Step 1: Initialize schema and reference tables
        init_database_schema(conn)
        if config.reference_dir.exists():
            populate_reference_tables(conn, config.reference_dir)
        ref_store = ReferenceStore.load_from_dir(config.reference_dir)

        # Step 2: Ingestion & Schema Enforcement
        try:
            header, row_iter = read_source_csv(config.input_file)
        except FatalSchemaError as fse:
            # Fatal schema errors fail closed for the entire file
            audit.status = RunStatus.FAILED.value
            audit.failure_reason = str(fse)
            err_json = f'{{"error": "{str(fse)}"}}'
            all_quarantined.append(
                QuarantineRecord(
                    quarantine_id=str(uuid.uuid4()),
                    run_id=run_id,
                    source_file=source_file_name,
                    source_row_num=1,
                    raw_record_json=err_json,
                    rejection_reason="FATAL_SCHEMA_MISMATCH",
                    rejection_category=RejectionCategory.FATAL_SCHEMA.value,
                )
            )
            # Write quarantine and audit directly
            export_quarantine_records(conn, config.output_dir, run_id)
            export_run_summary(audit, all_quarantined, None, config.output_dir)
            record_run_audit(conn, audit)
            logger.error("Pipeline run failed on schema validation: %s", fse)
            return audit

        # If file is empty, finish gracefully
        if not header:
            audit.end_time = datetime.now(timezone.utc)
            audit.duration_seconds = round(time.monotonic() - start_mono, 4)
            record_run_audit(conn, audit)
            export_run_summary(audit, all_quarantined, None, config.output_dir)
            logger.info("Empty input file processed. Zero records to ingest.")
            return audit

        # Step 3: Row-by-Row Business and Reference Validation
        valid_events = []
        raw_row_count = 0
        arrival_time = datetime.now(timezone.utc)

        for line_num, row_dict in row_iter:
            raw_row_count += 1
            event, quarantine = validate_row(
                row=row_dict,
                row_num=line_num,
                source_file=source_file_name,
                run_id=run_id,
                ref_store=ref_store,
                arrival_time=arrival_time,
            )
            if event:
                valid_events.append(event)
            elif quarantine:
                all_quarantined.append(quarantine)

        audit.input_row_count = raw_row_count
        audit.validated_row_count = len(valid_events)

        # Step 4: Deduplication and Conflict Resolution
        (
            new_events,
            exact_duplicates,
            conflict_quarantines,
        ) = resolve_batch_duplicates_and_conflicts(
            events=valid_events,
            conn=conn,
            run_id=run_id,
            source_file=source_file_name,
        )

        all_quarantined.extend(conflict_quarantines)
        audit.new_event_count = len(new_events)
        audit.exact_duplicate_count = len(exact_duplicates)
        audit.conflicting_duplicate_count = len(conflict_quarantines)
        audit.rejected_row_count = len(all_quarantined)

        # Step 5: Atomic Transactional Publishing
        target_rows_written = publish_batch_atomically(
            conn=conn,
            new_events=new_events,
            quarantined_records=all_quarantined,
            run_id=run_id,
        )
        audit.target_row_count = target_rows_written

        # Step 6: Independent Reconciliation
        if config.auto_reconcile:
            reconciler = Reconciler(conn)
            recon_report = reconciler.run_reconciliation()
            audit.reconciliation_status = recon_report["overall_status"]
            export_reconciliation_report(recon_report, config.output_dir)

            if recon_report["overall_status"] != "PASSED":
                audit.status = RunStatus.RECONCILIATION_FAILED.value
                audit.failure_reason = f"Reconciliation detected {recon_report['mismatch_count']} variance(s)."
                logger.error("Pipeline run failed reconciliation check.")

        # Step 7: Export Outputs
        export_published_metrics(conn, config.output_dir, run_id)
        if all_quarantined:
            export_quarantine_records(conn, config.output_dir, run_id)

    except Exception as e:
        audit.status = RunStatus.FAILED.value
        audit.failure_reason = str(e)
        logger.exception("Pipeline run encountered unhandled failure: %s", e)

    finally:
        audit.end_time = datetime.now(timezone.utc)
        audit.duration_seconds = round(time.monotonic() - start_mono, 4)
        try:
            record_run_audit(conn, audit)
            export_run_summary(audit, all_quarantined, recon_report, config.output_dir)
        except Exception as audit_err:
            logger.error("Failed to persist final run audit: %s", audit_err)
        conn.close()

    logger.info(
        "Run %s completed in %.4fs with status %s. Input: %d, Valid: %d, New: %d, Dup: %d, Rej: %d, Targets: %d",
        audit.run_id,
        audit.duration_seconds or 0.0,
        audit.status,
        audit.input_row_count,
        audit.validated_row_count,
        audit.new_event_count,
        audit.exact_duplicate_count,
        audit.rejected_row_count,
        audit.target_row_count,
    )
    return audit


def main() -> None:
    parser = argparse.ArgumentParser(description="Daily Metrics Incremental Processing Pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Subcommand: run
    run_parser = subparsers.add_parser("run", help="Run the pipeline on an input transaction CSV")
    run_parser.add_argument("--input-file", type=str, required=True, help="Path to input CSV")
    run_parser.add_argument("--db-path", type=str, default="data/metrics.duckdb", help="DuckDB database path")
    run_parser.add_argument("--reference-dir", type=str, default="data/sample", help="Directory with reference data")
    run_parser.add_argument("--output-dir", type=str, default="outputs/sample", help="Directory to save outputs")
    run_parser.add_argument("--run-id", type=str, default=None, help="Explicit run ID (UUID generated if omitted)")
    run_parser.add_argument("--no-reconcile", action="store_true", help="Skip automatic reconciliation")

    # Subcommand: generate
    gen_parser = subparsers.add_parser("generate", help="Generate synthetic test data")
    gen_parser.add_argument("--output-dir", type=str, default="data/sample", help="Output directory")
    gen_parser.add_argument("--count", type=int, default=100, help="Number of records")
    gen_parser.add_argument("--seed", type=int, default=42, help="Random seed")
    gen_parser.add_argument("--filename", type=str, default="transactions_sample.csv", help="Filename")

    # Subcommand: reconcile
    rec_parser = subparsers.add_parser("reconcile", help="Run standalone reconciliation on existing database")
    rec_parser.add_argument("--db-path", type=str, default="data/metrics.duckdb", help="DuckDB database path")
    rec_parser.add_argument("--output-dir", type=str, default="outputs/sample", help="Directory to save report")

    # Subcommand: init-db
    init_parser = subparsers.add_parser("init-db", help="Initialize database schema and reference entities")
    init_parser.add_argument("--db-path", type=str, default="data/metrics.duckdb", help="DuckDB database path")
    init_parser.add_argument("--reference-dir", type=str, default="data/sample", help="Reference data directory")

    # Subcommand: audit-history
    audit_parser = subparsers.add_parser("audit-history", help="View recent pipeline run audit records")
    audit_parser.add_argument("--db-path", type=str, default="data/metrics.duckdb", help="DuckDB database path")
    audit_parser.add_argument("--limit", type=int, default=10, help="Number of runs to display")

    args = parser.parse_args()

    if args.command == "run":
        cfg = PipelineConfig.from_paths(
            db_path=args.db_path,
            input_file=args.input_file,
            reference_dir=args.reference_dir,
            output_dir=args.output_dir,
            run_id=args.run_id,
            auto_reconcile=not args.no_reconcile,
        )
        audit_result = run_pipeline(cfg)
        if audit_result.status != RunStatus.SUCCESS.value:
            sys.exit(1)

    elif args.command == "generate":
        out_dir = Path(args.output_dir)
        write_reference_data(out_dir)
        rows = generate_synthetic_transactions(count=args.count, seed=args.seed)
        csv_file = out_dir / args.filename
        write_dataset_to_csv(rows, csv_file)
        print(f"Generated {len(rows)} records at {csv_file}")

    elif args.command == "reconcile":
        conn = duckdb.connect(args.db_path)
        reconciler = Reconciler(conn)
        report = reconciler.run_reconciliation()
        out_path = export_reconciliation_report(report, Path(args.output_dir))
        conn.close()
        print(f"Reconciliation {report['overall_status']}. Detailed report at {out_path}")
        if report["overall_status"] != "PASSED":
            sys.exit(1)

    elif args.command == "init-db":
        conn = duckdb.connect(args.db_path)
        init_database_schema(conn)
        ref_dir = Path(args.reference_dir)
        if ref_dir.exists():
            populate_reference_tables(conn, ref_dir)
        conn.close()
        print(f"Database initialized at {args.db_path}")

    elif args.command == "audit-history":
        conn = duckdb.connect(args.db_path)
        df = conn.execute(f"SELECT run_id, source_file, status, input_row_count, new_event_count, duration_seconds FROM run_audit ORDER BY start_time DESC LIMIT {args.limit}").df()
        conn.close()
        print(df.to_string(index=False))


if __name__ == "__main__":
    main()
