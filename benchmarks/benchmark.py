"""
Performance benchmark and scalability validation script.
Generates >= 100,000 synthetic transaction records, measures execution time with
a monotonic clock, compares initial ingestion against duplicate rerun, and outputs
a detailed performance report.
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
import platform
import sys
import time
from datetime import datetime, timezone
import duckdb

from daily_metrics.cli import run_pipeline
from daily_metrics.config import PipelineConfig
from daily_metrics.generator import generate_synthetic_transactions, write_dataset_to_csv, write_reference_data
from daily_metrics.reconciliation import Reconciler

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("benchmark")


def run_benchmark(record_count: int = 100000, seed: int = 42, output_dir: Path = Path("reports")) -> dict:
    """Runs end-to-end benchmark on generated synthetic dataset."""
    output_dir.mkdir(parents=True, exist_ok=True)
    bench_data_dir = Path("data/generated")
    bench_data_dir.mkdir(parents=True, exist_ok=True)
    db_path = bench_data_dir / "benchmark.duckdb"

    # Clean previous benchmark DB if exists
    if db_path.exists():
        db_path.unlink()

    # Step 1: Generate reference data and large dataset
    logger.info("Generating reference catalog...")
    write_reference_data(bench_data_dir)

    logger.info("Generating %d synthetic transaction records (seed=%d)...", record_count, seed)
    t0_gen = time.monotonic()
    records = generate_synthetic_transactions(count=record_count, seed=seed)
    bench_csv = bench_data_dir / f"benchmark_{record_count}.csv"
    write_dataset_to_csv(records, bench_csv)
    gen_duration = time.monotonic() - t0_gen
    logger.info("Generated %d records in %.2f seconds.", len(records), gen_duration)

    # Step 2: Initial Batch Ingestion Benchmark
    logger.info("Running initial ingestion benchmark...")
    cfg_initial = PipelineConfig.from_paths(
        db_path=db_path,
        input_file=bench_csv,
        reference_dir=bench_data_dir,
        output_dir=output_dir / "bench_initial",
        run_id="bench-initial-001",
        auto_reconcile=True,
    )

    t0_ingest = time.monotonic()
    audit_initial = run_pipeline(cfg_initial)
    total_ingest_time = time.monotonic() - t0_ingest

    throughput_initial = audit_initial.input_row_count / total_ingest_time if total_ingest_time > 0 else 0.0

    # Step 3: Standalone Reconciliation Measurement
    logger.info("Benchmarking standalone reconciliation...")
    conn = duckdb.connect(str(db_path))
    t0_recon = time.monotonic()
    reconciler = Reconciler(conn)
    recon_report = reconciler.run_reconciliation()
    recon_duration = time.monotonic() - t0_recon
    conn.close()

    # Step 4: Duplicate-only Rerun Benchmark
    logger.info("Benchmarking duplicate batch rerun...")
    cfg_rerun = PipelineConfig.from_paths(
        db_path=db_path,
        input_file=bench_csv,
        reference_dir=bench_data_dir,
        output_dir=output_dir / "bench_rerun",
        run_id="bench-rerun-002",
        auto_reconcile=False,
    )

    t0_rerun = time.monotonic()
    audit_rerun = run_pipeline(cfg_rerun)
    rerun_duration = time.monotonic() - t0_rerun
    throughput_rerun = audit_rerun.input_row_count / rerun_duration if rerun_duration > 0 else 0.0

    # Assemble comprehensive benchmark summary
    report = {
        "benchmark_timestamp": datetime.now(timezone.utc).isoformat(),
        "environment": {
            "os": platform.system(),
            "os_release": platform.release(),
            "python_version": platform.python_version(),
            "processor": platform.processor(),
        },
        "dataset": {
            "record_count": record_count,
            "seed": seed,
            "file_size_mb": round(bench_csv.stat().st_size / (1024 * 1024), 2),
            "generation_time_seconds": round(gen_duration, 3),
        },
        "initial_ingestion": {
            "input_row_count": audit_initial.input_row_count,
            "validated_row_count": audit_initial.validated_row_count,
            "new_event_count": audit_initial.new_event_count,
            "exact_duplicate_count": audit_initial.exact_duplicate_count,
            "conflicting_duplicate_count": audit_initial.conflicting_duplicate_count,
            "rejected_row_count": audit_initial.rejected_row_count,
            "target_partitions_written": audit_initial.target_row_count,
            "total_elapsed_seconds": round(total_ingest_time, 3),
            "throughput_rows_per_second": round(throughput_initial, 1),
            "reconciliation_status": audit_initial.reconciliation_status,
        },
        "reconciliation_benchmark": {
            "reconciliation_time_seconds": round(recon_duration, 4),
            "overall_status": recon_report["overall_status"],
            "mismatch_count": recon_report["mismatch_count"],
        },
        "duplicate_rerun": {
            "input_row_count": audit_rerun.input_row_count,
            "exact_duplicate_count": audit_rerun.exact_duplicate_count,
            "new_event_count": audit_rerun.new_event_count,
            "total_elapsed_seconds": round(rerun_duration, 3),
            "throughput_rows_per_second": round(throughput_rerun, 1),
        },
    }

    report_path = output_dir / "benchmark_report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    logger.info("Benchmark complete! Full report written to %s", report_path)
    print("\n" + "=" * 60)
    print("BENCHMARK EXECUTION SUMMARY")
    print("=" * 60)
    print(f"Total Input Records      : {record_count:,}")
    print(f"Initial Ingestion Time   : {total_ingest_time:.2f} s")
    print(f"Initial Throughput       : {throughput_initial:,.1f} rows/sec")
    print(f"New Events Accepted      : {audit_initial.new_event_count:,}")
    print(f"Quarantined Records      : {audit_initial.rejected_row_count:,}")
    print(f"Target Partitions Updated: {audit_initial.target_row_count:,}")
    print(f"Reconciliation Time      : {recon_duration:.4f} s ({recon_report['overall_status']})")
    print(f"Duplicate Rerun Time     : {rerun_duration:.2f} s")
    print(f"Duplicate Throughput     : {throughput_rerun:,.1f} rows/sec")
    print("=" * 60)

    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run pipeline benchmark")
    parser.add_argument("--count", type=int, default=100000, help="Number of records (>=100000)")
    parser.add_argument("--seed", type=int, default=42, help="Seed")
    parser.add_argument("--output-dir", type=str, default="reports", help="Output directory")

    args = parser.parse_args()
    run_benchmark(record_count=args.count, seed=args.seed, output_dir=Path(args.output_dir))
