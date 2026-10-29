"""
Source data ingestion module with fail-closed schema enforcement and lineage tracking.
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Dict, Iterator, List, Tuple

from daily_metrics.config import (
    ALL_ALLOWED_COLUMNS,
    CANONICAL_SOURCE_COLUMNS,
    FatalSchemaError,
)

logger = logging.getLogger(__name__)


def validate_source_header(header: List[str], source_file: str) -> None:
    """
    Validates CSV header strictly against canonical source schema.
    Fails closed if any required column is missing or any unknown column is present.
    """
    cleaned_header = [col.strip() for col in header]

    # Check for missing required columns
    missing_columns = [col for col in CANONICAL_SOURCE_COLUMNS if col not in cleaned_header]
    if missing_columns:
        err_msg = (
            f"Fatal schema error in {source_file}: Missing required column(s): {missing_columns}. "
            f"Expected at least: {CANONICAL_SOURCE_COLUMNS}"
        )
        logger.error(err_msg)
        raise FatalSchemaError(err_msg)

    # Check for unexpected/unknown columns
    unexpected_columns = [col for col in cleaned_header if col not in ALL_ALLOWED_COLUMNS]
    if unexpected_columns:
        err_msg = (
            f"Fatal schema error in {source_file}: Unexpected column(s) detected: {unexpected_columns}. "
            f"Allowed schema: {sorted(list(ALL_ALLOWED_COLUMNS))}"
        )
        logger.error(err_msg)
        raise FatalSchemaError(err_msg)


def read_source_csv(
    file_path: Path,
) -> Tuple[List[str], Iterator[Tuple[int, Dict[str, str]]]]:
    """
    Reads a source CSV file, validates its header, and yields (line_number, row_dict).
    
    Raises:
        FileNotFoundError: If input file does not exist.
        FatalSchemaError: If header fails schema enforcement or file is unparseable.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Source file not found: {file_path}")

    # Check for empty file
    if file_path.stat().st_size == 0:
        logger.warning("Source file is empty (0 bytes): %s", file_path)
        return [], iter([])

    try:
        # We read the file using standard csv.reader first to extract and validate the header
        with open(file_path, mode="r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            try:
                raw_header = next(reader)
            except StopIteration:
                logger.warning("Source file has no content: %s", file_path)
                return [], iter([])

            if not raw_header:
                raise FatalSchemaError(f"Empty header in source file: {file_path}")

            header = [col.strip() for col in raw_header]
            validate_source_header(header, file_path.name)

        # Generator function for streaming rows with line numbers
        def row_generator() -> Iterator[Tuple[int, Dict[str, str]]]:
            with open(file_path, mode="r", encoding="utf-8-sig", newline="") as f:
                dict_reader = csv.DictReader(f)
                try:
                    for line_idx, raw_row in enumerate(dict_reader, start=2):
                        # Detect uneven column count (too few or too many fields)
                        if None in raw_row or any(v is None for v in raw_row.values()):
                            raise FatalSchemaError(
                                f"Malformed CSV row with uneven column count at line {line_idx} in {file_path.name}"
                            )
                        cleaned_row = {
                            k.strip(): (v.strip() if isinstance(v, str) else v)
                            for k, v in raw_row.items()
                            if k is not None
                        }
                        yield line_idx, cleaned_row
                except csv.Error as e:
                    raise FatalSchemaError(f"Malformed CSV parsing error in {file_path.name}: {e}") from e

        return header, row_generator()

    except csv.Error as e:
        err_msg = f"Malformed CSV structure in {file_path}: {e}"
        logger.error(err_msg)
        raise FatalSchemaError(err_msg) from e
