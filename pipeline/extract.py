"""
pipeline/extract.py
────────────────────────────────────────────────────────────
Extracts raw CSV rows from the eCommerce dataset files.
Handles malformed rows, encoding issues, and missing columns.
Returns a clean pandas DataFrame ready for transform.py.
"""

import logging
import os
import time
from pathlib import Path
from typing import Optional

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Expected schema ──────────────────────────────────────────
REQUIRED_COLUMNS = {
    "event_time", "event_type", "product_id",
    "category_id", "category_code", "brand", "price",
    "user_id", "user_session",
}

DTYPE_MAP = {
    "event_type":    "category",
    "product_id":    "Int64",
    "category_id":   "Int64",
    "category_code": "string",
    "brand":         "string",
    "price":         "float64",
    "user_id":       "Int64",
    "user_session":  "string",
}


def extract(filepath: str | Path, chunksize: int = 200_000) -> tuple[pd.DataFrame, dict]:
    """
    Read a single CSV file in chunks, concatenate, and return
    a raw DataFrame along with extraction metadata.

    Parameters
    ----------
    filepath  : Path to the CSV file (e.g. data/raw/2019-Oct.csv)
    chunksize : Rows per chunk – tune to available RAM

    Returns
    -------
    (df, meta)  where meta contains rows_read and rows_dropped_extract
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"CSV not found: {filepath}")

    log.info("Extracting: %s", filepath.name)
    t0 = time.perf_counter()

    chunks = []
    rows_read = 0
    bad_chunks = 0

    try:
        reader = pd.read_csv(
            filepath,
            dtype=DTYPE_MAP,
            parse_dates=["event_time"],
            chunksize=chunksize,
            on_bad_lines="warn",      # skip malformed rows; pandas logs them
            encoding="utf-8",
            low_memory=False,
        )

        for chunk in reader:
            rows_read += len(chunk)

            # ── Validate required columns present ────────────
            missing = REQUIRED_COLUMNS - set(chunk.columns)
            if missing:
                raise ValueError(f"Missing columns in {filepath.name}: {missing}")

            chunks.append(chunk)

    except Exception as exc:
        log.error("Failed to read %s: %s", filepath.name, exc)
        raise

    if not chunks:
        raise ValueError(f"No data read from {filepath.name}")

    df = pd.concat(chunks, ignore_index=True)
    elapsed = time.perf_counter() - t0

    meta = {
        "source_file":   filepath.name,
        "rows_extracted": rows_read,
        "elapsed_sec":   round(elapsed, 2),
    }
    log.info(
        "Extracted %d rows from %s in %.2fs",
        rows_read, filepath.name, elapsed,
    )
    return df, meta


def extract_all(data_dir: str | Path = "data/raw") -> tuple[pd.DataFrame, list[dict]]:
    """
    Extract and concatenate all CSV files found in data_dir.
    Returns combined DataFrame and list of per-file metadata.
    """
    data_dir = Path(data_dir)
    csv_files = sorted(data_dir.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"No CSV files in {data_dir}")

    frames, metas = [], []
    for f in csv_files:
        df, meta = extract(f)
        frames.append(df)
        metas.append(meta)

    combined = pd.concat(frames, ignore_index=True)
    log.info("Total rows after combining all files: %d", len(combined))
    return combined, metas


if __name__ == "__main__":
    # Quick smoke-test: python -m pipeline.extract
    df, metas = extract_all()
    print(df.dtypes)
    print(df.head(3))