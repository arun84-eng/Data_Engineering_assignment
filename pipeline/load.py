"""
pipeline/load.py
────────────────────────────────────────────────────────────
Loads transformed DataFrames into DuckDB in batches.
Supports idempotent incremental loads – running the pipeline
again on the same file will NOT create duplicate rows.
"""

import logging
import time
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

log = logging.getLogger(__name__)

DB_PATH      = "data/ecommerce.duckdb"
DDL_PATH     = "data/schema/ddl.sql"
BATCH_SIZE   = 100_000          # tune for benchmark B2


# ════════════════════════════════════════════════════════════
#  CONNECTION HELPER
# ════════════════════════════════════════════════════════════

def get_connection(db_path: str = DB_PATH) -> duckdb.DuckDBPyConnection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(db_path)
    # Performance tuning
    con.execute("PRAGMA threads=4")
    con.execute("PRAGMA memory_limit='2GB'")
    return con


def initialise_schema(con: duckdb.DuckDBPyConnection, ddl_path: str = DDL_PATH):
    """Run DDL file to create tables if they don't exist."""
    ddl = Path(ddl_path).read_text()
    con.executescript(ddl)
    log.info("Schema initialised from %s", ddl_path)


# ════════════════════════════════════════════════════════════
#  UPSERT HELPERS  (idempotency)
# ════════════════════════════════════════════════════════════

def _upsert_dim(
    con: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    table: str,
    pk: str,
):
    """
    Insert rows that don't already exist (keyed on pk).
    Uses a staging temp table approach for efficiency.
    """
    if df.empty:
        return

    staging = f"_stage_{table}"
    con.execute(f"DROP TABLE IF EXISTS {staging}")
    con.execute(f"CREATE TEMP TABLE {staging} AS SELECT * FROM df LIMIT 0")
    _batch_insert(con, df, staging)

    cols = ", ".join(df.columns)
    con.execute(f"""
        INSERT INTO {table} ({cols})
        SELECT {cols} FROM {staging}
        WHERE {pk} NOT IN (SELECT {pk} FROM {table})
    """)
    con.execute(f"DROP TABLE IF EXISTS {staging}")
    log.info("Upserted %s rows into %s", len(df), table)


def _batch_insert(
    con: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    table: str,
    batch_size: int = BATCH_SIZE,
):
    """Batch INSERT for large fact tables – avoids single huge transaction."""
    cols  = ", ".join(df.columns)
    total = len(df)

    for start in range(0, total, batch_size):
        chunk = df.iloc[start : start + batch_size]
        con.register("_chunk", chunk)
        con.execute(f"INSERT INTO {table} ({cols}) SELECT {cols} FROM _chunk")
        con.unregister("_chunk")

    log.info("Batch-inserted %d rows into %s", total, table)


# ════════════════════════════════════════════════════════════
#  INCREMENTAL LOAD GUARD
# ════════════════════════════════════════════════════════════

def _already_loaded(con: duckdb.DuckDBPyConnection, source_file: str) -> bool:
    """Return True if this file was already loaded successfully."""
    result = con.execute("""
        SELECT COUNT(*) FROM pipeline_log
        WHERE source_file = ? AND status = 'success'
    """, [source_file]).fetchone()
    return result[0] > 0


def _start_log(con: duckdb.DuckDBPyConnection, source_file: str) -> int:
    run_id = con.execute("""
        INSERT INTO pipeline_log (source_file, run_started_at, status)
        VALUES (?, current_timestamp, 'running')
        RETURNING run_id
    """, [source_file]).fetchone()[0]
    return run_id


def _end_log(
    con: duckdb.DuckDBPyConnection,
    run_id: int,
    rows_extracted: int,
    rows_dropped: int,
    rows_loaded: int,
    status: str = "success",
    error: Optional[str] = None,
):
    con.execute("""
        UPDATE pipeline_log
        SET run_ended_at   = current_timestamp,
            rows_extracted = ?,
            rows_dropped   = ?,
            rows_loaded    = ?,
            status         = ?,
            error_message  = ?
        WHERE run_id = ?
    """, [rows_extracted, rows_dropped, rows_loaded, status, error, run_id])


# ════════════════════════════════════════════════════════════
#  MAIN LOAD FUNCTION
# ════════════════════════════════════════════════════════════

def load(
    tables: dict[str, pd.DataFrame],
    quality_report: dict,
    source_file: str,
    db_path: str = DB_PATH,
    batch_size: int = BATCH_SIZE,
    force: bool = False,
) -> dict:
    """
    Load all dimension and fact tables into DuckDB.

    Parameters
    ----------
    tables         : Output of transform.transform()
    quality_report : Output of transform.transform()
    source_file    : Original CSV filename (for idempotency check)
    db_path        : DuckDB file path
    batch_size     : Rows per batch insert
    force          : If True, skip idempotency check and reload

    Returns
    -------
    dict with load timing and row counts
    """
    con = get_connection(db_path)
    initialise_schema(con)

    # ── Idempotency guard ─────────────────────────────────
    if not force and _already_loaded(con, source_file):
        log.warning("'%s' already loaded – skipping. Use force=True to reload.", source_file)
        con.close()
        return {"skipped": True, "source_file": source_file}

    run_id = _start_log(con, source_file)
    t0     = time.perf_counter()

    try:
        con.execute("BEGIN TRANSACTION")

        # Dimensions must be loaded before fact (FK constraints)
        _upsert_dim(con, tables["dim_category"], "dim_category", "category_id")
        _upsert_dim(con, tables["dim_product"],  "dim_product",  "product_id")
        _upsert_dim(con, tables["dim_date"],     "dim_date",     "date_key")

        # Fact table – large, batch-insert only (always append new events)
        _batch_insert(con, tables["fact_events"], "fact_events", batch_size)

        con.execute("COMMIT")

        rows_loaded = len(tables["fact_events"])
        elapsed     = round(time.perf_counter() - t0, 2)

        _end_log(
            con, run_id,
            rows_extracted = quality_report["total_rows_processed"],
            rows_dropped   = quality_report["rows_dropped"],
            rows_loaded    = rows_loaded,
        )

        result = {
            "source_file":  source_file,
            "rows_loaded":  rows_loaded,
            "elapsed_sec":  elapsed,
            "batch_size":   batch_size,
            "quality":      quality_report,
        }
        log.info("Load complete: %d rows in %.2fs", rows_loaded, elapsed)
        return result

    except Exception as exc:
        con.execute("ROLLBACK")
        _end_log(con, run_id, 0, 0, 0, status="failed", error=str(exc))
        log.error("Load failed: %s", exc)
        raise

    finally:
        con.close()


# ════════════════════════════════════════════════════════════
#  CONVENIENCE – run full pipeline for one file
# ════════════════════════════════════════════════════════════

def run_pipeline(
    csv_path: str,
    db_path: str = DB_PATH,
    batch_size: int = BATCH_SIZE,
    force: bool = False,
):
    """End-to-end: extract → transform → load for a single CSV file."""
    import pipeline.extract   as E
    import pipeline.transform as T

    log.info("══ Pipeline start: %s ══", csv_path)
    t_start = time.perf_counter()

    df_raw, meta = E.extract(csv_path)
    tables, quality = T.transform(df_raw)
    result = load(tables, quality, meta["source_file"],
                  db_path=db_path, batch_size=batch_size, force=force)

    result["total_elapsed_sec"] = round(time.perf_counter() - t_start, 2)
    log.info("══ Pipeline end: total %.2fs ══", result["total_elapsed_sec"])

    # Print quality summary
    q = quality
    print(f"\n{'─'*50}")
    print(f"  Data Quality Summary – {meta['source_file']}")
    print(f"{'─'*50}")
    print(f"  Rows processed : {q['total_rows_processed']:,}")
    print(f"  Rows dropped   : {q['rows_dropped']:,}")
    print(f"  Rows loaded    : {q['rows_passed']:,}")
    print(f"  Pass rate      : {q['pass_rate_pct']}%")
    for flag, count in q["flag_breakdown"].items():
        if count:
            print(f"    ↳ {flag}: {count:,}")
    print(f"{'─'*50}\n")

    return result


if __name__ == "__main__":
    import sys
    files = sys.argv[1:] or ["data/raw/2019-Oct.csv", "data/raw/2019-Nov.csv"]
    for f in files:
        run_pipeline(f)