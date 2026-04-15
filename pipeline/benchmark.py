"""
pipeline/benchmark.py
────────────────────────────────────────────────────────────
Section B2 – Performance Benchmarking

Measures and reports:
  • Data load time (per file + combined)
  • Batch insert throughput at 10k / 50k / 100k / 500k rows
  • Query execution time WITH and WITHOUT indexes
  • Peak memory usage during full load
  • Disk I/O: raw input size vs. DuckDB file size
  • Index speedup factor table
"""

import gc
import logging
import os
import time
from pathlib import Path

import duckdb
import pandas as pd

try:
    from memory_profiler import memory_usage
    HAS_MEMORY_PROFILER = True
except ImportError:
    HAS_MEMORY_PROFILER = False

log = logging.getLogger(__name__)
DB_PATH = "data/ecommerce.duckdb"


# ════════════════════════════════════════════════════════════
#  HELPERS
# ════════════════════════════════════════════════════════════

def _con() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(DB_PATH, read_only=False)


def _time_query(con, sql: str, runs: int = 3) -> float:
    """Return median execution time in seconds over `runs` runs."""
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        con.execute(sql).fetchall()
        times.append(time.perf_counter() - t0)
    return round(sorted(times)[len(times) // 2], 4)   # median


def _file_size_mb(path: str) -> float:
    p = Path(path)
    return round(p.stat().st_size / 1_048_576, 2) if p.exists() else 0.0


# ════════════════════════════════════════════════════════════
#  B2-1: DATA LOAD TIME
# ════════════════════════════════════════════════════════════

def benchmark_load_time(csv_files: list[str]) -> pd.DataFrame:
    """
    Re-runs the full pipeline for each file and records wall-clock
    load time. Drops and recreates DB to get clean measurements.
    """
    from pipeline.load import run_pipeline

    records = []
    for f in csv_files:
        t0 = time.perf_counter()
        result = run_pipeline(f, force=True)
        elapsed = time.perf_counter() - t0
        records.append({
            "file":          Path(f).name,
            "rows_loaded":   result.get("rows_loaded", 0),
            "elapsed_sec":   round(elapsed, 2),
        })

    # Combined (both files already in DB – just measure total)
    df = pd.DataFrame(records)
    total = pd.DataFrame([{
        "file":          "COMBINED",
        "rows_loaded":   df["rows_loaded"].sum(),
        "elapsed_sec":   df["elapsed_sec"].sum(),
    }])
    return pd.concat([df, total], ignore_index=True)


# ════════════════════════════════════════════════════════════
#  B2-2: BATCH INSERT THROUGHPUT
# ════════════════════════════════════════════════════════════

def benchmark_batch_throughput(
    sample_rows: int = 500_000,
    batch_sizes: list[int] = None,
) -> pd.DataFrame:
    """
    Inserts `sample_rows` rows in batches of different sizes into a
    temp table and reports rows/second for each batch size.
    """
    if batch_sizes is None:
        batch_sizes = [10_000, 50_000, 100_000, 500_000]

    # Build a synthetic DataFrame that matches fact_events schema
    con = _con()
    con.execute("DROP TABLE IF EXISTS _bench_fact")
    con.execute("""
        CREATE TEMP TABLE _bench_fact (
            event_id   BIGINT, date_key INTEGER, event_time TIMESTAMP,
            event_type VARCHAR, product_id BIGINT,
            user_id    BIGINT, user_session VARCHAR, price DOUBLE
        )
    """)

    sample = con.execute(f"""
        SELECT event_id, date_key, event_time, event_type, product_id,
               user_id, user_session, price
        FROM fact_events LIMIT {sample_rows}
    """).df()

    records = []
    for bs in batch_sizes:
        con.execute("DELETE FROM _bench_fact")
        t0 = time.perf_counter()
        for start in range(0, len(sample), bs):
            chunk = sample.iloc[start:start + bs]
            con.register("_c", chunk)
            con.execute("INSERT INTO _bench_fact SELECT * FROM _c")
            con.unregister("_c")
        elapsed = time.perf_counter() - t0

        rows_per_sec = round(len(sample) / elapsed)
        records.append({
            "batch_size":    bs,
            "total_rows":    len(sample),
            "elapsed_sec":   round(elapsed, 3),
            "rows_per_sec":  rows_per_sec,
        })
        log.info("Batch=%d → %d rows/s", bs, rows_per_sec)

    con.close()
    return pd.DataFrame(records)


# ════════════════════════════════════════════════════════════
#  B2-3: QUERY EXECUTION TIME  (with vs. without indexes)
# ════════════════════════════════════════════════════════════

BENCHMARK_QUERIES = {
    "Q15_funnel": """
        SELECT p.category_id,
               COUNT(DISTINCT CASE WHEN e.event_type='view'     THEN e.user_id END) AS views,
               COUNT(DISTINCT CASE WHEN e.event_type='cart'     THEN e.user_id END) AS carts,
               COUNT(DISTINCT CASE WHEN e.event_type='purchase' THEN e.user_id END) AS purchases
        FROM fact_events e
        JOIN dim_product p USING (product_id)
        GROUP BY p.category_id
        ORDER BY views DESC
        LIMIT 10
    """,
    "Q17_revenue": """
        SELECT p.brand,
               d.month,
               SUM(e.price) AS total_revenue
        FROM fact_events e
        JOIN dim_product p USING (product_id)
        JOIN dim_date    d USING (date_key)
        WHERE e.event_type = 'purchase'
        GROUP BY p.brand, d.month
        ORDER BY total_revenue DESC
        LIMIT 10
    """,
    "Q19_hourly": """
        SELECT d.hour,
               COUNT(*) AS purchase_count
        FROM fact_events e
        JOIN dim_date d USING (date_key)
        WHERE e.event_type = 'purchase'
        GROUP BY d.hour
        ORDER BY purchase_count DESC
    """,
}

DROP_INDEXES = [
    "DROP INDEX IF EXISTS idx_fe_event_type",
    "DROP INDEX IF EXISTS idx_fe_product",
    "DROP INDEX IF EXISTS idx_fe_date",
    "DROP INDEX IF EXISTS idx_fe_user",
    "DROP INDEX IF EXISTS idx_fe_session",
    "DROP INDEX IF EXISTS idx_fe_type_date",
]

CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_fe_event_type ON fact_events(event_type)",
    "CREATE INDEX IF NOT EXISTS idx_fe_product    ON fact_events(product_id)",
    "CREATE INDEX IF NOT EXISTS idx_fe_date       ON fact_events(date_key)",
    "CREATE INDEX IF NOT EXISTS idx_fe_user       ON fact_events(user_id)",
    "CREATE INDEX IF NOT EXISTS idx_fe_session    ON fact_events(user_id, user_session)",
    "CREATE INDEX IF NOT EXISTS idx_fe_type_date  ON fact_events(event_type, date_key)",
]


def benchmark_query_times() -> pd.DataFrame:
    con = _con()
    records = []

    for phase, setup_sqls in [("without_index", DROP_INDEXES),
                               ("with_index",    CREATE_INDEXES)]:
        for sql in setup_sqls:
            con.execute(sql)

        for qname, qsql in BENCHMARK_QUERIES.items():
            t = _time_query(con, qsql)
            records.append({"query": qname, "phase": phase, "median_sec": t})
            log.info("%s  %-20s  %.4fs", phase, qname, t)

    con.close()
    df = pd.DataFrame(records)
    pivot = df.pivot(index="query", columns="phase", values="median_sec").reset_index()
    pivot["speedup_factor"] = (
        pivot["without_index"] / pivot["with_index"]
    ).round(2)
    return pivot


# ════════════════════════════════════════════════════════════
#  B2-4: MEMORY USAGE
# ════════════════════════════════════════════════════════════

def benchmark_memory(csv_path: str) -> dict:
    if not HAS_MEMORY_PROFILER:
        log.warning("memory_profiler not installed – skipping memory benchmark")
        return {"peak_mb": "N/A (install memory_profiler)"}

    from pipeline.load import run_pipeline

    def _task():
        run_pipeline(csv_path, force=True)

    mem = memory_usage(_task, interval=0.5, max_usage=True)
    return {"peak_mb": round(mem, 1), "source_file": Path(csv_path).name}


# ════════════════════════════════════════════════════════════
#  B2-5: DISK I/O
# ════════════════════════════════════════════════════════════

def benchmark_disk_io(csv_files: list[str], db_path: str = DB_PATH) -> pd.DataFrame:
    raw_total = sum(_file_size_mb(f) for f in csv_files)
    db_size   = _file_size_mb(db_path)
    ratio     = round(db_size / raw_total, 3) if raw_total else 0
    return pd.DataFrame([{
        "raw_csv_total_mb":  raw_total,
        "duckdb_file_mb":    db_size,
        "compression_ratio": ratio,
    }])


# ════════════════════════════════════════════════════════════
#  MAIN REPORT
# ════════════════════════════════════════════════════════════

def run_all_benchmarks(
    csv_files: list[str] = None,
    db_path: str = DB_PATH,
) -> dict[str, pd.DataFrame]:
    """Run all benchmarks and return a dict of DataFrames."""
    if csv_files is None:
        csv_files = ["data/raw/2019-Oct.csv", "data/raw/2019-Nov.csv"]

    print("\n" + "═"*60)
    print("  PERFORMANCE BENCHMARKS")
    print("═"*60)

    results = {}

    print("\n[B2-2] Batch Insert Throughput")
    results["batch_throughput"] = benchmark_batch_throughput()
    print(results["batch_throughput"].to_string(index=False))

    print("\n[B2-3] Query Execution Time (with vs. without indexes)")
    results["query_times"] = benchmark_query_times()
    print(results["query_times"].to_string(index=False))

    print("\n[B2-5] Disk I/O")
    results["disk_io"] = benchmark_disk_io(csv_files, db_path)
    print(results["disk_io"].to_string(index=False))

    print("\n" + "═"*60)
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    run_all_benchmarks()