"""
pipeline/transform.py
────────────────────────────────────────────────────────────
Transforms raw extracted DataFrame into four clean DataFrames
ready to load into DuckDB dimension and fact tables.

Also runs B3 Data Quality checks and returns a quality report.
"""

import logging
import time
from typing import Optional

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

# ── Business rules ───────────────────────────────────────────
VALID_EVENT_TYPES   = {"view", "cart", "purchase"}
PRICE_MIN           = 0.01        # prices below this are suspicious
PRICE_MAX           = 50_000.0    # prices above this are suspicious
EXPECTED_DATE_START = pd.Timestamp("2019-09-01")
EXPECTED_DATE_END   = pd.Timestamp("2019-12-31")


# ════════════════════════════════════════════════════════════
#  MAIN TRANSFORM ENTRY POINT
# ════════════════════════════════════════════════════════════

def transform(df_raw: pd.DataFrame) -> tuple[dict[str, pd.DataFrame], dict]:
    """
    Parameters
    ----------
    df_raw : Raw DataFrame from extract.py

    Returns
    -------
    (tables, quality_report)
        tables = {
            'dim_category': ...,
            'dim_product':  ...,
            'dim_date':     ...,
            'fact_events':  ...,
        }
        quality_report = summary counts for B3
    """
    t0 = time.perf_counter()
    total_rows = len(df_raw)
    log.info("Transform started on %d rows", total_rows)

    df, quality_flags = _run_quality_checks(df_raw)

    # Build dimensions first (needed for FK references)
    dim_category = _build_dim_category(df)
    dim_product  = _build_dim_product(df, dim_category)
    dim_date     = _build_dim_date(df)

    # Build fact table last
    fact_events  = _build_fact_events(df, dim_date)

    tables = {
        "dim_category": dim_category,
        "dim_product":  dim_product,
        "dim_date":     dim_date,
        "fact_events":  fact_events,
    }

    quality_report = _build_quality_report(total_rows, len(df), quality_flags)
    elapsed = time.perf_counter() - t0
    log.info("Transform finished in %.2fs", elapsed)

    return tables, quality_report


# ════════════════════════════════════════════════════════════
#  DATA QUALITY CHECKS  (Section B3)
# ════════════════════════════════════════════════════════════

def _run_quality_checks(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Applies quality rules, flags bad rows, drops them, and
    returns the clean DataFrame plus a flags summary dict.
    """
    flags: dict[str, int] = {}
    drop_mask = pd.Series(False, index=df.index)

    # 1. NOT NULL checks on critical columns
    not_null_cols = ["event_time", "event_type", "product_id", "user_id",
                     "user_session", "price"]
    null_mask = df[not_null_cols].isnull().any(axis=1)
    flags["null_critical_fields"] = int(null_mask.sum())
    drop_mask |= null_mask

    # 2. Invalid event_type
    invalid_type = ~df["event_type"].isin(VALID_EVENT_TYPES)
    flags["invalid_event_type"] = int(invalid_type.sum())
    drop_mask |= invalid_type

    # 3. Price out of range (zero, negative, unreasonably large)
    bad_price = (df["price"] <= 0) | (df["price"] > PRICE_MAX)
    flags["bad_price"] = int(bad_price.sum())
    drop_mask |= bad_price

    # 4. Timestamps outside expected range
    bad_ts = (df["event_time"] < EXPECTED_DATE_START) | \
             (df["event_time"] > EXPECTED_DATE_END)
    flags["out_of_range_timestamp"] = int(bad_ts.sum())
    drop_mask |= bad_ts

    # 5. Duplicate events (same user, product, timestamp, event_type)
    dup_mask = df.duplicated(
        subset=["user_id", "product_id", "event_time", "event_type"],
        keep="first",
    )
    flags["duplicate_events"] = int(dup_mask.sum())
    drop_mask |= dup_mask

    total_dropped = int(drop_mask.sum())
    log.info("Quality check: dropping %d / %d rows", total_dropped, len(df))
    df_clean = df[~drop_mask].copy().reset_index(drop=True)

    return df_clean, flags


def _build_quality_report(total: int, clean: int, flags: dict) -> dict:
    dropped = total - clean
    return {
        "total_rows_processed": total,
        "rows_dropped":         dropped,
        "rows_passed":          clean,
        "pass_rate_pct":        round(clean / total * 100, 2) if total else 0,
        "flag_breakdown":       flags,
    }


# ════════════════════════════════════════════════════════════
#  DIMENSION BUILDERS
# ════════════════════════════════════════════════════════════

def _build_dim_category(df: pd.DataFrame) -> pd.DataFrame:
    cat = (
        df[["category_id", "category_code"]]
        .dropna(subset=["category_id"])
        .drop_duplicates("category_id")
        .copy()
    )
    cat["category_id"] = cat["category_id"].astype("int64")

    # Split "electronics.smartphone" → main=electronics, sub=smartphone
    split = cat["category_code"].str.split(".", n=1, expand=True)
    cat["main_category"] = split[0].fillna("unknown")
    cat["sub_category"]  = split[1] if 1 in split.columns else None

    # Rows with no category_code still need a placeholder
    cat["main_category"] = cat["main_category"].fillna("unknown")

    log.info("dim_category: %d rows", len(cat))
    return cat[["category_id", "category_code", "main_category", "sub_category"]]


def _build_dim_product(df: pd.DataFrame, dim_category: pd.DataFrame) -> pd.DataFrame:
    prod = (
        df[["product_id", "category_id", "brand", "price"]]
        .dropna(subset=["product_id"])
        .copy()
    )
    # Take latest observed price per product as base_price
    prod = (
        prod.sort_values("price")
        .drop_duplicates("product_id", keep="last")
    )
    prod = prod.rename(columns={"price": "base_price"})
    prod["product_id"]  = prod["product_id"].astype("int64")
    prod["category_id"] = prod["category_id"].astype("int64")

    # Ensure every product's category_id exists in dim_category
    valid_cats = set(dim_category["category_id"])
    orphans = ~prod["category_id"].isin(valid_cats)
    if orphans.any():
        log.warning("Dropping %d products with unknown category_id", orphans.sum())
        prod = prod[~orphans]

    log.info("dim_product: %d rows", len(prod))
    return prod[["product_id", "category_id", "brand", "base_price"]]


def _build_dim_date(df: pd.DataFrame) -> pd.DataFrame:
    # Truncate to hour to reduce cardinality of the dimension
    hours = df["event_time"].dt.floor("h").drop_duplicates().dropna()
    dim = pd.DataFrame({"event_hour": hours}).reset_index(drop=True)

    dim["year"]        = dim["event_hour"].dt.year.astype("int16")
    dim["month"]       = dim["event_hour"].dt.month.astype("int16")
    dim["day"]         = dim["event_hour"].dt.day.astype("int16")
    dim["hour"]        = dim["event_hour"].dt.hour.astype("int16")
    dim["day_of_week"] = dim["event_hour"].dt.dayofweek.astype("int16")
    dim["is_weekend"]  = dim["day_of_week"] >= 5

    # Surrogate key: YYYYMMDDHH integer (unique per hour-bucket)
    dim["date_key"] = (
        dim["year"] * 1_000_000
        + dim["month"] * 10_000
        + dim["day"] * 100
        + dim["hour"]
    ).astype("int32")

    log.info("dim_date: %d rows", len(dim))
    return dim[["date_key", "event_hour", "year", "month", "day",
                "hour", "day_of_week", "is_weekend"]]


# ════════════════════════════════════════════════════════════
#  FACT TABLE BUILDER
# ════════════════════════════════════════════════════════════

def _build_fact_events(df: pd.DataFrame, dim_date: pd.DataFrame) -> pd.DataFrame:
    fact = df[["event_time", "event_type", "product_id",
               "user_id", "user_session", "price"]].copy()

    fact["product_id"] = fact["product_id"].astype("int64")
    fact["user_id"]    = fact["user_id"].astype("int64")
    fact["price"]      = fact["price"].astype("float64")

    # Map event_time → date_key via hour truncation
    fact["event_hour"] = fact["event_time"].dt.floor("h")
    date_map = dim_date.set_index("event_hour")["date_key"]
    fact["date_key"] = fact["event_hour"].map(date_map).astype("int32")
    fact = fact.drop(columns=["event_hour"])

    # Assign surrogate event_id (sequential)
    fact = fact.reset_index(drop=True)
    fact.index.name = "event_id"
    fact = fact.reset_index()
    fact["event_id"] = fact["event_id"] + 1   # 1-based

    log.info("fact_events: %d rows", len(fact))
    return fact[["event_id", "date_key", "event_time", "event_type",
                 "product_id", "user_id", "user_session", "price"]]