--  E-Commerce Behavior Data  –  DuckDB DDL
--  Schema: Star schema (3NF dims + fact table)
--  Justification: Optimised for analytical GROUP BY / JOIN
--                 queries in Section C without full scans.
-- ============================================================

-- ── Dimension: Category ─────────────────────────────────────
-- Splits raw category_code ("electronics.smartphone") into
-- main_category and sub_category for flexible GROUP BY.
CREATE TABLE IF NOT EXISTS dim_category (
    category_id     BIGINT      PRIMARY KEY,
    category_code   VARCHAR,                     -- raw e.g. "electronics.smartphone"
    main_category   VARCHAR     NOT NULL,
    sub_category    VARCHAR                      -- NULL when no subcategory present
);

CREATE INDEX IF NOT EXISTS idx_cat_main ON dim_category(main_category);

-- ── Dimension: Product ──────────────────────────────────────
-- One row per unique product_id. Price stored in fact table
-- (varies per event); base_price here = latest observed price.
CREATE TABLE IF NOT EXISTS dim_product (
    product_id      BIGINT      PRIMARY KEY,
    category_id     BIGINT      NOT NULL REFERENCES dim_category(category_id),
    brand           VARCHAR,                     -- nullable – not all products have brand
    base_price      DECIMAL(10,2)                -- informational; authoritative price in fact
);

CREATE INDEX IF NOT EXISTS idx_prod_cat    ON dim_product(category_id);
CREATE INDEX IF NOT EXISTS idx_prod_brand  ON dim_product(brand);

-- ── Dimension: Date ─────────────────────────────────────────
-- Pre-computed time attributes allow fast GROUP BY month/hour
-- without repeated EXTRACT() calls on billions of rows.
CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INTEGER     PRIMARY KEY,     -- YYYYMMDDHH integer surrogate
    event_hour      TIMESTAMP   NOT NULL UNIQUE,
    year            SMALLINT    NOT NULL,
    month           SMALLINT    NOT NULL,
    day             SMALLINT    NOT NULL,
    hour            SMALLINT    NOT NULL,
    day_of_week     SMALLINT    NOT NULL,        -- 0=Mon … 6=Sun
    is_weekend      BOOLEAN     NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_date_month ON dim_date(year, month);
CREATE INDEX IF NOT EXISTS idx_date_hour  ON dim_date(hour);

-- ── Fact: Events ─────────────────────────────────────────────
-- Central fact table – one row per e-commerce event.
-- Partitioned logically by (year, month) via date_key range.
CREATE SEQUENCE IF NOT EXISTS seq_event_id START 1;

CREATE TABLE IF NOT EXISTS fact_events (
    event_id        BIGINT      DEFAULT nextval('seq_event_id') PRIMARY KEY,
    date_key        INTEGER     NOT NULL REFERENCES dim_date(date_key),
    event_time      TIMESTAMP   NOT NULL,
    event_type      VARCHAR(10) NOT NULL CHECK (event_type IN ('view','cart','purchase')),
    product_id      BIGINT      NOT NULL REFERENCES dim_product(product_id),
    user_id         BIGINT      NOT NULL,
    user_session    VARCHAR     NOT NULL,
    price           DECIMAL(10,2) NOT NULL CHECK (price >= 0)
);

-- Indexes justified by Section C queries:
--   Q15 funnel   → event_type + category_id (via product join)
--   Q16 session  → user_id + user_session
--   Q17 revenue  → date_key (month filter) + product_id
--   Q18 cohort   → user_id + date_key
--   Q19 hourly   → date_key.hour
CREATE INDEX IF NOT EXISTS idx_fe_event_type   ON fact_events(event_type);
CREATE INDEX IF NOT EXISTS idx_fe_user         ON fact_events(user_id);
CREATE INDEX IF NOT EXISTS idx_fe_product      ON fact_events(product_id);
CREATE INDEX IF NOT EXISTS idx_fe_date         ON fact_events(date_key);
CREATE INDEX IF NOT EXISTS idx_fe_session      ON fact_events(user_id, user_session);
CREATE INDEX IF NOT EXISTS idx_fe_type_date    ON fact_events(event_type, date_key);

-- ── Audit / Pipeline Log ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_log (
    run_id          INTEGER     PRIMARY KEY,
    source_file     VARCHAR     NOT NULL,
    run_started_at  TIMESTAMP   NOT NULL DEFAULT current_timestamp,
    run_ended_at    TIMESTAMP,
    rows_extracted  INTEGER,
    rows_dropped    INTEGER,
    rows_loaded     INTEGER,
    status          VARCHAR(20) CHECK (status IN ('running','success','failed')),
    error_message   VARCHAR
);