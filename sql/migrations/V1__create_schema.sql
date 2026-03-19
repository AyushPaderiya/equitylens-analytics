-- ── Safe setup ────────────────────────────────────────────────────────────────
SET statement_timeout   = 0;
SET lock_timeout        = 0;
SET client_encoding     = 'UTF8';
SET standard_conforming_strings = ON;

-- ── Extension ─────────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;  -- Query performance monitoring


-- ══════════════════════════════════════════════════════════════════════════════
-- DIMENSION TABLES
-- ══════════════════════════════════════════════════════════════════════════════

-- ── dim_dates ─────────────────────────────────────────────────────────────────
-- Standard BI date dimension. Pre-populated for 2020-2030.
-- Avoids expensive DATE_PART() calls in analytical queries.
CREATE TABLE IF NOT EXISTS dim_dates (
    date_id         SERIAL          PRIMARY KEY,
    date            DATE            NOT NULL UNIQUE,
    year            SMALLINT        NOT NULL,
    quarter         SMALLINT        NOT NULL    CHECK (quarter BETWEEN 1 AND 4),
    month           SMALLINT        NOT NULL    CHECK (month BETWEEN 1 AND 12),
    week_of_year    SMALLINT        NOT NULL    CHECK (week_of_year BETWEEN 1 AND 53),
    day_of_week     SMALLINT        NOT NULL    CHECK (day_of_week BETWEEN 0 AND 6),
    day_of_year     SMALLINT        NOT NULL,
    month_name      VARCHAR(10)     NOT NULL,
    quarter_label   VARCHAR(6)      NOT NULL,   -- e.g. 'Q1 23'
    is_weekend      BOOLEAN         NOT NULL    DEFAULT FALSE,
    created_at      TIMESTAMPTZ     NOT NULL    DEFAULT NOW()
);

COMMENT ON TABLE  dim_dates            IS 'Pre-populated calendar dimension for all analytical joins';
COMMENT ON COLUMN dim_dates.date_id    IS 'Surrogate key — used in fact table FK joins';
COMMENT ON COLUMN dim_dates.date       IS 'Calendar date — natural key';


-- ── dim_sectors ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_sectors (
    sector_id       SERIAL          PRIMARY KEY,
    sector_name     VARCHAR(60)     NOT NULL    UNIQUE,
    created_at      TIMESTAMPTZ     NOT NULL    DEFAULT NOW()
);

COMMENT ON TABLE dim_sectors IS 'GICS sector classification lookup';


-- ── dim_companies ─────────────────────────────────────────────────────────────
-- Fundamentals are Type 1 SCD (overwrite on update — we keep only latest snapshot)
-- Production: use Type 2 SCD with effective_from/effective_to for history
CREATE TABLE IF NOT EXISTS dim_companies (
    company_id          SERIAL          PRIMARY KEY,
    ticker              VARCHAR(10)     NOT NULL    UNIQUE,
    company_name        VARCHAR(100),
    sector_id           INT             REFERENCES dim_sectors(sector_id) ON DELETE SET NULL,
    industry            VARCHAR(100),
    market_cap          BIGINT          CHECK (market_cap > 0),
    shares_outstanding  BIGINT,
    beta                NUMERIC(6,4),
    trailing_pe         NUMERIC(10,4),
    forward_pe          NUMERIC(10,4),
    price_to_book       NUMERIC(10,4),
    revenue_growth      NUMERIC(8,4),
    earnings_growth     NUMERIC(8,4),
    profit_margins      NUMERIC(8,4),
    operating_margins   NUMERIC(8,4),
    total_revenue       BIGINT,
    total_debt          BIGINT,
    total_cash          BIGINT,
    debt_to_equity      NUMERIC(10,4),
    return_on_equity    NUMERIC(8,4),
    return_on_assets    NUMERIC(8,4),
    current_ratio       NUMERIC(8,4),
    quick_ratio         NUMERIC(8,4),
    dividend_yield      NUMERIC(8,4),
    fifty_two_week_high NUMERIC(12,4),
    fifty_two_week_low  NUMERIC(12,4),
    average_volume      BIGINT,
    is_benchmark        BOOLEAN         NOT NULL    DEFAULT FALSE,
    updated_at          TIMESTAMPTZ     NOT NULL    DEFAULT NOW()
);

COMMENT ON TABLE  dim_companies              IS 'S&P 500 company master — 29 stocks + 1 benchmark';
COMMENT ON COLUMN dim_companies.is_benchmark IS 'TRUE only for ^GSPC index';
COMMENT ON COLUMN dim_companies.trailing_pe  IS 'Trailing 12-month P/E ratio';


-- ══════════════════════════════════════════════════════════════════════════════
-- FACT TABLES
-- ══════════════════════════════════════════════════════════════════════════════

-- ── fact_daily_prices ─────────────────────────────────────────────────────────
-- Primary analytical table. ~30,000 rows (30 tickers × 1,054 trading days).
-- Contains OHLCV + all computed technical indicators.
CREATE TABLE IF NOT EXISTS fact_daily_prices (
    price_id        BIGSERIAL       PRIMARY KEY,
    ticker          VARCHAR(10)     NOT NULL    REFERENCES dim_companies(ticker) ON DELETE CASCADE,
    date_id         INT             REFERENCES dim_dates(date_id) ON DELETE RESTRICT,
    date            DATE            NOT NULL,

    -- OHLCV
    open            NUMERIC(12,4)   CHECK (open > 0),
    high            NUMERIC(12,4)   CHECK (high > 0),
    low             NUMERIC(12,4)   CHECK (low > 0),
    close           NUMERIC(12,4)   NOT NULL    CHECK (close > 0),
    volume          BIGINT          CHECK (volume >= 0),

    -- Derived price metrics
    daily_return    NUMERIC(10,6),   -- pct change: (close - prev_close) / prev_close
    volatility_30d  NUMERIC(8,4),    -- annualised 30-day rolling std, in %

    -- Momentum
    rsi_14          NUMERIC(8,4)    CHECK (rsi_14 BETWEEN 0 AND 100),
    macd            NUMERIC(12,6),
    macd_signal     NUMERIC(12,6),
    macd_hist       NUMERIC(12,6),

    -- Volatility envelope
    bb_upper        NUMERIC(12,4),
    bb_mid          NUMERIC(12,4),
    bb_lower        NUMERIC(12,4),
    bb_pct_b        NUMERIC(8,4),
    atr_14          NUMERIC(10,4)   CHECK (atr_14 >= 0),

    -- Trend
    sma_50          NUMERIC(12,4),
    sma_200         NUMERIC(12,4),
    golden_cross    SMALLINT        CHECK (golden_cross IN (0, 1)),

    -- Idempotency
    created_at      TIMESTAMPTZ     NOT NULL    DEFAULT NOW(),

    -- Unique constraint: one row per ticker per day
    CONSTRAINT uq_ticker_date UNIQUE (ticker, date)
);

COMMENT ON TABLE  fact_daily_prices             IS 'Daily OHLCV + technical indicators for all 30 tickers';
COMMENT ON COLUMN fact_daily_prices.daily_return IS 'Decimal return: 0.02 = 2%';
COMMENT ON COLUMN fact_daily_prices.golden_cross IS '1 = SMA50 > SMA200 (bullish), 0 = bearish';
COMMENT ON COLUMN fact_daily_prices.volatility_30d IS 'Annualised volatility % (x100 already applied)';


-- ── fact_macro_indicators ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_macro_indicators (
    macro_id        BIGSERIAL       PRIMARY KEY,
    series_id       VARCHAR(20)     NOT NULL,
    date            DATE            NOT NULL,
    value           NUMERIC(12,4),
    description     VARCHAR(100),
    units           VARCHAR(40),
    frequency       VARCHAR(5),     -- D/M/Q/A
    created_at      TIMESTAMPTZ     NOT NULL    DEFAULT NOW(),

    CONSTRAINT uq_series_date UNIQUE (series_id, date)
);

COMMENT ON TABLE  fact_macro_indicators           IS 'FRED macro time series: FEDFUNDS, CPI, GS10, UNRATE, VIXCLS';
COMMENT ON COLUMN fact_macro_indicators.series_id IS 'FRED series identifier e.g. FEDFUNDS';
COMMENT ON COLUMN fact_macro_indicators.frequency IS 'D=Daily M=Monthly Q=Quarterly A=Annual';
