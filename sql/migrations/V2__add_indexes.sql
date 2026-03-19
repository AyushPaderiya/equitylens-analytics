-- ── fact_daily_prices indexes ─────────────────────────────────────────────────

-- Most common filter: WHERE ticker = 'AAPL' ORDER BY date
-- Covers every single-stock time series query
CREATE INDEX IF NOT EXISTS idx_fdp_ticker_date
    ON fact_daily_prices (ticker, date DESC);

-- Sector-level analysis: join with dim_companies, group by date range
CREATE INDEX IF NOT EXISTS idx_fdp_date
    ON fact_daily_prices (date DESC);

-- RSI signal screening: WHERE rsi_14 < 30 (oversold scanner)
CREATE INDEX IF NOT EXISTS idx_fdp_rsi
    ON fact_daily_prices (rsi_14)
    WHERE rsi_14 IS NOT NULL;

-- Golden cross filter: WHERE golden_cross = 1
CREATE INDEX IF NOT EXISTS idx_fdp_golden_cross
    ON fact_daily_prices (golden_cross, ticker);

-- Volume anomaly detection: ORDER BY volume DESC
CREATE INDEX IF NOT EXISTS idx_fdp_volume
    ON fact_daily_prices (volume DESC)
    WHERE volume IS NOT NULL;

-- date_id FK join (for date dimension lookups)
CREATE INDEX IF NOT EXISTS idx_fdp_date_id
    ON fact_daily_prices (date_id);


-- ── fact_macro_indicators indexes ─────────────────────────────────────────────

-- Time-range queries per series: WHERE series_id = 'FEDFUNDS'
CREATE INDEX IF NOT EXISTS idx_fmi_series_date
    ON fact_macro_indicators (series_id, date DESC);

-- Date-range overlay queries
CREATE INDEX IF NOT EXISTS idx_fmi_date
    ON fact_macro_indicators (date DESC);


-- ── dim_companies indexes ─────────────────────────────────────────────────────

-- Sector join (used in every sector-level aggregation)
CREATE INDEX IF NOT EXISTS idx_dc_sector_id
    ON dim_companies (sector_id);

-- Fundamental screening: ORDER BY market_cap DESC
CREATE INDEX IF NOT EXISTS idx_dc_market_cap
    ON dim_companies (market_cap DESC)
    WHERE market_cap IS NOT NULL;

-- P/E screening: WHERE trailing_pe IS NOT NULL ORDER BY trailing_pe
CREATE INDEX IF NOT EXISTS idx_dc_trailing_pe
    ON dim_companies (trailing_pe)
    WHERE trailing_pe IS NOT NULL;


-- ── dim_dates indexes ─────────────────────────────────────────────────────────

-- Quarter and year filters (common in BI tools)
CREATE INDEX IF NOT EXISTS idx_dd_year_quarter
    ON dim_dates (year, quarter);

-- Month grouping
CREATE INDEX IF NOT EXISTS idx_dd_year_month
    ON dim_dates (year, month);
