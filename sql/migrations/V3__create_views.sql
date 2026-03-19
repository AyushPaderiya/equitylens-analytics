-- ── View 1: vw_sector_performance ────────────────────────────────────────────
-- Answers BQ1: Which sectors delivered the best risk-adjusted returns?
-- Used in: Executive Summary, Sector Analysis pages
CREATE OR REPLACE VIEW vw_sector_performance AS
WITH daily_sector AS (
    SELECT
        dd.date,
        dd.year,
        dd.quarter,
        ds.sector_name,
        AVG(fdp.close)          AS avg_close,
        AVG(fdp.daily_return)   AS avg_daily_return,
        AVG(fdp.volatility_30d) AS avg_volatility,
        COUNT(fdp.ticker)       AS active_stocks
    FROM fact_daily_prices    fdp
    JOIN dim_companies        dc  ON fdp.ticker    = dc.ticker
    JOIN dim_sectors          ds  ON dc.sector_id  = ds.sector_id
    JOIN dim_dates            dd  ON fdp.date_id   = dd.date_id
    WHERE dc.is_benchmark = FALSE
    GROUP BY dd.date, dd.year, dd.quarter, ds.sector_name
),
sector_stats AS (
    SELECT
        sector_name,
        date,
        year,
        quarter,
        avg_daily_return,
        avg_volatility,
        active_stocks,
        -- Cumulative return (base = first available date per sector)
        EXP(SUM(LN(1 + COALESCE(avg_daily_return, 0)))
            OVER (PARTITION BY sector_name ORDER BY date)) - 1
            AS cumulative_return,
        -- 30-day rolling Sharpe (annualised, risk-free = 0 for simplicity)
        CASE
            WHEN STDDEV(avg_daily_return)
                     OVER (PARTITION BY sector_name
                           ORDER BY date
                           ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) > 0
            THEN (AVG(avg_daily_return)
                     OVER (PARTITION BY sector_name
                           ORDER BY date
                           ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
                 / STDDEV(avg_daily_return)
                     OVER (PARTITION BY sector_name
                           ORDER BY date
                           ROWS BETWEEN 29 PRECEDING AND CURRENT ROW))
                 * SQRT(252)
            ELSE NULL
        END AS sharpe_30d
    FROM daily_sector
)
SELECT
    sector_name,
    date,
    year,
    quarter,
    ROUND(avg_daily_return::NUMERIC, 6)   AS avg_daily_return,
    ROUND(avg_volatility::NUMERIC, 4)     AS avg_volatility_pct,
    ROUND(cumulative_return::NUMERIC, 6)  AS cumulative_return,
    ROUND(sharpe_30d::NUMERIC, 4)         AS sharpe_30d,
    active_stocks
FROM sector_stats
ORDER BY sector_name, date;

COMMENT ON VIEW vw_sector_performance IS
    'Daily sector-level returns, cumulative performance, and 30-day rolling Sharpe ratio';


-- ── View 2: vw_risk_metrics ───────────────────────────────────────────────────
-- Answers BQ2, BQ4: Rolling volatility, drawdown, RSI signals
-- Used in: Risk Analysis, Deep Dive pages
CREATE OR REPLACE VIEW vw_risk_metrics AS
WITH price_series AS (
    SELECT
        fdp.ticker,
        dc.company_name,
        ds.sector_name,
        fdp.date,
        fdp.close,
        fdp.volume,
        fdp.daily_return,
        fdp.volatility_30d,
        fdp.rsi_14,
        fdp.macd,
        fdp.macd_signal,
        fdp.macd_hist,
        fdp.sma_50,
        fdp.sma_200,
        fdp.golden_cross,
        fdp.atr_14,
        -- Running max close (for drawdown calc)
        MAX(fdp.close) OVER (
            PARTITION BY fdp.ticker
            ORDER BY fdp.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_max_close
    FROM fact_daily_prices fdp
    JOIN dim_companies     dc ON fdp.ticker    = dc.ticker
    JOIN dim_sectors       ds ON dc.sector_id  = ds.sector_id
    WHERE dc.is_benchmark = FALSE
),
with_drawdown AS (
    SELECT
        *,
        -- Drawdown: how far below the all-time-high is current price?
        ROUND(((close - running_max_close) / running_max_close * 100)::NUMERIC, 4)
            AS drawdown_pct,
        -- 30-day average volume for anomaly baseline
        AVG(volume) OVER (
            PARTITION BY ticker
            ORDER BY date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS avg_volume_30d
    FROM price_series
)
SELECT
    ticker,
    company_name,
    sector_name,
    date,
    close,
    ROUND(daily_return::NUMERIC, 6)     AS daily_return,
    volatility_30d,
    rsi_14,
    macd,
    macd_signal,
    macd_hist,
    sma_50,
    sma_200,
    golden_cross,
    atr_14,
    drawdown_pct,
    ROUND(avg_volume_30d::NUMERIC, 0)   AS avg_volume_30d,
    volume,
    -- Volume spike flag: today's volume > 2x 30-day average
    CASE WHEN avg_volume_30d > 0
         AND volume > (avg_volume_30d * 2)
         THEN TRUE ELSE FALSE
    END AS volume_spike_flag,
    -- RSI classification
    CASE
        WHEN rsi_14 < 30  THEN 'oversold'
        WHEN rsi_14 < 45  THEN 'mild_oversold'
        WHEN rsi_14 <= 55 THEN 'neutral'
        WHEN rsi_14 <= 70 THEN 'mild_overbought'
        ELSE 'overbought'
    END AS rsi_signal
FROM with_drawdown
ORDER BY ticker, date;

COMMENT ON VIEW vw_risk_metrics IS
    'Per-stock risk metrics: drawdown, volatility, RSI signals, volume spike detection';


-- ── View 3: vw_macro_overlay ──────────────────────────────────────────────────
-- Answers BQ3: How do macro conditions correlate with market performance?
-- Used in: Macro Overlay page
CREATE OR REPLACE VIEW vw_macro_overlay AS
WITH monthly_market AS (
    -- Aggregate daily prices to monthly for macro alignment
    -- (FRED series are monthly; VIX is daily)
    SELECT
        DATE_TRUNC('month', fdp.date)::DATE     AS month,
        ds.sector_name,
        AVG(fdp.close)                          AS avg_close,
        AVG(fdp.daily_return)                   AS avg_daily_return,
        AVG(fdp.volatility_30d)                 AS avg_volatility
    FROM fact_daily_prices   fdp
    JOIN dim_companies       dc ON fdp.ticker   = dc.ticker
    JOIN dim_sectors         ds ON dc.sector_id = ds.sector_id
    WHERE dc.is_benchmark = FALSE
    GROUP BY DATE_TRUNC('month', fdp.date)::DATE, ds.sector_name
),
macro_monthly AS (
    -- Pivot FRED series into columns for easy correlation analysis
    SELECT
        DATE_TRUNC('month', date)::DATE         AS month,
        MAX(CASE WHEN series_id = 'FEDFUNDS'  THEN value END) AS fed_funds_rate,
        MAX(CASE WHEN series_id = 'CPIAUCSL'  THEN value END) AS cpi,
        MAX(CASE WHEN series_id = 'GS10'      THEN value END) AS treasury_10y,
        MAX(CASE WHEN series_id = 'UNRATE'    THEN value END) AS unemployment,
        MAX(CASE WHEN series_id = 'VIXCLS'    THEN value END) AS vix
    FROM fact_macro_indicators
    GROUP BY DATE_TRUNC('month', date)::DATE
)
SELECT
    mm.month,
    mm.sector_name,
    ROUND(mm.avg_daily_return::NUMERIC, 6)  AS avg_daily_return,
    ROUND(mm.avg_volatility::NUMERIC, 4)    AS avg_volatility_pct,
    mac.fed_funds_rate,
    mac.cpi,
    mac.treasury_10y,
    mac.unemployment,
    mac.vix
FROM monthly_market mm
LEFT JOIN macro_monthly mac ON mm.month = mac.month
ORDER BY mm.month, mm.sector_name;

COMMENT ON VIEW vw_macro_overlay IS
    'Monthly sector performance aligned with FRED macro indicators for correlation analysis';
