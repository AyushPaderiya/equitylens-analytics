-- vw_macro_overlay.sql
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
