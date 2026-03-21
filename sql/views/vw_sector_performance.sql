-- vw_sector_performance.sql
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
