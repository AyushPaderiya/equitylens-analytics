-- vw_stock_performance.sql
-- Answers BQ1: Best risk-adjusted returns by stock and sector
-- Refresh: runs after every daily load
-- Used by: Executive Summary, Sector Analysis dashboard pages

CREATE OR REPLACE VIEW vw_stock_performance AS
WITH daily_base AS (
    SELECT
        fdp.ticker,
        dc.company_name,
        ds.sector_name,
        dd.date,
        dd.year,
        dd.quarter,
        dd.month,
        dd.month_name,
        fdp.close,
        fdp.volume,
        fdp.daily_return,
        fdp.volatility_30d,
        fdp.rsi_14,
        fdp.golden_cross,
        fdp.sma_50,
        fdp.sma_200
    FROM fact_daily_prices    fdp
    JOIN dim_companies        dc  ON fdp.ticker   = dc.ticker
    JOIN dim_sectors          ds  ON dc.sector_id = ds.sector_id
    JOIN dim_dates            dd  ON fdp.date_id  = dd.date_id
    WHERE dc.is_benchmark = FALSE
        AND fdp.close IS NOT NULL
),
with_cumulative AS (
    SELECT
        *,
        -- Cumulative return from first available date per ticker
        EXP(
            SUM(LN(1 + COALESCE(daily_return, 0)))
            OVER (PARTITION BY ticker ORDER BY date)
        ) - 1 AS cumulative_return,

        -- 30-day rolling Sharpe ratio (annualised, zero risk-free rate)
        CASE
            WHEN STDDEV(daily_return)
                OVER (
                    PARTITION BY ticker
                    ORDER BY date
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ) > 0
            THEN (
                AVG(daily_return)
                    OVER (
                        PARTITION BY ticker
                        ORDER BY date
                        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                    )
                /
                STDDEV(daily_return)
                    OVER (
                        PARTITION BY ticker
                        ORDER BY date
                        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                    )
            ) * SQRT(252)
            ELSE NULL
        END AS sharpe_30d,

        -- 30-day rolling max drawdown
        MAX(close)
            OVER (
                PARTITION BY ticker
                ORDER BY date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) AS rolling_30d_high,

        -- All-time high up to each date (for drawdown from ATH)
        MAX(close)
            OVER (
                PARTITION BY ticker
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS all_time_high,

        -- Rank within sector by latest close (for sector leaderboard)
        RANK()
            OVER (
                PARTITION BY sector_name, date
                ORDER BY close DESC
            ) AS rank_in_sector

    FROM daily_base
)
SELECT
    ticker,
    company_name,
    sector_name,
    date,
    year,
    quarter,
    month,
    month_name,
    ROUND(close::NUMERIC, 4)                                    AS close,
    volume,
    ROUND(daily_return::NUMERIC, 6)                             AS daily_return,
    ROUND((daily_return * 100)::NUMERIC, 4)                     AS daily_return_pct,
    ROUND(cumulative_return::NUMERIC, 6)                        AS cumulative_return,
    ROUND((cumulative_return * 100)::NUMERIC, 4)                AS cumulative_return_pct,
    ROUND(volatility_30d::NUMERIC, 4)                           AS volatility_30d,
    ROUND(sharpe_30d::NUMERIC, 4)                               AS sharpe_30d,
    ROUND(((close - rolling_30d_high) / NULLIF(rolling_30d_high, 0) * 100)::NUMERIC, 4)
                                                                AS drawdown_30d_pct,
    ROUND(((close - all_time_high) / NULLIF(all_time_high, 0) * 100)::NUMERIC, 4)
                                                                AS drawdown_from_ath_pct,
    ROUND(all_time_high::NUMERIC, 4)                            AS all_time_high,
    rsi_14,
    golden_cross,
    sma_50,
    sma_200,
    rank_in_sector,
    -- Simple signal label for dashboard filters
    CASE
        WHEN golden_cross = 1 AND rsi_14 BETWEEN 45 AND 70 THEN 'bullish'
        WHEN golden_cross = 0 AND rsi_14 < 40              THEN 'bearish'
        WHEN rsi_14 < 30                                   THEN 'oversold'
        WHEN rsi_14 > 70                                   THEN 'overbought'
        ELSE 'neutral'
    END AS market_signal
FROM with_cumulative
ORDER BY ticker, date;

COMMENT ON VIEW vw_stock_performance IS
    'Daily stock performance with cumulative returns, Sharpe ratio, drawdown, and market signals. Primary view for Executive Summary page.';
