-- vw_fundamental_scorecard.sql
-- Answers BQ5: Fundamental quality screening — value traps, growth leaders
-- Refresh: weekly (fundamentals don't change daily)
-- Used by: Fundamental Analysis dashboard page

CREATE OR REPLACE VIEW vw_fundamental_scorecard AS
WITH fundamentals_base AS (
    SELECT
        dc.ticker,
        dc.company_name,
        ds.sector_name,
        dc.market_cap,
        dc.trailing_pe,
        dc.forward_pe,
        dc.price_to_book,
        dc.revenue_growth,
        dc.earnings_growth,
        dc.profit_margins,
        dc.operating_margins,
        dc.total_revenue,
        dc.total_debt,
        dc.total_cash,
        dc.debt_to_equity,
        dc.return_on_equity,
        dc.return_on_assets,
        dc.current_ratio,
        dc.quick_ratio,
        dc.dividend_yield,
        dc.beta,
        dc.fifty_two_week_high,
        dc.fifty_two_week_low,
        dc.updated_at
    FROM dim_companies dc
    JOIN dim_sectors   ds ON dc.sector_id = ds.sector_id
    WHERE dc.is_benchmark = FALSE
        AND dc.market_cap IS NOT NULL
),
with_latest_price AS (
    SELECT
        fb.*,
        lp.close          AS latest_close,
        lp.volatility_30d AS latest_volatility,
        lp.rsi_14         AS latest_rsi
    FROM fundamentals_base fb
    LEFT JOIN LATERAL (
        SELECT close, volatility_30d, rsi_14
        FROM fact_daily_prices
        WHERE ticker = fb.ticker
        ORDER BY date DESC
        LIMIT 1
    ) lp ON TRUE
),
with_scores AS (
    SELECT
        *,
        CASE
            WHEN market_cap >= 200000000000 THEN 'Mega Cap (>$200B)'
            WHEN market_cap >= 10000000000  THEN 'Large Cap ($10B-$200B)'
            WHEN market_cap >= 2000000000   THEN 'Mid Cap ($2B-$10B)'
            ELSE 'Small Cap (<$2B)'
        END AS market_cap_tier,

        CASE
            WHEN trailing_pe > 30
                AND (revenue_growth IS NULL OR revenue_growth < 0.05)
            THEN TRUE
            ELSE FALSE
        END AS value_trap_flag,

        ROUND((
            LEAST(COALESCE(return_on_equity, 0) * 100, 25) +
            LEAST(COALESCE(profit_margins, 0) * 100, 25) +
            CASE
                WHEN COALESCE(debt_to_equity, 999) < 100 THEN 25
                WHEN COALESCE(debt_to_equity, 999) < 200 THEN 15
                ELSE 5
            END +
            LEAST(GREATEST(COALESCE(revenue_growth, 0) * 100, 0), 25)
        )::NUMERIC, 2) AS quality_score,

        ROUND(trailing_pe::NUMERIC, 2) AS pe_ratio,

        COALESCE(total_cash, 0) - COALESCE(total_debt, 0) AS net_cash,

        CASE
            WHEN fifty_two_week_high IS NOT NULL
                AND fifty_two_week_low IS NOT NULL
                AND (fifty_two_week_high - fifty_two_week_low) > 0
            THEN ROUND(
                ((latest_close - fifty_two_week_low)
                / (fifty_two_week_high - fifty_two_week_low) * 100)::NUMERIC, 2)
            ELSE NULL
        END AS price_52w_position_pct

    FROM with_latest_price
),

-- ── FIX: Compute sector median P/E in a separate CTE ─────────────────────
-- PERCENTILE_CONT cannot be used as a window function in PostgreSQL.
-- We aggregate first, then join back — equivalent result.
sector_median_pe AS (
    SELECT
        sector_name,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY trailing_pe) AS sector_median_pe
    FROM with_scores
    WHERE trailing_pe IS NOT NULL
    GROUP BY sector_name
),

with_sector_ranks AS (
    SELECT
        ws.*,
        smp.sector_median_pe,
        RANK() OVER (
            PARTITION BY ws.sector_name
            ORDER BY ws.quality_score DESC
        ) AS quality_rank_in_sector,
        RANK() OVER (
            ORDER BY ws.market_cap DESC
        ) AS market_cap_rank
    FROM with_scores ws
    LEFT JOIN sector_median_pe smp ON ws.sector_name = smp.sector_name
)
SELECT
    ticker,
    company_name,
    sector_name,
    market_cap_tier,
    market_cap_rank,
    ROUND((market_cap / 1e9)::NUMERIC, 2)        AS market_cap_billions,
    latest_close,
    latest_volatility                             AS volatility_30d,
    latest_rsi                                    AS rsi_14,
    pe_ratio,
    ROUND(forward_pe::NUMERIC, 2)                 AS forward_pe,
    ROUND(sector_median_pe::NUMERIC, 2)           AS sector_median_pe,
    ROUND((pe_ratio - sector_median_pe)::NUMERIC, 2)
                                                  AS pe_vs_sector_median,
    ROUND(price_to_book::NUMERIC, 4)              AS price_to_book,
    ROUND((revenue_growth * 100)::NUMERIC, 2)     AS revenue_growth_pct,
    ROUND((earnings_growth * 100)::NUMERIC, 2)    AS earnings_growth_pct,
    ROUND((profit_margins * 100)::NUMERIC, 2)     AS profit_margin_pct,
    ROUND((operating_margins * 100)::NUMERIC, 2)  AS operating_margin_pct,
    ROUND((return_on_equity * 100)::NUMERIC, 2)   AS roe_pct,
    ROUND((return_on_assets * 100)::NUMERIC, 2)   AS roa_pct,
    ROUND(debt_to_equity::NUMERIC, 2)             AS debt_to_equity,
    ROUND((net_cash / 1e9)::NUMERIC, 2)           AS net_cash_billions,
    ROUND(current_ratio::NUMERIC, 2)              AS current_ratio,
    ROUND(beta::NUMERIC, 4)                       AS beta,
    ROUND((dividend_yield * 100)::NUMERIC, 2)     AS dividend_yield_pct,
    quality_score,
    quality_rank_in_sector,
    price_52w_position_pct,
    value_trap_flag,
    CASE
        WHEN quality_score >= 60 AND value_trap_flag = FALSE THEN 'high_quality'
        WHEN quality_score >= 40 AND value_trap_flag = FALSE THEN 'moderate_quality'
        WHEN value_trap_flag = TRUE                          THEN 'value_trap_risk'
        ELSE 'low_quality'
    END AS fundamental_signal,
    updated_at
FROM with_sector_ranks
ORDER BY quality_score DESC;

COMMENT ON VIEW vw_fundamental_scorecard IS
    'Company fundamental quality scores, value trap detection, and sector-relative valuation. Used in Fundamental Analysis page.';
