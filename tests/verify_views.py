import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sqlalchemy import create_engine, text
from config.settings import settings

engine = create_engine(settings.DATABASE_URL, connect_args={"sslmode": "require"})

queries = {
    "BQ1 — Sector Sharpe (last 90 days)": """
        SELECT sector_name,
               ROUND(AVG(sharpe_30d)::NUMERIC, 3)         AS avg_sharpe,
               ROUND(AVG(avg_volatility_pct)::NUMERIC, 2) AS avg_vol
        FROM vw_sector_performance
        WHERE date >= CURRENT_DATE - INTERVAL '90 days'
        GROUP BY sector_name
        ORDER BY avg_sharpe DESC
    """,

    "BQ5 — Value Trap Candidates": """
        SELECT ticker, company_name, pe_ratio,
               revenue_growth_pct, quality_score, fundamental_signal
        FROM vw_fundamental_scorecard
        WHERE value_trap_flag = TRUE
        ORDER BY pe_ratio DESC
    """,

    "BQ3 — Top Volume Anomalies (last 30 days)": """
        SELECT ticker, date, volume_ratio,
               spike_severity, institutional_signal
        FROM vw_volume_anomalies
        WHERE is_anomaly = TRUE
          AND date >= CURRENT_DATE - INTERVAL '30 days'
        ORDER BY volume_ratio DESC
        LIMIT 10
    """,

    "BQ6 — Forecast Input Check (AAPL last 5 rows)": """
        SELECT ds, y, rsi_14, vix, fed_funds_rate
        FROM vw_forecast_input
        WHERE ticker = 'AAPL'
        ORDER BY ds DESC
        LIMIT 5
    """,

    "Row counts — all tables": """
        SELECT 'dim_dates'             AS tbl, COUNT(*) AS rows FROM dim_dates
        UNION ALL
        SELECT 'dim_sectors',                  COUNT(*)         FROM dim_sectors
        UNION ALL
        SELECT 'dim_companies',                COUNT(*)         FROM dim_companies
        UNION ALL
        SELECT 'fact_daily_prices',            COUNT(*)         FROM fact_daily_prices
        UNION ALL
        SELECT 'fact_macro_indicators',        COUNT(*)         FROM fact_macro_indicators
        ORDER BY tbl
    """,
}

with engine.connect() as conn:
    for name, sql in queries.items():
        print(f"\n{'='*60}")
        print(f"  {name}")
        print(f"{'='*60}")
        result = conn.execute(text(sql))
        rows = result.fetchall()
        cols = result.keys()

        # Print header
        print("  " + " | ".join(f"{c:25s}" for c in cols))
        print("  " + "-" * (28 * len(list(cols))))

        # Print rows
        for row in rows:
            print("  " + " | ".join(f"{str(v):25s}" for v in row))

        if not rows:
            print("  (no rows returned)")
