"""
Loads transformed data into PostgreSQL using upsert logic.
Every load is idempotent — re-running never creates duplicates.

Production context: In a dbt + Airflow stack, this would be
a dbt incremental model with unique_key configured.
Here we use SQLAlchemy Core with PostgreSQL's ON CONFLICT DO UPDATE.
"""

from datetime import date, timedelta
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine

from config.settings import settings
from config.logging_config import setup_logging

logger = setup_logging("loader.postgres")


class PostgresLoader:

    def __init__(self):
        if not settings.DATABASE_URL:
            raise EnvironmentError("DATABASE_URL missing from .env")
        self.engine: Engine = create_engine(
            settings.DATABASE_URL,
            pool_size=5,
            max_overflow=2,
            pool_pre_ping=True,      # Reconnect if Neon connection drops
            connect_args={"sslmode": "require"},
        )
        logger.info("PostgresLoader initialised — connection pool ready")

    # ── Schema migrations ─────────────────────────────────────────────────────

    def run_migration(self, filepath: str) -> None:
        """
        Execute a SQL migration file.
        Wraps in a transaction — rolls back entirely on any error.
        """
        with open(filepath, "r", encoding="utf-8") as f:
            sql = f.read()

        logger.info(f"Running migration: {filepath}")
        with self.engine.begin() as conn:
            conn.execute(text(sql))
        logger.info(f"✓ Migration complete: {filepath}")

    # ── dim_dates ──────────────────────────────────────────────────────────────

    def load_dim_dates(self, start: str = "2020-01-01", end: str = "2030-12-31") -> int:
        logger.info(f"Loading dim_dates: {start} → {end}")

        rows = []
        d = date.fromisoformat(start)
        end_d = date.fromisoformat(end)

        while d <= end_d:
            rows.append({
                "date":          d,
                "year":          d.year,
                "quarter":       (d.month - 1) // 3 + 1,
                "month":         d.month,
                "week_of_year":  int(d.isocalendar()[1]),
                "day_of_week":   d.weekday(),
                "day_of_year":   d.timetuple().tm_yday,
                "month_name":    d.strftime("%B"),
                "quarter_label": f"Q{(d.month-1)//3+1} {str(d.year)[2:]}",
                "is_weekend":    d.weekday() >= 5,
            })
            d += timedelta(days=1)

        df = pd.DataFrame(rows)

        df.to_sql(
            "dim_dates",
            con=self.engine,
            if_exists="append",
            index=False,
            method="multi",      # Sends all rows in one statement
            chunksize=1000,      # 4 chunks of 1000 = 4 round trips total
        )

        logger.info(f"✓ dim_dates loaded: {len(rows)} date rows")
        return len(rows)

    # ── dim_sectors ────────────────────────────────────────────────────────────

    def load_dim_sectors(self) -> int:
        """Upsert sector names. Returns count of rows processed."""
        sectors = list(settings.TICKERS.keys())
        logger.info(f"Loading dim_sectors: {sectors}")

        with self.engine.begin() as conn:
            stmt = text("""
                INSERT INTO dim_sectors (sector_name)
                VALUES (:sector_name)
                ON CONFLICT (sector_name) DO NOTHING
            """)
            conn.execute(stmt, [{"sector_name": s} for s in sectors])

        logger.info(f"✓ dim_sectors loaded: {len(sectors)} sectors")
        return len(sectors)

    # ── dim_companies ──────────────────────────────────────────────────────────

    def load_dim_companies(self, fundamentals: dict) -> int:
        """
        Upsert company dimension from yfinance fundamentals dict.
        Type 1 SCD: always overwrites with the latest snapshot.
        """
        logger.info(f"Loading dim_companies: {len(fundamentals)} companies")

        # Build sector_id lookup map
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT sector_id, sector_name FROM dim_sectors"))
            sector_map = {row.sector_name: row.sector_id for row in result}

        rows, skipped = [], 0
        for ticker, data in fundamentals.items():
            if not data:
                skipped += 1
                continue

            sector_id = sector_map.get(data.get("sector"))

            rows.append({
                "ticker":             ticker,
                "company_name":       data.get("shortName"),
                "sector_id":          sector_id,
                "industry":           data.get("industry"),
                "market_cap":         data.get("marketCap"),
                "shares_outstanding": data.get("sharesOutstanding"),
                "beta":               data.get("beta"),
                "trailing_pe":        data.get("trailingPE"),
                "forward_pe":         data.get("forwardPE"),
                "price_to_book":      data.get("priceToBook"),
                "revenue_growth":     data.get("revenueGrowth"),
                "earnings_growth":    data.get("earningsGrowth"),
                "profit_margins":     data.get("profitMargins"),
                "operating_margins":  data.get("operatingMargins"),
                "total_revenue":      data.get("totalRevenue"),
                "total_debt":         data.get("totalDebt"),
                "total_cash":         data.get("totalCash"),
                "debt_to_equity":     data.get("debtToEquity"),
                "return_on_equity":   data.get("returnOnEquity"),
                "return_on_assets":   data.get("returnOnAssets"),
                "current_ratio":      data.get("currentRatio"),
                "quick_ratio":        data.get("quickRatio"),
                "dividend_yield":     data.get("dividendYield"),
                "fifty_two_week_high":data.get("fiftyTwoWeekHigh"),
                "fifty_two_week_low": data.get("fiftyTwoWeekLow"),
                "average_volume":     data.get("averageVolume"),
                "is_benchmark":       ticker == settings.BENCHMARK_TICKER,
            })

        with self.engine.begin() as conn:
            stmt = text("""
                INSERT INTO dim_companies (
                    ticker, company_name, sector_id, industry,
                    market_cap, shares_outstanding, beta,
                    trailing_pe, forward_pe, price_to_book,
                    revenue_growth, earnings_growth, profit_margins,
                    operating_margins, total_revenue, total_debt, total_cash,
                    debt_to_equity, return_on_equity, return_on_assets,
                    current_ratio, quick_ratio, dividend_yield,
                    fifty_two_week_high, fifty_two_week_low,
                    average_volume, is_benchmark, updated_at
                ) VALUES (
                    :ticker, :company_name, :sector_id, :industry,
                    :market_cap, :shares_outstanding, :beta,
                    :trailing_pe, :forward_pe, :price_to_book,
                    :revenue_growth, :earnings_growth, :profit_margins,
                    :operating_margins, :total_revenue, :total_debt, :total_cash,
                    :debt_to_equity, :return_on_equity, :return_on_assets,
                    :current_ratio, :quick_ratio, :dividend_yield,
                    :fifty_two_week_high, :fifty_two_week_low,
                    :average_volume, :is_benchmark, NOW()
                )
                ON CONFLICT (ticker) DO UPDATE SET
                    company_name        = EXCLUDED.company_name,
                    sector_id           = EXCLUDED.sector_id,
                    market_cap          = EXCLUDED.market_cap,
                    beta                = EXCLUDED.beta,
                    trailing_pe         = EXCLUDED.trailing_pe,
                    forward_pe          = EXCLUDED.forward_pe,
                    revenue_growth      = EXCLUDED.revenue_growth,
                    profit_margins      = EXCLUDED.profit_margins,
                    debt_to_equity      = EXCLUDED.debt_to_equity,
                    return_on_equity    = EXCLUDED.return_on_equity,
                    fifty_two_week_high = EXCLUDED.fifty_two_week_high,
                    fifty_two_week_low  = EXCLUDED.fifty_two_week_low,
                    updated_at          = NOW()
            """)
            conn.execute(stmt, rows)

        logger.info(f"✓ dim_companies loaded: {len(rows)} rows | {skipped} skipped")
        return len(rows)

    # ── fact_daily_prices ──────────────────────────────────────────────────────

    def load_fact_prices(self, df: pd.DataFrame) -> int:
        """
        Upsert daily prices + technical indicators for one ticker.
        ON CONFLICT (ticker, date) → updates all mutable columns.
        Batched in 500-row chunks to avoid Neon free-tier timeouts.
        """
        if df is None or df.empty:
            return 0

        ticker = df["ticker"].iloc[0]

        # Build date_id lookup for this ticker's date range
        min_d, max_d = df["date"].min(), df["date"].max()
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT date_id, date FROM dim_dates
                WHERE date BETWEEN :min_d AND :max_d
            """), {"min_d": min_d, "max_d": max_d})
            date_map = {row.date: row.date_id for row in result}

        df = df.copy()
        df["date_id"] = df["date"].map(date_map)

        # Columns to load — explicit list, never SELECT *
        cols = [
            "ticker", "date_id", "date", "open", "high", "low", "close",
            "volume", "daily_return", "volatility_30d", "rsi_14",
            "macd", "macd_signal", "macd_hist", "bb_upper", "bb_mid",
            "bb_lower", "bb_pct_b", "atr_14", "sma_50", "sma_200",
            "golden_cross",
        ]
        available = [c for c in cols if c in df.columns]
        records = df[available].where(pd.notnull(df[available]), None).to_dict("records")

        inserted = 0
        chunk_size = 500
        for i in range(0, len(records), chunk_size):
            chunk = records[i : i + chunk_size]
            with self.engine.begin() as conn:
                stmt = text(f"""
                    INSERT INTO fact_daily_prices ({", ".join(available)})
                    VALUES ({", ".join(":" + c for c in available)})
                    ON CONFLICT (ticker, date) DO UPDATE SET
                        close          = EXCLUDED.close,
                        volume         = EXCLUDED.volume,
                        daily_return   = EXCLUDED.daily_return,
                        volatility_30d = EXCLUDED.volatility_30d,
                        rsi_14         = EXCLUDED.rsi_14,
                        macd           = EXCLUDED.macd,
                        macd_signal    = EXCLUDED.macd_signal,
                        macd_hist      = EXCLUDED.macd_hist,
                        bb_upper       = EXCLUDED.bb_upper,
                        bb_lower       = EXCLUDED.bb_lower,
                        sma_50         = EXCLUDED.sma_50,
                        sma_200        = EXCLUDED.sma_200,
                        golden_cross   = EXCLUDED.golden_cross,
                        atr_14         = EXCLUDED.atr_14
                """)
                conn.execute(stmt, chunk)
            inserted += len(chunk)

        logger.info(f"✓ {ticker}: {inserted} price rows upserted")
        return inserted

    # ── fact_macro_indicators ─────────────────────────────────────────────────

    def load_fact_macro(self, series_data: dict) -> int:
        """
        Upsert all FRED series into fact_macro_indicators.
        series_data: {series_id: DataFrame} from FredExtractor.load_series()
        """
        total = 0
        for series_id, df in series_data.items():
            if df is None or df.empty:
                logger.warning(f"Skipping {series_id} — no data")
                continue

            records = []
            for _, row in df.iterrows():
                records.append({
                    "series_id":   series_id,
                    "date":        row["date"].date() if hasattr(row["date"], "date") else row["date"],
                    "value":       row[series_id.lower()] if series_id.lower() in row else None,
                    "description": row.get("description"),
                    "units":       row.get("units"),
                    "frequency":   str(df.attrs.get("frequency", "M")) if hasattr(df, "attrs") else "M",
                })

            with self.engine.begin() as conn:
                stmt = text("""
                    INSERT INTO fact_macro_indicators
                        (series_id, date, value, description, units, frequency)
                    VALUES
                        (:series_id, :date, :value, :description, :units, :frequency)
                    ON CONFLICT (series_id, date) DO UPDATE SET
                        value = EXCLUDED.value
                """)
                conn.execute(stmt, records)

            logger.info(f"✓ {series_id}: {len(records)} macro rows upserted")
            total += len(records)

        return total

    # ── Validation queries ────────────────────────────────────────────────────

    def validate_load(self) -> dict:
        """
        Post-load sanity checks. Returns row counts per table.
        Run after every full pipeline load to confirm data integrity.
        Production equivalent: dbt test suite or Great Expectations checkpoint.
        """
        checks = {
            "dim_dates":             "SELECT COUNT(*) FROM dim_dates",
            "dim_sectors":           "SELECT COUNT(*) FROM dim_sectors",
            "dim_companies":         "SELECT COUNT(*) FROM dim_companies",
            "fact_daily_prices":     "SELECT COUNT(*) FROM fact_daily_prices",
            "fact_macro_indicators": "SELECT COUNT(*) FROM fact_macro_indicators",
            "distinct_tickers":      "SELECT COUNT(DISTINCT ticker) FROM fact_daily_prices",
            "date_range":            "SELECT MIN(date)::TEXT || ' → ' || MAX(date)::TEXT FROM fact_daily_prices",
            "null_close_count":      "SELECT COUNT(*) FROM fact_daily_prices WHERE close IS NULL",
            "orphan_prices":         """
                SELECT COUNT(*) FROM fact_daily_prices fdp
                LEFT JOIN dim_companies dc ON fdp.ticker = dc.ticker
                WHERE dc.ticker IS NULL
            """,
        }

        results = {}
        with self.engine.connect() as conn:
            for check_name, query in checks.items():
                result = conn.execute(text(query)).fetchone()
                results[check_name] = result[0]
                logger.info(f"  ✓ {check_name:30s}: {result[0]}")

        # Hard assertions — pipeline fails loudly if data is wrong
        assert results["null_close_count"] == 0, \
            f"Data quality FAIL: {results['null_close_count']} NULL close prices found"
        assert results["orphan_prices"] == 0, \
            f"Referential integrity FAIL: {results['orphan_prices']} orphan price rows"
        assert results["distinct_tickers"] >= 29, \
            f"Completeness FAIL: only {results['distinct_tickers']} tickers loaded (expected 29+)"

        logger.info("✓ All data quality checks passed")
        return results
