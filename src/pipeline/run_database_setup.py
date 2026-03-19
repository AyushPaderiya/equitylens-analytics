"""
Full database setup pipeline.
Runs once to initialise the database, then safely re-runs
on subsequent days as an incremental upsert loader.

Usage:
  python -m src.pipeline.run_database_setup          # Full setup
  python -m src.pipeline.run_database_setup --load-only  # Skip migrations, load data only
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

from config.settings import settings, BASE_DIR
from config.logging_config import setup_logging
from src.extractors import YFinanceExtractor, FredExtractor
from src.transformers.technical_indicators import TechnicalIndicatorsTransformer
from src.loaders import PostgresLoader

logger = setup_logging("pipeline.db_setup")

MIGRATIONS = [
    BASE_DIR / "sql" / "migrations" / "V1__create_schema.sql",
    BASE_DIR / "sql" / "migrations" / "V2__add_indexes.sql",
    BASE_DIR / "sql" / "migrations" / "V3__create_views.sql",
]


def run_migrations(loader: PostgresLoader) -> None:
    logger.info("── Running schema migrations ──")
    for migration_file in MIGRATIONS:
        if not migration_file.exists():
            logger.error(f"Migration file not found: {migration_file}")
            sys.exit(1)
        loader.run_migration(str(migration_file))
    logger.info("✓ All migrations complete")


def run_seed_data(loader: PostgresLoader) -> None:
    """Populate dimension tables that don't depend on API data."""
    logger.info("── Seeding dimension tables ──")
    date_count = loader.load_dim_dates(start="2020-01-01", end="2030-12-31")
    sector_count = loader.load_dim_sectors()
    logger.info(f"✓ Seed complete: {date_count} dates | {sector_count} sectors")


def run_data_load(loader: PostgresLoader) -> None:
    """Load all Bronze layer data into PostgreSQL."""

    # ── Step 1: Load company fundamentals → dim_companies ────────────────────
    logger.info("── Loading dim_companies ──")
    yf = YFinanceExtractor()

    fundamentals = {}
    for ticker in settings.ALL_TICKERS:
        data = yf.load_fundamentals(ticker)
        fundamentals[ticker] = data

    # Add benchmark with minimal data
    fundamentals[settings.BENCHMARK_TICKER] = {
        "shortName": "S&P 500 Index",
        "sector": None,
        "industry": None,
        "marketCap": None,
        "beta": 1.0,
    }

    loader.load_dim_companies(fundamentals)

    # ── Step 2: Transform + load prices → fact_daily_prices ──────────────────
    logger.info("── Loading fact_daily_prices ──")
    transformer = TechnicalIndicatorsTransformer()
    total_price_rows = 0

    # Load benchmark (^GSPC) separately — no technical indicators needed
    benchmark_df = yf.load_prices(settings.BENCHMARK_TICKER)
    if benchmark_df is not None:
        benchmark_df["date"] = benchmark_df.index if "date" not in benchmark_df.columns \
                               else benchmark_df["date"]
        benchmark_df = benchmark_df.reset_index() if "date" not in benchmark_df.columns \
                      else benchmark_df
        rows = loader.load_fact_prices(benchmark_df)
        total_price_rows += rows
        logger.info(f"✓ Benchmark ^GSPC: {rows} rows")

    # Load all tickers with full technical indicators
    for ticker in settings.ALL_TICKERS:
        try:
            df = transformer.transform_ticker(ticker)
            if df is not None:
                rows = loader.load_fact_prices(df)
                total_price_rows += rows
        except Exception as exc:
            logger.error(f"✗ {ticker} price load failed: {exc}")

    logger.info(f"✓ fact_daily_prices total: {total_price_rows} rows loaded")

    # ── Step 3: Load macro series → fact_macro_indicators ────────────────────
    logger.info("── Loading fact_macro_indicators ──")
    fred = FredExtractor()
    macro_data = {}

    for series_id in settings.FRED_SERIES.keys():
        df = fred.load_series(series_id)
        if df is not None:
            macro_data[series_id] = df

    macro_rows = loader.load_fact_macro(macro_data)
    logger.info(f"✓ fact_macro_indicators total: {macro_rows} rows loaded")


def run(load_only: bool = False) -> None:
    run_start = datetime.utcnow()
    logger.info("=" * 60)
    logger.info(f"DATABASE SETUP PIPELINE STARTED — {run_start.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    logger.info(f"Mode: {'load-only' if load_only else 'full setup (migrations + load)'}")
    logger.info("=" * 60)

    try:
        settings.validate()
        logger.info("✓ Config validation passed")
    except EnvironmentError as exc:
        logger.critical(f"Config validation failed: {exc}")
        sys.exit(1)

    loader = PostgresLoader()

    if not load_only:
        run_migrations(loader)
        run_seed_data(loader)

    run_data_load(loader)

    # ── Final validation ──────────────────────────────────────────────────────
    logger.info("── Running data quality checks ──")
    results = loader.validate_load()

    elapsed = (datetime.utcnow() - run_start).total_seconds()
    logger.info("=" * 60)
    logger.info(f"DATABASE SETUP COMPLETE — {elapsed:.1f}s elapsed")
    logger.info(f"  dim_dates:             {results['dim_dates']:>8,} rows")
    logger.info(f"  dim_sectors:           {results['dim_sectors']:>8,} rows")
    logger.info(f"  dim_companies:         {results['dim_companies']:>8,} rows")
    logger.info(f"  fact_daily_prices:     {results['fact_daily_prices']:>8,} rows")
    logger.info(f"  fact_macro_indicators: {results['fact_macro_indicators']:>8,} rows")
    logger.info(f"  Date range:            {results['date_range']}")
    logger.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EquityLens database setup")
    parser.add_argument("--load-only", action="store_true",
                        help="Skip migrations, only load data (for daily runs)")
    args = parser.parse_args()
    run(load_only=args.load_only)
