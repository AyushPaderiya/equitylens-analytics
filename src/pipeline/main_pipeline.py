"""
Master ETL pipeline — runs the full Extract → Validate → Transform → Load cycle.
This is the single entry point for all daily automated runs.

Run modes:
  Full run  : python -m src.pipeline.main_pipeline
  Force     : python -m src.pipeline.main_pipeline --force
  Load only : python -m src.pipeline.main_pipeline --load-only

Production equivalent: This would be an Airflow DAG with one
task group per phase, SLA alerts, and retry policies per task.
The function structure here maps directly to Airflow tasks:
  extract_task >> validate_task >> transform_task >> load_task
"""

import argparse
import sys
from datetime import datetime

from config.logging_config import setup_logging
from config.settings import settings
from src.extractors import YFinanceExtractor, FredExtractor, AlphaVantageExtractor
from src.transformers import PriceTransformer, MacroTransformer
from src.validators.data_validator import DataValidator
from src.loaders import PostgresLoader

logger = setup_logging("pipeline.main")


# ── Phase functions (map to Airflow tasks in production) ─────────────────────

def extract(force: bool = False) -> dict:
    """Task 1: Pull fresh data from all 3 API sources."""
    logger.info("── EXTRACT ──────────────────────────────────────────")
    results = {}

    try:
        yf = YFinanceExtractor()
        results["yfinance"] = yf.extract(force_refresh=force)
        logger.info("✓ yfinance extraction complete")
    except Exception as exc:
        logger.error(f"✗ yfinance extraction failed: {exc}")
        results["yfinance"] = None

    try:
        fred = FredExtractor()
        results["fred"] = fred.extract(force_refresh=force)
        logger.info("✓ FRED extraction complete")
    except Exception as exc:
        logger.error(f"✗ FRED extraction failed: {exc}")
        results["fred"] = None

    try:
        av = AlphaVantageExtractor()
        results["alphavantage"] = av.extract(force_refresh=force)
        logger.info("✓ Alpha Vantage extraction complete")
    except Exception as exc:
        logger.error(f"✗ Alpha Vantage extraction failed: {exc}")
        results["alphavantage"] = None

    return results


def validate_and_transform() -> dict:
    """
    Task 2+3: Transform all data and validate before loading.
    Validation failures are logged but do NOT halt the pipeline —
    bad tickers are skipped, good ones still load.
    Production pattern: failed validations raise Airflow warnings,
    trigger Slack alerts, but don't fail the entire DAG.
    """
    logger.info("── VALIDATE + TRANSFORM ─────────────────────────────")

    validator   = DataValidator()
    price_trans = PriceTransformer()
    macro_trans = MacroTransformer()

    # Transform
    price_data = price_trans.transform_all()
    macro_data = macro_trans.transform_all()

    # Validate prices
    validation_results = validator.validate_all_prices(price_data)

    # Filter out tickers that failed validation
    clean_prices = {
        ticker: df
        for ticker, df in price_data.items()
        if validation_results.get(ticker) and validation_results[ticker].passed
    }

    failed = len(price_data) - len(clean_prices)
    if failed > 0:
        logger.warning(
            f"{failed} tickers failed validation and will be skipped"
        )

    # Validate macro
    for series_id, df in macro_data.items():
        result = validator.validate_macro(df, series_id)
        if not result.passed:
            logger.warning(f"Macro validation failed for {series_id} — skipping")
            macro_data[series_id] = None

    logger.info(
        f"✓ Transform + validate complete | "
        f"Prices: {len(clean_prices)}/{len(price_data)} clean | "
        f"Macro: {sum(1 for v in macro_data.values() if v is not None)}/"
        f"{len(macro_data)} clean"
    )

    return {"prices": clean_prices, "macro": macro_data}


def load(transformed: dict) -> dict:
    """Task 4: Upsert all clean data into PostgreSQL."""
    logger.info("── LOAD ─────────────────────────────────────────────")

    loader = PostgresLoader()
    stats  = {"prices": 0, "macro": 0}

    # Load prices
    for ticker, df in transformed["prices"].items():
        try:
            rows = loader.load_fact_prices(df)
            stats["prices"] += rows
        except Exception as exc:
            logger.error(f"✗ {ticker} load failed: {exc}")

    # Load macro
    macro_rows = loader.load_fact_macro(transformed["macro"])
    stats["macro"] += macro_rows

    logger.info(
        f"✓ Load complete | "
        f"Price rows: {stats['prices']:,} | "
        f"Macro rows: {stats['macro']:,}"
    )
    return stats


def run(force: bool = False, load_only: bool = False) -> None:
    """
    Full pipeline orchestrator.
    Idempotent: safe to run multiple times per day.
    """
    run_start = datetime.utcnow()
    logger.info("=" * 60)
    logger.info(
        f"MAIN PIPELINE STARTED — "
        f"{run_start.strftime('%Y-%m-%d %H:%M:%S')} UTC"
    )
    logger.info(f"Force refresh : {force}")
    logger.info(f"Load only     : {load_only}")
    logger.info("=" * 60)

    try:
        settings.validate()
    except EnvironmentError as exc:
        logger.critical(f"Config validation failed: {exc}")
        sys.exit(1)

    # Extract (skip if load_only)
    if not load_only:
        extract(force=force)

    # Transform + validate + load
    transformed = validate_and_transform()
    stats       = load(transformed)

    # Final validation
    loader = PostgresLoader()
    counts = loader.validate_load()

    elapsed = (datetime.utcnow() - run_start).total_seconds()
    logger.info("=" * 60)
    logger.info(f"MAIN PIPELINE COMPLETE — {elapsed:.1f}s elapsed")
    logger.info(f"  Price rows upserted  : {stats['prices']:>8,}")
    logger.info(f"  Macro rows upserted  : {stats['macro']:>8,}")
    logger.info(f"  Total DB price rows  : {counts['fact_daily_prices']:>8,}")
    logger.info(f"  Distinct tickers     : {counts['distinct_tickers']:>8,}")
    logger.info(f"  Date range           : {counts['date_range']}")
    logger.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EquityLens main ETL pipeline")
    parser.add_argument("--force",     action="store_true", help="Force re-fetch all data")
    parser.add_argument("--load-only", action="store_true", help="Skip extraction")
    args = parser.parse_args()
    run(force=args.force, load_only=args.load_only)
