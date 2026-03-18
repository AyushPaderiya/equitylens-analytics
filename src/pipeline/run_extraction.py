import sys
from datetime import datetime

from config.settings import settings
from config.logging_config import setup_logging
from src.extractors import YFinanceExtractor, FredExtractor, AlphaVantageExtractor

logger = setup_logging("pipeline.extraction")


def run(force_refresh: bool = False) -> None:
    """
    Full Phase 1 extraction run.
    Idempotent: safe to re-run any number of times in the same day.

    Args:
        force_refresh: If True, bypass cache and re-fetch all data.
                       Use this after market close to get fresh EOD data.
    """
    run_start = datetime.utcnow()
    logger.info("=" * 60)
    logger.info(f"EXTRACTION PIPELINE STARTED — {run_start.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    logger.info(f"Environment : {settings.ENVIRONMENT}")
    logger.info(f"Force refresh: {force_refresh}")
    logger.info("=" * 60)

    # ── Step 0: Validate all credentials before making a single API call ─────
    try:
        settings.validate()
        logger.info("✓ Config validation passed")
    except EnvironmentError as exc:
        logger.critical(f"Config validation failed: {exc}")
        sys.exit(1)

    pipeline_results = {}

    # ── Step 1: yfinance ──────────────────────────────────────────────────────
    logger.info("── STEP 1/3: yfinance extraction ──")
    try:
        yf_extractor       = YFinanceExtractor()
        pipeline_results["yfinance"] = yf_extractor.extract(force_refresh=force_refresh)
    except Exception as exc:
        logger.error(f"yfinance extractor crashed: {exc}")
        pipeline_results["yfinance"] = {"status": "crashed", "error": str(exc)}

    # ── Step 2: FRED ──────────────────────────────────────────────────────────
    logger.info("── STEP 2/3: FRED extraction ──")
    try:
        fred_extractor       = FredExtractor()
        pipeline_results["fred"] = fred_extractor.extract(force_refresh=force_refresh)
    except Exception as exc:
        logger.error(f"FRED extractor crashed: {exc}")
        pipeline_results["fred"] = {"status": "crashed", "error": str(exc)}

    # ── Step 3: Alpha Vantage ─────────────────────────────────────────────────
    logger.info("── STEP 3/3: Alpha Vantage extraction ──")
    try:
        av_extractor       = AlphaVantageExtractor()
        pipeline_results["alphavantage"] = av_extractor.extract(force_refresh=force_refresh)
    except Exception as exc:
        logger.error(f"Alpha Vantage extractor crashed: {exc}")
        pipeline_results["alphavantage"] = {"status": "crashed", "error": str(exc)}

    # ── Final summary ─────────────────────────────────────────────────────────
    elapsed = (datetime.utcnow() - run_start).total_seconds()
    logger.info("=" * 60)
    logger.info(f"EXTRACTION PIPELINE COMPLETE — {elapsed:.1f}s elapsed")
    logger.info("=" * 60)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="EquityLens extraction pipeline")
    parser.add_argument("--force", action="store_true", help="Bypass cache and re-fetch all data")
    args = parser.parse_args()
    run(force_refresh=args.force)
