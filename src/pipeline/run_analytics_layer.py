# src/pipeline/run_analytics_layer.py

"""
Deploys the SQL analytics layer (Gold views) to PostgreSQL.
Safe to re-run — all views use CREATE OR REPLACE.
Run this after any view SQL is updated.

Usage:
    python -m src.pipeline.run_analytics_layer
"""

import sys
from pathlib import Path
from config.settings import BASE_DIR
from config.logging_config import setup_logging
from src.loaders import PostgresLoader

logger = setup_logging("pipeline.analytics_layer")

# All views deployed in dependency order
VIEWS = [
    BASE_DIR / "sql" / "views" / "vw_stock_performance.sql",
    BASE_DIR / "sql" / "views" / "vw_fundamental_scorecard.sql",
    BASE_DIR / "sql" / "views" / "vw_volume_anomalies.sql",
    BASE_DIR / "sql" / "views" / "vw_forecast_input.sql",
]


def run() -> None:
    logger.info("=" * 60)
    logger.info("ANALYTICS LAYER DEPLOYMENT STARTED")
    logger.info("=" * 60)

    loader = PostgresLoader()
    success, failed = 0, 0

    for view_path in VIEWS:
        if not view_path.exists():
            logger.error(f"View file not found: {view_path}")
            failed += 1
            continue
        try:
            loader.run_migration(str(view_path))
            logger.info(f"✓ Deployed: {view_path.name}")
            success += 1
        except Exception as exc:
            logger.error(f"✗ Failed: {view_path.name} — {exc}")
            failed += 1

    logger.info("=" * 60)
    logger.info(f"ANALYTICS LAYER COMPLETE | ✓ {success} views | ✗ {failed} failed")
    logger.info("=" * 60)

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    run()
