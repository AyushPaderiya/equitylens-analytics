import json
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from config.logging_config import setup_logging
from config.settings import DATA_RAW_DIR, settings


class BaseExtractor(ABC):
    """
    Abstract base class for all API extractors.

    Design contract:
    - Every extractor MUST implement extract()
    - Retry logic, Bronze layer I/O, and idempotency are handled here
    - Subclasses focus only on API-specific parsing logic

    Production equivalent: This would be a Celery task base class or
    an Airflow BaseOperator subclass — same pattern, different runner.
    """

    def __init__(self, source_name: str):
        self.source_name = source_name
        self.raw_dir     = DATA_RAW_DIR / source_name
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.logger      = setup_logging(f"extractor.{source_name}")
        self.max_retries = settings.MAX_RETRIES
        self.retry_delay = settings.RETRY_DELAY_SEC

    # ── Abstract interface ───────────────────────────────────────────────────

    @abstractmethod
    def extract(self, *args, **kwargs) -> Any:
        """Core extraction logic — must be implemented by each subclass."""
        pass

    # ── Bronze layer I/O ─────────────────────────────────────────────────────

    def _save_raw(self, data: Any, filename: str) -> Path:
        """
        Persist raw API response to the Bronze layer as JSON.
        Overwrite is intentional — Bronze always holds the freshest raw data.
        """
        filepath = self.raw_dir / f"{filename}.json"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)
        self.logger.debug(f"Bronze layer write → {filepath.relative_to(filepath.parent.parent.parent)}")
        return filepath

    def _load_raw(self, filename: str) -> Optional[dict]:
        """Load a previously saved Bronze layer file."""
        filepath = self.raw_dir / f"{filename}.json"
        if not filepath.exists():
            return None
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)

    def _is_fresh(self, filename: str, max_age_hours: int = 23) -> bool:
        """
        Idempotency check: returns True if this file was written
        within max_age_hours. Prevents duplicate API calls when the
        pipeline is re-run on the same day.

        Production equivalent: In dbt/Airflow this is managed via
        run state tables or last_modified timestamps in a metadata DB.
        """
        filepath = self.raw_dir / f"{filename}.json"
        if not filepath.exists():
            return False
        age_seconds = time.time() - filepath.stat().st_mtime
        return age_seconds < (max_age_hours * 3600)

    # ── Retry logic ──────────────────────────────────────────────────────────

    def _with_retry(self, func, *args, **kwargs) -> Any:
        """
        Execute any callable with exponential backoff retry.

        Retry schedule (RETRY_DELAY_SEC = 5):
        Attempt 1 → fail → wait 5s
        Attempt 2 → fail → wait 10s
        Attempt 3 → fail → raise

        Production equivalent: Celery retry with max_retries and
        countdown; Airflow retry_delay with on_retry_callback.
        """
        last_exception = None
        for attempt in range(1, self.max_retries + 1):
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                last_exception = exc
                wait = self.retry_delay * (2 ** (attempt - 1))
                if attempt == self.max_retries:
                    self.logger.error(
                        f"All {self.max_retries} attempts exhausted for "
                        f"{func.__name__}. Final error: {exc}"
                    )
                    raise last_exception
                self.logger.warning(
                    f"Attempt {attempt}/{self.max_retries} failed: {exc} "
                    f"— retrying in {wait}s"
                )
                time.sleep(wait)

    # ── Utilities ─────────────────────────────────────────────────────────────

    @staticmethod
    def _utc_now() -> str:
        """ISO 8601 UTC timestamp. Used in every payload envelope."""
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
