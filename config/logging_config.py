import logging
import logging.handlers
from config.settings import LOGS_DIR, settings


def setup_logging(module_name: str) -> logging.Logger:
    """
    Create a logger for a given module.
    Writes to both the console (coloured) and a rotating log file.
    Call once per module: logger = setup_logging(__name__)

    Production pattern: In enterprise stacks this would ship logs to
    Datadog, CloudWatch, or Splunk via a log handler — same interface,
    different sink.
    """
    logger = logging.getLogger(module_name)

    # Guard: don't add duplicate handlers if module is imported multiple times
    if logger.handlers:
        return logger

    logger.setLevel(getattr(logging, settings.LOG_LEVEL, logging.INFO))

    _fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler
    ch = logging.StreamHandler()
    ch.setFormatter(_fmt)
    logger.addHandler(ch)

    # Rotating file handler — 5 MB max, keep 3 backups
    fh = logging.handlers.RotatingFileHandler(
        LOGS_DIR / "pipeline.log",
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    fh.setFormatter(_fmt)
    logger.addHandler(fh)

    return logger
