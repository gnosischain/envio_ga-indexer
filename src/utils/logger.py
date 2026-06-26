"""Structured logging (structlog), adapted from beacon-indexer.

Human-readable console output by default; set FORCE_JSON_LOGS=true for JSON.
"""
import logging
import os

import structlog

from src.config import config


def setup_logger():
    """Configure structlog. Idempotent enough for repeated CLI invocations."""
    force_json = os.getenv("FORCE_JSON_LOGS", "false").lower() == "true"

    logging.basicConfig(level=getattr(logging, config.LOG_LEVEL, logging.INFO),
                        format="%(message)s")

    if force_json:
        processors = [
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ]
    else:
        processors = [
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
            _simple_console_renderer,
        ]

    structlog.configure(
        processors=processors,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def _simple_console_renderer(logger, method_name, event_dict):
    """Concise single-line console renderer."""
    timestamp = event_dict.pop("timestamp", "")
    level = event_dict.pop("level", "INFO").upper()
    event = event_dict.pop("event", "")

    msg = f"{timestamp} [{level:<5}] {event}"

    if event_dict:
        important = ["entity", "worker", "partition_key", "page_seq", "rows", "cursor_end"]
        parts = []
        for field in important:
            if field in event_dict:
                parts.append(f"{field}={event_dict.pop(field)}")
        for k, v in event_dict.items():
            if k not in ("logger", "stack", "exception"):
                parts.append(f"{k}={v}")
        if parts:
            msg += " | " + " ".join(parts)

    return msg


logger = structlog.get_logger()
