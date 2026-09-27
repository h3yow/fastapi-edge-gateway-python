"""Decision logging helpers.

Every gateway decision (upstream selection, rate limiting, cache lookup,
failover) is rendered as a single structured line so test output can explain
why a request was accepted, rejected or served from cache.
"""
from __future__ import annotations

import logging
import sys
from typing import Any

_LOGGER_NAME = "edge_gateway"


def get_logger() -> logging.Logger:
    logger = logging.getLogger(_LOGGER_NAME)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def log_decision(kind: str, **fields: Any) -> None:
    """Emit one structured decision record, e.g. kind='upstream_select'."""
    parts = " ".join(f"{k}={_quote(v)}" for k, v in fields.items())
    get_logger().info("decision=%s %s", kind, parts)


def _quote(value: Any) -> str:
    text = str(value)
    if any(ch in text for ch in (" ", "=", '"')):
        return '"' + text.replace('"', "'") + '"'
    return text
