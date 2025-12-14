"""Loguru-based logging configuration."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    from btc_bch_proxy.config.models import LoggingConfig


def setup_logging(config: LoggingConfig) -> None:
    """
    Configure Loguru based on the provided configuration.

    Args:
        config: Logging configuration object.
    """
    # Remove default handler
    logger.remove()

    # Console handler with colors
    logger.add(
        sys.stderr,
        level=config.level,
        format=config.format,
        colorize=True,
    )

    # File handler (if configured)
    if config.file:
        logger.add(
            config.file,
            level=config.level,
            format=config.format,
            rotation=config.rotation,
            retention=config.retention,
            compression="zip",
            encoding="utf-8",
        )

    logger.debug(f"Logging configured: level={config.level}, file={config.file}")


def get_logger(name: str):
    """
    Get a contextualized logger for a module.

    Args:
        name: Module name to bind to the logger.

    Returns:
        Logger instance with module context bound.
    """
    return logger.bind(module=name)
