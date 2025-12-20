"""Shared utility functions for the proxy module."""

from __future__ import annotations

import asyncio
from typing import Coroutine, Any, Optional

from loguru import logger

from crypto_stratum_proxy.proxy.constants import MAX_BACKGROUND_ERROR_LENGTH


def fire_and_forget(coro: Coroutine[Any, Any, Any]) -> Optional[asyncio.Task]:
    """
    Create a task that logs exceptions instead of silently dropping them.

    Use this for background tasks where we don't want to await the result
    but still want to know if something went wrong.

    Design Rationale:
        This is intentionally used for stats recording operations where:
        1. The caller should not block waiting for stats updates
        2. Stats failures should not affect critical path operations (share submission)
        3. Any errors are logged for debugging but don't impact functionality
        4. Stats are "best effort" - missing a few data points is acceptable

        This is NOT appropriate for operations where:
        - Failure requires retry or error handling
        - Data integrity depends on completion
        - The result is needed by the caller

    Args:
        coro: Coroutine to run as a task.

    Returns:
        The created task, or None if no event loop is running.
    """
    try:
        task = asyncio.create_task(coro)
    except RuntimeError as e:
        # No running event loop - close the coroutine to avoid warning
        coro.close()
        logger.debug(f"Cannot create background task (no event loop): {e}")
        return None

    def _handle_exception(t: asyncio.Task) -> None:
        try:
            if t.cancelled():
                return
            exc = t.exception()
            if exc:
                # Truncate long error messages to prevent log bloat
                exc_str = str(exc)
                if len(exc_str) > MAX_BACKGROUND_ERROR_LENGTH:
                    exc_str = exc_str[:MAX_BACKGROUND_ERROR_LENGTH] + "... (truncated)"
                logger.error(f"Background task failed: {exc_str}")
        except Exception as e:
            # Defensive: should never happen in done callback, but don't crash
            logger.error(f"Error in fire_and_forget exception handler: {e}")

    task.add_done_callback(_handle_exception)
    return task
