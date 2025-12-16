"""Statistics tracking for proxy connections and shares."""

from __future__ import annotations

import asyncio
import re
import threading
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from loguru import logger


@dataclass
class ServerConfigInfo:
    """Server configuration info for stats display."""

    name: str
    host: str
    port: int
    username: str
    schedule_start: str  # HH:MM format
    schedule_end: str  # HH:MM format

    @property
    def address(self) -> str:
        """Get host:port string."""
        return f"{self.host}:{self.port}"


def normalize_rejection_reason(reason: str) -> str:
    """
    Normalize a rejection reason to a category for aggregated stats.

    Converts detailed messages like:
    - "Duplicate share (job=XXX, nonce=YYY)" → "duplicate share"
    - "Stale job (job=XXX)" → "stale job"
    - "low difficulty share (12345.67)" → "low difficulty share"

    Args:
        reason: The detailed rejection reason.

    Returns:
        Normalized category string.
    """
    if not reason:
        return "unknown"

    # Check for common patterns FIRST (no truncation needed for keyword matching)
    # These use 'in' which is O(n) and safe for any input length
    reason_lower = reason.lower()

    if "duplicate" in reason_lower:
        return "duplicate share"
    if "stale" in reason_lower:
        return "stale job"
    if "low difficulty" in reason_lower:
        return "low difficulty share"
    if "job not found" in reason_lower:
        return "job not found"
    if "unauthorized" in reason_lower:
        return "unauthorized"
    if "timeout" in reason_lower:
        return "timeout"
    if "not connected" in reason_lower:
        return "not connected"
    if "invalid" in reason_lower:
        return "invalid share"

    # For unknown patterns, truncate input to prevent regex DoS on very long strings
    # Only do this AFTER pattern matching to preserve full context for known patterns
    MAX_REASON_INPUT = 200
    if len(reason) > MAX_REASON_INPUT:
        reason = reason[:MAX_REASON_INPUT]

    # Remove any parenthetical details for unknown patterns
    # e.g., "some error (details=xyz)" → "some error"
    cleaned = re.sub(r'\s*\([^)]*\)\s*$', '', reason).strip()
    if cleaned:
        # Truncate to prevent very long category names from unique error messages
        # Also remove any numbers/hex which could create many variations
        cleaned = re.sub(r'[0-9a-fA-F]{4,}', 'X', cleaned.lower())
        cleaned = cleaned[:50]  # Max 50 chars for category name
        # Ensure ASCII-safe for JSON/logging (replace non-ASCII with ?)
        cleaned = cleaned.encode('ascii', errors='replace').decode('ascii')
        return cleaned

    return "unknown"


# Maximum number of unique rejection reason categories to track per server
# This prevents unbounded memory growth from varied error messages
MAX_REJECTION_REASONS = 50


@dataclass
class ServerStats:
    """Statistics for a single upstream server."""

    name: str
    connections: int = 0
    reconnections: int = 0
    accepted_shares: int = 0
    rejected_shares: int = 0
    rejection_reasons: Dict[str, int] = field(default_factory=lambda: defaultdict(int))

    @property
    def total_shares(self) -> int:
        """Total shares submitted."""
        return self.accepted_shares + self.rejected_shares

    @property
    def accept_rate(self) -> float:
        """Acceptance rate as percentage."""
        if self.total_shares == 0:
            return 0.0
        return (self.accepted_shares / self.total_shares) * 100

    @property
    def reject_rate(self) -> float:
        """Rejection rate as percentage."""
        if self.total_shares == 0:
            return 0.0
        return (self.rejected_shares / self.total_shares) * 100


class ProxyStats:
    """
    Global statistics tracker for the proxy.

    Tracks:
    - Miner connections (current and total)
    - Upstream connections and reconnections per server
    - Share accepts/rejects per server
    - Rejection reasons breakdown

    Thread Safety:
        This class uses a singleton pattern with thread-safe instance creation
        (via threading.Lock). All statistic modifications are protected by
        an asyncio.Lock, so all async methods are safe to call concurrently.
        The synchronous get_uptime() method only reads immutable state (start_time).
    """

    _instance: Optional[ProxyStats] = None
    _instance_lock = threading.Lock()  # Thread-safe singleton creation

    def __init__(self):
        """Initialize statistics tracker."""
        # Miner connection stats
        self.current_miners: int = 0
        self.total_miner_connections: int = 0
        self.total_miner_disconnections: int = 0

        # Per-server stats
        self._server_stats: Dict[str, ServerStats] = {}

        # Currently active server (for display in stats)
        self._active_server: Optional[str] = None
        self._active_server_addr: Optional[str] = None  # host:port

        # Previous server and last switch time (for display in stats)
        self._previous_server: Optional[str] = None
        self._previous_server_addr: Optional[str] = None  # host:port
        self._last_switch_time: Optional[datetime] = None

        # Server configuration info (for display in stats)
        self._server_configs: List[ServerConfigInfo] = []

        # Lock for thread safety - created lazily to avoid event loop issues
        # when singleton is instantiated outside async context
        self._lock: Optional[asyncio.Lock] = None

        # Start time for uptime tracking (timezone-aware for consistency)
        self.start_time = datetime.now(timezone.utc).astimezone()

    def _get_lock(self) -> asyncio.Lock:
        """
        Get or create the asyncio lock (lazy initialization).

        Uses double-checked locking with the class-level threading lock
        to prevent race conditions during lock creation.

        Thread Safety:
            - The outer check avoids lock overhead after initialization
            - The inner check prevents multiple Lock creations
            - Once _lock is set, it's never changed back to None
            - CPython's GIL ensures atomic reference assignment visibility
        """
        if self._lock is None:
            with ProxyStats._instance_lock:
                if self._lock is None:
                    self._lock = asyncio.Lock()
        return self._lock

    @classmethod
    def get_instance(cls) -> ProxyStats:
        """Get or create the singleton stats instance (thread-safe)."""
        # Double-checked locking: avoid lock overhead after initialization
        # The outer check is safe because CPython's GIL ensures atomic reference reads
        if cls._instance is None:
            with cls._instance_lock:
                # Inner check prevents race condition during creation
                if cls._instance is None:
                    cls._instance = ProxyStats()
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        """Reset the stats instance (mainly for testing)."""
        with cls._instance_lock:
            cls._instance = None

    def set_server_configs(self, configs: List[ServerConfigInfo]) -> None:
        """
        Set server configuration info for stats display.

        Args:
            configs: List of server configuration info.
        """
        self._server_configs = configs

    def _get_server_stats(self, server_name: str) -> ServerStats:
        """Get or create stats for a server."""
        # Validate server_name to prevent KeyError or stats pollution
        if not server_name:
            server_name = "unknown"
        if server_name not in self._server_stats:
            self._server_stats[server_name] = ServerStats(name=server_name)
        return self._server_stats[server_name]

    async def record_miner_connect(self) -> None:
        """Record a new miner connection."""
        async with self._get_lock():
            self.current_miners += 1
            self.total_miner_connections += 1

    async def record_miner_disconnect(self) -> None:
        """Record a miner disconnection."""
        async with self._get_lock():
            if self.current_miners <= 0:
                # This indicates a bug: disconnect called more times than connect
                logger.warning(
                    f"Miner disconnect recorded but current_miners already {self.current_miners}. "
                    f"Possible double-disconnect bug."
                )
                self.current_miners = 0
            else:
                self.current_miners -= 1
            self.total_miner_disconnections += 1

    async def set_active_server(self, server_name: str, server_addr: Optional[str] = None) -> None:
        """
        Set the currently active server for routing.

        Args:
            server_name: Name of the server.
            server_addr: Server address as "host:port" (optional).
        """
        async with self._get_lock():
            # Track previous server and switch time when server changes
            if self._active_server is not None and self._active_server != server_name:
                self._previous_server = self._active_server
                self._previous_server_addr = self._active_server_addr
                # Store local time with timezone info for proper offset display
                self._last_switch_time = datetime.now(timezone.utc).astimezone()
            self._active_server = server_name
            self._active_server_addr = server_addr

    async def record_upstream_connect(self, server_name: str) -> None:
        """Record an upstream server connection."""
        async with self._get_lock():
            stats = self._get_server_stats(server_name)
            stats.connections += 1

    async def record_upstream_reconnect(self, server_name: str) -> None:
        """Record an upstream server reconnection."""
        async with self._get_lock():
            stats = self._get_server_stats(server_name)
            stats.reconnections += 1

    async def record_share_accepted(self, server_name: str) -> None:
        """Record an accepted share."""
        async with self._get_lock():
            stats = self._get_server_stats(server_name)
            stats.accepted_shares += 1

    async def record_share_rejected(self, server_name: str, reason: str = "unknown") -> None:
        """Record a rejected share with normalized reason category."""
        # Normalize OUTSIDE the lock to avoid blocking other stats updates
        # (normalization involves regex operations that could take time on long strings)
        normalized = normalize_rejection_reason(reason)

        async with self._get_lock():
            stats = self._get_server_stats(server_name)
            stats.rejected_shares += 1

            # Only add new categories if under the limit (prevents unbounded growth)
            if normalized in stats.rejection_reasons:
                stats.rejection_reasons[normalized] += 1
            elif len(stats.rejection_reasons) < MAX_REJECTION_REASONS:
                stats.rejection_reasons[normalized] = 1
            else:
                # Over limit - aggregate into "other" category
                stats.rejection_reasons["other"] = stats.rejection_reasons.get("other", 0) + 1

    def get_uptime(self) -> str:
        """Get formatted uptime string."""
        delta = datetime.now(timezone.utc).astimezone() - self.start_time
        days = delta.days
        hours, remainder = divmod(delta.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)

        parts = []
        if days > 0:
            parts.append(f"{days}d")
        if hours > 0 or days > 0:
            parts.append(f"{hours}h")
        parts.append(f"{minutes}m")

        return " ".join(parts)

    async def log_stats(self) -> None:
        """Log current statistics (async-safe)."""
        uptime = self.get_uptime()

        # Copy stats under lock to avoid race conditions
        async with self._get_lock():
            current_miners = self.current_miners
            total_connections = self.total_miner_connections
            total_disconnections = self.total_miner_disconnections
            active_server = self._active_server
            active_server_addr = self._active_server_addr
            previous_server = self._previous_server
            previous_server_addr = self._previous_server_addr
            last_switch_time = self._last_switch_time
            server_configs = list(self._server_configs)
            server_stats_copy = {
                name: ServerStats(
                    name=stats.name,
                    connections=stats.connections,
                    reconnections=stats.reconnections,
                    accepted_shares=stats.accepted_shares,
                    rejected_shares=stats.rejected_shares,
                    rejection_reasons=dict(stats.rejection_reasons),
                )
                for name, stats in self._server_stats.items()
            }

        logger.info("=" * 60)
        logger.info(f"PROXY STATISTICS (uptime: {uptime})")
        logger.info("=" * 60)

        # Server configuration
        if server_configs:
            logger.info("Servers:")
            for cfg in server_configs:
                logger.info(
                    f"  - {cfg.name} ({cfg.address}) "
                    f"Schedule: {cfg.schedule_start}-{cfg.schedule_end} "
                    f"Username: {cfg.username}"
                )

        # Miner stats
        logger.info(
            f"Miners: {current_miners} active | "
            f"{total_connections} total connections | "
            f"{total_disconnections} disconnections"
        )

        # Server routing info
        if active_server:
            # Format server name with address if available
            active_str = f"{active_server} ({active_server_addr})" if active_server_addr else active_server
            if previous_server and last_switch_time:
                prev_str = f"{previous_server} ({previous_server_addr})" if previous_server_addr else previous_server
                switch_time_str = last_switch_time.strftime("%Y-%m-%d %H:%M:%S%z")
                logger.info(f"Active server: {active_str}")
                logger.info(f"Previous server: {prev_str}")
                logger.info(f"Last switch: {switch_time_str}")
            else:
                logger.info(f"Active server: {active_str} (no switches yet)")

        # Per-server stats
        if not server_stats_copy:
            logger.info("No server statistics yet")
        else:
            for name, stats in server_stats_copy.items():
                logger.info("-" * 40)
                # Show (active) indicator for currently active server
                active_indicator = " (active)" if name == active_server else ""
                logger.info(f"Server: {name}{active_indicator}")
                logger.info(
                    f"  Connections: {stats.connections} | "
                    f"Reconnections: {stats.reconnections}"
                )

                if stats.total_shares > 0:
                    logger.info(
                        f"  Shares: {stats.accepted_shares} accepted / "
                        f"{stats.rejected_shares} rejected "
                        f"({stats.accept_rate:.1f}% / {stats.reject_rate:.1f}%)"
                    )

                    # Log rejection reasons if any
                    if stats.rejection_reasons:
                        reasons_str = ", ".join(
                            f'"{reason}": {count}'
                            for reason, count in sorted(
                                stats.rejection_reasons.items(),
                                key=lambda x: -x[1]
                            )
                        )
                        logger.info(f"  Rejection reasons: {reasons_str}")
                else:
                    logger.info("  Shares: none submitted yet")

        logger.info("=" * 60)


async def run_stats_logger(stop_event: asyncio.Event) -> None:
    """
    Run periodic stats logging at minute 0, 15, 30, 45 of each hour.

    Args:
        stop_event: Event to signal shutdown.
    """
    stats = ProxyStats.get_instance()

    while not stop_event.is_set():
        now = datetime.now()

        # Calculate next logging time (0, 15, 30, or 45 minute mark)
        current_minute = now.minute
        current_second = now.second

        # Find the next quarter hour
        next_quarter = ((current_minute // 15) + 1) * 15
        if next_quarter >= 60:
            next_quarter = 0

        # Calculate seconds until next quarter hour
        if next_quarter > current_minute:
            wait_minutes = next_quarter - current_minute
        else:
            # Next quarter is in the next hour
            wait_minutes = (60 - current_minute) + next_quarter

        wait_seconds = (wait_minutes * 60) - current_second
        # Ensure minimum wait time to prevent tight loop
        wait_seconds = max(1, wait_seconds)

        # Wait until the next quarter hour (or stop event)
        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=wait_seconds
            )
            # Stop event was set, exit
            break
        except asyncio.TimeoutError:
            # Time to log stats
            await stats.log_stats()
