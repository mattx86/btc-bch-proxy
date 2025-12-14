"""Statistics tracking for proxy connections and shares."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional

from loguru import logger


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
    """

    _instance: Optional[ProxyStats] = None

    def __init__(self):
        """Initialize statistics tracker."""
        # Miner connection stats
        self.current_miners: int = 0
        self.total_miner_connections: int = 0
        self.total_miner_disconnections: int = 0

        # Per-server stats
        self._server_stats: Dict[str, ServerStats] = {}

        # Lock for thread safety
        self._lock = asyncio.Lock()

        # Start time for uptime tracking
        self.start_time = datetime.now()

    @classmethod
    def get_instance(cls) -> ProxyStats:
        """Get or create the singleton stats instance."""
        if cls._instance is None:
            cls._instance = ProxyStats()
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        """Reset the stats instance (mainly for testing)."""
        cls._instance = None

    def _get_server_stats(self, server_name: str) -> ServerStats:
        """Get or create stats for a server."""
        if server_name not in self._server_stats:
            self._server_stats[server_name] = ServerStats(name=server_name)
        return self._server_stats[server_name]

    async def record_miner_connect(self) -> None:
        """Record a new miner connection."""
        async with self._lock:
            self.current_miners += 1
            self.total_miner_connections += 1

    async def record_miner_disconnect(self) -> None:
        """Record a miner disconnection."""
        async with self._lock:
            self.current_miners = max(0, self.current_miners - 1)
            self.total_miner_disconnections += 1

    async def record_upstream_connect(self, server_name: str) -> None:
        """Record an upstream server connection."""
        async with self._lock:
            stats = self._get_server_stats(server_name)
            stats.connections += 1

    async def record_upstream_reconnect(self, server_name: str) -> None:
        """Record an upstream server reconnection."""
        async with self._lock:
            stats = self._get_server_stats(server_name)
            stats.reconnections += 1

    async def record_share_accepted(self, server_name: str) -> None:
        """Record an accepted share."""
        async with self._lock:
            stats = self._get_server_stats(server_name)
            stats.accepted_shares += 1

    async def record_share_rejected(self, server_name: str, reason: str = "unknown") -> None:
        """Record a rejected share with reason."""
        async with self._lock:
            stats = self._get_server_stats(server_name)
            stats.rejected_shares += 1
            stats.rejection_reasons[reason] += 1

    def get_uptime(self) -> str:
        """Get formatted uptime string."""
        delta = datetime.now() - self.start_time
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

    def log_stats(self) -> None:
        """Log current statistics."""
        uptime = self.get_uptime()

        logger.info("=" * 60)
        logger.info(f"PROXY STATISTICS (uptime: {uptime})")
        logger.info("=" * 60)

        # Miner stats
        logger.info(
            f"Miners: {self.current_miners} active | "
            f"{self.total_miner_connections} total connections | "
            f"{self.total_miner_disconnections} disconnections"
        )

        # Per-server stats
        if not self._server_stats:
            logger.info("No server statistics yet")
        else:
            for name, stats in self._server_stats.items():
                logger.info("-" * 40)
                logger.info(f"Server: {name}")
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
            stats.log_stats()
