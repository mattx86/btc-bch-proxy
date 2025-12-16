"""Main stratum proxy server."""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from typing import TYPE_CHECKING, Dict, List, Optional

from loguru import logger

# Rate limiting constants
MAX_CONNECTIONS_PER_IP_PER_MINUTE = 10
RATE_LIMIT_WINDOW_SECONDS = 60
RATE_LIMIT_CLEANUP_INTERVAL = 300  # Clean up stale entries every 5 minutes
RATE_LIMIT_MAX_IPS = 10000  # Maximum unique IPs to track (prevents memory exhaustion)

from btc_bch_proxy.proxy.router import TimeBasedRouter
from btc_bch_proxy.proxy.session import MinerSession
from btc_bch_proxy.proxy.stats import ProxyStats, run_stats_logger

if TYPE_CHECKING:
    from btc_bch_proxy.config.models import Config


def _log_task_exception(task: asyncio.Task, task_name: str) -> None:
    """Add exception logging callback to a task."""
    def _callback(t: asyncio.Task) -> None:
        try:
            if t.cancelled():
                return
            exc = t.exception()
            if exc:
                logger.error(f"{task_name} failed with exception: {exc}")
        except Exception as e:
            # Defensive: should never happen in done callback, but don't crash
            logger.error(f"Error in task exception callback for {task_name}: {e}")
    task.add_done_callback(_callback)


class StratumProxyServer:
    """
    Main stratum proxy server.

    Handles:
    - Accepting miner connections
    - Managing miner sessions
    - Coordinating with the time-based router
    - Graceful shutdown

    Each miner session creates its own upstream connection for isolation.
    """

    def __init__(self, config: Config):
        """
        Initialize the proxy server.

        Args:
            config: Application configuration.
        """
        self.config = config

        # Create time-based router
        self.router = TimeBasedRouter(config)

        # Session management
        self._sessions: Dict[str, MinerSession] = {}
        self._session_lock = asyncio.Lock()

        # Server state
        self._server: Optional[asyncio.Server] = None
        self._stop_event = asyncio.Event()
        self._scheduler_task: Optional[asyncio.Task] = None
        self._stats_task: Optional[asyncio.Task] = None

        # Rate limiting - track connection timestamps per IP
        self._connection_attempts: Dict[str, List[float]] = defaultdict(list)
        self._rate_limit_last_cleanup = time.time()
        self._rate_limit_lock = asyncio.Lock()  # Lock for rate limit operations

    async def start(self) -> None:
        """Start the proxy server."""
        logger.info("Starting stratum proxy server...")

        # Log initial server
        initial_server = self.router.get_current_server()
        logger.info(f"Initial server: {initial_server}")

        # Register for server switch notifications
        await self.router.register_switch_callback(self._on_server_switch)

        # Start the time-based scheduler
        self._scheduler_task = asyncio.create_task(
            self.router.run_scheduler(self._stop_event)
        )
        _log_task_exception(self._scheduler_task, "Scheduler task")

        # Start the stats logger (logs at minute 0, 15, 30, 45 of every hour)
        self._stats_task = asyncio.create_task(
            run_stats_logger(self._stop_event)
        )
        _log_task_exception(self._stats_task, "Stats logger task")

        # Start accepting connections
        self._server = await asyncio.start_server(
            self._handle_connection,
            self.config.proxy.bind_host,
            self.config.proxy.bind_port,
        )

        addr = self._server.sockets[0].getsockname()
        logger.info(f"Proxy server listening on {addr[0]}:{addr[1]}")

        # Serve until stopped
        async with self._server:
            await self._server.serve_forever()

    async def stop(self) -> None:
        """Stop the proxy server gracefully."""
        logger.info("Stopping proxy server...")

        # Signal stop
        self._stop_event.set()

        # Stop accepting new connections
        if self._server:
            self._server.close()
            await self._server.wait_closed()

        # Stop scheduler
        if self._scheduler_task:
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass

        # Stop stats logger
        if self._stats_task:
            self._stats_task.cancel()
            try:
                await self._stats_task
            except asyncio.CancelledError:
                pass

        # Log final stats before shutdown
        await ProxyStats.get_instance().log_stats()

        # Close all sessions IN PARALLEL (each session closes its own upstream connection)
        # Don't hold the lock while waiting - just grab the session list
        async with self._session_lock:
            sessions = list(self._sessions.values())

        if sessions:
            logger.info(f"Closing {len(sessions)} active sessions...")
            # Close all sessions concurrently with a timeout
            close_tasks = [session.close() for session in sessions]
            try:
                await asyncio.wait_for(
                    asyncio.gather(*close_tasks, return_exceptions=True),
                    timeout=5.0  # 5 second overall timeout for all sessions
                )
            except asyncio.TimeoutError:
                logger.warning("Timeout waiting for sessions to close, forcing shutdown")

        # Clear the sessions dict
        async with self._session_lock:
            self._sessions.clear()

        logger.info("Proxy server stopped")

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """
        Handle a new miner connection.

        Args:
            reader: Async stream reader.
            writer: Async stream writer.
        """
        # Get client IP for rate limiting
        client_addr = writer.get_extra_info("peername")
        client_ip = client_addr[0] if client_addr else "unknown"

        # Check per-IP rate limit
        if not await self._check_rate_limit(client_ip):
            logger.warning(f"Rate limit exceeded for {client_ip}, rejecting connection")
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass  # Connection already broken
            return

        # Check connection limit
        async with self._session_lock:
            if len(self._sessions) >= self.config.proxy.max_connections:
                logger.warning("Max connections reached, rejecting new connection")
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass  # Connection already broken
                return

        # Create session (each session creates its own upstream connection)
        session = MinerSession(
            reader=reader,
            writer=writer,
            router=self.router,
            config=self.config,
        )

        # Record miner connection
        stats = ProxyStats.get_instance()
        await stats.record_miner_connect()

        # Register session
        async with self._session_lock:
            self._sessions[session.session_id] = session

        try:
            # Run session
            await session.run()
        finally:
            # Record miner disconnection
            await stats.record_miner_disconnect()

            # Unregister session
            async with self._session_lock:
                self._sessions.pop(session.session_id, None)

    async def _check_rate_limit(self, client_ip: str) -> bool:
        """
        Check if a client IP is within rate limits and record the attempt.

        Args:
            client_ip: Client IP address.

        Returns:
            True if connection is allowed, False if rate limited.
        """
        async with self._rate_limit_lock:
            now = time.time()
            cutoff = now - RATE_LIMIT_WINDOW_SECONDS

            # Periodic cleanup of stale entries
            if now - self._rate_limit_last_cleanup > RATE_LIMIT_CLEANUP_INTERVAL:
                self._cleanup_rate_limit_entries_unlocked(cutoff)
                self._rate_limit_last_cleanup = now

            # Get recent attempts for this IP
            attempts = self._connection_attempts[client_ip]

            # Remove old attempts
            attempts[:] = [t for t in attempts if t > cutoff]

            # Check if over limit
            if len(attempts) >= MAX_CONNECTIONS_PER_IP_PER_MINUTE:
                return False

            # Record this attempt
            attempts.append(now)
            return True

    def _cleanup_rate_limit_entries_unlocked(self, cutoff: float) -> None:
        """
        Remove stale rate limit entries to prevent memory growth.

        Note: Caller must hold _rate_limit_lock.

        Performance: Uses incremental cleanup to avoid O(n²) worst case.
        - Phase 1: Clean expired timestamps and remove empty entries (O(n))
        - Phase 2: If over limit, evict entries incrementally using partial sort
        """
        # Phase 1: Clean expired timestamps and collect empty IPs
        empty_ips = []
        for ip, attempts in self._connection_attempts.items():
            # Filter in-place to avoid creating new list
            original_len = len(attempts)
            attempts[:] = [t for t in attempts if t > cutoff]
            if not attempts:
                empty_ips.append(ip)

        # Remove empty entries
        for ip in empty_ips:
            del self._connection_attempts[ip]

        # Phase 2: Enforce maximum tracked IPs (incremental eviction)
        current_count = len(self._connection_attempts)
        if current_count > RATE_LIMIT_MAX_IPS:
            excess = current_count - RATE_LIMIT_MAX_IPS
            # Add 10% margin to prevent immediate re-triggering on next burst
            margin = max(1, RATE_LIMIT_MAX_IPS // 10)
            evict_count = excess + margin

            # Use heapq.nsmallest for O(n log k) instead of O(n log n) full sort
            # where k = evict_count (typically small compared to n)
            import heapq
            oldest_entries = heapq.nsmallest(
                evict_count,
                self._connection_attempts.items(),
                key=lambda x: max(x[1]) if x[1] else 0
            )

            for ip, _ in oldest_entries:
                del self._connection_attempts[ip]

            logger.warning(
                f"Rate limit cache exceeded {RATE_LIMIT_MAX_IPS} IPs, "
                f"evicted {len(oldest_entries)} oldest entries (excess={excess}, margin={margin})"
            )

    async def _on_server_switch(self, new_server: str) -> None:
        """
        Handle server switch notification from router.

        Each session manages its own upstream connection, so we just tell
        all sessions to switch.

        Args:
            new_server: Name of the new server.
        """
        logger.info(f"Server switch triggered: switching to {new_server}")

        # Switch all active sessions (each session will create its own new connection)
        async with self._session_lock:
            sessions = list(self._sessions.values())

        switch_tasks = [
            session.handle_server_switch(new_server)
            for session in sessions
            if session.is_active
        ]

        if switch_tasks:
            results = await asyncio.gather(*switch_tasks, return_exceptions=True)
            # Count and log any exceptions that occurred during switch
            failed_count = 0
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    failed_count += 1
                    logger.error(f"Session switch failed: {result}")

            successful_count = len(switch_tasks) - failed_count
            if failed_count > 0:
                logger.warning(
                    f"Server switch to {new_server}: {successful_count} succeeded, "
                    f"{failed_count} failed"
                )
            else:
                logger.info(f"Switched {len(switch_tasks)} sessions to {new_server}")

    @property
    def active_sessions(self) -> int:
        """Get count of active miner sessions."""
        return len(self._sessions)

    @property
    def current_server(self) -> str:
        """Get the current active server name."""
        return self.router.get_current_server()


async def run_proxy(config: Config, stop_event: Optional[asyncio.Event] = None) -> None:
    """
    Run the proxy server.

    Args:
        config: Application configuration.
        stop_event: Optional event to signal shutdown.
    """
    server = StratumProxyServer(config)
    stop_task: Optional[asyncio.Task] = None

    # Handle shutdown signals
    if stop_event:
        async def wait_for_stop():
            await stop_event.wait()
            await server.stop()

        stop_task = asyncio.create_task(wait_for_stop())
        _log_task_exception(stop_task, "Stop signal handler")

    try:
        await server.start()
    except asyncio.CancelledError:
        pass
    finally:
        # Cancel stop_task if it's still running
        if stop_task and not stop_task.done():
            stop_task.cancel()
            try:
                await stop_task
            except asyncio.CancelledError:
                pass
        await server.stop()
