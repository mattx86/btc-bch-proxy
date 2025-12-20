"""Main stratum proxy server."""

from __future__ import annotations

import asyncio
import heapq
import time
from collections import defaultdict
from typing import TYPE_CHECKING, Dict, List, Optional

from loguru import logger

# Rate limiting constants
MAX_CONNECTIONS_PER_IP_PER_MINUTE = 10
RATE_LIMIT_WINDOW_SECONDS = 60
RATE_LIMIT_CLEANUP_INTERVAL = 300  # Clean up stale entries every 5 minutes
RATE_LIMIT_MAX_IPS = 10000  # Maximum unique IPs to track (prevents memory exhaustion)

from crypto_stratum_proxy.proxy.router import TimeBasedRouter
from crypto_stratum_proxy.proxy.session import MinerSession
from crypto_stratum_proxy.proxy.stats import ProxyStats, ServerConfigInfo, run_stats_logger

if TYPE_CHECKING:
    from crypto_stratum_proxy.config.models import Config


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
    - Accepting miner connections on per-algorithm ports
    - Managing miner sessions
    - Coordinating with time-based routers (one per algorithm)
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

        # Get enabled algorithms
        self._enabled_algorithms = config.proxy.get_enabled_algorithms()

        # Create time-based router per algorithm
        self._routers: Dict[str, TimeBasedRouter] = {
            algo: TimeBasedRouter(config, algo)
            for algo in self._enabled_algorithms
        }

        # Session management (shared across all algorithms)
        self._sessions: Dict[str, MinerSession] = {}
        self._session_lock = asyncio.Lock()

        # Server state - one server per algorithm
        self._servers: Dict[str, asyncio.Server] = {}
        self._stop_event = asyncio.Event()
        self._scheduler_tasks: Dict[str, asyncio.Task] = {}
        self._stats_task: Optional[asyncio.Task] = None

        # Rate limiting - track connection timestamps per IP (shared across algorithms)
        self._connection_attempts: Dict[str, List[float]] = defaultdict(list)
        self._rate_limit_last_cleanup = time.time()
        self._rate_limit_lock = asyncio.Lock()  # Lock for rate limit operations

    async def start(self) -> None:
        """Start the proxy server."""
        logger.info("Starting stratum proxy server...")
        logger.info(f"Enabled algorithms: {', '.join(self._enabled_algorithms)}")

        # Initialize stats with server configuration
        self._init_stats_server_configs()

        # Start per-algorithm routers and servers
        for algo in self._enabled_algorithms:
            router = self._routers[algo]

            # Log initial server for this algorithm
            try:
                initial_server = router.get_current_server()
                logger.info(f"[{algo}] Initial server: {initial_server}")
            except RuntimeError as e:
                logger.error(f"[{algo}] {e}")
                continue

            # Register for server switch notifications
            await router.register_switch_callback(
                lambda new_server, a=algo: self._on_server_switch(a, new_server)
            )

            # Start the time-based scheduler for this algorithm
            task = asyncio.create_task(
                router.run_scheduler(self._stop_event)
            )
            _log_task_exception(task, f"Scheduler task ({algo})")
            self._scheduler_tasks[algo] = task

            # Start accepting connections for this algorithm
            algo_config = getattr(self.config.proxy, algo)
            server = await asyncio.start_server(
                lambda r, w, a=algo: self._handle_connection(r, w, a),
                algo_config.bind_host,
                algo_config.bind_port,
            )
            self._servers[algo] = server

            addr = server.sockets[0].getsockname()
            logger.info(f"[{algo}] Listening on {addr[0]}:{addr[1]}")

        # Start the stats logger (logs at minute 0, 15, 30, 45 of every hour)
        self._stats_task = asyncio.create_task(
            run_stats_logger(self._stop_event)
        )
        _log_task_exception(self._stats_task, "Stats logger task")

        # Serve all algorithms until stopped
        if self._servers:
            serve_tasks = []
            for algo, server in self._servers.items():
                async def serve_algo(s: asyncio.Server, a: str):
                    async with s:
                        await s.serve_forever()
                serve_tasks.append(asyncio.create_task(serve_algo(server, algo)))

            # Wait for all to complete (they won't unless stopped)
            await asyncio.gather(*serve_tasks, return_exceptions=True)

    async def stop(self) -> None:
        """Stop the proxy server gracefully."""
        logger.info("Stopping proxy server...")

        # Signal stop
        self._stop_event.set()

        # Stop accepting new connections on all servers (but don't wait yet - that would deadlock)
        for algo, server in self._servers.items():
            server.close()

        # CLOSE SESSIONS FIRST - this must happen before wait_closed()
        # because wait_closed() waits for all _handle_connection callbacks to finish,
        # and those won't finish until session.run() exits, which requires closing sessions
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

        # NOW wait for all connection handlers to finish on all servers
        for algo, server in self._servers.items():
            try:
                await asyncio.wait_for(server.wait_closed(), timeout=2.0)
            except asyncio.TimeoutError:
                logger.warning(f"Timeout waiting for {algo} server to close")

        # Stop all scheduler tasks
        for algo, task in self._scheduler_tasks.items():
            task.cancel()
            try:
                await task
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

        logger.info("Proxy server stopped")

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        algorithm: str,
    ) -> None:
        """
        Handle a new miner connection.

        Args:
            reader: Async stream reader.
            writer: Async stream writer.
            algorithm: The algorithm for this connection (sha256, randomx, zksnark).
        """
        # Get client IP for rate limiting
        client_addr = writer.get_extra_info("peername")
        client_ip = client_addr[0] if client_addr else "unknown"

        # Check per-IP rate limit
        if not await self._check_rate_limit(client_ip):
            logger.warning(f"[{algorithm}] Rate limit exceeded for {client_ip}, rejecting connection")
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass  # Connection already broken
            return

        # Check connection limit (global across all algorithms)
        async with self._session_lock:
            if len(self._sessions) >= self.config.proxy.global_.max_connections:
                logger.warning(f"[{algorithm}] Max connections reached, rejecting new connection")
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass  # Connection already broken
                return

        # Get the router for this algorithm
        router = self._routers[algorithm]

        # Create session (each session creates its own upstream connection)
        session = MinerSession(
            reader=reader,
            writer=writer,
            router=router,
            config=self.config,
            algorithm=algorithm,
        )

        # Record miner connection
        stats = ProxyStats.get_instance()
        await stats.record_miner_connect(session.session_id)

        # Register session
        async with self._session_lock:
            self._sessions[session.session_id] = session

        try:
            # Run session
            await session.run()
        finally:
            # Record miner disconnection
            await stats.record_miner_disconnect(session.session_id)

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
            # Filter in-place
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

    async def _on_server_switch(self, algorithm: str, new_server: str) -> None:
        """
        Handle server switch notification from router.

        Each session manages its own upstream connection, so we just tell
        sessions for this algorithm to switch.

        Args:
            algorithm: The algorithm for which the switch is happening.
            new_server: Name of the new server.
        """
        logger.info(f"[{algorithm}] Server switch triggered: switching to {new_server}")

        # Switch only sessions for this algorithm
        async with self._session_lock:
            sessions = [
                s for s in self._sessions.values()
                if s.is_active and s.algorithm == algorithm
            ]

        switch_tasks = [
            session.handle_server_switch(new_server)
            for session in sessions
        ]

        if switch_tasks:
            results = await asyncio.gather(*switch_tasks, return_exceptions=True)
            # Count and log any exceptions that occurred during switch
            failed_count = 0
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    failed_count += 1
                    logger.error(f"[{algorithm}] Session switch failed: {result}")

            successful_count = len(switch_tasks) - failed_count
            if failed_count > 0:
                logger.warning(
                    f"[{algorithm}] Server switch to {new_server}: {successful_count} succeeded, "
                    f"{failed_count} failed"
                )
            else:
                logger.info(f"[{algorithm}] Switched {len(switch_tasks)} sessions to {new_server}")

    def _init_stats_server_configs(self) -> None:
        """Initialize ProxyStats with server configuration info."""
        from crypto_stratum_proxy.config.models import END_OF_DAY

        configs = []

        for algo in self._enabled_algorithms:
            # Build ServerConfigInfo for each server in this algorithm
            for server in self.config.servers.get(algo, []):
                if not server.enabled:
                    continue

                # Get schedule times from server
                if server.has_schedule:
                    start_str = server.start.strftime("%H:%M")
                    # Handle end-of-day sentinel (23:59:59 -> "24:00")
                    if server.end == END_OF_DAY:
                        end_str = "24:00"
                    else:
                        end_str = server.end.strftime("%H:%M")
                else:
                    start_str = "--:--"
                    end_str = "--:--"

                configs.append(ServerConfigInfo(
                    name=server.name,
                    host=server.host,
                    port=server.port,
                    username=server.username,
                    schedule_start=start_str,
                    schedule_end=end_str,
                    algorithm=algo,
                ))

        stats = ProxyStats.get_instance()
        stats.set_server_configs(configs)

    @property
    def active_sessions(self) -> int:
        """Get count of active miner sessions."""
        return len(self._sessions)

    def get_current_server(self, algorithm: str) -> str:
        """Get the current active server name for an algorithm."""
        if algorithm in self._routers:
            return self._routers[algorithm].get_current_server()
        raise RuntimeError(f"No router for algorithm: {algorithm}")


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
