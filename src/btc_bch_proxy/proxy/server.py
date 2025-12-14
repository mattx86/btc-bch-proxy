"""Main stratum proxy server."""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Dict, Optional

from loguru import logger

from btc_bch_proxy.proxy.router import TimeBasedRouter
from btc_bch_proxy.proxy.session import MinerSession
from btc_bch_proxy.proxy.upstream import UpstreamManager

if TYPE_CHECKING:
    from btc_bch_proxy.config.models import Config


class StratumProxyServer:
    """
    Main stratum proxy server.

    Handles:
    - Accepting miner connections
    - Managing miner sessions
    - Coordinating with the time-based router
    - Graceful shutdown
    """

    def __init__(self, config: Config):
        """
        Initialize the proxy server.

        Args:
            config: Application configuration.
        """
        self.config = config

        # Create upstream manager for all configured servers
        self.upstream_manager = UpstreamManager(config.servers)

        # Create time-based router
        self.router = TimeBasedRouter(config)

        # Session management
        self._sessions: Dict[str, MinerSession] = {}
        self._session_lock = asyncio.Lock()

        # Server state
        self._server: Optional[asyncio.Server] = None
        self._stop_event = asyncio.Event()
        self._scheduler_task: Optional[asyncio.Task] = None

        # Failover tracking
        self._failover_start_time: Optional[float] = None
        self._primary_server_retrying = False

    async def start(self) -> None:
        """Start the proxy server."""
        logger.info("Starting stratum proxy server...")

        # Pre-connect to the initial server
        initial_server = self.router.get_current_server()
        logger.info(f"Initial server: {initial_server}")

        if not await self.upstream_manager.connect_server(initial_server):
            logger.warning(f"Failed to connect to initial server {initial_server}")
            # Will retry during operation

        # Register for server switch notifications
        self.router.register_switch_callback(self._on_server_switch)

        # Start the time-based scheduler
        self._scheduler_task = asyncio.create_task(
            self.router.run_scheduler(self._stop_event)
        )

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

        # Close all sessions
        async with self._session_lock:
            for session in list(self._sessions.values()):
                await session.close()
            self._sessions.clear()

        # Disconnect from upstream servers
        await self.upstream_manager.disconnect_all()

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
        # Check connection limit
        async with self._session_lock:
            if len(self._sessions) >= self.config.proxy.max_connections:
                logger.warning("Max connections reached, rejecting new connection")
                writer.close()
                await writer.wait_closed()
                return

        # Create session
        session = MinerSession(
            reader=reader,
            writer=writer,
            router=self.router,
            upstream_manager=self.upstream_manager,
            config=self.config,
        )

        # Register session
        async with self._session_lock:
            self._sessions[session.session_id] = session

        try:
            # Run session
            await session.run()
        finally:
            # Unregister session
            async with self._session_lock:
                self._sessions.pop(session.session_id, None)

    async def _on_server_switch(self, new_server: str) -> None:
        """
        Handle server switch notification from router.

        Args:
            new_server: Name of the new server.
        """
        logger.info(f"Server switch triggered: switching to {new_server}")

        # Try to connect to new server
        if not await self.upstream_manager.connect_server(new_server):
            logger.error(f"Failed to connect to {new_server}")
            # Start failover logic
            await self._handle_failover(new_server)
            return

        # Reset failover state
        self._failover_start_time = None
        self._primary_server_retrying = False

        # Switch all active sessions
        async with self._session_lock:
            sessions = list(self._sessions.values())

        switch_tasks = [
            session.handle_server_switch(new_server)
            for session in sessions
            if session.is_active
        ]

        if switch_tasks:
            await asyncio.gather(*switch_tasks, return_exceptions=True)

        logger.info(f"Switched {len(switch_tasks)} sessions to {new_server}")

    async def _handle_failover(self, failed_server: str) -> None:
        """
        Handle failover when a server is unavailable.

        Args:
            failed_server: Name of the server that failed.
        """
        retry_timeout = self.config.failover.retry_timeout_minutes * 60

        # Start tracking retry time
        if self._failover_start_time is None:
            self._failover_start_time = time.time()
            self._primary_server_retrying = True
            logger.warning(
                f"Server {failed_server} unavailable, starting {self.config.failover.retry_timeout_minutes}min retry period"
            )

        # Check if we've exceeded retry timeout
        elapsed = time.time() - self._failover_start_time
        if elapsed >= retry_timeout:
            # Find failover server
            failover_server = self.router.get_failover_server(failed_server)
            if failover_server:
                logger.warning(
                    f"Retry timeout exceeded, failing over to {failover_server}"
                )
                self.router.activate_failover(failover_server)

                # Try to connect to failover
                if await self.upstream_manager.connect_server(failover_server):
                    # Switch all sessions to failover
                    async with self._session_lock:
                        sessions = list(self._sessions.values())

                    for session in sessions:
                        if session.is_active:
                            await session.handle_server_switch(failover_server)
                else:
                    logger.error(f"Failover server {failover_server} also unavailable")
            else:
                logger.error("No failover server available")

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

    # Handle shutdown signals
    if stop_event:
        async def wait_for_stop():
            await stop_event.wait()
            await server.stop()

        stop_task = asyncio.create_task(wait_for_stop())

    try:
        await server.start()
    except asyncio.CancelledError:
        pass
    finally:
        await server.stop()
