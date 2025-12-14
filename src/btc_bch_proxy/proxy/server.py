"""Main stratum proxy server."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Dict, Optional

from loguru import logger

from btc_bch_proxy.proxy.router import TimeBasedRouter
from btc_bch_proxy.proxy.session import MinerSession

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

    async def start(self) -> None:
        """Start the proxy server."""
        logger.info("Starting stratum proxy server...")

        # Log initial server
        initial_server = self.router.get_current_server()
        logger.info(f"Initial server: {initial_server}")

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

        # Close all sessions (each session closes its own upstream connection)
        async with self._session_lock:
            for session in list(self._sessions.values()):
                await session.close()
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
        # Check connection limit
        async with self._session_lock:
            if len(self._sessions) >= self.config.proxy.max_connections:
                logger.warning("Max connections reached, rejecting new connection")
                writer.close()
                await writer.wait_closed()
                return

        # Create session (each session creates its own upstream connection)
        session = MinerSession(
            reader=reader,
            writer=writer,
            router=self.router,
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
            await asyncio.gather(*switch_tasks, return_exceptions=True)

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
