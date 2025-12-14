"""Miner session handler - manages individual miner connections."""

from __future__ import annotations

import asyncio
import uuid
from typing import TYPE_CHECKING, Optional

from loguru import logger

from btc_bch_proxy.stratum.messages import (
    StratumMethods,
    StratumNotification,
    StratumRequest,
    StratumResponse,
)
from btc_bch_proxy.stratum.protocol import StratumProtocol
from btc_bch_proxy.proxy.upstream import UpstreamConnection

if TYPE_CHECKING:
    from btc_bch_proxy.config.models import Config
    from btc_bch_proxy.proxy.router import TimeBasedRouter


class MinerSession:
    """
    Handles a single miner's connection lifecycle.

    Manages:
    - Miner subscription and authorization
    - Message relay between miner and upstream
    - Server switching during time-based transitions
    - Graceful handling of pending shares during switches
    """

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        router: TimeBasedRouter,
        config: Config,
    ):
        """
        Initialize a miner session.

        Args:
            reader: Async stream reader for miner connection.
            writer: Async stream writer for miner connection.
            router: Time-based router for server selection.
            config: Application configuration.
        """
        self.reader = reader
        self.writer = writer
        self.router = router
        self.config = config

        self.session_id = uuid.uuid4().hex[:8]
        self._protocol = StratumProtocol()

        # Session state
        self._subscribed = False
        self._authorized = False
        self._running = False
        self._closing = False

        # Miner info
        self.worker_name: Optional[str] = None
        self.user_agent: Optional[str] = None

        # Current upstream - each session gets its OWN connection
        self._current_server: Optional[str] = None
        self._upstream: Optional[UpstreamConnection] = None

        # Pending requests and shares
        self._pending_shares: dict[int, asyncio.Future] = {}
        self._miner_request_id = 0

        # Client address
        peername = writer.get_extra_info("peername")
        self.client_addr = f"{peername[0]}:{peername[1]}" if peername else "unknown"

        logger.info(f"[{self.session_id}] New miner session from {self.client_addr}")

    @property
    def is_active(self) -> bool:
        """Check if the session is active."""
        return self._running and not self._closing

    async def run(self) -> None:
        """Main session loop - handle the miner connection lifecycle."""
        self._running = True

        try:
            # Connect to initial upstream server
            server_name = self.router.get_current_server()
            if not await self._connect_upstream(server_name):
                logger.error(f"[{self.session_id}] Failed to connect to upstream")
                return

            # Start relay tasks
            miner_task = asyncio.create_task(self._miner_read_loop())
            upstream_task = asyncio.create_task(self._upstream_read_loop())

            # Wait for either task to complete (connection closed or error)
            done, pending = await asyncio.wait(
                [miner_task, upstream_task],
                return_when=asyncio.FIRST_COMPLETED,
            )

            # Cancel pending tasks
            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        except Exception as e:
            logger.error(f"[{self.session_id}] Session error: {e}")
        finally:
            await self.close()

    async def _connect_upstream(self, server_name: str) -> bool:
        """
        Connect to an upstream server.

        Each session creates its OWN upstream connection for isolation.

        Args:
            server_name: Name of the server to connect to.

        Returns:
            True if connected successfully.
        """
        # Wait for pending shares on current upstream before switching
        if self._upstream and self._upstream.has_pending_shares:
            logger.info(
                f"[{self.session_id}] Waiting for {self._upstream.pending_share_count} "
                f"pending shares before switching"
            )
            start_time = asyncio.get_event_loop().time()
            while self._upstream.has_pending_shares:
                if asyncio.get_event_loop().time() - start_time > 10.0:
                    logger.warning(f"[{self.session_id}] Timeout waiting for pending shares")
                    break
                await self._upstream.read_messages()
                await asyncio.sleep(0.1)

        # Disconnect old upstream if switching
        if self._upstream:
            await self._upstream.disconnect()

        # Get server config
        server_config = self.router.get_server_config(server_name)
        if not server_config:
            logger.error(f"[{self.session_id}] Unknown server: {server_name}")
            return False

        # Create a NEW upstream connection for this session
        self._upstream = UpstreamConnection(server_config)

        # Connect
        if not await self._upstream.connect():
            logger.error(f"[{self.session_id}] Failed to connect to {server_name}")
            return False

        # Subscribe
        if not await self._upstream.subscribe():
            await self._upstream.disconnect()
            logger.error(f"[{self.session_id}] Failed to subscribe to {server_name}")
            return False

        # Authorize
        if not await self._upstream.authorize():
            await self._upstream.disconnect()
            logger.error(f"[{self.session_id}] Failed to authorize with {server_name}")
            return False

        self._current_server = server_name
        logger.info(f"[{self.session_id}] Connected to upstream {server_name}")
        return True

    async def _miner_read_loop(self) -> None:
        """Read and process messages from the miner."""
        while self._running and not self._closing:
            try:
                data = await asyncio.wait_for(
                    self.reader.read(8192),
                    timeout=self.config.proxy.connection_timeout,
                )

                if not data:
                    logger.info(f"[{self.session_id}] Miner disconnected")
                    break

                messages = self._protocol.feed_data(data)
                for msg in messages:
                    await self._handle_miner_message(msg)

            except asyncio.TimeoutError:
                logger.warning(f"[{self.session_id}] Miner connection timeout")
                break
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[{self.session_id}] Error reading from miner: {e}")
                break

    async def _upstream_read_loop(self) -> None:
        """Read and forward messages from upstream to miner."""
        while self._running and not self._closing:
            try:
                if not self._upstream or not self._upstream.connected:
                    await asyncio.sleep(0.1)
                    continue

                messages = await self._upstream.read_messages()
                for msg in messages:
                    await self._handle_upstream_message(msg)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[{self.session_id}] Error reading from upstream: {e}")
                # Try to reconnect
                if self._current_server:
                    await self._connect_upstream(self._current_server)
                await asyncio.sleep(1)

    async def _handle_miner_message(self, msg) -> None:
        """
        Handle a message from the miner.

        Args:
            msg: Parsed stratum message.
        """
        if isinstance(msg, StratumRequest):
            if msg.method == StratumMethods.MINING_SUBSCRIBE:
                await self._handle_subscribe(msg)
            elif msg.method == StratumMethods.MINING_AUTHORIZE:
                await self._handle_authorize(msg)
            elif msg.method == StratumMethods.MINING_SUBMIT:
                await self._handle_submit(msg)
            else:
                # Forward other requests to upstream
                await self._forward_to_upstream(msg)

    async def _handle_subscribe(self, msg: StratumRequest) -> None:
        """Handle mining.subscribe from miner."""
        logger.debug(f"[{self.session_id}] Miner subscribe: {msg.params}")

        # Extract user agent if provided
        if msg.params:
            self.user_agent = str(msg.params[0])

        # Use upstream's subscription data
        if self._upstream and self._upstream.subscribed:
            # Send response with upstream's extranonce values
            result = [
                [
                    ["mining.set_difficulty", self.session_id],
                    ["mining.notify", self.session_id],
                ],
                self._upstream.extranonce1,
                self._upstream.extranonce2_size,
            ]
            await self._send_to_miner(
                self._protocol.build_response(msg.id, result)
            )
            self._subscribed = True
            logger.info(f"[{self.session_id}] Miner subscribed")
        else:
            # No upstream connected yet
            error = [20, "Upstream not available", None]
            await self._send_to_miner(
                self._protocol.build_response(msg.id, None, error)
            )

    async def _handle_authorize(self, msg: StratumRequest) -> None:
        """Handle mining.authorize from miner."""
        # Accept any credentials from miner
        if len(msg.params) >= 1:
            self.worker_name = str(msg.params[0])

        logger.info(f"[{self.session_id}] Miner authorized as {self.worker_name}")

        # Always accept authorization (we use our own credentials for upstream)
        await self._send_to_miner(
            self._protocol.build_response(msg.id, True)
        )
        self._authorized = True

    async def _handle_submit(self, msg: StratumRequest) -> None:
        """Handle mining.submit (share submission) from miner."""
        if not self._upstream or not self._upstream.authorized:
            error = [24, "Not authorized", None]
            await self._send_to_miner(
                self._protocol.build_response(msg.id, False, error)
            )
            return

        # Parse submit params: [worker_name, job_id, extranonce2, ntime, nonce]
        if len(msg.params) < 5:
            error = [20, "Invalid submit parameters", None]
            await self._send_to_miner(
                self._protocol.build_response(msg.id, False, error)
            )
            return

        worker_name, job_id, extranonce2, ntime, nonce = msg.params[:5]

        logger.debug(
            f"[{self.session_id}] Share submit: job={job_id}, "
            f"nonce={nonce}, worker={worker_name}"
        )

        # Submit to upstream
        accepted, error = await self._upstream.submit_share(
            worker_name, job_id, extranonce2, ntime, nonce
        )

        if accepted:
            logger.info(f"[{self.session_id}] Share accepted")
        else:
            logger.warning(f"[{self.session_id}] Share rejected: {error}")

        await self._send_to_miner(
            self._protocol.build_response(msg.id, accepted, error)
        )

    async def _forward_to_upstream(self, msg: StratumRequest) -> None:
        """Forward a request to the upstream server."""
        if not self._upstream or not self._upstream.connected:
            error = [20, "Upstream not connected", None]
            await self._send_to_miner(
                self._protocol.build_response(msg.id, None, error)
            )
            return

        data = StratumProtocol.encode_message(msg)
        await self._upstream.send_raw(data)

    async def _handle_upstream_message(self, msg) -> None:
        """
        Handle a message from upstream and forward to miner.

        Args:
            msg: Parsed stratum message.
        """
        if isinstance(msg, StratumNotification):
            # Forward notifications directly to miner
            if msg.method in (
                StratumMethods.MINING_NOTIFY,
                StratumMethods.MINING_SET_DIFFICULTY,
                StratumMethods.MINING_SET_EXTRANONCE,
            ):
                data = StratumProtocol.encode_message(msg)
                await self._send_to_miner(data)

                if msg.method == StratumMethods.MINING_NOTIFY:
                    logger.debug(f"[{self.session_id}] Job notification forwarded")
                elif msg.method == StratumMethods.MINING_SET_DIFFICULTY:
                    logger.debug(f"[{self.session_id}] Difficulty set: {msg.params}")

        elif isinstance(msg, StratumResponse):
            # Responses are handled by the upstream connection's pending request system
            pass

    async def _send_to_miner(self, data: bytes) -> None:
        """
        Send data to the miner.

        Args:
            data: Raw bytes to send.
        """
        if self._closing or not self.writer:
            return
        try:
            self.writer.write(data)
            await self.writer.drain()
        except Exception as e:
            logger.error(f"[{self.session_id}] Error sending to miner: {e}")
            self._closing = True

    async def handle_server_switch(self, new_server: str) -> None:
        """
        Handle switching to a different upstream server.

        Args:
            new_server: Name of the new server to use.
        """
        if new_server == self._current_server:
            return

        logger.info(
            f"[{self.session_id}] Switching from {self._current_server} to {new_server}"
        )

        # Connect to new server (this waits for pending shares)
        if not await self._connect_upstream(new_server):
            logger.error(f"[{self.session_id}] Failed to switch to {new_server}")
            return

        # Re-subscribe with new upstream's extranonce
        if self._upstream and self._upstream.subscribed:
            # Send set_extranonce notification to miner if supported
            notification = StratumNotification(
                method=StratumMethods.MINING_SET_EXTRANONCE,
                params=[self._upstream.extranonce1, self._upstream.extranonce2_size],
            )
            await self._send_to_miner(StratumProtocol.encode_message(notification))

            logger.info(
                f"[{self.session_id}] Server switch complete, "
                f"new extranonce1={self._upstream.extranonce1}"
            )

    async def close(self) -> None:
        """Close the session and cleanup resources."""
        if self._closing:
            return

        self._closing = True
        self._running = False

        logger.info(f"[{self.session_id}] Closing session")

        # Close upstream connection
        if self._upstream:
            await self._upstream.disconnect()
            self._upstream = None

        # Close miner connection
        if self.writer:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass

        self.reader = None
        self.writer = None
