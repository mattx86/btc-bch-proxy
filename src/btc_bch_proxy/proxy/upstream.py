"""Upstream stratum server connection management."""

from __future__ import annotations

import asyncio
import ssl
import time as time_module
from typing import TYPE_CHECKING, Dict, Optional, Set

from loguru import logger

from btc_bch_proxy.stratum.messages import (
    StratumMessage,
    StratumMethods,
    StratumNotification,
    StratumRequest,
    StratumResponse,
)
from btc_bch_proxy.stratum.protocol import StratumProtocol
from btc_bch_proxy.proxy.stats import ProxyStats

if TYPE_CHECKING:
    from btc_bch_proxy.config.models import StratumServerConfig


class UpstreamConnectionError(Exception):
    """Error connecting to upstream server."""

    pass


class UpstreamConnection:
    """
    Manages a connection to a single upstream stratum server.

    Handles:
    - Connection establishment with retry logic
    - SSL/TLS support
    - mining.subscribe and mining.authorize
    - Message sending/receiving
    - Pending share submission tracking
    """

    def __init__(self, config: StratumServerConfig):
        """
        Initialize upstream connection.

        Args:
            config: Server configuration.
        """
        self.config = config
        self.name = config.name

        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._protocol = StratumProtocol()

        self._connected = False
        self._subscribed = False
        self._authorized = False

        # Subscription data from pool
        self.extranonce1: Optional[str] = None
        self.extranonce2_size: Optional[int] = None
        self.subscription_id: Optional[str] = None

        # Version-rolling support (negotiated with pool)
        self.version_rolling_supported: bool = False
        self.version_rolling_mask: Optional[str] = None

        # Request tracking
        self._request_id = 0
        self._pending_requests: Dict[int, asyncio.Future] = {}
        self._pending_shares: Set[int] = set()

        # Connection state
        self._retry_count = 0
        self._last_connect_attempt = 0.0
        self._has_connected_before = False  # Track if this is a reconnection

        # Lock to prevent concurrent socket reads
        self._read_lock = asyncio.Lock()

        # Queue for notifications received during handshake
        self._pending_notifications: list[StratumMessage] = []

    @property
    def connected(self) -> bool:
        """Check if connected to the server."""
        return self._connected and self._writer is not None

    @property
    def subscribed(self) -> bool:
        """Check if subscribed to the pool."""
        return self._subscribed

    @property
    def authorized(self) -> bool:
        """Check if authorized with the pool."""
        return self._authorized

    @property
    def has_pending_shares(self) -> bool:
        """Check if there are pending share submissions."""
        return len(self._pending_shares) > 0

    @property
    def pending_share_count(self) -> int:
        """Get the number of pending shares."""
        return len(self._pending_shares)

    def get_pending_notifications(self) -> list[StratumMessage]:
        """
        Get and clear any notifications received during handshake.

        Returns:
            List of queued notifications.
        """
        notifications = self._pending_notifications
        self._pending_notifications = []
        return notifications

    def _next_id(self) -> int:
        """Get the next request ID."""
        self._request_id += 1
        return self._request_id

    async def connect(self) -> bool:
        """
        Establish connection to the upstream server.

        Returns:
            True if connection successful, False otherwise.
        """
        if self._connected:
            return True

        # Rate limit connection attempts
        now = time_module.time()
        if now - self._last_connect_attempt < self.config.retry_interval:
            return False
        self._last_connect_attempt = now

        try:
            logger.info(
                f"Connecting to upstream {self.name} ({self.config.host}:{self.config.port})"
            )

            # Setup SSL if needed
            ssl_context = None
            if self.config.ssl:
                ssl_context = ssl.create_default_context()

            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(
                    self.config.host,
                    self.config.port,
                    ssl=ssl_context,
                ),
                timeout=self.config.timeout,
            )

            self._connected = True
            self._retry_count = 0
            self._protocol.reset_buffer()

            # Record connection stats
            stats = ProxyStats.get_instance()
            if self._has_connected_before:
                asyncio.create_task(stats.record_upstream_reconnect(self.name))
            else:
                asyncio.create_task(stats.record_upstream_connect(self.name))
                self._has_connected_before = True

            logger.info(f"Connected to upstream {self.name}")
            return True

        except asyncio.TimeoutError:
            logger.warning(f"Connection to {self.name} timed out")
            self._retry_count += 1
            return False
        except OSError as e:
            logger.warning(f"Connection to {self.name} failed: {e}")
            self._retry_count += 1
            return False
        except Exception as e:
            logger.error(f"Unexpected error connecting to {self.name}: {e}")
            self._retry_count += 1
            return False

    async def disconnect(self) -> None:
        """Close the connection to the upstream server."""
        if self._writer:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception as e:
                logger.debug(f"Error closing connection to {self.name}: {e}")

        self._connected = False
        self._subscribed = False
        self._authorized = False
        self._reader = None
        self._writer = None
        self.extranonce1 = None
        self.extranonce2_size = None
        self.subscription_id = None

        # Cancel pending requests
        for future in self._pending_requests.values():
            if not future.done():
                future.cancel()
        self._pending_requests.clear()

        logger.info(f"Disconnected from upstream {self.name}")

    async def configure(self, extensions: list[str] = None) -> bool:
        """
        Send mining.configure to negotiate extensions with the pool.

        Args:
            extensions: List of extensions to request (default: version-rolling).

        Returns:
            True if configuration successful (even if no extensions supported).
        """
        if not self._connected:
            return False

        if extensions is None:
            extensions = ["version-rolling"]

        req_id = self._next_id()
        # Request version-rolling with full mask
        params = [extensions, {"version-rolling.mask": "ffffffff"}]

        try:
            response = await self._send_request(
                req_id, "mining.configure", params
            )

            if response.is_error:
                # Pool doesn't support mining.configure - that's OK
                logger.debug(f"Pool {self.name} doesn't support mining.configure: {response.error}")
                return True

            # Parse the response for version-rolling support
            result = response.result
            if isinstance(result, dict):
                if result.get("version-rolling"):
                    self.version_rolling_supported = True
                    self.version_rolling_mask = result.get("version-rolling.mask", "ffffffff")
                    logger.info(
                        f"Pool {self.name} supports version-rolling with mask {self.version_rolling_mask}"
                    )
                else:
                    logger.info(f"Pool {self.name} does not support version-rolling")
            else:
                logger.debug(f"Pool {self.name} returned unexpected configure result: {result}")

            return True

        except asyncio.TimeoutError:
            # Timeout on configure is OK - pool might not support it
            logger.debug(f"Configure timeout for {self.name} (pool may not support it)")
            return True
        except Exception as e:
            logger.debug(f"Configure error for {self.name}: {e}")
            return True  # Don't fail the connection for configure errors

    async def subscribe(self, user_agent: str = "btc-bch-proxy/0.1.0") -> bool:
        """
        Send mining.subscribe to the pool.

        Args:
            user_agent: User agent string to send.

        Returns:
            True if subscription successful.
        """
        if not self._connected:
            return False

        req_id = self._next_id()
        params = [user_agent]

        try:
            response = await self._send_request(
                req_id, StratumMethods.MINING_SUBSCRIBE, params
            )

            if response.is_error:
                logger.error(f"Subscribe failed for {self.name}: {response.error}")
                return False

            # Parse subscription response
            # Result format: [[["mining.set_difficulty", "subscription_id"], ["mining.notify", "subscription_id"]], extranonce1, extranonce2_size]
            result = response.result
            if isinstance(result, list) and len(result) >= 3:
                self.subscription_id = str(result[0])
                self.extranonce1 = str(result[1])
                self.extranonce2_size = int(result[2])
                self._subscribed = True
                logger.info(
                    f"Subscribed to {self.name}: extranonce1={self.extranonce1}, "
                    f"extranonce2_size={self.extranonce2_size}"
                )
                return True
            else:
                logger.error(f"Invalid subscribe response from {self.name}: {result}")
                return False

        except asyncio.TimeoutError:
            logger.error(f"Subscribe timeout for {self.name}")
            return False
        except Exception as e:
            logger.error(f"Subscribe error for {self.name}: {e}")
            return False

    async def authorize(self) -> bool:
        """
        Send mining.authorize to the pool using configured credentials.

        Returns:
            True if authorization successful.
        """
        if not self._connected or not self._subscribed:
            return False

        req_id = self._next_id()
        params = [self.config.username, self.config.password]

        try:
            response = await self._send_request(
                req_id, StratumMethods.MINING_AUTHORIZE, params
            )

            if response.is_error:
                logger.error(f"Authorize failed for {self.name}: {response.error}")
                return False

            if response.result:
                self._authorized = True
                logger.info(f"Authorized with {self.name} as {self.config.username}")
                return True
            else:
                logger.error(f"Authorization rejected by {self.name}")
                return False

        except asyncio.TimeoutError:
            logger.error(f"Authorize timeout for {self.name}")
            return False
        except Exception as e:
            logger.error(f"Authorize error for {self.name}: {e}")
            return False

    async def submit_share(
        self,
        worker_name: str,
        job_id: str,
        extranonce2: str,
        ntime: str,
        nonce: str,
        version_bits: Optional[str] = None,
    ) -> tuple[bool, Optional[list]]:
        """
        Submit a share to the pool.

        Args:
            worker_name: Worker name (from miner).
            job_id: Job ID.
            extranonce2: Extranonce2 value.
            ntime: Block time.
            nonce: Block nonce.
            version_bits: Version bits for version-rolling (optional).

        Returns:
            Tuple of (accepted, error).
        """
        if not self._connected or not self._authorized:
            return False, [20, "Not connected or authorized", None]

        req_id = self._next_id()
        # Use the pool's configured username, not the miner's worker name
        params = [self.config.username, job_id, extranonce2, ntime, nonce]
        # Include version_bits if version-rolling is enabled
        if version_bits is not None and self.version_rolling_supported:
            params.append(version_bits)

        self._pending_shares.add(req_id)

        try:
            response = await self._send_request(
                req_id, StratumMethods.MINING_SUBMIT, params
            )

            if response.is_error:
                return False, response.error
            return response.result is True, None

        except asyncio.TimeoutError:
            logger.warning(f"Share submit timeout for {self.name}")
            return False, [20, "Timeout", None]
        except Exception as e:
            logger.error(f"Share submit error for {self.name}: {e}")
            return False, [20, str(e), None]
        finally:
            self._pending_shares.discard(req_id)

    async def _send_request(
        self, req_id: int, method: str, params: list
    ) -> StratumResponse:
        """
        Send a request and wait for the response.

        Args:
            req_id: Request ID.
            method: Method name.
            params: Method parameters.

        Returns:
            Response message.

        Raises:
            asyncio.TimeoutError: If response not received in time.
        """
        if not self._writer or not self._reader:
            raise UpstreamConnectionError("Not connected")

        # Create future for response
        future: asyncio.Future[StratumResponse] = asyncio.Future()
        self._pending_requests[req_id] = future

        # Send request
        msg = self._protocol.build_request(req_id, method, params)
        logger.debug(f"Sending to {self.name}: {msg.decode().strip()}")
        self._writer.write(msg)
        await self._writer.drain()

        try:
            # Read from socket while waiting for the response (with lock to prevent concurrent reads)
            deadline = asyncio.get_event_loop().time() + self.config.timeout
            while not future.done():
                remaining = deadline - asyncio.get_event_loop().time()
                if remaining <= 0:
                    raise asyncio.TimeoutError()

                # Use lock to prevent concurrent socket reads
                async with self._read_lock:
                    # Check again if future is done (another coroutine may have resolved it)
                    if future.done():
                        break

                    try:
                        data = await asyncio.wait_for(
                            self._reader.read(8192),
                            timeout=min(remaining, 1.0),
                        )
                        if not data:
                            raise UpstreamConnectionError("Connection closed by server")

                        logger.debug(f"Received from {self.name}: {data.decode().strip()}")
                        messages = self._protocol.feed_data(data)

                        # Process responses and queue notifications
                        for recv_msg in messages:
                            if isinstance(recv_msg, StratumResponse):
                                pending_future = self._pending_requests.get(recv_msg.id)
                                if pending_future and not pending_future.done():
                                    pending_future.set_result(recv_msg)
                            elif isinstance(recv_msg, StratumNotification):
                                # Queue notifications for later processing
                                self._pending_notifications.append(recv_msg)
                                logger.debug(f"Queued notification: {recv_msg.method}")

                    except asyncio.TimeoutError:
                        # Just a read timeout, keep waiting if we have time left
                        continue

            return future.result()
        finally:
            self._pending_requests.pop(req_id, None)

    async def send_raw(self, data: bytes) -> None:
        """
        Send raw data to the upstream server.

        Args:
            data: Raw bytes to send.
        """
        if not self._writer:
            raise UpstreamConnectionError("Not connected")
        self._writer.write(data)
        await self._writer.drain()

    async def read_messages(self) -> list[StratumMessage]:
        """
        Read and parse available messages from the upstream server.

        Returns:
            List of parsed messages.
        """
        if not self._reader:
            return []

        # Use lock to prevent concurrent socket reads
        async with self._read_lock:
            try:
                data = await asyncio.wait_for(
                    self._reader.read(8192),
                    timeout=0.1,  # Short timeout for non-blocking read
                )
                if not data:
                    # Connection closed
                    logger.warning(f"Connection closed by {self.name}")
                    await self.disconnect()
                    return []

                messages = self._protocol.feed_data(data)

                # Process responses to pending requests
                for msg in messages:
                    if isinstance(msg, StratumResponse):
                        future = self._pending_requests.get(msg.id)
                        if future and not future.done():
                            future.set_result(msg)

                return messages

            except asyncio.TimeoutError:
                return []
            except Exception as e:
                logger.error(f"Error reading from {self.name}: {e}")
                await self.disconnect()
                return []

    def can_retry(self) -> bool:
        """Check if we can retry connecting."""
        return self._retry_count < self.config.max_retries


class UpstreamManager:
    """
    Manages connections to multiple upstream stratum servers.

    Handles:
    - Connection pool for all configured servers
    - Pre-connecting to servers
    - Server switching with pending share handling
    """

    def __init__(self, servers: list[StratumServerConfig]):
        """
        Initialize the upstream manager.

        Args:
            servers: List of server configurations.
        """
        self._connections: Dict[str, UpstreamConnection] = {}
        for server in servers:
            self._connections[server.name] = UpstreamConnection(server)

    def get(self, server_name: str) -> Optional[UpstreamConnection]:
        """
        Get a connection by server name.

        Args:
            server_name: Name of the server.

        Returns:
            UpstreamConnection or None.
        """
        return self._connections.get(server_name)

    async def connect_server(self, server_name: str) -> bool:
        """
        Connect to a specific server and complete handshake.

        Args:
            server_name: Name of the server.

        Returns:
            True if connected and authorized.
        """
        conn = self._connections.get(server_name)
        if not conn:
            logger.error(f"Unknown server: {server_name}")
            return False

        if conn.authorized:
            return True

        if not await conn.connect():
            return False

        if not await conn.subscribe():
            await conn.disconnect()
            return False

        if not await conn.authorize():
            await conn.disconnect()
            return False

        return True

    async def connect_all(self) -> Dict[str, bool]:
        """
        Attempt to connect to all configured servers.

        Returns:
            Dict of server_name -> success status.
        """
        results = {}
        for name in self._connections:
            results[name] = await self.connect_server(name)
        return results

    async def disconnect_all(self) -> None:
        """Disconnect from all servers."""
        for conn in self._connections.values():
            await conn.disconnect()

    async def wait_for_pending_shares(
        self, server_name: str, timeout: float = 10.0
    ) -> bool:
        """
        Wait for pending shares to be acknowledged.

        Args:
            server_name: Name of the server.
            timeout: Maximum time to wait.

        Returns:
            True if all shares completed, False if timeout.
        """
        conn = self._connections.get(server_name)
        if not conn:
            return True

        start = time_module.time()
        while conn.has_pending_shares:
            if time_module.time() - start > timeout:
                logger.warning(
                    f"Timeout waiting for {conn.pending_share_count} pending shares on {server_name}"
                )
                return False

            # Process any incoming messages to complete share responses
            await conn.read_messages()
            await asyncio.sleep(0.1)

        return True

    @property
    def server_names(self) -> list[str]:
        """Get list of all server names."""
        return list(self._connections.keys())
