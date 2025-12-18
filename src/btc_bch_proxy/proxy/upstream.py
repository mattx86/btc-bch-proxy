"""Upstream stratum server connection management."""

from __future__ import annotations

import asyncio
import errno
import ssl
import time as time_module
from typing import TYPE_CHECKING, Dict, Optional, Set

from loguru import logger

from btc_bch_proxy import __version__
from btc_bch_proxy.stratum.messages import (
    StratumMessage,
    StratumMethods,
    StratumNotification,
    StratumResponse,
)
from btc_bch_proxy.stratum.protocol import StratumProtocol
from btc_bch_proxy.proxy.stats import ProxyStats
from btc_bch_proxy.proxy.constants import (
    DEFAULT_PENDING_SHARES_WAIT_TIMEOUT,
    DEFAULT_UPSTREAM_HEALTH_TIMEOUT,
    MAX_EXTRANONCE1_HEX_LENGTH,
    MAX_EXTRANONCE2_SIZE,
    MAX_PENDING_NOTIFICATIONS,
    NON_BLOCKING_READ_TIMEOUT,
    POLL_SLEEP_INTERVAL,
    SOCKET_READ_BUFFER_SIZE,
    UPSTREAM_DISCONNECT_TIMEOUT,
)
from btc_bch_proxy.proxy.utils import fire_and_forget

if TYPE_CHECKING:
    from btc_bch_proxy.config.models import ProxyConfig, StratumServerConfig


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
    - TCP keepalive for connection health

    Lock Ordering (to prevent deadlocks):
        When acquiring multiple locks, always acquire in this order:
        1. _read_lock - Protects socket read operations
        2. _request_id_lock - Protects request ID counter
        3. _pending_requests_lock - Protects pending request futures
        4. _pending_notifications_lock - Protects notification queue
        5. _pending_shares_lock - Protects share tracking set

        Note: In _next_id(), we acquire _request_id_lock -> _pending_requests_lock.
        This is the only place where multiple locks are held simultaneously.

    Notification Handling:
        During the initial handshake (configure, subscribe, authorize), any
        mining.notify and mining.set_difficulty notifications are queued in
        _pending_notifications. Call get_pending_notifications() after handshake
        to retrieve them.

        Note: Notifications that arrive during the brief window between disconnect
        and reconnect are lost. This is acceptable because the pool will send
        fresh notifications after reconnection.
    """

    def __init__(self, config: StratumServerConfig, proxy_config: Optional[ProxyConfig] = None):
        """
        Initialize upstream connection.

        Args:
            config: Server configuration.
            proxy_config: Proxy configuration (for keepalive settings).
        """
        self.config = config
        self.proxy_config = proxy_config
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
        self._request_id_lock = asyncio.Lock()  # Lock for _request_id increment
        self._pending_requests: Dict[int, asyncio.Future] = {}
        self._pending_requests_lock = asyncio.Lock()  # Lock for _pending_requests access
        self._pending_shares: Set[int] = set()
        self._pending_shares_lock = asyncio.Lock()  # Lock for _pending_shares access

        # Connection state
        self._retry_count = 0
        self._last_connect_attempt = 0.0
        self._has_connected_before = False  # Track if this is a reconnection

        # Lock to prevent concurrent socket reads
        self._read_lock = asyncio.Lock()

        # Queue for notifications received during handshake
        self._pending_notifications: list[StratumMessage] = []
        self._pending_notifications_lock = asyncio.Lock()  # Lock for _pending_notifications

        # Connection health tracking
        self._last_message_time: float = 0.0
        self._connection_start_time: float = 0.0
        # Health timeout from config or 5-minute default
        self._connection_health_timeout: float = float(
            proxy_config.upstream_health_timeout if proxy_config else DEFAULT_UPSTREAM_HEALTH_TIMEOUT
        )
        # Track whether we've logged the unhealthy state (to avoid log spam)
        self._unhealthy_logged: bool = False

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
        """
        Check if there are pending share submissions (approximate).

        Note: This is a synchronous property for convenience. For strict
        consistency in async contexts, use get_pending_share_count() instead.
        Safe for typical use due to CPython's GIL guaranteeing atomic len().

        WARNING: Do not use this for critical decisions that require exact
        counts. The value may be stale by the time it's used. For critical
        paths, use async with _pending_shares_lock directly.
        """
        return len(self._pending_shares) > 0

    @property
    def pending_share_count(self) -> int:
        """
        Get the number of pending shares.

        Note: This is a synchronous property for convenience. For strict
        consistency in async contexts, use get_pending_share_count() instead.
        """
        return len(self._pending_shares)

    async def get_pending_share_count(self) -> int:
        """Get the number of pending shares (async-safe)."""
        async with self._pending_shares_lock:
            return len(self._pending_shares)

    @property
    def is_healthy(self) -> bool:
        """
        Check if the connection is healthy (recently received messages).

        Note: Reads _last_message_time without lock. This is safe because
        reading a float is atomic in Python (GIL guarantee), and slight
        timing inaccuracy is acceptable for health checks.
        """
        if not self._connected:
            return False
        now = time_module.time()
        if self._last_message_time == 0:
            # No messages yet - check how long since connection
            # If we've been connected for longer than the health timeout without
            # receiving any messages, that's unhealthy
            if self._connection_start_time > 0:
                elapsed_since_connect = now - self._connection_start_time
                return elapsed_since_connect < self._connection_health_timeout
            return True  # Just connected, no messages yet
        elapsed = now - self._last_message_time
        return elapsed < self._connection_health_timeout

    @property
    def seconds_since_last_message(self) -> float:
        """
        Get seconds since last message was received.

        Note: Returns approximate value due to concurrent access (see is_healthy).
        """
        if self._last_message_time == 0:
            return 0.0
        return time_module.time() - self._last_message_time

    async def get_pending_notifications(self) -> list[StratumMessage]:
        """
        Get and clear any notifications received during handshake.

        Returns:
            List of queued notifications.
        """
        async with self._pending_notifications_lock:
            notifications = self._pending_notifications
            self._pending_notifications = []
            return notifications

    # Maximum request ID before wrapping (32-bit unsigned max)
    MAX_REQUEST_ID = 0xFFFFFFFF
    # Maximum attempts to find an unused ID before giving up
    MAX_ID_ATTEMPTS = 100

    async def _next_id(self) -> int:
        """
        Get the next request ID, wrapping at MAX_REQUEST_ID.

        Avoids IDs that are currently in use by pending requests to prevent
        ID collisions after wrap-around.

        Lock ordering: _request_id_lock -> _pending_requests_lock
        This ordering must be maintained to prevent deadlocks.
        """
        async with self._request_id_lock:
            for _ in range(self.MAX_ID_ATTEMPTS):
                self._request_id = (self._request_id + 1) % self.MAX_REQUEST_ID
                # Avoid 0 as some pools may treat it as null/missing
                if self._request_id == 0:
                    self._request_id = 1
                # Check for collision with pending requests under lock
                async with self._pending_requests_lock:
                    if self._request_id not in self._pending_requests:
                        return self._request_id
            # If we couldn't find a free ID after MAX_ID_ATTEMPTS, just use the
            # current one. This should never happen in practice (would require
            # 100+ concurrent pending requests with perfect wrap-around collision).
            logger.warning(f"Could not find unused request ID after {self.MAX_ID_ATTEMPTS} attempts")
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
            # create_default_context() enables certificate verification by default.
            # This is the secure default - pools should have valid certificates.
            #
            # SECURITY WARNING: Never set check_hostname=False or verify_mode=CERT_NONE
            # in production. Disabling verification allows man-in-the-middle attacks
            # where an attacker can intercept and steal mining rewards.
            ssl_context = None
            server_hostname = None
            if self.config.ssl:
                ssl_context = ssl.create_default_context()
                # Enable hostname verification to prevent MITM attacks
                ssl_context.check_hostname = True
                server_hostname = self.config.host

            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(
                    self.config.host,
                    self.config.port,
                    ssl=ssl_context,
                    server_hostname=server_hostname,
                ),
                timeout=self.config.timeout,
            )

            self._connected = True
            self._retry_count = 0
            self._protocol.reset_buffer()
            now = time_module.time()
            # Don't set _last_message_time here - leave at 0 until we receive a message
            # This allows is_healthy to distinguish "just connected" from "received messages"
            self._last_message_time = 0.0
            self._connection_start_time = now  # Track connection start for health check
            self._unhealthy_logged = False  # Reset unhealthy log flag on new connection

            # Enable TCP keepalive
            if self.proxy_config and self._writer:
                from btc_bch_proxy.proxy.keepalive import enable_tcp_keepalive
                enable_tcp_keepalive(self._writer, self.proxy_config, f"upstream:{self.name}")

            # Record connection stats
            stats = ProxyStats.get_instance()
            if self._has_connected_before:
                fire_and_forget(stats.record_upstream_reconnect(self.name))
            else:
                fire_and_forget(stats.record_upstream_connect(self.name))
                self._has_connected_before = True

            logger.info(f"Connected to upstream {self.name}")
            return True

        except asyncio.TimeoutError:
            self._retry_count += 1
            # Use INFO for first few retries to avoid log spam, WARNING for persistent issues
            if self._retry_count <= 2:
                logger.info(f"Connection to {self.name} timed out (attempt {self._retry_count})")
            else:
                logger.warning(f"Connection to {self.name} timed out (attempt {self._retry_count})")
            return False
        except OSError as e:
            # Provide more specific error messages for common connection failures
            error_msg = str(e)
            if hasattr(e, 'errno'):
                if e.errno == errno.ECONNREFUSED:
                    error_msg = "connection refused (is the pool server running?)"
                elif e.errno == errno.EHOSTUNREACH:
                    error_msg = "host unreachable (check network connectivity)"
                elif e.errno == errno.ENETUNREACH:
                    error_msg = "network unreachable (check network configuration)"
                elif e.errno in (errno.ENOENT, getattr(errno, 'EAI_NONAME', -2)):
                    error_msg = "DNS resolution failed (check hostname)"
            elif "getaddrinfo failed" in str(e).lower():
                error_msg = f"DNS resolution failed: {e}"
            self._retry_count += 1
            # Use INFO for first few retries
            if self._retry_count <= 2:
                logger.info(f"Connection to {self.name} failed: {error_msg} (attempt {self._retry_count})")
            else:
                logger.warning(f"Connection to {self.name} failed: {error_msg} (attempt {self._retry_count})")
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
                # Use timeout to prevent indefinite hang if remote doesn't close gracefully
                await asyncio.wait_for(
                    self._writer.wait_closed(),
                    timeout=UPSTREAM_DISCONNECT_TIMEOUT
                )
            except asyncio.TimeoutError:
                logger.debug(f"Timeout waiting for {self.name} socket to close")
            except Exception as e:
                logger.warning(f"Error closing connection to {self.name}: {e}")

        # Signal pending requests that connection was closed
        # Use set_exception() instead of cancel() to properly propagate error message
        # Must hold lock while copying, clearing, AND setting state flags
        # This prevents TOCTOU race where code sees _connected=False but finds stale futures
        async with self._pending_requests_lock:
            # Set state flags inside lock for atomicity
            self._connected = False
            self._subscribed = False
            self._authorized = False
            self._reader = None
            self._writer = None
            self.extranonce1 = None
            self.extranonce2_size = None
            self.subscription_id = None
            # Copy and clear pending requests
            pending_copy = dict(self._pending_requests)
            self._pending_requests.clear()

        # Set exceptions outside the lock to prevent potential deadlocks
        for req_id, future in pending_copy.items():
            if not future.done():
                try:
                    future.set_exception(
                        UpstreamConnectionError(f"Connection to {self.name} closed")
                    )
                except (asyncio.InvalidStateError, RuntimeError) as e:
                    # InvalidStateError: Future already completed
                    # RuntimeError: Event loop closed (during shutdown)
                    logger.debug(f"Could not set exception on future {req_id}: {e}")
                except Exception as e:
                    # Catch-all for any unexpected errors (defensive)
                    logger.warning(f"Unexpected error setting exception on future {req_id}: {e}")

        # Clear pending notifications - they're no longer valid after disconnect
        async with self._pending_notifications_lock:
            self._pending_notifications.clear()

        # Clear pending shares tracking (under lock)
        async with self._pending_shares_lock:
            self._pending_shares.clear()

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

        req_id = await self._next_id()
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
                    mask = result.get("version-rolling.mask", "ffffffff")
                    # Handle mask as int or string
                    if isinstance(mask, int):
                        mask_str = f"{mask:08x}"
                    else:
                        mask_str = str(mask)

                    # Validate mask is valid hex and fits in 32 bits
                    # BIP320 version-rolling uses a 32-bit mask (max 0xFFFFFFFF)
                    try:
                        # Strip 0x/0X prefix if present (some pools include it)
                        mask_hex = mask_str.lower().removeprefix("0x")
                        mask_value = int(mask_hex, 16)
                        if len(mask_hex) > 8:
                            raise ValueError(f"mask too long: {len(mask_hex)} hex chars")
                        if mask_value > 0xFFFFFFFF:
                            raise ValueError(f"mask exceeds 32-bit range: 0x{mask_value:x}")
                        self.version_rolling_supported = True
                        # Store normalized hex (without 0x prefix) for consistency
                        self.version_rolling_mask = mask_hex
                        logger.info(
                            f"Pool {self.name} supports version-rolling with mask {self.version_rolling_mask}"
                        )
                    except ValueError as e:
                        logger.warning(
                            f"Pool {self.name} returned invalid version-rolling mask "
                            f"'{mask_str}': {e}. Disabling version-rolling."
                        )
                        self.version_rolling_supported = False
                        self.version_rolling_mask = None
                else:
                    logger.info(f"Pool {self.name} does not support version-rolling")
            else:
                logger.debug(f"Pool {self.name} returned unexpected configure result: {result}")

            return True

        except asyncio.TimeoutError:
            # Timeout on configure is OK - pool might not support it
            logger.debug(f"Configure timeout for {self.name} (pool may not support it)")
            # Check if connection is still alive after timeout
            return self._connected
        except Exception as e:
            logger.debug(f"Configure error for {self.name}: {e}")
            # Don't fail the connection for configure errors, but verify it's still open
            return self._connected

    async def subscribe(self, user_agent: str = None) -> bool:
        """
        Send mining.subscribe to the pool.

        Args:
            user_agent: User agent string to send.

        Returns:
            True if subscription successful.
        """
        if not self._connected:
            return False

        if user_agent is None:
            user_agent = f"btc-bch-proxy/{__version__}"

        req_id = await self._next_id()
        params = [user_agent]

        try:
            response = await self._send_request(
                req_id, StratumMethods.MINING_SUBSCRIBE, params
            )

            if response.is_error:
                logger.error(f"Subscribe failed for {self.name}: {response.error}")
                return False

            # Parse subscription response
            # Result format: [[["mining.set_difficulty", "sub_id1"], ["mining.notify", "sub_id2"]], extranonce1, extranonce2_size]
            result = response.result
            if isinstance(result, list) and len(result) >= 3:
                subscriptions = result[0]
                self.extranonce1 = str(result[1])

                # Validate extranonce2_size type before conversion
                raw_size = result[2]
                if not isinstance(raw_size, (int, float, str)):
                    logger.error(
                        f"Invalid extranonce2_size type from {self.name}: "
                        f"{type(raw_size).__name__}"
                    )
                    return False
                try:
                    self.extranonce2_size = int(raw_size)
                except (ValueError, TypeError) as e:
                    logger.error(
                        f"Cannot convert extranonce2_size from {self.name}: "
                        f"'{raw_size}' ({e})"
                    )
                    return False

                # Validate extranonce1 is a valid non-empty hex string
                # Note: result[1] could be None/null from malformed pool responses,
                # which str() converts to "None" - check for this case too
                if not self.extranonce1 or self.extranonce1 == "None":
                    logger.error(
                        f"Invalid extranonce1 from {self.name}: "
                        f"empty or null value received"
                    )
                    return False
                try:
                    bytes.fromhex(self.extranonce1)
                except ValueError as hex_err:
                    logger.error(
                        f"Invalid extranonce1 from {self.name}: '{self.extranonce1}' "
                        f"is not valid hexadecimal ({hex_err})"
                    )
                    return False

                # Validate extranonce1 length (typically 4-8 bytes = 8-16 hex chars)
                # Very long values could indicate malicious/misconfigured pool
                if len(self.extranonce1) > MAX_EXTRANONCE1_HEX_LENGTH:
                    logger.error(
                        f"Invalid extranonce1 from {self.name}: length {len(self.extranonce1)} "
                        f"exceeds maximum {MAX_EXTRANONCE1_HEX_LENGTH} hex chars"
                    )
                    return False

                # Validate extranonce2_size is reasonable (typically 2-8 bytes)
                # Upper bound prevents memory issues with very large extranonce2 values
                if not (1 <= self.extranonce2_size <= MAX_EXTRANONCE2_SIZE):
                    logger.error(
                        f"Invalid extranonce2_size from {self.name}: {self.extranonce2_size} "
                        f"(expected 1-{MAX_EXTRANONCE2_SIZE})"
                    )
                    return False

                # Extract subscription ID from mining.notify entry if present
                self.subscription_id = None
                if isinstance(subscriptions, list):
                    for sub in subscriptions:
                        if isinstance(sub, list) and len(sub) >= 2:
                            if sub[0] == "mining.notify":
                                self.subscription_id = str(sub[1])
                                break

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

        req_id = await self._next_id()
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

    async def suggest_difficulty(self, difficulty: float) -> bool:
        """
        Send mining.suggest_difficulty to suggest a preferred difficulty to the pool.

        This is used to request the pool set a specific difficulty, which can help
        restore accurate hashrate reporting when the proxy is overriding difficulty.
        Pools may honor or ignore this suggestion.

        Args:
            difficulty: The suggested difficulty value.

        Returns:
            True if suggestion was sent (not necessarily honored by pool).
        """
        if not self._connected:
            return False

        req_id = await self._next_id()
        params = [difficulty]

        try:
            # Send the suggestion - pools may or may not respond
            # Some pools respond with true/false, others just acknowledge with set_difficulty
            response = await self._send_request(
                req_id, StratumMethods.MINING_SUGGEST_DIFFICULTY, params
            )

            if response.is_error:
                # Pool doesn't support suggest_difficulty - that's OK
                logger.debug(
                    f"Pool {self.name} rejected suggest_difficulty: {response.error}"
                )
                return False

            logger.info(
                f"Sent suggest_difficulty({difficulty}) to {self.name}, "
                f"result={response.result}"
            )
            return True

        except asyncio.TimeoutError:
            # Timeout is common - some pools don't respond to suggest_difficulty
            logger.debug(
                f"suggest_difficulty({difficulty}) timeout for {self.name} "
                f"(pool may not support it)"
            )
            return False
        except Exception as e:
            logger.debug(f"suggest_difficulty error for {self.name}: {e}")
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

        req_id = await self._next_id()
        # Use the pool's configured username, not the miner's worker name
        params = [self.config.username, job_id, extranonce2, ntime, nonce]
        # Include version_bits if version-rolling is enabled
        if version_bits is not None and self.version_rolling_supported:
            params.append(version_bits)

        async with self._pending_shares_lock:
            self._pending_shares.add(req_id)

        try:
            response = await self._send_request(
                req_id, StratumMethods.MINING_SUBMIT, params
            )

            if response.is_error:
                return False, response.error
            if response.is_rejected:
                # Pool returned result=false, possibly with a reject-reason
                if response.reject_reason:
                    return False, [20, response.reject_reason, None]
                return False, [20, "Rejected by pool", None]
            return response.result is True, None

        except asyncio.TimeoutError:
            logger.warning(f"Share submit timeout for {self.name}")
            return False, [20, "Timeout", None]
        except Exception as e:
            logger.error(f"Share submit error for {self.name}: {e}")
            return False, [20, str(e), None]
        finally:
            async with self._pending_shares_lock:
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

        # Create future for response and register it (protected by lock)
        future: asyncio.Future[StratumResponse] = asyncio.Future()
        async with self._pending_requests_lock:
            self._pending_requests[req_id] = future

        # Send request
        msg = self._protocol.build_request(req_id, method, params)
        logger.debug(f"Sending to {self.name}: {msg.decode().strip()}")
        self._writer.write(msg)
        await self._writer.drain()

        try:
            # Read from socket while waiting for the response (with lock to prevent concurrent reads)
            deadline = time_module.time() + self.config.timeout
            while not future.done():
                remaining = deadline - time_module.time()
                if remaining <= 0:
                    raise asyncio.TimeoutError()

                # Use lock to prevent concurrent socket reads
                async with self._read_lock:
                    # Check again if future is done (another coroutine may have resolved it)
                    if future.done():
                        break

                    try:
                        data = await asyncio.wait_for(
                            self._reader.read(SOCKET_READ_BUFFER_SIZE),
                            timeout=min(remaining, 1.0),
                        )
                        if not data:
                            raise UpstreamConnectionError("Connection closed by server")

                        logger.debug(f"Received from {self.name}: {data.decode().strip()}")
                        messages = self._protocol.feed_data(data)
                        if messages:
                            self._last_message_time = time_module.time()

                        # Separate responses from notifications for efficient lock usage
                        responses = []
                        notifications = []
                        for recv_msg in messages:
                            if isinstance(recv_msg, StratumResponse):
                                responses.append(recv_msg)
                            elif isinstance(recv_msg, StratumNotification):
                                notifications.append(recv_msg)

                        # Process responses under lock (minimal time in lock)
                        if responses:
                            async with self._pending_requests_lock:
                                for resp in responses:
                                    logger.debug(f"Got response id={resp.id}")
                                    pending_future = self._pending_requests.get(resp.id)
                                    if pending_future and not pending_future.done():
                                        try:
                                            pending_future.set_result(resp)
                                        except asyncio.InvalidStateError:
                                            # Future was cancelled/completed between check and set
                                            logger.debug(
                                                f"Future for request {resp.id} already completed"
                                            )

                        # Queue notifications under lock for thread safety
                        if notifications:
                            async with self._pending_notifications_lock:
                                for notif in notifications:
                                    # Limit queue size to prevent unbounded growth
                                    if len(self._pending_notifications) >= MAX_PENDING_NOTIFICATIONS:
                                        logger.warning(
                                            f"Pending notifications queue full ({MAX_PENDING_NOTIFICATIONS}), "
                                            f"dropping oldest notification"
                                        )
                                        self._pending_notifications.pop(0)
                                    self._pending_notifications.append(notif)
                                    logger.debug(f"Queued notification: {notif.method}")

                    except asyncio.TimeoutError:
                        # Just a read timeout, keep waiting if we have time left
                        continue

            # Get the result, handling cancellation and exceptions
            if future.cancelled():
                raise UpstreamConnectionError(f"Request {req_id} cancelled (connection closed)")
            try:
                return future.result()
            except asyncio.CancelledError:
                # Future was cancelled between our check and result() call
                raise UpstreamConnectionError(f"Request {req_id} cancelled (connection closed)")
            except UpstreamConnectionError:
                # Re-raise our own exceptions directly
                raise
            except Exception as e:
                # Wrap unexpected exceptions
                raise UpstreamConnectionError(f"Request {req_id} failed: {e}") from e
        finally:
            async with self._pending_requests_lock:
                self._pending_requests.pop(req_id, None)

    async def send_raw(self, data: bytes) -> None:
        """
        Send raw data to the upstream server.

        Args:
            data: Raw bytes to send.

        Raises:
            UpstreamConnectionError: If not connected or send fails.
        """
        if not self._writer:
            raise UpstreamConnectionError("Not connected")
        try:
            self._writer.write(data)
            await asyncio.wait_for(self._writer.drain(), timeout=self.config.timeout)
        except asyncio.TimeoutError:
            logger.warning(f"Send timeout to {self.name}")
            raise UpstreamConnectionError(f"Send timeout to {self.name}")
        except OSError as e:
            logger.error(f"Send error to {self.name}: {e}")
            raise UpstreamConnectionError(f"Send error: {e}") from e

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
                    self._reader.read(SOCKET_READ_BUFFER_SIZE),
                    timeout=NON_BLOCKING_READ_TIMEOUT,
                )
                if not data:
                    # Connection closed
                    logger.warning(f"Connection closed by {self.name}")
                    await self.disconnect()
                    return []

                messages = self._protocol.feed_data(data)
                if messages:
                    self._last_message_time = time_module.time()

                # Process responses to pending requests (protected by lock)
                async with self._pending_requests_lock:
                    for msg in messages:
                        if isinstance(msg, StratumResponse):
                            future = self._pending_requests.get(msg.id)
                            if future and not future.done():
                                try:
                                    future.set_result(msg)
                                except asyncio.InvalidStateError:
                                    # Future was cancelled/completed between check and set
                                    pass

                return messages

            except asyncio.TimeoutError:
                # Check connection health during timeout (log only once per unhealthy period)
                if not self.is_healthy and not self._unhealthy_logged:
                    logger.warning(
                        f"Connection to {self.name} appears unhealthy "
                        f"(no messages for {self.seconds_since_last_message:.0f}s)"
                    )
                    self._unhealthy_logged = True
                return []
            except Exception as e:
                logger.error(f"Error reading from {self.name}: {e}")
                await self.disconnect()
                return []

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
        self, server_name: str, timeout: float = DEFAULT_PENDING_SHARES_WAIT_TIMEOUT
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
            await asyncio.sleep(POLL_SLEEP_INTERVAL)

        return True

    @property
    def server_names(self) -> list[str]:
        """Get list of all server names."""
        return list(self._connections.keys())
