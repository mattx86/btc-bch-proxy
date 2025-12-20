"""Miner session handler - manages individual miner connections."""

from __future__ import annotations

import asyncio
import re
import time
import uuid
from dataclasses import dataclass
from enum import Enum, auto
from typing import TYPE_CHECKING, Any, Optional

from loguru import logger

from crypto_stratum_proxy.config.models import get_difficulty_buffer
from crypto_stratum_proxy.stratum.messages import (
    StratumMessage,
    StratumMethods,
    StratumNotification,
    StratumRequest,
    StratumResponse,
)
from crypto_stratum_proxy.stratum.protocol import StratumProtocol, StratumProtocolError
from crypto_stratum_proxy.proxy.upstream import UpstreamConnection
from crypto_stratum_proxy.proxy.stats import ProxyStats
from crypto_stratum_proxy.proxy.validation import ShareValidator
from crypto_stratum_proxy.proxy.keepalive import enable_tcp_keepalive
from crypto_stratum_proxy.proxy.utils import fire_and_forget
from crypto_stratum_proxy.proxy.circuit_breaker import CircuitBreakerManager
from crypto_stratum_proxy.proxy.constants import (
    GRACE_PERIOD_EXTENSION,
    MAX_ERROR_MESSAGE_LENGTH,
    MAX_MINER_STRING_LENGTH,
    MAX_PENDING_NOTIFICATIONS,
    MAX_WORKER_USERNAME_LENGTH,
    MINER_SEND_QUEUE_MAX_SIZE,
    MINER_WRITE_LOOP_TIMEOUT,
    POLL_SLEEP_INTERVAL,
    QUEUE_DRAIN_TIMEOUT,
    QUEUE_DRAIN_WRITE_TIMEOUT,
    SERVER_SWITCH_GRACE_PERIOD,
    SESSION_ID_LENGTH,
    SHARE_SUBMIT_INITIAL_RETRY_DELAY,
    SHARE_SUBMIT_MAX_RETRY_DELAY,
    SHARE_SUBMIT_MAX_TOTAL_TIME,
    SOCKET_READ_BUFFER_SIZE,
    UPSTREAM_HEALTH_CHECK_INTERVAL,
)


def _get_error_message(error: Any, max_length: int = MAX_ERROR_MESSAGE_LENGTH) -> str:
    """
    Extract error message from various error formats.

    Pools return errors in different formats:
    - Stratum style: [code, "message", traceback] (list)
    - JSON-RPC 2.0 style: {"code": N, "message": "..."} (dict)

    Args:
        error: Error in list or dict format.
        max_length: Maximum length for returned message (prevents log bloat).

    Returns:
        Error message string (truncated if exceeds max_length).
    """
    if error is None:
        return "unknown"

    result: str
    if isinstance(error, dict):
        # JSON-RPC 2.0 style: {"code": N, "message": "..."}
        result = str(error.get("message", error.get("code", "unknown")))
    elif isinstance(error, (list, tuple)) and len(error) >= 2:
        # Stratum style: [code, "message", traceback]
        result = str(error[1])
    else:
        result = str(error) if error else "unknown"

    # Truncate to prevent excessively long error messages in logs/stats
    if len(result) > max_length:
        # Log full message at DEBUG level for troubleshooting
        logger.debug(f"Full error message (truncated in logs): {result}")
        return result[:max_length] + "..."
    return result


def _sanitize_miner_string(value: str, max_length: int = MAX_MINER_STRING_LENGTH) -> str:
    """
    Sanitize a string provided by a miner.

    Prevents log injection and excessive memory usage.

    Args:
        value: Raw string from miner.
        max_length: Maximum allowed length.

    Returns:
        Sanitized string.
    """
    if not value:
        return ""
    # Truncate to max length
    value = str(value)[:max_length]
    # Remove control characters that could affect logging/display
    # Keep only printable ASCII and common unicode
    return "".join(c if c.isprintable() or c == " " else "?" for c in value)


def _is_valid_hex(value: str, max_length: int = 64) -> bool:
    """
    Check if a string is valid hexadecimal.

    Args:
        value: String to check.
        max_length: Maximum allowed length.

    Returns:
        True if valid hex within length limit.
    """
    if not value or not isinstance(value, str):
        return False
    if len(value) > max_length:
        return False
    try:
        int(value, 16)
        return True
    except ValueError:
        return False


if TYPE_CHECKING:
    from crypto_stratum_proxy.config.models import Config
    from crypto_stratum_proxy.proxy.router import TimeBasedRouter


class MessagePriority(Enum):
    """Priority levels for queued messages."""
    HIGH = auto()    # Share responses, critical notifications
    NORMAL = auto()  # Regular notifications (mining.notify, set_difficulty)
    LOW = auto()     # Other messages


@dataclass(order=False)
class QueuedMessage:
    """
    A message queued for sending to the miner.

    Note: order=False disables comparison operators (<, <=, >, >=) because
    this class contains bytes which has non-intuitive ordering behavior.
    The PriorityQueue uses (priority.value, sequence, message) tuples where
    comparison should never reach the message element due to unique sequences.
    """
    data: bytes
    priority: MessagePriority = MessagePriority.NORMAL


class MinerSession:
    """
    Handles a single miner's connection lifecycle.

    Manages:
    - Miner subscription and authorization
    - Message relay between miner and upstream
    - Server switching during time-based transitions
    - Graceful handling of pending shares during switches

    Lock Ordering (to prevent deadlocks):
        When acquiring multiple locks, always acquire in this order:
        1. _closing_lock - Protects session shutdown state
        2. _upstream_lock - Protects upstream connection during reconnection/submission
        3. _queue_sequence_lock - Protects send queue sequence counter
        4. _queue_drain_lock - Protects queue drain flag

        Individual locks can be acquired independently, but if multiple
        locks are needed, follow the order above. Currently no code path
        requires holding multiple locks simultaneously.

    Protocol Error Handling:
        Consecutive protocol errors are tracked to prevent DoS from malformed
        data. After MAX_CONSECUTIVE_PROTOCOL_ERRORS, the connection is closed.
    """

    # Maximum consecutive protocol errors before closing connection
    MAX_CONSECUTIVE_PROTOCOL_ERRORS = 10

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        router: TimeBasedRouter,
        config: Config,
        algorithm: str,
    ):
        """
        Initialize a miner session.

        Args:
            reader: Async stream reader for miner connection.
            writer: Async stream writer for miner connection.
            router: Time-based router for server selection.
            config: Application configuration.
            algorithm: The algorithm this session handles (sha256, randomx, zksnark).
        """
        self.reader = reader
        self.writer = writer
        self.router = router
        self.config = config
        self.algorithm = algorithm

        # Use SESSION_ID_LENGTH hex chars for session ID to reduce collision probability
        # With 48 bits (12 chars), collision probability is ~1 in 1000 at 17 million sessions
        self.session_id = uuid.uuid4().hex[:SESSION_ID_LENGTH]
        self._protocol = StratumProtocol()

        # Session state
        self._subscribed = False
        self._authorized = False
        self._running = False
        self._closing = False
        self._closing_lock = asyncio.Lock()  # Protects _closing flag

        # Miner info
        self.worker_name: Optional[str] = None
        self.user_agent: Optional[str] = None

        # Current upstream - each session gets its OWN connection
        self._current_server: Optional[str] = None
        self._upstream: Optional[UpstreamConnection] = None
        self._upstream_lock = asyncio.Lock()  # Protects _upstream during reconnection

        # Server switching state - when True, new shares are rejected
        self._switching_servers = False
        self._switch_target_server: Optional[str] = None  # Server we're trying to switch to

        # Old upstream kept alive during grace period for stale share submission
        self._old_upstream: Optional[UpstreamConnection] = None
        self._old_upstream_server_name: Optional[str] = None
        self._grace_period_end_time: float = 0.0  # When grace period expires (extended on each old share)

        # Pending requests and shares
        self._pending_shares: dict[int, asyncio.Future] = {}
        self._miner_request_id = 0

        # Send failure tracking
        # Note: Not protected by lock. The += operation is not atomic, so concurrent
        # increments could be lost. This is acceptable because the counter only affects
        # when the session closes (off by 1 failure at worst), not correctness.
        self._consecutive_send_failures = 0
        self._max_send_failures = 3  # Close session after this many consecutive failures

        # Difficulty tracking
        self._pool_difficulty: Optional[float] = None  # Difficulty set by the pool
        self._miner_difficulty: Optional[float] = None  # Difficulty sent to miner (may be overridden)
        self._max_pool_difficulty: Optional[float] = None  # Highest pool difficulty seen (for top-dynamic mode)

        # Difficulty smoothing state
        self._difficulty_config = config.proxy.difficulty  # Reference to difficulty config
        self._last_difficulty_change_time: float = 0.0  # For cooldown between changes
        self._last_decay_check_time: float = 0.0  # For time-based decay

        # Share rate monitoring state
        self._share_timestamps: list[float] = []  # For share rate calculation
        self._baseline_share_rate: Optional[float] = None  # Learned baseline rate (shares/min)
        self._last_share_rate_check: float = 0.0  # Last time we checked share rate

        # Adaptive buffer state
        self._adaptive_buffer: float = config.proxy.difficulty.buffer_start  # Current buffer
        self._last_low_diff_rejection: float = 0.0  # Time of last low-diff rejection

        # Per-server difficulty overrides (set when connecting to server)
        self._server_buffer_percent: Optional[float] = None
        self._server_min_difficulty: Optional[float] = None
        self._server_max_difficulty: Optional[float] = None

        # Queued notifications before authorization OR after pool switch (jobs and difficulty)
        # We buffer these until we know the worker's username so we can apply difficulty override,
        # OR until we receive the first difficulty from a new pool after switching
        self._pending_notifications: list[StratumNotification] = []

        # Flag set after pool switch to queue jobs until first difficulty is received
        # This ensures difficulty override is applied before miner receives any new jobs
        self._awaiting_switch_difficulty: bool = False

        # Client address (defensive check for malformed peername tuple)
        peername = writer.get_extra_info("peername")
        if peername and len(peername) >= 2:
            self.client_addr = f"{peername[0]}:{peername[1]}"
        else:
            self.client_addr = "unknown"

        # Share validator (algorithm-aware for different stratum formats)
        self._validator = ShareValidator(self.session_id, config.proxy.validation, algorithm)

        # Async queue for miner-bound messages (decouples read from write)
        # Using PriorityQueue to ensure high-priority messages (share responses) are sent first
        # Bounded queue provides backpressure to prevent memory exhaustion
        self._miner_send_queue: asyncio.PriorityQueue[tuple[int, int, QueuedMessage]] = asyncio.PriorityQueue(
            maxsize=MINER_SEND_QUEUE_MAX_SIZE
        )
        # Priority queue uses tuple comparison: (priority.value, sequence, message)
        # Python compares tuples element-by-element left-to-right, so lower priority
        # values are dequeued first, with sequence number as tiebreaker for FIFO within priority
        self._queue_sequence = 0  # For stable ordering within same priority
        # Wrap at 2^32 - at 1000 msg/sec, wraps after ~50 days continuous operation.
        # After wrap, messages with same priority may briefly reorder (acceptable trade-off).
        self._queue_sequence_modulo = 0x100000000
        self._queue_sequence_lock = asyncio.Lock()  # Lock for _queue_sequence
        self._queue_drained = False  # Flag to prevent double-draining
        self._queue_drain_lock = asyncio.Lock()  # Protects _queue_drained flag

        # Enable TCP keepalive on miner connection
        enable_tcp_keepalive(writer, config.proxy.global_, f"miner:{self.session_id}")

        # Protocol error tracking (for DoS protection)
        self._consecutive_protocol_errors = 0

        # Relay tasks (initialized here, populated in run())
        self._relay_tasks: list[asyncio.Task] = []

        logger.info(f"[{self.session_id}] New miner session from {self.client_addr}")

    @property
    def is_active(self) -> bool:
        """
        Check if the session is active (approximate).

        Note: This reads two volatile fields without synchronization.
        This is acceptable because both are booleans (atomic reads due to
        Python's GIL), and callers don't rely on strict accuracy.

        WARNING: The returned value may become stale immediately. Do not
        use this for critical control flow that requires precise state.
        For server switch coordination, additional synchronization is used.
        """
        return self._running and not self._closing

    async def _set_closing(self) -> None:
        """Thread-safe method to set closing flag."""
        async with self._closing_lock:
            self._closing = True

    @property
    def send_queue_depth(self) -> int:
        """
        Get the number of messages waiting to be sent to the miner.

        Note: This returns an approximate value as the queue may be
        modified concurrently by other tasks.
        """
        return self._miner_send_queue.qsize()

    @property
    def _log_prefix(self) -> str:
        """
        Build consistent log prefix: [session_id] [algorithm] [server] [worker].

        Components are omitted if not yet available (e.g., before authorization).
        """
        parts = [f"[{self.session_id}]"]
        if self.algorithm:
            parts.append(f"[{self.algorithm}]")
        if self._current_server:
            parts.append(f"[{self._current_server}]")
        if self.worker_name:
            parts.append(f"[{self.worker_name}]")
        return " ".join(parts)

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
            # - miner_read: reads from miner, processes messages
            # - miner_write: consumes queue and sends to miner (decoupled from reads)
            # - upstream_read: reads from pool, queues messages for miner
            miner_read_task = asyncio.create_task(self._miner_read_loop())
            miner_write_task = asyncio.create_task(self._miner_write_loop())
            upstream_read_task = asyncio.create_task(self._upstream_read_loop())

            self._relay_tasks = [miner_read_task, miner_write_task, upstream_read_task]

            # Wait for any task to complete (connection closed or error)
            done, pending = await asyncio.wait(
                self._relay_tasks,
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
            self._relay_tasks = []
            await self.close()

    async def _connect_upstream(self, server_name: str) -> bool:
        """
        Connect to an upstream server.

        Each session creates its OWN upstream connection for isolation.
        Protected by _upstream_lock to prevent concurrent access during
        connection/reconnection.

        Args:
            server_name: Name of the server to connect to.

        Returns:
            True if connected successfully.
        """
        async with self._upstream_lock:
            return await self._connect_upstream_internal(server_name)

    async def _connect_upstream_internal(self, server_name: str) -> bool:
        """
        Internal implementation of upstream connection (must hold _upstream_lock).

        Server Switch Protocol:
            1. Validate server config exists
            2. Check circuit breaker (skip if pool is in open state)
            3. Retry connecting to new server for up to 20 minutes
               (old server stays connected and handles shares during this time)
            4. Once new server is fully connected:
               - Enter switching state (new shares rejected briefly)
               - Drain pending shares on old upstream
               - Disconnect old upstream
               - Activate new upstream
               - Exit switching state
            5. If new server never connects after 20 min, keep old server

        Concurrency Note:
            While in switching state, share handlers reject new shares with "server switch
            in progress" error. This is a brief window (few seconds) while draining old shares.
        """
        # Get server config first (fail fast if invalid)
        server_config = self.router.get_server_config(server_name)
        if not server_config:
            logger.error(f"[{self.session_id}] Unknown server: {server_name}")
            return False

        # Check circuit breaker before attempting connection
        circuit_breaker = CircuitBreakerManager.get_instance(self.config.proxy.circuit_breaker)
        can_connect, block_reason = circuit_breaker.can_connect(server_name)
        if not can_connect:
            logger.info(
                f"[{self.session_id}] Skipping {server_name}: {block_reason}"
            )
            return False

        # Keep reference to old upstream
        old_upstream = self._upstream
        is_server_switch = old_upstream is not None and self._current_server != server_name

        if is_server_switch:
            logger.info(
                f"[{self.session_id}] Starting server switch: "
                f"{self._current_server} -> {server_name} "
                f"(old server stays active during connection attempts)"
            )

        # Retry connecting to new server for up to server_switch_timeout
        # Old server stays connected and handles shares during this time
        retry_interval = server_config.retry_interval
        retry_timeout = self.config.proxy.global_.server_switch_timeout
        start_time = time.time()
        new_upstream = None
        attempt = 0

        while True:
            attempt += 1

            # Check if session is closing
            if self._closing:
                logger.info(f"[{self.session_id}] Switch cancelled - session closing")
                return False

            # Create a NEW upstream connection for this attempt
            new_upstream = UpstreamConnection(server_config, self.config.proxy.global_)

            # Attempt full handshake: connect -> configure -> subscribe -> authorize
            connected = await new_upstream.connect()
            if connected:
                await new_upstream.configure()
                subscribed = await new_upstream.subscribe()
                if subscribed:
                    authorized = await new_upstream.authorize()
                    if authorized:
                        # SUCCESS - new server is fully ready!
                        break
                    else:
                        await new_upstream.disconnect()
                        new_upstream = None
                else:
                    await new_upstream.disconnect()
                    new_upstream = None
            else:
                new_upstream = None

            # Check if we've exceeded the retry timeout
            elapsed = time.time() - start_time
            if elapsed >= retry_timeout:
                logger.error(
                    f"[{self.session_id}] Failed to connect to {server_name} after "
                    f"{int(elapsed)}s ({attempt} attempts) - keeping old server"
                )
                # Record failure for circuit breaker
                circuit_breaker.record_failure(server_name)
                return False

            # Log progress every ~1 minute (12 attempts at 5s interval)
            if attempt % 12 == 0:
                minutes_elapsed = int(elapsed / 60)
                minutes_remaining = int((retry_timeout - elapsed) / 60)
                logger.warning(
                    f"[{self.session_id}] Still trying to connect to {server_name}... "
                    f"({minutes_elapsed}m elapsed, {minutes_remaining}m remaining)"
                )

            # Wait before next retry
            await asyncio.sleep(retry_interval)

        # NEW SERVER IS READY - now do the brief switchover
        # Enter switching state - new shares will be rejected during drain
        if is_server_switch:
            self._switching_servers = True
            self._switch_target_server = server_name

        try:
            # Wait for pending shares on old upstream before switching
            if old_upstream and old_upstream.has_pending_shares:
                initial_pending = old_upstream.pending_share_count
                logger.info(
                    f"[{self.session_id}] Draining {initial_pending} pending shares..."
                )
                drain_start = time.time()
                timeout = float(self.config.proxy.global_.pending_shares_timeout)
                while old_upstream.has_pending_shares:
                    if time.time() - drain_start > timeout:
                        remaining = old_upstream.pending_share_count
                        logger.warning(
                            f"[{self.session_id}] Timeout draining pending shares: "
                            f"{remaining} shares abandoned"
                        )
                        break
                    await old_upstream.read_messages()
                    await asyncio.sleep(POLL_SLEEP_INTERVAL)

            # Keep old upstream alive for grace period to accept stale shares
            # (miner may have in-flight work with old jobs after switch)
            if old_upstream and is_server_switch:
                # Clean up any previous old upstream first
                if self._old_upstream:
                    await self._old_upstream.disconnect()

                self._old_upstream = old_upstream
                self._old_upstream_server_name = self._current_server
                self._grace_period_end_time = time.time() + SERVER_SWITCH_GRACE_PERIOD
                logger.info(
                    f"[{self.session_id}] Keeping old upstream {self._current_server} alive "
                    f"for {SERVER_SWITCH_GRACE_PERIOD}s grace period"
                )
            elif old_upstream:
                # Not a server switch (reconnect), just disconnect old
                await old_upstream.disconnect()

            # Clear share cache - old jobs are no longer valid for NEW upstream
            # (but we can still submit old jobs to old upstream during grace period)
            self._validator.clear_share_cache()

            # Reset difficulty tracking for new server
            # (new pool will send mining.set_difficulty which will be logged as initial)
            self._pool_difficulty = None
            self._miner_difficulty = None
            self._max_pool_difficulty = None  # Reset highest-seen tracking for new pool

            # Set flag to queue jobs until we receive first difficulty from new pool
            # This ensures difficulty override is applied before any jobs are sent
            if is_server_switch and self.worker_name:
                self._awaiting_switch_difficulty = True
                logger.debug(
                    f"[{self.session_id}] Waiting for difficulty from new pool before sending jobs"
                )

            # Activate new upstream
            self._upstream = new_upstream
            self._current_server = server_name

            # Load per-server difficulty overrides
            self._server_buffer_percent = server_config.buffer_percent
            self._server_min_difficulty = server_config.min_difficulty
            self._server_max_difficulty = server_config.max_difficulty
            if self._server_buffer_percent is not None:
                logger.info(
                    f"[{self.session_id}] Server {server_name} uses custom buffer: "
                    f"{self._server_buffer_percent:.1%}"
                )

            elapsed = time.time() - start_time
            logger.info(f"{self._log_prefix} Connected to upstream (after {int(elapsed)}s)")

            # Record success for circuit breaker
            circuit_breaker.record_success(server_name)
            return True

        finally:
            # Always clear switching state when done
            self._switching_servers = False
            self._switch_target_server = None

    async def _reconnect_upstream(self) -> None:
        """
        Reconnect to the current upstream server.

        Protected by _upstream_lock to prevent races with handle_server_switch.
        If already connected (e.g., another caller reconnected while we waited
        for the lock), skip the reconnect.
        """
        if not self._current_server:
            return

        async with self._upstream_lock:
            # Check if already connected (another caller may have reconnected
            # while we were waiting for the lock)
            if self._upstream and self._upstream.connected and self._upstream.authorized:
                logger.debug(
                    f"[{self.session_id}] Skipping reconnect - already connected to "
                    f"{self._current_server}"
                )
                return

            logger.info(f"[{self.session_id}] Attempting to reconnect to {self._current_server}")

            # Disconnect current connection and clear reference
            if self._upstream:
                await self._upstream.disconnect()
                self._upstream = None  # Clear to avoid stale reference

            # Wait a bit before reconnecting
            await asyncio.sleep(1)

            # Reconnect (calls _connect_upstream_internal since we already hold lock)
            if await self._connect_upstream_internal(self._current_server):
                logger.info(f"{self._log_prefix} Reconnected to upstream")

                # Send set_extranonce to miner with new values
                if self._upstream and self._upstream.subscribed:
                    # Update validator with extranonce2 size (may have changed on reconnect)
                    if self._upstream.extranonce2_size is not None:
                        self._validator.set_extranonce2_size(self._upstream.extranonce2_size)

                    notification = StratumNotification(
                        method=StratumMethods.MINING_SET_EXTRANONCE,
                        params=[self._upstream.extranonce1, self._upstream.extranonce2_size],
                    )
                    await self._send_to_miner(StratumProtocol.encode_message(notification))

                    # Forward any notifications received during upstream handshake
                    # This includes job notifications that were queued during subscribe/authorize
                    pending = await self._upstream.get_pending_notifications()
                    if pending:
                        logger.info(
                            f"[{self.session_id}] Forwarding {len(pending)} notifications after reconnect"
                        )
                        for pending_notification in pending:
                            await self._handle_upstream_message(pending_notification)
            else:
                logger.error(f"[{self.session_id}] Failed to reconnect to {self._current_server}")
                # Ensure upstream is None on failure (defensive)
                self._upstream = None

    async def _miner_read_loop(self) -> None:
        """Read and process messages from the miner."""
        while self._running and not self._closing:
            try:
                # Use a long timeout - miners only send data when submitting shares
                # which can be infrequent depending on difficulty
                data = await asyncio.wait_for(
                    self.reader.read(SOCKET_READ_BUFFER_SIZE),
                    timeout=float(self.config.proxy.global_.miner_read_timeout),
                )

                if not data:
                    logger.info(f"[{self.session_id}] Miner disconnected")
                    break

                # Sanitize for logging (prevent log injection via control characters)
                log_data = data.decode(errors='replace').strip()
                log_data = "".join(c if c.isprintable() or c == " " else "?" for c in log_data[:500])
                logger.debug(f"[{self.session_id}] Received from miner: {log_data}")

                messages = self._protocol.feed_data(data)
                logger.debug(f"[{self.session_id}] Parsed {len(messages)} messages from miner")

                # Reset protocol error counter on successful parse
                if messages:
                    self._consecutive_protocol_errors = 0

                for msg in messages:
                    # Check closing flag between messages for faster shutdown
                    if self._closing:
                        break
                    await self._handle_miner_message(msg)

            except asyncio.TimeoutError:
                # Long timeout without any data from miner - likely dead connection
                timeout_mins = self.config.proxy.global_.miner_read_timeout // 60
                logger.warning(f"{self._log_prefix} Miner connection timeout ({timeout_mins} min no data)")
                break
            except asyncio.CancelledError:
                break
            except (OSError, ConnectionError) as e:
                logger.error(
                    f"[{self.session_id}] Miner connection error: {type(e).__name__}: {e}"
                )
                break
            except StratumProtocolError as e:
                # Protocol error - reset buffer and track consecutive errors
                # This handles malformed JSON or oversized messages
                self._consecutive_protocol_errors += 1
                logger.warning(
                    f"[{self.session_id}] Protocol error from miner: {e} "
                    f"({self._consecutive_protocol_errors}/{self.MAX_CONSECUTIVE_PROTOCOL_ERRORS})"
                )
                self._protocol.reset_buffer()

                # Close connection after too many consecutive errors (DoS protection)
                if self._consecutive_protocol_errors >= self.MAX_CONSECUTIVE_PROTOCOL_ERRORS:
                    logger.error(
                        f"[{self.session_id}] Too many protocol errors, closing connection"
                    )
                    break
            except Exception as e:
                logger.error(
                    f"[{self.session_id}] Error reading from miner: {type(e).__name__}: {e}",
                    exc_info=True,
                )
                break

    async def _miner_write_loop(self) -> None:
        """
        Dedicated writer task for sending messages to the miner.

        Consumes messages from the priority queue and sends them.
        This decouples reads from writes, allowing smooth handling of
        message bursts and preventing read blocking on slow writes.
        """
        while self._running and not self._closing:
            priority_tuple = None
            try:
                # Wait for a message with timeout (allows periodic state checks)
                try:
                    priority_tuple = await asyncio.wait_for(
                        self._miner_send_queue.get(),
                        timeout=MINER_WRITE_LOOP_TIMEOUT,
                    )
                except asyncio.TimeoutError:
                    # No message available, check if still running
                    continue

                _, _, queued_msg = priority_tuple
                await self._write_to_miner(queued_msg.data)

            except asyncio.CancelledError:
                # Drain remaining messages before exiting
                # task_done for current item handled in finally block
                await self._drain_miner_queue()
                break
            except Exception as e:
                logger.error(f"[{self.session_id}] Error in miner write loop: {e}")
                if self._closing:
                    break
                await asyncio.sleep(POLL_SLEEP_INTERVAL)
            finally:
                # Always mark queue item as done if we got one (required for join())
                if priority_tuple is not None:
                    self._miner_send_queue.task_done()

    async def _drain_miner_queue(self) -> None:
        """
        Drain any remaining messages in the queue before shutdown.

        Uses timeouts to prevent indefinite blocking if miner socket is broken.
        The entire drain operation is protected by lock to prevent:
        1. Concurrent drains from multiple tasks
        2. Flag being set before drain completes (would prevent retry on failure)
        """
        # Lock covers entire drain operation to ensure atomicity
        async with self._queue_drain_lock:
            if self._queue_drained:
                return

            drained = 0
            should_stop = False
            drain_start = time.time()

            try:
                while not self._miner_send_queue.empty() and not should_stop:
                    # Overall drain timeout
                    if time.time() - drain_start > QUEUE_DRAIN_TIMEOUT:
                        remaining = self._miner_send_queue.qsize()
                        logger.debug(
                            f"[{self.session_id}] Queue drain timeout, "
                            f"abandoning {remaining} remaining messages"
                        )
                        break

                    try:
                        priority_tuple = self._miner_send_queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break

                    # Ensure task_done is called even if write fails
                    try:
                        _, _, queued_msg = priority_tuple
                        # Use timeout for individual write to prevent hanging
                        await asyncio.wait_for(
                            self._write_to_miner(queued_msg.data),
                            timeout=QUEUE_DRAIN_WRITE_TIMEOUT,
                        )
                        drained += 1
                    except asyncio.TimeoutError:
                        logger.debug(f"[{self.session_id}] Write timeout during drain, stopping")
                        should_stop = True
                    except Exception as e:
                        logger.debug(f"[{self.session_id}] Error draining queue: {e}")
                        # Stop draining on write error (connection likely broken)
                        should_stop = True
                    finally:
                        self._miner_send_queue.task_done()

                if drained > 0:
                    logger.debug(f"[{self.session_id}] Drained {drained} messages from queue")
            finally:
                # Set flag AFTER drain attempt (success or failure)
                # This prevents retries, but that's correct since write failures
                # indicate broken socket where retries would also fail
                self._queue_drained = True

    async def _upstream_read_loop(self) -> None:
        """Read and forward messages from upstream to miner."""
        health_check_counter = 0
        while self._running and not self._closing:
            try:
                if not self._upstream or not self._upstream.connected:
                    # Upstream disconnected - attempt to reconnect
                    logger.info(f"[{self.session_id}] Upstream disconnected, attempting reconnect...")
                    await self._reconnect_upstream()
                    # If still not connected after reconnect attempt, wait before retrying
                    if not self._upstream or not self._upstream.connected:
                        await asyncio.sleep(1)
                    continue

                messages = await self._upstream.read_messages()
                for msg in messages:
                    await self._handle_upstream_message(msg)

                # Periodic health check (every ~10 seconds since read timeout is 0.1s)
                health_check_counter += 1
                if health_check_counter >= UPSTREAM_HEALTH_CHECK_INTERVAL:
                    health_check_counter = 0
                    if self._upstream and not self._upstream.is_healthy:
                        logger.warning(
                            f"[{self.session_id}] Upstream connection unhealthy, reconnecting..."
                        )
                        try:
                            await self._reconnect_upstream()
                        except Exception as reconnect_err:
                            # Log but don't crash - outer loop will catch and retry
                            logger.error(
                                f"[{self.session_id}] Health check reconnect failed: {reconnect_err}"
                            )

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[{self.session_id}] Error reading from upstream: {e}")
                # Try to reconnect
                await self._reconnect_upstream()
                await asyncio.sleep(1)

    async def _handle_miner_message(self, msg: StratumMessage) -> None:
        """
        Handle a message from the miner.

        Args:
            msg: Parsed stratum message.
        """
        logger.debug(f"{self._log_prefix} Handling miner message: {type(msg).__name__}")
        if isinstance(msg, StratumRequest):
            logger.info(f"{self._log_prefix} Miner request: {msg.method}")
            if msg.method == StratumMethods.MINING_CONFIGURE:
                await self._handle_configure(msg)
            elif msg.method == StratumMethods.MINING_SUBSCRIBE:
                await self._handle_subscribe(msg)
            elif msg.method == StratumMethods.MINING_AUTHORIZE:
                await self._handle_authorize(msg)
            elif msg.method == StratumMethods.MINING_SUBMIT:
                if self.algorithm == "sha256":
                    await self._handle_submit_sha256(msg)
                elif self.algorithm == "zksnark":
                    await self._handle_submit_zksnark(msg)
                elif self.algorithm == "randomx":
                    await self._handle_submit_randomx(msg)
                else:
                    # Fallback for other algorithms
                    await self._handle_submit_generic(msg)
            elif msg.method == StratumMethods.MINING_SUGGEST_DIFFICULTY:
                await self._handle_suggest_difficulty(msg)
            elif msg.method == "mining.extranonce.subscribe":
                # Acknowledge extranonce subscription - we support dynamic extranonce
                await self._send_to_miner(
                    self._protocol.build_response(msg.id, True)
                )
                logger.debug(f"{self._log_prefix} Acknowledged mining.extranonce.subscribe")
            else:
                # Forward other requests to upstream
                await self._forward_to_upstream(msg)

    async def _handle_configure(self, msg: StratumRequest) -> None:
        """Handle mining.configure from miner (stratum extension for version-rolling)."""
        logger.debug(f"[{self.session_id}] Miner configure: {msg.params}")

        # mining.configure params: [["extension1", "extension2"], {extension_params}]
        # Return the version-rolling settings we negotiated with the upstream pool
        result = {}

        if msg.params and len(msg.params) >= 1:
            extensions = msg.params[0] if isinstance(msg.params[0], list) else []
            if "version-rolling" in extensions:
                # Check if upstream supports version-rolling
                if self._upstream and self._upstream.version_rolling_supported:
                    result["version-rolling"] = True
                    result["version-rolling.mask"] = self._upstream.version_rolling_mask
                    logger.info(
                        f"[{self.session_id}] Version-rolling enabled with mask "
                        f"{self._upstream.version_rolling_mask} (from pool)"
                    )
                else:
                    # Pool doesn't support version-rolling
                    logger.info(
                        f"[{self.session_id}] Version-rolling requested but pool doesn't support it"
                    )

        await self._send_to_miner(
            self._protocol.build_response(msg.id, result)
        )

    async def _handle_subscribe(self, msg: StratumRequest) -> None:
        """Handle mining.subscribe from miner."""
        logger.debug(f"[{self.session_id}] Miner subscribe: {msg.params}")

        # Extract and sanitize user agent if provided
        if msg.params:
            self.user_agent = _sanitize_miner_string(str(msg.params[0]))

        # Use upstream's subscription data
        if self._upstream and self._upstream.subscribed:
            # Configure validator with extranonce2 size for length validation
            if self._upstream.extranonce2_size is not None:
                self._validator.set_extranonce2_size(self._upstream.extranonce2_size)

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
            logger.info(f"{self._log_prefix} Miner subscribed")

            # Forward any notifications received during upstream handshake
            # These must be sent AFTER the subscribe response
            pending = await self._upstream.get_pending_notifications()
            if pending:
                logger.info(
                    f"[{self.session_id}] Forwarding {len(pending)} queued notifications to miner"
                )
                for notification in pending:
                    await self._handle_upstream_message(notification)
        else:
            # No upstream connected yet
            error = [20, "Upstream not available", None]
            await self._send_to_miner(
                self._protocol.build_response(msg.id, None, error)
            )

    async def _handle_authorize(self, msg: StratumRequest) -> None:
        """Handle mining.authorize from miner."""
        # Accept any credentials from miner (sanitize to prevent log injection)
        # Use shorter limit for worker names than general miner strings
        if len(msg.params) >= 1:
            self.worker_name = _sanitize_miner_string(
                str(msg.params[0]), max_length=MAX_WORKER_USERNAME_LENGTH
            )

        logger.info(f"{self._log_prefix} Miner authorized")

        # Record worker for stats tracking
        if self.worker_name:
            stats = ProxyStats.get_instance()
            await stats.record_worker_authorized(self.session_id, self.worker_name)

        # Always accept authorization (we use our own credentials for upstream)
        await self._send_to_miner(
            self._protocol.build_response(msg.id, True)
        )
        self._authorized = True

        # Send queued notifications (difficulty with override, then jobs)
        await self._send_queued_notifications()

    async def _handle_suggest_difficulty(self, msg: StratumRequest) -> None:
        """
        Handle mining.suggest_difficulty from miner.

        Always forwards the miner's difficulty suggestion to the pool unchanged.

        Args:
            msg: The suggest_difficulty request from the miner.
        """
        # Extract miner's suggested difficulty for logging
        miner_suggestion = None
        if msg.params and len(msg.params) > 0:
            try:
                miner_suggestion = float(msg.params[0])
            except (ValueError, TypeError):
                logger.warning(
                    f"[{self.session_id}] Invalid suggest_difficulty value: {msg.params[0]}"
                )

        # Always forward miner's request to pool
        logger.debug(
            f"[{self.session_id}] Forwarding miner suggest_difficulty({miner_suggestion})"
        )
        await self._forward_to_upstream(msg)

    async def _send_queued_notifications(self) -> None:
        """
        Send queued notifications after authorization.

        Difficulty and job notifications are queued until the miner authorizes,
        so we can apply worker difficulty override from the start. After authorization:
        1. Send correct difficulty (with override if configured)
        2. Send queued job notifications
        """
        if not self._pending_notifications:
            return

        # First, send the correct difficulty (with override if applicable)
        if self._pool_difficulty is not None:
            await self._send_difficulty_to_miner(self._pool_difficulty)

        # Then send all queued job notifications
        jobs_sent = 0
        for notification in self._pending_notifications:
            if notification.method == StratumMethods.MINING_NOTIFY:
                data = StratumProtocol.encode_message(notification)
                await self._send_to_miner(data)
                jobs_sent += 1

        if jobs_sent > 0:
            logger.info(f"{self._log_prefix} Sent {jobs_sent} queued job(s) to miner")

        # Clear the queue
        self._pending_notifications.clear()

    async def _send_queued_jobs_after_switch(self) -> None:
        """
        Send queued jobs after receiving first difficulty from new pool.

        Called after a pool switch when the first difficulty is received.
        Sends all queued job notifications and clears the switch flag.
        """
        jobs_sent = 0
        for notification in self._pending_notifications:
            if notification.method == StratumMethods.MINING_NOTIFY:
                data = StratumProtocol.encode_message(notification)
                await self._send_to_miner(data)
                jobs_sent += 1

        if jobs_sent > 0:
            logger.info(
                f"[{self.session_id}] Sent {jobs_sent} queued job(s) after pool switch"
            )

        # Clear the queue and flag
        self._pending_notifications.clear()
        self._awaiting_switch_difficulty = False

    async def _send_difficulty_to_miner(self, pool_difficulty: float) -> None:
        """
        Send difficulty to miner with buffer, highest-seen tracking, and smoothing.

        Features:
        - Configurable buffer percentage added to pool difficulty
        - Per-server buffer overrides
        - Adaptive buffer sizing based on rejection patterns
        - Min/max difficulty constraints per server
        - Highest-seen tracking (difficulty only increases normally)
        - Progressive floor-reset when pool drops significantly
        - Time-based decay toward pool difficulty
        - Cooldown between changes to prevent oscillation

        Args:
            pool_difficulty: The difficulty set by the pool.
        """
        server_name = self._current_server or "unknown"
        now = time.time()
        cfg = self._difficulty_config

        # Get effective buffer (considers per-server override and adaptive buffer)
        effective_buffer_percent = self._get_effective_buffer_percent()

        # Get difficulty buffer using effective percentage
        diff_buffer = get_difficulty_buffer(
            pool_difficulty, effective_buffer_percent
        )

        # Log pool difficulty changes
        pool_diff_changed = self._pool_difficulty != pool_difficulty
        if pool_diff_changed and self._pool_difficulty is not None:
            logger.info(
                f"[{self.session_id}] Pool {server_name} difficulty changed: "
                f"{self._pool_difficulty} -> {pool_difficulty}"
            )
        self._pool_difficulty = pool_difficulty

        # Progressive floor-reset: If pool difficulty has dropped significantly,
        # reset our tracking to prevent excessive divergence from pool expectations
        if self._miner_difficulty is not None and self._max_pool_difficulty is not None:
            trigger_threshold = self._miner_difficulty * cfg.floor_trigger_ratio
            if pool_difficulty < trigger_threshold:
                floor_reset = self._miner_difficulty * cfg.floor_reset_ratio
                # Only apply floor-reset if it still maintains the buffer
                if floor_reset > pool_difficulty + diff_buffer:
                    logger.info(
                        f"[{self.session_id}] Floor-reset: pool dropped to {pool_difficulty} "
                        f"(< {cfg.floor_trigger_ratio:.0%} of miner {self._miner_difficulty}), "
                        f"resetting to {floor_reset:.2f}"
                    )
                    self._max_pool_difficulty = floor_reset

        # Time-based decay: If we're above pool+buffer, gradually decay toward it
        if cfg.decay_enabled and self._max_pool_difficulty is not None:
            target_difficulty = pool_difficulty + diff_buffer
            if self._max_pool_difficulty > target_difficulty:
                # Check if decay interval has passed
                if now - self._last_decay_check_time >= cfg.decay_interval:
                    self._last_decay_check_time = now
                    # Calculate decay amount
                    decay_amount = self._max_pool_difficulty * cfg.decay_percent
                    new_max = self._max_pool_difficulty - decay_amount
                    # Don't decay below target
                    if new_max > target_difficulty:
                        logger.info(
                            f"{self._log_prefix} Decay: {self._max_pool_difficulty:.2f} -> "
                            f"{new_max:.2f} (target: {target_difficulty:.2f})"
                        )
                        self._max_pool_difficulty = new_max

        # Calculate target difficulty: pool + buffer
        target_difficulty = pool_difficulty + diff_buffer

        # Track highest difficulty (only increases, never decreases)
        if self._max_pool_difficulty is None or target_difficulty > self._max_pool_difficulty:
            self._max_pool_difficulty = target_difficulty

        # Use highest seen (prevents vardiff from lowering difficulty)
        miner_difficulty = self._max_pool_difficulty

        # Apply per-server min/max difficulty constraints
        if self._server_min_difficulty is not None:
            if miner_difficulty < self._server_min_difficulty:
                logger.debug(
                    f"[{self.session_id}] Difficulty {miner_difficulty:.2f} below server min, "
                    f"using {self._server_min_difficulty:.2f}"
                )
                miner_difficulty = self._server_min_difficulty
        if self._server_max_difficulty is not None:
            if miner_difficulty > self._server_max_difficulty:
                logger.debug(
                    f"[{self.session_id}] Difficulty {miner_difficulty:.2f} above server max, "
                    f"using {self._server_max_difficulty:.2f}"
                )
                miner_difficulty = self._server_max_difficulty

        # Don't send if miner difficulty hasn't changed
        if self._miner_difficulty is not None and miner_difficulty == self._miner_difficulty:
            if pool_diff_changed:
                logger.debug(
                    f"[{self.session_id}] Miner difficulty unchanged at {miner_difficulty} "
                    f"(pool: {pool_difficulty}, highest-seen)"
                )
            return

        # Check cooldown: don't change difficulty too frequently
        if self._miner_difficulty is not None:
            time_since_last_change = now - self._last_difficulty_change_time
            if time_since_last_change < cfg.change_cooldown:
                logger.debug(
                    f"[{self.session_id}] Difficulty change cooldown: {time_since_last_change:.1f}s "
                    f"< {cfg.change_cooldown}s"
                )
                return

        # Send difficulty to miner
        diff_msg = StratumNotification(
            method=StratumMethods.MINING_SET_DIFFICULTY,
            params=[miner_difficulty]
        )
        data = StratumProtocol.encode_message(diff_msg)
        await self._send_to_miner(data)

        # Track miner's effective difficulty for validation
        self._validator.set_difficulty(miner_difficulty)

        # Update timing
        self._last_difficulty_change_time = now

        # Log difficulty
        if self._miner_difficulty is None:
            logger.info(
                f"{self._log_prefix} Difficulty: {miner_difficulty:.2f} "
                f"(pool: {pool_difficulty:.2f})"
            )
        else:
            logger.info(
                f"{self._log_prefix} Difficulty: "
                f"{self._miner_difficulty:.2f} -> {miner_difficulty:.2f} "
                f"(pool: {pool_difficulty:.2f})"
            )
        self._miner_difficulty = miner_difficulty

    async def _handle_low_difficulty_rejection(self, reason: str) -> None:
        """
        Handle a difficulty-based share rejection by raising difficulty.

        When a pool rejects a share as "low difficulty" (or similar), we set
        the difficulty to (rejection_difficulty + buffer) if that's higher than
        our current highest-seen value. The buffer is coin-type specific.

        Args:
            reason: The rejection reason string, e.g. "low difficulty share (19188.02)"
        """
        # Check for any difficulty-based rejection
        reason_lower = reason.lower()
        is_difficulty_rejection = any(phrase in reason_lower for phrase in [
            "low difficulty",
            "above target",
            "high hash",
            "high-hash",  # ALEO format (with hyphen)
            "share above target",
            "difficulty too low",
            "hash > target",
        ])

        if not is_difficulty_rejection:
            return

        # Extract difficulty from rejection message: "low difficulty share (19188.02)"
        match = re.search(r"\((\d+(?:\.\d+)?)\)", reason)
        if not match:
            # For zkSNARK/ALEO pools, difficulty is in the job target, not rejection message
            # Still update adaptive buffer to track rejection rate
            self._update_adaptive_buffer(increase=True)

            if self.algorithm == "zksnark":
                # Log the job target for diagnostics - ALEO target is in the job itself
                current_target = self._validator.get_current_zksnark_target()
                if current_target:
                    logger.debug(
                        f"{self._log_prefix} High-hash rejection (pool target: {current_target})"
                    )
                else:
                    logger.debug(
                        f"{self._log_prefix} High-hash rejection (no target available)"
                    )
            else:
                logger.debug(
                    f"{self._log_prefix} Low-diff rejection, no difficulty value in message: {reason}"
                )
            return

        try:
            rejected_diff = float(match.group(1))
        except ValueError:
            return

        # Update adaptive buffer (increase on rejection)
        self._update_adaptive_buffer(increase=True)

        # Get effective buffer (considers per-server override and adaptive buffer)
        effective_buffer_percent = self._get_effective_buffer_percent()

        # Get difficulty buffer using effective percentage
        diff_buffer = get_difficulty_buffer(
            rejected_diff, effective_buffer_percent
        )

        # Calculate new target: rejection difficulty + buffer
        new_target = rejected_diff + diff_buffer

        # Only update if this is higher than our current max
        if self._max_pool_difficulty is not None and new_target <= self._max_pool_difficulty:
            logger.info(
                f"[{self.session_id}] Low-diff rejection but new target {new_target} "
                f"<= current max {self._max_pool_difficulty}"
            )
            return

        old_max = self._max_pool_difficulty
        self._max_pool_difficulty = new_target

        old_max_str = f"{old_max:.2f}" if old_max else "None"
        logger.info(
            f"{self._log_prefix} Raising difficulty due to low-diff rejection: "
            f"{old_max_str} -> {new_target:.2f} (rejected at {rejected_diff:.2f})"
        )

        # Send new difficulty to miner
        # Note: Low-diff rejections bypass cooldown because we need to act
        # immediately to prevent continued share rejections
        diff_msg = StratumNotification(
            method=StratumMethods.MINING_SET_DIFFICULTY,
            params=[new_target]
        )
        data = StratumProtocol.encode_message(diff_msg)
        await self._send_to_miner(data)

        # Update tracking
        self._validator.set_difficulty(new_target)
        self._miner_difficulty = new_target
        self._last_difficulty_change_time = time.time()

    def _get_effective_buffer_percent(self) -> float:
        """
        Get the effective buffer percentage.

        Priority order:
        1. Per-server override (if set)
        2. Adaptive buffer (learned from rejection patterns)

        Returns:
            Buffer percentage to use.
        """
        # Per-server override takes highest priority
        if self._server_buffer_percent is not None:
            return self._server_buffer_percent

        # Use adaptive buffer
        return self._adaptive_buffer

    def _record_share_for_rate_monitoring(self) -> None:
        """Record a share submission for rate monitoring."""
        now = time.time()
        cfg = self._difficulty_config

        self._share_timestamps.append(now)

        # Clean up old timestamps outside the window
        cutoff = now - cfg.share_rate_window
        self._share_timestamps = [t for t in self._share_timestamps if t > cutoff]

    async def _check_share_rate_and_adjust(self) -> None:
        """
        Check share rate and adjust difficulty if needed.

        This learns a baseline share rate and lowers difficulty if the rate
        drops significantly below baseline (indicating difficulty is too high).
        """
        cfg = self._difficulty_config
        now = time.time()

        # Don't check too frequently (at least 60 seconds between checks)
        if now - self._last_share_rate_check < 60:
            return

        # Need minimum shares and time elapsed
        if len(self._share_timestamps) < cfg.share_rate_min_shares:
            return

        if self._miner_difficulty is None or self._pool_difficulty is None:
            return

        self._last_share_rate_check = now

        # Calculate actual share rate (shares per minute)
        window_duration = now - self._share_timestamps[0]
        if window_duration < 60:  # Need at least 60 seconds of data
            return

        actual_rate = len(self._share_timestamps) / (window_duration / 60.0)

        logger.debug(
            f"[{self.session_id}] Share rate: {actual_rate:.2f}/min "
            f"(window: {window_duration:.0f}s, shares: {len(self._share_timestamps)}, "
            f"baseline: {self._baseline_share_rate:.2f}/min)"
            if self._baseline_share_rate else
            f"[{self.session_id}] Share rate: {actual_rate:.2f}/min "
            f"(window: {window_duration:.0f}s, shares: {len(self._share_timestamps)}, "
            f"learning baseline...)"
        )

        # Learn baseline if we don't have one yet
        if self._baseline_share_rate is None:
            # Need at least 2 minutes of data to establish baseline
            if window_duration >= 120 and len(self._share_timestamps) >= cfg.share_rate_min_shares:
                self._baseline_share_rate = actual_rate
                logger.info(
                    f"[{self.session_id}] Share rate baseline established: "
                    f"{self._baseline_share_rate:.2f}/min"
                )
            return

        # Update baseline with exponential moving average (slowly adapt to changes)
        alpha = 0.1  # Weight for new observation
        self._baseline_share_rate = (
            alpha * actual_rate + (1 - alpha) * self._baseline_share_rate
        )

        # Check if rate has dropped significantly below baseline
        rate_ratio = actual_rate / self._baseline_share_rate if self._baseline_share_rate > 0 else 1.0

        if rate_ratio < cfg.share_rate_low_multiplier:
            # Share rate is too low - difficulty might be too high
            # Lower difficulty by reducing the buffer
            await self._lower_difficulty_due_to_low_rate(actual_rate, self._baseline_share_rate)

    async def _lower_difficulty_due_to_low_rate(
        self, actual_rate: float, baseline_rate: float
    ) -> None:
        """
        Lower difficulty when share rate has dropped significantly.

        Args:
            actual_rate: Current share rate (shares/min).
            baseline_rate: Baseline share rate (shares/min).
        """
        cfg = self._difficulty_config
        now = time.time()

        # Respect cooldown
        if now - self._last_difficulty_change_time < cfg.change_cooldown:
            return

        # Can't lower if we don't have current difficulty
        if self._miner_difficulty is None or self._pool_difficulty is None:
            return

        # Get effective buffer and calculate reduction
        effective_buffer = self._get_effective_buffer_percent()
        diff_buffer = get_difficulty_buffer(
            self._pool_difficulty, effective_buffer
        )

        # Calculate new target: current difficulty minus buffer
        new_target = self._miner_difficulty - diff_buffer

        # Don't go below pool difficulty
        if new_target <= self._pool_difficulty:
            logger.debug(
                f"[{self.session_id}] Can't lower difficulty further: "
                f"new_target {new_target:.2f} <= pool {self._pool_difficulty:.2f}"
            )
            return

        # Also reset the max tracking so it can drift down
        old_difficulty = self._miner_difficulty
        self._max_pool_difficulty = new_target

        logger.warning(
            f"[{self.session_id}] Lowering difficulty due to low share rate "
            f"({actual_rate:.2f}/min vs baseline {baseline_rate:.2f}/min): "
            f"{old_difficulty:.2f} -> {new_target:.2f}"
        )

        # Send new difficulty to miner
        diff_msg = StratumNotification(
            method=StratumMethods.MINING_SET_DIFFICULTY,
            params=[new_target]
        )
        data = StratumProtocol.encode_message(diff_msg)
        await self._send_to_miner(data)

        # Update tracking
        self._validator.set_difficulty(new_target)
        self._miner_difficulty = new_target
        self._last_difficulty_change_time = now

        # Reset baseline so we learn a new one at the lower difficulty
        self._baseline_share_rate = None
        self._share_timestamps.clear()

    def _update_adaptive_buffer(self, increase: bool) -> None:
        """
        Update the adaptive buffer based on rejection patterns.

        Args:
            increase: If True, increase buffer (after low-diff rejection).
                     If False, decrease buffer (after period without rejections).
        """
        cfg = self._difficulty_config
        now = time.time()

        if increase:
            # Increase buffer after low-diff rejection
            old_buffer = self._adaptive_buffer
            self._adaptive_buffer = min(
                self._adaptive_buffer + cfg.buffer_increase_step,
                cfg.buffer_max
            )
            if self._adaptive_buffer != old_buffer:
                logger.info(
                    f"{self._log_prefix} Adaptive buffer increased: "
                    f"{old_buffer:.1%} -> {self._adaptive_buffer:.1%}"
                )
            self._last_low_diff_rejection = now
        else:
            # Decrease buffer if no rejections for a while
            time_since_rejection = now - self._last_low_diff_rejection
            if time_since_rejection > cfg.buffer_decrease_interval:
                old_buffer = self._adaptive_buffer
                self._adaptive_buffer = max(
                    self._adaptive_buffer - cfg.buffer_increase_step,
                    cfg.buffer_min
                )
                if self._adaptive_buffer != old_buffer:
                    logger.info(
                        f"{self._log_prefix} Adaptive buffer decreased: "
                        f"{old_buffer:.1%} -> {self._adaptive_buffer:.1%}"
                    )
                # Reset timer so we don't keep decreasing
                self._last_low_diff_rejection = now

    # =========================================================================
    # Share submission helpers (consolidated checks used by all algorithms)
    # =========================================================================

    async def _reject_share_if_closing(self, msg_id: int) -> bool:
        """
        Reject share if session is closing.

        Returns True if share was rejected (caller should return early).
        """
        if not self._closing:
            return False

        error = [25, "Session closing", None]
        logger.debug(f"{self._log_prefix} Share rejected: session closing")
        await self._send_to_miner(
            self._protocol.build_response(msg_id, False, error),
            priority=MessagePriority.HIGH,
        )
        return True

    async def _reject_share_if_switching(self, msg_id: int) -> bool:
        """
        Reject share if server switch is in progress.

        Returns True if share was rejected (caller should return early).
        """
        if not self._switching_servers:
            return False

        error = [25, "Server switch in progress", None]
        logger.info(
            f"{self._log_prefix} Share rejected: server switch in progress "
            f"(switching to {self._switch_target_server})"
        )
        stats = ProxyStats.get_instance()
        fire_and_forget(stats.record_share_rejected(
            self._switch_target_server or self._current_server, "server switch"
        ))
        await self._send_to_miner(
            self._protocol.build_response(msg_id, False, error),
            priority=MessagePriority.HIGH,
        )
        return True

    async def _cleanup_expired_grace_period(self) -> None:
        """Clean up old upstream if grace period has expired."""
        if (
            self._old_upstream is not None
            and self._grace_period_end_time > 0
            and time.time() >= self._grace_period_end_time
        ):
            logger.info(
                f"{self._log_prefix} Grace period expired, disconnecting old upstream "
                f"({self._old_upstream_server_name})"
            )
            try:
                await self._old_upstream.disconnect()
            except Exception as e:
                logger.debug(f"{self._log_prefix} Error disconnecting old upstream: {e}")
            self._old_upstream = None
            self._old_upstream_server_name = None
            self._grace_period_end_time = 0.0

    async def _reject_share_if_not_connected(self, msg_id: int) -> bool:
        """
        Reject share if upstream is not connected/authorized.

        Returns True if share was rejected (caller should return early).
        """
        if self._upstream and self._upstream.authorized:
            return False

        error = [24, "Not authorized", None]
        logger.warning(f"{self._log_prefix} Share rejected: upstream not connected/authorized")
        stats = ProxyStats.get_instance()
        fire_and_forget(stats.record_share_rejected(self._current_server, "not connected"))
        await self._send_to_miner(
            self._protocol.build_response(msg_id, False, error),
            priority=MessagePriority.HIGH,
        )
        return True

    async def _reject_share_with_error(
        self, msg_id: int, error: list, reason: str, log_level: str = "warning"
    ) -> None:
        """
        Reject share with a given error and log it.

        Args:
            msg_id: The message ID to respond to.
            error: The error list [code, message, traceback].
            reason: The reason for stats tracking.
            log_level: Log level to use (debug, info, warning, error).
        """
        log_func = getattr(logger, log_level)
        log_func(f"{self._log_prefix} Share rejected: {error[1]}")
        stats = ProxyStats.get_instance()
        fire_and_forget(stats.record_share_rejected(self._current_server, reason))
        await self._send_to_miner(
            self._protocol.build_response(msg_id, False, error),
            priority=MessagePriority.HIGH,
        )

    async def _handle_submit_sha256(self, msg: StratumRequest) -> None:
        """Handle mining.submit (share submission) from miner for SHA-256 algorithm."""
        # Common precondition checks
        if await self._reject_share_if_closing(msg.id):
            return
        if await self._reject_share_if_switching(msg.id):
            return
        await self._cleanup_expired_grace_period()
        if await self._reject_share_if_not_connected(msg.id):
            return

        # Validate params is a list/tuple
        if not isinstance(msg.params, (list, tuple)):
            error = [20, f"Invalid params type: {type(msg.params).__name__}", None]
            logger.error(f"{self._log_prefix} {error[1]}")
            await self._send_to_miner(
                self._protocol.build_response(msg.id, False, error),
                priority=MessagePriority.HIGH,
            )
            return

        # SHA-256 submit params: [worker_name, job_id, extranonce2, ntime, nonce, version_bits?]
        # version_bits is optional and only present when version-rolling is enabled
        if len(msg.params) < 5:
            error = [20, "Invalid submit parameters", None]
            await self._send_to_miner(
                self._protocol.build_response(msg.id, False, error),
                priority=MessagePriority.HIGH,
            )
            return

        try:
            # Access params by index directly to avoid creating a slice copy
            worker_name = msg.params[0]
            job_id = msg.params[1]
            extranonce2 = msg.params[2]
            ntime = msg.params[3]
            nonce = msg.params[4]
        except (TypeError, ValueError, IndexError) as e:
            error = [20, f"Failed to parse params: {type(e).__name__}: {e}", None]
            logger.error(f"{self._log_prefix} {error[1]}")
            await self._send_to_miner(
                self._protocol.build_response(msg.id, False, error),
                priority=MessagePriority.HIGH,
            )
            return
        # Get version_bits if present (6th param for version-rolling)
        version_bits = msg.params[5] if len(msg.params) > 5 else None

        # Validate types before string conversion (prevents objects with __str__ injection)
        for field_name, field_val in [
            ("extranonce2", extranonce2),
            ("ntime", ntime),
            ("nonce", nonce),
            ("version_bits", version_bits),
        ]:
            if field_val is not None and not isinstance(field_val, (str, int)):
                error = [20, f"Invalid type for {field_name}: expected string or int", None]
                logger.warning(f"{self._log_prefix} {error[1]}")
                await self._send_to_miner(
                    self._protocol.build_response(msg.id, False, error),
                    priority=MessagePriority.HIGH,
                )
                return

        # Convert all values to strings for validation
        extranonce2 = str(extranonce2) if extranonce2 is not None else ""
        ntime = str(ntime) if ntime is not None else ""
        nonce = str(nonce) if nonce is not None else ""
        version_bits = str(version_bits) if version_bits is not None else None

        # Validate hex fields to prevent invalid data forwarding
        # Max lengths: extranonce2 (16 for 8 bytes), ntime (8), nonce (8), version_bits (8)
        hex_fields = [
            ("extranonce2", extranonce2, 16),
            ("ntime", ntime, 8),
            ("nonce", nonce, 8),
        ]
        if version_bits:
            hex_fields.append(("version_bits", version_bits, 8))

        for field_name, field_value, max_len in hex_fields:
            if not _is_valid_hex(field_value, max_len):
                error = [20, f"Invalid {field_name}: not valid hexadecimal", None]
                logger.warning(f"{self._log_prefix} {error[1]}: {field_value!r}")
                await self._send_to_miner(
                    self._protocol.build_response(msg.id, False, error),
                    priority=MessagePriority.HIGH,
                )
                return

        logger.debug(
            f"{self._log_prefix} Share submit: job={job_id}, "
            f"nonce={nonce}, version_bits={version_bits}"
        )

        # Check job source to determine which pool should receive this share
        # This is the primary routing mechanism - route to the pool that issued the job
        job_source = self._validator.get_job_source(job_id)
        in_grace_period = (
            self._old_upstream is not None
            and self._old_upstream.connected
            and self._grace_period_end_time > 0
            and time.time() < self._grace_period_end_time
        )

        # Route to old pool if job was issued by old pool and we're in grace period
        if in_grace_period and job_source == self._old_upstream_server_name:
            logger.info(
                f"{self._log_prefix} Routing share to source pool "
                f"({self._old_upstream_server_name}): job={job_id}"
            )
            accepted, error = await self._old_upstream.submit_share(
                worker_name, job_id, extranonce2, ntime, nonce, version_bits
            )
            stats = ProxyStats.get_instance()
            if accepted:
                logger.info(
                    f"{self._log_prefix} Share accepted by source pool "
                    f"({self._old_upstream_server_name}): job={job_id}"
                )
                fire_and_forget(stats.record_share_accepted(self._old_upstream_server_name))
            else:
                reason = _get_error_message(error)
                logger.info(
                    f"{self._log_prefix} Share rejected by source pool "
                    f"({self._old_upstream_server_name}): {reason}"
                )
                fire_and_forget(stats.record_share_rejected(self._old_upstream_server_name, reason))

            # Extend grace period after routing share to old pool
            # This allows more time for any remaining in-flight work to complete
            self._grace_period_end_time = time.time() + GRACE_PERIOD_EXTENSION
            logger.debug(
                f"{self._log_prefix} Extended grace period by {GRACE_PERIOD_EXTENSION}s"
            )

            await self._send_to_miner(
                self._protocol.build_response(msg.id, accepted, error),
                priority=MessagePriority.HIGH,
            )
            return

        # Route to current pool (normal case)
        # Capture upstream reference to avoid null dereference if it changes mid-validation
        upstream = self._upstream
        if not upstream or not upstream.extranonce1:
            error = [20, "Upstream not ready", None]
            await self._send_to_miner(
                self._protocol.build_response(msg.id, False, error),
                priority=MessagePriority.HIGH,
            )
            return

        # Validate share locally before submitting to current pool
        valid, reject_reason = self._validator.validate_share(
            job_id=job_id,
            extranonce2=extranonce2,
            ntime=ntime,
            nonce=nonce,
            version_bits=version_bits,
        )

        if not valid:
            # Share rejected locally - can't forward to current pool
            logger.warning(f"{self._log_prefix} Share rejected locally: {reject_reason}")
            stats = ProxyStats.get_instance()
            fire_and_forget(stats.record_share_rejected(self._current_server, reject_reason))

            error = [20, reject_reason, None]
            await self._send_to_miner(
                self._protocol.build_response(msg.id, False, error),
                priority=MessagePriority.HIGH,
            )
            return

        # Submit to upstream with retry logic for transient failures
        max_retries = self.config.proxy.global_.share_submit_retries
        retry_delay = SHARE_SUBMIT_INITIAL_RETRY_DELAY
        start_time = time.time()
        accepted = False
        error = None

        for attempt in range(max_retries):
            # Check overall timeout to prevent indefinite blocking
            elapsed = time.time() - start_time
            if elapsed > SHARE_SUBMIT_MAX_TOTAL_TIME:
                logger.warning(
                    f"{self._log_prefix} Share submit exceeded max time "
                    f"({SHARE_SUBMIT_MAX_TOTAL_TIME}s), giving up"
                )
                error = [20, "Share submit timeout", None]
                break

            # Re-capture upstream reference at start of each retry attempt
            # This ensures we use the current connection after any reconnection
            current_upstream = self._upstream

            # Check if upstream is still connected
            if not current_upstream or not current_upstream.connected:
                if attempt < max_retries - 1:
                    logger.warning(
                        f"{self._log_prefix} Upstream not connected, reconnecting..."
                    )
                    await self._reconnect_upstream()
                    current_upstream = self._upstream  # Re-capture after reconnect
                    if not current_upstream or not current_upstream.connected:
                        error = [20, "Upstream connection failed", None]
                        continue
                else:
                    error = [20, "Upstream not connected", None]
                    break

            accepted, error = await current_upstream.submit_share(
                worker_name, job_id, extranonce2, ntime, nonce, version_bits
            )

            if accepted:
                break  # Success!

            # Check if error is retryable
            # Classify by error code first (more reliable), then by message text
            retryable = False

            # Check error code if available (Stratum style: [code, message, ...])
            error_code = None
            if isinstance(error, (list, tuple)) and len(error) >= 1:
                try:
                    error_code = int(error[0])
                except (ValueError, TypeError):
                    pass
            elif isinstance(error, dict):
                error_code = error.get("code")

            # Error code 20 is "unknown error" - often used for connection issues
            # Other codes (21-25) are explicit pool rejections - don't retry
            if error_code == 20:
                # Code 20 could be connection issue - check message
                error_msg = _get_error_message(error).lower()
                retryable = any(x in error_msg for x in [
                    "timeout", "not connected", "connection", "timed out"
                ])
            elif error_code is None:
                # No code - likely our internal error (timeout, connection), retry
                error_msg = _get_error_message(error).lower()
                retryable = any(x in error_msg for x in [
                    "timeout", "not connected", "connection", "timed out"
                ])
            # Codes 21-25 are explicit rejections - don't retry

            if retryable and attempt < max_retries - 1:
                retry_reason = _get_error_message(error)
                logger.warning(
                    f"{self._log_prefix} Share submit failed ({retry_reason}), "
                    f"retrying ({attempt + 1}/{max_retries})..."
                )
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, SHARE_SUBMIT_MAX_RETRY_DELAY)  # Exponential backoff with ceiling
                continue

            # Non-retryable error (explicit pool rejection) or last attempt
            break

        # Record stats and cache share appropriately
        stats = ProxyStats.get_instance()

        # Record share for rate monitoring (both accepted and rejected count)
        self._record_share_for_rate_monitoring()
        await self._check_share_rate_and_adjust()

        if accepted:
            logger.info(
                f"{self._log_prefix} Share accepted: job={job_id}, "
                f"nonce={nonce}, version_bits={version_bits}"
            )
            fire_and_forget(stats.record_share_accepted(self._current_server))
            # Cache accepted share to prevent duplicate submissions
            self._validator.record_accepted_share(
                job_id, extranonce2, ntime, nonce, version_bits
            )
            # Check if we should decrease adaptive buffer (no rejections for a while)
            self._update_adaptive_buffer(increase=False)
        else:
            # Extract rejection reason from error (handles both list and dict formats)
            reason = _get_error_message(error)
            logger.warning(f"{self._log_prefix} Share rejected: {reason}")
            fire_and_forget(stats.record_share_rejected(self._current_server, reason))

            # Auto-adjust difficulty for "highest-seen" mode when pool rejects as low difficulty
            # This handles pools that silently increase difficulty without sending mining.set_difficulty
            await self._handle_low_difficulty_rejection(reason)

            # If pool says "duplicate", cache it locally to prevent re-submission.
            # This is CORRECT behavior because:
            # 1. "Duplicate" means the pool already has this exact share
            # 2. The share IS counted in the pool's accounting (just not credited twice)
            # 3. Re-submitting would waste bandwidth and get rejected again
            # 4. Stats correctly count this as rejected (proxy's view), even though
            #    the pool has the share (the original submission succeeded)
            #
            # Edge case: If our original submission succeeded but we didn't get the
            # response, and miner retries, pool says "duplicate". The share is NOT
            # lost - it's in the pool. Only our local stats show it as "rejected".
            if "duplicate" in reason.lower():
                self._validator.record_accepted_share(
                    job_id, extranonce2, ntime, nonce, version_bits
                )

        await self._send_to_miner(
            self._protocol.build_response(msg.id, accepted, error),
            priority=MessagePriority.HIGH,
        )

    async def _handle_submit_generic(self, msg: StratumRequest) -> None:
        """
        Handle mining.submit for non-SHA256 algorithms (zkSNARK, RandomX, etc.).

        These algorithms have different submit formats, so we forward the request
        directly to the upstream pool with minimal validation. The pool will validate
        the share format and respond appropriately.

        Examples:
        - zkSNARK/ALEO: [worker_name, job_id, nonce, commitment, solution]
        - RandomX/Monero: [worker_name, job_id, nonce, result]
        """
        # Basic validation: need at least worker_name and job_id
        if len(msg.params) < 2:
            error = [20, "Invalid submit parameters: need at least worker and job_id", None]
            logger.warning(f"{self._log_prefix} {error[1]}: {msg.params}")
            await self._send_to_miner(
                self._protocol.build_response(msg.id, False, error),
                priority=MessagePriority.HIGH,
            )
            return

        worker_name = msg.params[0]
        job_id = msg.params[1]

        logger.debug(
            f"{self._log_prefix} Non-SHA256 share submit: "
            f"job={job_id}, params_count={len(msg.params)}"
        )

        # Record share for rate monitoring
        self._record_share_for_rate_monitoring()
        await self._check_share_rate_and_adjust()

        # Forward the submit directly to upstream
        if not self._upstream or not self._upstream.connected:
            error = [20, "Upstream not connected", None]
            stats = ProxyStats.get_instance()
            fire_and_forget(stats.record_share_rejected(self._current_server, "not connected"))
            await self._send_to_miner(
                self._protocol.build_response(msg.id, False, error),
                priority=MessagePriority.HIGH,
            )
            return

        # Submit to upstream and wait for response
        # Use the upstream's submit_share_raw method for non-standard formats
        accepted, error, bundled_notifications = await self._upstream.submit_share_raw(msg.params)

        # Process any notifications bundled with the response
        if bundled_notifications:
            for notification in bundled_notifications:
                await self._handle_upstream_message(notification)

        # Record stats
        stats = ProxyStats.get_instance()
        if accepted:
            logger.info(f"{self._log_prefix} Share accepted: job={job_id}")
            fire_and_forget(stats.record_share_accepted(self._current_server))
        else:
            reason = _get_error_message(error)
            logger.warning(f"{self._log_prefix} Share rejected: {reason}")
            fire_and_forget(stats.record_share_rejected(self._current_server, reason))

        await self._send_to_miner(
            self._protocol.build_response(msg.id, accepted, error),
            priority=MessagePriority.HIGH,
        )

    async def _handle_submit_zksnark(self, msg: StratumRequest) -> None:
        """
        Handle mining.submit for zkSNARK/ALEO algorithm.

        ALEO submit format: [worker_name, job_id, nonce, commitment, solution]
        """
        # Common precondition checks
        if await self._reject_share_if_closing(msg.id):
            return
        if await self._reject_share_if_switching(msg.id):
            return
        await self._cleanup_expired_grace_period()
        if await self._reject_share_if_not_connected(msg.id):
            return

        # Validate params is a list/tuple
        if not isinstance(msg.params, (list, tuple)):
            error = [20, f"Invalid params type: {type(msg.params).__name__}", None]
            logger.error(f"{self._log_prefix} {error[1]}")
            await self._send_to_miner(
                self._protocol.build_response(msg.id, False, error),
                priority=MessagePriority.HIGH,
            )
            return

        # IceRiver ALEO miner format: [job_id, counter, proof, nonce]
        # - params[0] = job_id (the pool's job_id, NOT worker_name!)
        # - params[1] = counter/extra nonce
        # - params[2] = proof (long commitment string)
        # - params[3] = nonce (short)
        #
        # Pool expects: [worker_name, job_id, nonce, proof]
        # We need to transform the miner's format to the pool's format.
        if len(msg.params) < 4:
            error = [20, "Invalid submit parameters: need job_id, counter, proof, and nonce", None]
            logger.warning(f"{self._log_prefix} {error[1]}: {msg.params}")
            await self._send_to_miner(
                self._protocol.build_response(msg.id, False, error),
                priority=MessagePriority.HIGH,
            )
            return

        # Validate types before conversion (prevents injection via objects with __str__)
        for i, field_name in enumerate(["job_id", "counter", "proof", "nonce"]):
            if not isinstance(msg.params[i], (str, int)):
                error = [20, f"Invalid type for {field_name}: expected string or int", None]
                logger.warning(f"{self._log_prefix} {error[1]}")
                await self._send_to_miner(
                    self._protocol.build_response(msg.id, False, error),
                    priority=MessagePriority.HIGH,
                )
                return

        # Parse miner's format
        miner_job_id = str(msg.params[0])  # Pool's job_id
        miner_counter = str(msg.params[1])  # Extra nonce/counter
        miner_proof = str(msg.params[2])    # Proof/commitment (bech32-like)
        miner_nonce = str(msg.params[3])    # Nonce

        # Validate field formats
        # counter: 16 hex chars, nonce: 8 hex chars
        if not _is_valid_hex(miner_counter, 16):
            error = [20, f"Invalid counter: not valid 16-char hex", None]
            logger.warning(f"{self._log_prefix} {error[1]}: {miner_counter!r}")
            await self._send_to_miner(
                self._protocol.build_response(msg.id, False, error),
                priority=MessagePriority.HIGH,
            )
            return

        if not _is_valid_hex(miner_nonce, 8):
            error = [20, f"Invalid nonce: not valid 8-char hex", None]
            logger.warning(f"{self._log_prefix} {error[1]}: {miner_nonce!r}")
            await self._send_to_miner(
                self._protocol.build_response(msg.id, False, error),
                priority=MessagePriority.HIGH,
            )
            return

        # proof should be bech32-like (ALEO address format starting with "ab1")
        if not miner_proof.startswith("ab1") or len(miner_proof) < 10:
            error = [20, f"Invalid proof: expected ALEO address format", None]
            logger.warning(f"{self._log_prefix} {error[1]}: {miner_proof[:20]!r}...")
            await self._send_to_miner(
                self._protocol.build_response(msg.id, False, error),
                priority=MessagePriority.HIGH,
            )
            return

        # Use the authorized worker name, not from params
        worker_name = self.worker_name or "unknown"

        logger.debug(
            f"[{self.session_id}] zkSNARK share submit: pool_job={miner_job_id}, "
            f"counter={miner_counter}, nonce={miner_nonce}"
        )

        # For local validation and logging, use the miner's job_id
        job_id = miner_job_id
        nonce = miner_nonce

        # Check job source to determine which pool should receive this share
        job_source = self._validator.get_job_source(job_id)
        in_grace_period = (
            self._old_upstream is not None
            and self._old_upstream.connected
            and self._grace_period_end_time > 0
            and time.time() < self._grace_period_end_time
        )

        # Route to old pool if job was issued by old pool and we're in grace period
        if in_grace_period and job_source == self._old_upstream_server_name:
            logger.info(
                f"{self._log_prefix} Routing share to source pool "
                f"({self._old_upstream_server_name}): job={job_id}"
            )
            # ALEO stratum: forward params as-is (no worker_name prefix)
            grace_pool_params = list(msg.params)
            accepted, error, _ = await self._old_upstream.submit_share_raw(
                grace_pool_params, replace_first_param=False
            )
            # Note: We ignore notifications from old pool - they're not relevant for the new pool
            stats = ProxyStats.get_instance()
            if accepted:
                logger.info(
                    f"{self._log_prefix} Share accepted by source pool "
                    f"({self._old_upstream_server_name}): job={job_id}"
                )
                fire_and_forget(stats.record_share_accepted(self._old_upstream_server_name))
            else:
                reason = _get_error_message(error)
                logger.info(
                    f"{self._log_prefix} Share rejected by source pool "
                    f"({self._old_upstream_server_name}): {reason}"
                )
                fire_and_forget(stats.record_share_rejected(self._old_upstream_server_name, reason))

            # Extend grace period after routing share to old pool
            self._grace_period_end_time = time.time() + GRACE_PERIOD_EXTENSION
            logger.debug(
                f"{self._log_prefix} Extended grace period by {GRACE_PERIOD_EXTENSION}s"
            )

            await self._send_to_miner(
                self._protocol.build_response(msg.id, accepted, error),
                priority=MessagePriority.HIGH,
            )
            return

        # Record share for rate monitoring
        self._record_share_for_rate_monitoring()
        await self._check_share_rate_and_adjust()

        # Validate share locally (check for duplicates and stale jobs)
        valid, reject_reason = self._validator.validate_zksnark_share(job_id, nonce)
        if not valid:
            error = [20, reject_reason, None]
            logger.debug(f"{self._log_prefix} Share rejected locally: {reject_reason}")
            stats = ProxyStats.get_instance()
            fire_and_forget(stats.record_share_rejected(self._current_server, reject_reason or "validation failed"))
            await self._send_to_miner(
                self._protocol.build_response(msg.id, False, error),
                priority=MessagePriority.HIGH,
            )
            return

        # ALEO stratum submit format (confirmed via protocol capture):
        # Miner sends: [job_id, counter, proof_address, nonce]
        # Pool expects: [job_id, counter, proof_address, nonce] - SAME FORMAT, no worker name!
        # This differs from Bitcoin stratum which prepends worker_name
        pool_params = list(msg.params)

        logger.debug(
            f"{self._log_prefix} Share submit: job={miner_job_id}, "
            f"counter={miner_counter}, nonce={miner_nonce}"
        )

        # Submit to upstream with retry logic for transient failures
        max_retries = self.config.proxy.global_.share_submit_retries
        retry_delay = SHARE_SUBMIT_INITIAL_RETRY_DELAY
        start_time = time.time()
        accepted = False
        error = None

        for attempt in range(max_retries):
            # Check overall timeout
            elapsed = time.time() - start_time
            if elapsed > SHARE_SUBMIT_MAX_TOTAL_TIME:
                logger.warning(
                    f"{self._log_prefix} Share submit exceeded max time "
                    f"({SHARE_SUBMIT_MAX_TOTAL_TIME}s), giving up"
                )
                error = [20, "Share submit timeout", None]
                break

            # Re-capture upstream reference
            current_upstream = self._upstream

            # Check if upstream is still connected
            if not current_upstream or not current_upstream.connected:
                if attempt < max_retries - 1:
                    logger.warning(
                        f"{self._log_prefix} Upstream not connected, reconnecting..."
                    )
                    await self._reconnect_upstream()
                    current_upstream = self._upstream
                    if not current_upstream or not current_upstream.connected:
                        error = [20, "Upstream connection failed", None]
                        await asyncio.sleep(retry_delay)
                        retry_delay = min(retry_delay * 2, SHARE_SUBMIT_MAX_RETRY_DELAY)
                        continue
                else:
                    error = [20, "Upstream not connected", None]
                    break

            accepted, error, bundled_notifications = await current_upstream.submit_share_raw(
                pool_params, replace_first_param=False
            )

            # Process any notifications bundled with the response IMMEDIATELY
            # This is critical for ALEO pools that send new jobs with share responses
            if bundled_notifications:
                logger.info(
                    f"{self._log_prefix} Processing {len(bundled_notifications)} "
                    f"notifications bundled with share response"
                )
                for notification in bundled_notifications:
                    await self._handle_upstream_message(notification)

            if accepted:
                break  # Success!

            # Check if error is retryable (connection issues)
            error_msg = _get_error_message(error).lower()
            retryable = any(x in error_msg for x in [
                "timeout", "not connected", "connection", "timed out"
            ])

            if retryable and attempt < max_retries - 1:
                logger.warning(
                    f"{self._log_prefix} Share submit failed ({error_msg}), "
                    f"retrying ({attempt + 1}/{max_retries})..."
                )
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, SHARE_SUBMIT_MAX_RETRY_DELAY)
                continue

            # Non-retryable error
            break

        # Record stats
        stats = ProxyStats.get_instance()
        if accepted:
            logger.info(f"{self._log_prefix} Share accepted: job={job_id}, nonce={nonce}")
            fire_and_forget(stats.record_share_accepted(self._current_server))
            # Record accepted share to prevent duplicate submissions
            self._validator.record_accepted_zksnark_share(job_id, nonce)
            # Check if we should decrease adaptive buffer (no rejections for a while)
            self._update_adaptive_buffer(increase=False)
        else:
            reason = _get_error_message(error)
            logger.warning(f"{self._log_prefix} Share rejected: {reason}")
            fire_and_forget(stats.record_share_rejected(self._current_server, reason))

            # Handle low difficulty rejection (if applicable to this pool)
            await self._handle_low_difficulty_rejection(reason)

            # If pool says duplicate, record it to prevent re-submission
            if reason and "duplicate" in reason.lower():
                self._validator.record_accepted_zksnark_share(job_id, nonce)

            # On stale/unknown-work rejection, mark the job as stale
            # Notifications are already processed above from bundled_notifications
            if reason and ("unknown" in reason.lower() or "stale" in reason.lower()):
                self._validator.mark_job_stale(job_id)

        await self._send_to_miner(
            self._protocol.build_response(msg.id, accepted, error),
            priority=MessagePriority.HIGH,
        )

    async def _handle_submit_randomx(self, msg: StratumRequest) -> None:
        """
        Handle mining.submit for RandomX/Monero algorithm.

        Monero submit format: [worker_name, job_id, nonce, result]
        - worker_name: Worker identifier
        - job_id: Job from mining.notify
        - nonce: Found nonce (hex, 8 chars / 4 bytes)
        - result: PoW hash result (hex, 64 chars / 32 bytes)
        """
        # Common precondition checks
        if await self._reject_share_if_closing(msg.id):
            return
        if await self._reject_share_if_switching(msg.id):
            return
        await self._cleanup_expired_grace_period()
        if await self._reject_share_if_not_connected(msg.id):
            return

        # Validate params is a list/tuple
        if not isinstance(msg.params, (list, tuple)):
            error = [20, f"Invalid params type: {type(msg.params).__name__}", None]
            logger.error(f"{self._log_prefix} {error[1]}")
            await self._send_to_miner(
                self._protocol.build_response(msg.id, False, error),
                priority=MessagePriority.HIGH,
            )
            return

        # Monero submit params: [worker_name, job_id, nonce, result]
        # Require at least worker_name, job_id, and nonce
        if len(msg.params) < 3:
            error = [20, "Invalid submit parameters: need worker, job_id, and nonce", None]
            logger.warning(f"{self._log_prefix} {error[1]}: {msg.params}")
            await self._send_to_miner(
                self._protocol.build_response(msg.id, False, error),
                priority=MessagePriority.HIGH,
            )
            return

        worker_name = msg.params[0]
        job_id = str(msg.params[1])
        nonce = str(msg.params[2])

        logger.debug(
            f"{self._log_prefix} Share submit: job={job_id}, params_count={len(msg.params)}"
        )

        # Check job source to determine which pool should receive this share
        job_source = self._validator.get_job_source(job_id)
        in_grace_period = (
            self._old_upstream is not None
            and self._old_upstream.connected
            and self._grace_period_end_time > 0
            and time.time() < self._grace_period_end_time
        )

        # Route to old pool if job was issued by old pool and we're in grace period
        if in_grace_period and job_source == self._old_upstream_server_name:
            logger.info(
                f"{self._log_prefix} Routing share to source pool "
                f"({self._old_upstream_server_name}): job={job_id}"
            )
            accepted, error, _ = await self._old_upstream.submit_share_raw(msg.params)
            # Note: We ignore notifications from old pool - they're not relevant for the new pool
            stats = ProxyStats.get_instance()
            if accepted:
                logger.info(
                    f"{self._log_prefix} Share accepted by source pool "
                    f"({self._old_upstream_server_name}): job={job_id}"
                )
                fire_and_forget(stats.record_share_accepted(self._old_upstream_server_name))
            else:
                reason = _get_error_message(error)
                logger.info(
                    f"{self._log_prefix} Share rejected by source pool "
                    f"({self._old_upstream_server_name}): {reason}"
                )
                fire_and_forget(stats.record_share_rejected(self._old_upstream_server_name, reason))

            # Extend grace period after routing share to old pool
            self._grace_period_end_time = time.time() + GRACE_PERIOD_EXTENSION
            logger.debug(
                f"{self._log_prefix} Extended grace period by {GRACE_PERIOD_EXTENSION}s"
            )

            await self._send_to_miner(
                self._protocol.build_response(msg.id, accepted, error),
                priority=MessagePriority.HIGH,
            )
            return

        # Record share for rate monitoring
        self._record_share_for_rate_monitoring()
        await self._check_share_rate_and_adjust()

        # Validate share locally (check for duplicates and stale jobs)
        valid, reject_reason = self._validator.validate_randomx_share(job_id, nonce)
        if not valid:
            error = [20, reject_reason, None]
            logger.debug(f"{self._log_prefix} Share rejected locally: {reject_reason}")
            stats = ProxyStats.get_instance()
            fire_and_forget(stats.record_share_rejected(self._current_server, reject_reason or "validation failed"))
            await self._send_to_miner(
                self._protocol.build_response(msg.id, False, error),
                priority=MessagePriority.HIGH,
            )
            return

        # Submit to upstream with retry logic for transient failures
        max_retries = self.config.proxy.global_.share_submit_retries
        retry_delay = SHARE_SUBMIT_INITIAL_RETRY_DELAY
        start_time = time.time()
        accepted = False
        error = None

        for attempt in range(max_retries):
            # Check overall timeout to prevent indefinite blocking
            elapsed = time.time() - start_time
            if elapsed > SHARE_SUBMIT_MAX_TOTAL_TIME:
                logger.warning(
                    f"{self._log_prefix} Share submit exceeded max time "
                    f"({SHARE_SUBMIT_MAX_TOTAL_TIME}s), giving up"
                )
                error = [20, "Share submit timeout", None]
                break

            # Re-capture upstream reference at start of each retry attempt
            current_upstream = self._upstream

            # Check if upstream is still connected
            if not current_upstream or not current_upstream.connected:
                if attempt < max_retries - 1:
                    logger.warning(
                        f"{self._log_prefix} Upstream not connected, reconnecting..."
                    )
                    await self._reconnect_upstream()
                    current_upstream = self._upstream
                    if not current_upstream or not current_upstream.connected:
                        error = [20, "Upstream connection failed", None]
                        continue
                else:
                    error = [20, "Upstream not connected", None]
                    break

            accepted, error, bundled_notifications = await current_upstream.submit_share_raw(msg.params)

            # Process any notifications bundled with the response
            if bundled_notifications:
                for notification in bundled_notifications:
                    await self._handle_upstream_message(notification)

            if accepted:
                break  # Success!

            # Check if error is retryable
            retryable = False
            error_code = None
            if isinstance(error, (list, tuple)) and len(error) >= 1:
                try:
                    error_code = int(error[0])
                except (ValueError, TypeError):
                    pass
            elif isinstance(error, dict):
                error_code = error.get("code")

            # Error code 20 is "unknown error" - often used for connection issues
            if error_code == 20:
                error_msg = _get_error_message(error).lower()
                retryable = any(x in error_msg for x in [
                    "timeout", "not connected", "connection", "timed out"
                ])
            elif error_code is None:
                error_msg = _get_error_message(error).lower()
                retryable = any(x in error_msg for x in [
                    "timeout", "not connected", "connection", "timed out"
                ])

            if retryable and attempt < max_retries - 1:
                retry_reason = _get_error_message(error)
                logger.warning(
                    f"{self._log_prefix} Share submit failed ({retry_reason}), "
                    f"retrying ({attempt + 1}/{max_retries})..."
                )
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, SHARE_SUBMIT_MAX_RETRY_DELAY)
                continue

            # Non-retryable error or last attempt
            break

        # Record stats
        stats = ProxyStats.get_instance()
        if accepted:
            logger.info(f"{self._log_prefix} Share accepted: job={job_id}, nonce={nonce}")
            fire_and_forget(stats.record_share_accepted(self._current_server))
            # Record accepted share to prevent duplicate submissions
            self._validator.record_accepted_randomx_share(job_id, nonce)
            # Check if we should decrease adaptive buffer (no rejections for a while)
            self._update_adaptive_buffer(increase=False)
        else:
            reason = _get_error_message(error)
            logger.warning(f"{self._log_prefix} Share rejected: {reason}")
            fire_and_forget(stats.record_share_rejected(self._current_server, reason))

            # Handle low difficulty rejection (if applicable to this pool)
            await self._handle_low_difficulty_rejection(reason)

            # If pool says duplicate, record it to prevent re-submission
            if reason and "duplicate" in reason.lower():
                self._validator.record_accepted_randomx_share(job_id, nonce)

        await self._send_to_miner(
            self._protocol.build_response(msg.id, accepted, error),
            priority=MessagePriority.HIGH,
        )

    async def _forward_to_upstream(self, msg: StratumRequest) -> None:
        """Forward a request to the upstream server."""
        if not self._upstream or not self._upstream.connected:
            error = [20, "Upstream not connected", None]
            await self._send_to_miner(
                self._protocol.build_response(msg.id, None, error),
                priority=MessagePriority.NORMAL,
            )
            return

        data = StratumProtocol.encode_message(msg)
        await self._upstream.send_raw(data)

    async def _handle_upstream_message(self, msg: StratumMessage) -> None:
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
                if msg.method == StratumMethods.MINING_NOTIFY:
                    # Log raw params for debugging (especially for zkSNARK)
                    if self.algorithm == "zksnark":
                        logger.info(
                            f"[{self.session_id}] Raw mining.notify params ({len(msg.params)}): "
                            f"{msg.params[:6] if len(msg.params) >= 6 else msg.params}"
                        )
                    # Queue job if:
                    # 1. Not yet authorized (waiting for worker_name)
                    # 2. After pool switch, waiting for first difficulty
                    if not self.worker_name or self._awaiting_switch_difficulty:
                        # Limit queue size to prevent unbounded growth
                        if len(self._pending_notifications) >= MAX_PENDING_NOTIFICATIONS:
                            logger.warning(
                                f"[{self.session_id}] Pending notifications queue full "
                                f"({MAX_PENDING_NOTIFICATIONS}), dropping oldest"
                            )
                            self._pending_notifications.pop(0)
                        self._pending_notifications.append(msg)
                        # Still track job for validation
                        self._validator.add_job_from_notify(msg.params, self._current_server)
                        reason = "authorization" if not self.worker_name else "switch difficulty"
                        logger.debug(f"{self._log_prefix} Job queued until {reason}")
                    else:
                        data = StratumProtocol.encode_message(msg)
                        await self._send_to_miner(data)
                        self._validator.add_job_from_notify(msg.params, self._current_server)
                        logger.debug(f"{self._log_prefix} Job notification forwarded")
                elif msg.method == StratumMethods.MINING_SET_DIFFICULTY:
                    # Handle difficulty - queue until authorization so we can apply override
                    if msg.params and len(msg.params) > 0:
                        try:
                            pool_difficulty = float(msg.params[0])
                            self._pool_difficulty = pool_difficulty

                            if not self.worker_name:
                                # Queue difficulty until after authorization
                                # Limit queue size to prevent unbounded growth
                                if len(self._pending_notifications) >= MAX_PENDING_NOTIFICATIONS:
                                    logger.warning(
                                        f"[{self.session_id}] Pending notifications queue full "
                                        f"({MAX_PENDING_NOTIFICATIONS}), dropping oldest"
                                    )
                                    self._pending_notifications.pop(0)
                                self._pending_notifications.append(msg)
                                logger.debug(
                                    f"[{self.session_id}] Difficulty {pool_difficulty} queued "
                                    f"until authorization"
                                )
                            elif self._awaiting_switch_difficulty:
                                # First difficulty after pool switch - send it, then queued jobs
                                await self._send_difficulty_to_miner(pool_difficulty)
                                await self._send_queued_jobs_after_switch()
                            else:
                                # Miner is authorized - apply override if configured
                                await self._send_difficulty_to_miner(pool_difficulty)
                        except (ValueError, TypeError):
                            logger.warning(f"{self._log_prefix} Invalid difficulty value: {msg.params}")
                else:
                    # MINING_SET_EXTRANONCE - forward as-is
                    data = StratumProtocol.encode_message(msg)
                    await self._send_to_miner(data)

        elif isinstance(msg, StratumResponse):
            # Responses are handled by the upstream connection's pending request system
            pass

    async def _send_to_miner(
        self,
        data: bytes,
        priority: MessagePriority = MessagePriority.NORMAL,
    ) -> None:
        """
        Queue data for sending to the miner.

        Messages are placed in a priority queue and sent by the dedicated
        writer task. This decouples message production from socket writes.

        Queue Saturation Behavior:
            During upstream reconnection, notifications (mining.notify, set_difficulty)
            continue flowing from the new connection and queue up. If the miner socket
            is slow and the queue fills (MINER_SEND_QUEUE_MAX_SIZE messages):

            - HIGH priority messages (share responses): Queue full triggers immediate
              connection close. This prevents miner from waiting indefinitely for a
              response that will never come.

            - NORMAL/LOW priority messages (notifications): Dropped silently. The
              miner will miss these updates but continue functioning. The next
              notification typically supersedes the dropped one anyway.

            This is acceptable because:
            1. Reconnection is typically fast (1-2 seconds)
            2. Mining notifications arrive every ~30 seconds during normal operation
            3. A slow miner that can't keep up should be disconnected anyway
            4. The queue size (1000) provides substantial buffer capacity

        Args:
            data: Raw bytes to send.
            priority: Message priority (HIGH for share responses, NORMAL for notifications).
        """
        if self._closing:
            logger.debug(f"[{self.session_id}] Dropping message, session closing")
            return
        # Capture writer reference to avoid TOCTOU race condition
        writer = self.writer
        if not writer:
            return

        # Create queued message with priority
        queued_msg = QueuedMessage(data=data, priority=priority)

        # Use sequence number for stable ordering within same priority (thread-safe).
        # Note: The lock only protects sequence assignment, not queue insertion.
        # This is correct because PriorityQueue sorts by (priority, seq, msg),
        # so even if insertions complete out of order, dequeue order is correct.
        async with self._queue_sequence_lock:
            self._queue_sequence = (self._queue_sequence + 1) % self._queue_sequence_modulo
            seq = self._queue_sequence
        priority_tuple = (priority.value, seq, queued_msg)

        try:
            self._miner_send_queue.put_nowait(priority_tuple)
        except asyncio.QueueFull:
            # Queue is full - miner is likely slow or unresponsive
            if priority == MessagePriority.HIGH:
                # For high-priority messages (share responses), this is critical
                # Miner will hang waiting for response - close connection immediately
                # to signal error rather than leaving miner waiting indefinitely
                logger.error(
                    f"[{self.session_id}] Send queue full ({MINER_SEND_QUEUE_MAX_SIZE}), "
                    f"dropping HIGH priority message - closing connection immediately"
                )
                # Set closing flag FIRST so other tasks see it immediately
                # This ensures visibility before we close the socket
                await self._set_closing()
                # Close socket to signal error to miner
                if self.writer and not self.writer.is_closing():
                    try:
                        self.writer.close()
                    except Exception:
                        pass  # Socket may already be broken
            else:
                logger.warning(
                    f"[{self.session_id}] Send queue full ({MINER_SEND_QUEUE_MAX_SIZE}), "
                    f"dropping message"
                )

    async def _write_to_miner(self, data: bytes) -> None:
        """
        Actually write data to the miner socket.

        Called by the writer task. Handles errors and failure tracking.

        Args:
            data: Raw bytes to send.
        """
        if self._closing or not self.writer:
            return

        try:
            self.writer.write(data)
            await asyncio.wait_for(
                self.writer.drain(),
                timeout=float(self.config.proxy.global_.miner_write_timeout)
            )
            # Reset failure counter on successful send
            self._consecutive_send_failures = 0
        except asyncio.TimeoutError:
            self._consecutive_send_failures += 1
            logger.warning(
                f"[{self.session_id}] Timeout sending to miner "
                f"({self._consecutive_send_failures}/{self._max_send_failures})"
            )
            if self._consecutive_send_failures >= self._max_send_failures:
                logger.error(f"[{self.session_id}] Too many send failures, closing session")
                fire_and_forget(self._set_closing())
        except (OSError, ConnectionError) as e:
            # Connection actually broken - close immediately
            logger.error(f"[{self.session_id}] Connection error sending to miner: {e}")
            fire_and_forget(self._set_closing())
        except Exception as e:
            self._consecutive_send_failures += 1
            logger.error(
                f"[{self.session_id}] Error sending to miner: {e} "
                f"({self._consecutive_send_failures}/{self._max_send_failures})"
            )
            if self._consecutive_send_failures >= self._max_send_failures:
                fire_and_forget(self._set_closing())

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
            # Update validator with new pool's extranonce2 size (may differ between pools)
            if self._upstream.extranonce2_size is not None:
                self._validator.set_extranonce2_size(self._upstream.extranonce2_size)

            # Send set_extranonce notification to miner if supported
            notification = StratumNotification(
                method=StratumMethods.MINING_SET_EXTRANONCE,
                params=[self._upstream.extranonce1, self._upstream.extranonce2_size],
            )
            await self._send_to_miner(StratumProtocol.encode_message(notification))

            # Forward any notifications received during upstream handshake
            # This includes difficulty and job notifications that were queued
            # These will be processed through _handle_upstream_message which:
            # 1. Sends difficulty (with override) when received
            # 2. Sends queued jobs after difficulty is received
            pending = await self._upstream.get_pending_notifications()
            if pending:
                logger.info(
                    f"[{self.session_id}] Processing {len(pending)} notifications from new pool"
                )
                for pending_notification in pending:
                    await self._handle_upstream_message(pending_notification)

            logger.info(
                f"[{self.session_id}] Server switch complete, "
                f"new extranonce1={self._upstream.extranonce1}, "
                f"extranonce2_size={self._upstream.extranonce2_size}"
            )

    async def close(self) -> None:
        """Close the session and cleanup resources."""
        async with self._closing_lock:
            if self._closing:
                return
            self._closing = True
            self._running = False  # Must be inside lock to prevent race condition

        logger.info(f"{self._log_prefix} Closing session")

        # Capture references to avoid TOCTOU issues
        writer = self.writer
        upstream = self._upstream
        old_upstream = self._old_upstream

        # Close the miner socket FIRST to break any blocking reads
        # This causes read() to return empty bytes, exiting the read loop
        if writer and not writer.is_closing():
            try:
                writer.close()
            except Exception:
                pass  # Socket may already be broken

        # Cancel relay tasks immediately to break out of any blocking operations
        relay_tasks = self._relay_tasks
        for task in relay_tasks:
            if not task.done():
                task.cancel()
        # Wait briefly for tasks to cancel (don't block forever)
        if relay_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*relay_tasks, return_exceptions=True),
                    timeout=2.0
                )
            except asyncio.TimeoutError:
                logger.debug(f"[{self.session_id}] Timeout waiting for tasks to cancel")

        # Close upstream connections (both current and old if still active)
        if upstream:
            try:
                await upstream.disconnect()
            except Exception as e:
                logger.debug(f"[{self.session_id}] Error closing upstream: {e}")
            self._upstream = None

        if old_upstream:
            try:
                await old_upstream.disconnect()
            except Exception as e:
                logger.debug(f"[{self.session_id}] Error closing old upstream: {e}")
            self._old_upstream = None

        # Wait for miner connection to finish closing (we called close() earlier)
        if writer:
            try:
                await writer.wait_closed()
            except Exception as e:
                # Expected during unclean disconnects, log at debug level
                logger.debug(f"[{self.session_id}] Error waiting for miner connection close: {e}")

        self.reader = None
        self.writer = None
