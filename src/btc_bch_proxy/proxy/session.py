"""Miner session handler - manages individual miner connections."""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass
from enum import Enum, auto
from typing import TYPE_CHECKING, Any, Optional

from loguru import logger

from btc_bch_proxy.stratum.messages import (
    StratumMessage,
    StratumMethods,
    StratumNotification,
    StratumRequest,
    StratumResponse,
)
from btc_bch_proxy.stratum.protocol import StratumProtocol, StratumProtocolError
from btc_bch_proxy.proxy.upstream import UpstreamConnection
from btc_bch_proxy.proxy.stats import ProxyStats
from btc_bch_proxy.proxy.validation import ShareValidator
from btc_bch_proxy.proxy.keepalive import enable_tcp_keepalive
from btc_bch_proxy.proxy.utils import fire_and_forget
from btc_bch_proxy.proxy.constants import (
    GRACE_PERIOD_EXTENSION,
    MAX_ERROR_MESSAGE_LENGTH,
    MAX_MINER_STRING_LENGTH,
    MINER_SEND_QUEUE_MAX_SIZE,
    MINER_WRITE_LOOP_TIMEOUT,
    POLL_SLEEP_INTERVAL,
    QUEUE_DRAIN_TIMEOUT,
    QUEUE_DRAIN_WRITE_TIMEOUT,
    SERVER_SWITCH_GRACE_PERIOD,
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
    from btc_bch_proxy.config.models import Config
    from btc_bch_proxy.proxy.router import TimeBasedRouter


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

        # Use 12 hex chars (48 bits) for session ID to reduce collision probability
        # With 48 bits, collision probability is ~1 in 1000 at 17 million sessions
        self.session_id = uuid.uuid4().hex[:12]
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

        # Client address (defensive check for malformed peername tuple)
        peername = writer.get_extra_info("peername")
        if peername and len(peername) >= 2:
            self.client_addr = f"{peername[0]}:{peername[1]}"
        else:
            self.client_addr = "unknown"

        # Share validator
        self._validator = ShareValidator(self.session_id, config.validation)

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
        enable_tcp_keepalive(writer, config.proxy, f"miner:{self.session_id}")

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
            2. Retry connecting to new server for up to 20 minutes
               (old server stays connected and handles shares during this time)
            3. Once new server is fully connected:
               - Enter switching state (new shares rejected briefly)
               - Drain pending shares on old upstream
               - Disconnect old upstream
               - Activate new upstream
               - Exit switching state
            4. If new server never connects after 20 min, keep old server

        Concurrency Note:
            While in switching state, _handle_submit() rejects new shares with
            "server switch in progress" error. This is a brief window (few seconds)
            while draining old shares.
        """
        # Get server config first (fail fast if invalid)
        server_config = self.router.get_server_config(server_name)
        if not server_config:
            logger.error(f"[{self.session_id}] Unknown server: {server_name}")
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

        # Retry connecting to new server for up to retry_timeout_minutes
        # Old server stays connected and handles shares during this time
        retry_interval = server_config.retry_interval
        retry_timeout = self.config.failover.retry_timeout_minutes * 60
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
            new_upstream = UpstreamConnection(server_config, self.config.proxy)

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
                timeout = float(self.config.proxy.pending_shares_timeout)
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

            # Activate new upstream
            self._upstream = new_upstream
            self._current_server = server_name

            elapsed = time.time() - start_time
            logger.info(
                f"[{self.session_id}] Connected to upstream {server_name} "
                f"(after {int(elapsed)}s)"
            )
            return True

        finally:
            # Always clear switching state when done
            self._switching_servers = False
            self._switch_target_server = None

    async def _reconnect_upstream(self) -> None:
        """
        Reconnect to the current upstream server.

        Protected by _upstream_lock to prevent races with handle_server_switch.
        """
        if not self._current_server:
            return

        async with self._upstream_lock:
            logger.info(f"[{self.session_id}] Attempting to reconnect to {self._current_server}")

            # Disconnect current connection and clear reference
            if self._upstream:
                await self._upstream.disconnect()
                self._upstream = None  # Clear to avoid stale reference

            # Wait a bit before reconnecting
            await asyncio.sleep(1)

            # Reconnect (calls _connect_upstream_internal since we already hold lock)
            if await self._connect_upstream_internal(self._current_server):
                logger.info(f"[{self.session_id}] Reconnected to {self._current_server}")

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
                    timeout=float(self.config.proxy.miner_read_timeout),
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
                timeout_mins = self.config.proxy.miner_read_timeout // 60
                logger.warning(f"[{self.session_id}] Miner connection timeout ({timeout_mins} min no data)")
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
        logger.debug(f"[{self.session_id}] Handling miner message: {type(msg).__name__}")
        if isinstance(msg, StratumRequest):
            logger.info(f"[{self.session_id}] Miner request: {msg.method}")
            if msg.method == StratumMethods.MINING_CONFIGURE:
                await self._handle_configure(msg)
            elif msg.method == StratumMethods.MINING_SUBSCRIBE:
                await self._handle_subscribe(msg)
            elif msg.method == StratumMethods.MINING_AUTHORIZE:
                await self._handle_authorize(msg)
            elif msg.method == StratumMethods.MINING_SUBMIT:
                await self._handle_submit(msg)
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
            logger.info(f"[{self.session_id}] Miner subscribed")

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
        if len(msg.params) >= 1:
            self.worker_name = _sanitize_miner_string(str(msg.params[0]))

        logger.info(f"[{self.session_id}] Miner authorized as {self.worker_name}")

        # Always accept authorization (we use our own credentials for upstream)
        await self._send_to_miner(
            self._protocol.build_response(msg.id, True)
        )
        self._authorized = True

    async def _handle_submit(self, msg: StratumRequest) -> None:
        """Handle mining.submit (share submission) from miner."""
        # Reject shares if session is closing
        if self._closing:
            error = [25, "Session closing", None]
            logger.debug(f"[{self.session_id}] Share rejected: session closing")
            await self._send_to_miner(
                self._protocol.build_response(msg.id, False, error),
                priority=MessagePriority.HIGH,
            )
            return

        # Reject shares during brief server switchover (while draining old shares)
        if self._switching_servers:
            error = [25, "Server switch in progress", None]
            logger.info(
                f"[{self.session_id}] Share rejected: server switch in progress "
                f"(switching to {self._switch_target_server})"
            )
            stats = ProxyStats.get_instance()
            fire_and_forget(stats.record_share_rejected(
                self._switch_target_server or self._current_server, "server switch"
            ))
            await self._send_to_miner(
                self._protocol.build_response(msg.id, False, error),
                priority=MessagePriority.HIGH,
            )
            return

        # Clean up old upstream if grace period has expired
        if (
            self._old_upstream is not None
            and self._grace_period_end_time > 0
            and time.time() >= self._grace_period_end_time
        ):
            logger.info(
                f"[{self.session_id}] Grace period expired, disconnecting old upstream "
                f"({self._old_upstream_server_name})"
            )
            try:
                await self._old_upstream.disconnect()
            except Exception as e:
                logger.debug(f"[{self.session_id}] Error disconnecting old upstream: {e}")
            self._old_upstream = None
            self._old_upstream_server_name = None
            self._grace_period_end_time = 0.0

        if not self._upstream or not self._upstream.authorized:
            error = [24, "Not authorized", None]
            logger.warning(
                f"[{self.session_id}] Share rejected: upstream not connected/authorized "
                f"(current_server={self._current_server})"
            )
            # Record the rejection in stats
            stats = ProxyStats.get_instance()
            fire_and_forget(stats.record_share_rejected(self._current_server, "not connected"))
            await self._send_to_miner(
                self._protocol.build_response(msg.id, False, error),
                priority=MessagePriority.HIGH,
            )
            return

        # Parse submit params: [worker_name, job_id, extranonce2, ntime, nonce, version_bits?]
        # version_bits is optional and only present when version-rolling is enabled
        if not isinstance(msg.params, (list, tuple)):
            error = [20, f"Invalid params type: {type(msg.params).__name__}", None]
            logger.error(f"[{self.session_id}] {error[1]}")
            await self._send_to_miner(
                self._protocol.build_response(msg.id, False, error),
                priority=MessagePriority.HIGH,
            )
            return

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
            logger.error(f"[{self.session_id}] {error[1]}")
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
                logger.warning(f"[{self.session_id}] {error[1]}")
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
                logger.warning(f"[{self.session_id}] {error[1]}: {field_value!r}")
                await self._send_to_miner(
                    self._protocol.build_response(msg.id, False, error),
                    priority=MessagePriority.HIGH,
                )
                return

        logger.debug(
            f"[{self.session_id}] Share submit: job={job_id}, "
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
                f"[{self.session_id}] Routing share to source pool "
                f"({self._old_upstream_server_name}): job={job_id}"
            )
            accepted, error = await self._old_upstream.submit_share(
                worker_name, job_id, extranonce2, ntime, nonce, version_bits
            )
            stats = ProxyStats.get_instance()
            if accepted:
                logger.info(
                    f"[{self.session_id}] Share accepted by source pool "
                    f"({self._old_upstream_server_name}): job={job_id}"
                )
                fire_and_forget(stats.record_share_accepted(self._old_upstream_server_name))
            else:
                reason = _get_error_message(error)
                logger.info(
                    f"[{self.session_id}] Share rejected by source pool "
                    f"({self._old_upstream_server_name}): {reason}"
                )
                fire_and_forget(stats.record_share_rejected(self._old_upstream_server_name, reason))

            # Extend grace period after routing share to old pool
            # This allows more time for any remaining in-flight work to complete
            self._grace_period_end_time = time.time() + GRACE_PERIOD_EXTENSION
            logger.debug(
                f"[{self.session_id}] Extended grace period by {GRACE_PERIOD_EXTENSION}s"
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
            extranonce1=upstream.extranonce1,
            version_bits=version_bits,
        )

        if not valid:
            # Share rejected locally - can't forward to current pool
            logger.warning(f"[{self.session_id}] Share rejected locally: {reject_reason}")
            stats = ProxyStats.get_instance()
            fire_and_forget(stats.record_share_rejected(self._current_server, reject_reason))

            error = [20, reject_reason, None]
            await self._send_to_miner(
                self._protocol.build_response(msg.id, False, error),
                priority=MessagePriority.HIGH,
            )
            return

        # Submit to upstream with retry logic for transient failures
        max_retries = self.config.proxy.share_submit_retries
        retry_delay = SHARE_SUBMIT_INITIAL_RETRY_DELAY
        start_time = time.time()
        accepted = False
        error = None

        for attempt in range(max_retries):
            # Check overall timeout to prevent indefinite blocking
            elapsed = time.time() - start_time
            if elapsed > SHARE_SUBMIT_MAX_TOTAL_TIME:
                logger.warning(
                    f"[{self.session_id}] Share submit exceeded max time "
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
                        f"[{self.session_id}] Upstream not connected, reconnecting before retry..."
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
                    f"[{self.session_id}] Share submit failed ({retry_reason}), "
                    f"retrying ({attempt + 1}/{max_retries})..."
                )
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, SHARE_SUBMIT_MAX_RETRY_DELAY)  # Exponential backoff with ceiling
                continue

            # Non-retryable error (explicit pool rejection) or last attempt
            break

        # Record stats and cache share appropriately
        stats = ProxyStats.get_instance()
        if accepted:
            logger.info(
                f"[{self.session_id}] Share accepted: job={job_id}, "
                f"nonce={nonce}, version_bits={version_bits}"
            )
            fire_and_forget(stats.record_share_accepted(self._current_server))
            # Cache accepted share to prevent duplicate submissions
            self._validator.record_accepted_share(
                job_id, extranonce2, ntime, nonce, version_bits
            )
        else:
            # Extract rejection reason from error (handles both list and dict formats)
            reason = _get_error_message(error)
            logger.warning(f"[{self.session_id}] Share rejected: {reason}")
            fire_and_forget(stats.record_share_rejected(self._current_server, reason))

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
                    data = StratumProtocol.encode_message(msg)
                    await self._send_to_miner(data)
                    # Track job for validation (include source server for grace period routing)
                    self._validator.add_job_from_notify(msg.params, self._current_server)
                    logger.debug(f"[{self.session_id}] Job notification forwarded")
                elif msg.method == StratumMethods.MINING_SET_DIFFICULTY:
                    # Handle difficulty with potential worker override
                    if msg.params and len(msg.params) > 0:
                        try:
                            pool_difficulty = float(msg.params[0])
                            self._pool_difficulty = pool_difficulty

                            # Check for worker difficulty override
                            miner_difficulty = pool_difficulty
                            if self.worker_name:
                                worker_diff = self.config.get_worker_difficulty(self.worker_name)
                                if worker_diff is not None and worker_diff > pool_difficulty:
                                    miner_difficulty = float(worker_diff)

                            # Send difficulty to miner (possibly overridden)
                            if miner_difficulty != pool_difficulty:
                                # Create modified message with override difficulty
                                modified_msg = StratumNotification(
                                    method=msg.method,
                                    params=[miner_difficulty]
                                )
                                data = StratumProtocol.encode_message(modified_msg)
                            else:
                                data = StratumProtocol.encode_message(msg)
                            await self._send_to_miner(data)

                            # Track miner's effective difficulty for validation
                            self._validator.set_difficulty(miner_difficulty)

                            # Log difficulty changes (include server name for clarity)
                            server_name = self._current_server or "unknown"
                            if self._miner_difficulty is None:
                                if miner_difficulty != pool_difficulty:
                                    logger.info(
                                        f"[{self.session_id}] Difficulty from {server_name}: {miner_difficulty} "
                                        f"(pool: {pool_difficulty}, worker override applied)"
                                    )
                                else:
                                    logger.info(
                                        f"[{self.session_id}] Difficulty from {server_name}: {miner_difficulty}"
                                    )
                            elif miner_difficulty != self._miner_difficulty:
                                if miner_difficulty != pool_difficulty:
                                    logger.info(
                                        f"[{self.session_id}] Difficulty from {server_name}: "
                                        f"{self._miner_difficulty} -> {miner_difficulty} "
                                        f"(pool: {pool_difficulty}, worker override applied)"
                                    )
                                else:
                                    logger.info(
                                        f"[{self.session_id}] Difficulty from {server_name}: "
                                        f"{self._miner_difficulty} -> {miner_difficulty}"
                                    )
                            self._miner_difficulty = miner_difficulty
                        except (ValueError, TypeError):
                            logger.warning(f"[{self.session_id}] Invalid difficulty value: {msg.params}")
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
                timeout=float(self.config.proxy.send_timeout)
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

        logger.info(f"[{self.session_id}] Closing session")

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
