"""TCP socket optimization utilities for stratum connections."""

from __future__ import annotations

import socket
import sys
from typing import TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    import asyncio
    from crypto_stratum_proxy.config.models import GlobalProxyConfig

# IP ToS value for low-delay traffic (DSCP EF / Expedited Forwarding)
IPTOS_LOWDELAY = 0x10


def enable_tcp_keepalive(
    writer: asyncio.StreamWriter,
    config: GlobalProxyConfig,
    connection_name: str = "connection",
) -> bool:
    """
    Enable TCP keepalive and optimize socket for stratum connections.

    Sets:
    - TCP keepalive: Periodic probes to detect dead connections at the OS level
    - TCP_NODELAY: Disable Nagle's algorithm for lower latency on small packets
    - IP_TOS: Mark packets as low-delay for QoS prioritization

    Args:
        writer: The asyncio StreamWriter (contains the socket).
        config: Global proxy configuration with keepalive settings.
        connection_name: Name for logging purposes.

    Returns:
        True if optimizations were applied successfully.
    """
    try:
        sock = writer.get_extra_info("socket")
        if sock is None:
            logger.debug(f"[{connection_name}] Cannot get socket for optimization")
            return False

        # TCP_NODELAY - disable Nagle's algorithm for lower latency
        # Critical for stratum: small JSON-RPC messages need immediate transmission
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except (OSError, AttributeError) as e:
            logger.debug(f"[{connection_name}] TCP_NODELAY not available: {e}")

        # IP_TOS - mark packets as low-delay for QoS prioritization
        # Routers/switches may prioritize these packets
        try:
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_TOS, IPTOS_LOWDELAY)
        except (OSError, AttributeError) as e:
            logger.debug(f"[{connection_name}] IP_TOS not available: {e}")

        # TCP keepalive (if enabled in config)
        if not config.tcp_keepalive:
            logger.debug(f"[{connection_name}] Socket optimized (keepalive disabled)")
            return True

        # Enable keepalive
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

        # Set platform-specific keepalive parameters
        if sys.platform == "linux":
            # Linux: TCP_KEEPIDLE, TCP_KEEPINTVL, TCP_KEEPCNT
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, config.keepalive_idle)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, config.keepalive_interval)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, config.keepalive_count)
            logger.debug(
                f"[{connection_name}] TCP keepalive enabled: "
                f"idle={config.keepalive_idle}s, interval={config.keepalive_interval}s, "
                f"count={config.keepalive_count}"
            )
        elif sys.platform == "darwin":
            # macOS: TCP_KEEPALIVE (idle time only)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPALIVE, config.keepalive_idle)
            logger.debug(f"[{connection_name}] TCP keepalive enabled: idle={config.keepalive_idle}s")
        elif sys.platform == "win32":
            # Windows: Use SIO_KEEPALIVE_VALS via ioctl
            try:
                # Enable keepalive with (onoff, keepalivetime_ms, keepaliveinterval_ms)
                sock.ioctl(
                    socket.SIO_KEEPALIVE_VALS,
                    (1, config.keepalive_idle * 1000, config.keepalive_interval * 1000),
                )
                logger.debug(
                    f"[{connection_name}] TCP keepalive enabled: "
                    f"idle={config.keepalive_idle}s, interval={config.keepalive_interval}s"
                )
            except (AttributeError, OSError) as e:
                # SIO_KEEPALIVE_VALS might not be available on all Windows versions
                logger.debug(f"[{connection_name}] Windows keepalive ioctl failed: {e}")
                # Fall back to just enabling keepalive
                logger.debug(f"[{connection_name}] TCP keepalive enabled (default settings)")
        else:
            logger.debug(f"[{connection_name}] TCP keepalive enabled (platform defaults)")

        return True

    except Exception as e:
        logger.warning(f"[{connection_name}] Failed to enable TCP keepalive: {e}")
        return False
