"""Circuit breaker pattern for pool connections.

Prevents repeated connection attempts to failing pools, reducing log spam
and improving overall system stability.

States:
- CLOSED: Normal operation, connections allowed
- OPEN: Circuit tripped, connections blocked for recovery_timeout
- HALF_OPEN: Testing if pool recovered, limited connections allowed
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING, Dict, Optional

from loguru import logger

if TYPE_CHECKING:
    from crypto_stratum_proxy.config.models import CircuitBreakerConfig


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = auto()      # Normal - connections allowed
    OPEN = auto()        # Tripped - connections blocked
    HALF_OPEN = auto()   # Testing - limited connections allowed


@dataclass
class CircuitStatus:
    """Status of a single circuit breaker."""
    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    success_count: int = 0  # For half-open state
    last_failure_time: float = 0.0
    last_state_change: float = field(default_factory=time.time)
    open_until: float = 0.0  # When to transition from OPEN to HALF_OPEN


class CircuitBreakerManager:
    """
    Manages circuit breakers for multiple pools.

    This is a singleton that can be shared across all sessions.
    Each pool has its own circuit breaker state.

    Usage:
        manager = CircuitBreakerManager.get_instance(config)
        if manager.can_connect("pool1"):
            try:
                connect()
                manager.record_success("pool1")
            except Exception:
                manager.record_failure("pool1")
    """

    _instance: Optional["CircuitBreakerManager"] = None

    def __init__(self, config: CircuitBreakerConfig):
        """
        Initialize the circuit breaker manager.

        Args:
            config: Circuit breaker configuration.
        """
        self._config = config
        self._circuits: Dict[str, CircuitStatus] = {}

    @classmethod
    def get_instance(cls, config: Optional[CircuitBreakerConfig] = None) -> "CircuitBreakerManager":
        """
        Get the singleton instance.

        Args:
            config: Configuration (required on first call).

        Returns:
            The singleton CircuitBreakerManager instance.
        """
        if cls._instance is None:
            if config is None:
                # Return a disabled manager if no config
                from crypto_stratum_proxy.config.models import CircuitBreakerConfig
                config = CircuitBreakerConfig(enabled=False)
            cls._instance = cls(config)
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        """Reset the singleton instance (for testing)."""
        cls._instance = None

    def _get_circuit(self, server_name: str) -> CircuitStatus:
        """Get or create circuit status for a server."""
        if server_name not in self._circuits:
            self._circuits[server_name] = CircuitStatus()
        return self._circuits[server_name]

    def can_connect(self, server_name: str) -> tuple[bool, Optional[str]]:
        """
        Check if connection to a server is allowed.

        Args:
            server_name: Name of the server.

        Returns:
            Tuple of (allowed, reason). If not allowed, reason explains why.
        """
        if not self._config.enabled:
            return True, None

        circuit = self._get_circuit(server_name)
        now = time.time()

        if circuit.state == CircuitState.CLOSED:
            return True, None

        if circuit.state == CircuitState.OPEN:
            # Check if recovery timeout has passed
            if now >= circuit.open_until:
                # Transition to half-open
                circuit.state = CircuitState.HALF_OPEN
                circuit.success_count = 0
                circuit.last_state_change = now
                logger.info(
                    f"Circuit breaker for {server_name}: OPEN -> HALF_OPEN "
                    f"(testing recovery)"
                )
                return True, None
            else:
                remaining = int(circuit.open_until - now)
                return False, f"Circuit open for {remaining}s more"

        if circuit.state == CircuitState.HALF_OPEN:
            # Allow connection attempts in half-open state
            return True, None

        return True, None

    def record_success(self, server_name: str) -> None:
        """
        Record a successful connection.

        Args:
            server_name: Name of the server.
        """
        if not self._config.enabled:
            return

        circuit = self._get_circuit(server_name)
        now = time.time()

        if circuit.state == CircuitState.HALF_OPEN:
            circuit.success_count += 1
            if circuit.success_count >= self._config.success_threshold:
                # Recovery confirmed, close the circuit
                circuit.state = CircuitState.CLOSED
                circuit.failure_count = 0
                circuit.success_count = 0
                circuit.last_state_change = now
                logger.info(
                    f"Circuit breaker for {server_name}: HALF_OPEN -> CLOSED "
                    f"(recovery confirmed)"
                )
        elif circuit.state == CircuitState.CLOSED:
            # Reset failure count on success
            if circuit.failure_count > 0:
                circuit.failure_count = 0

    def record_failure(self, server_name: str) -> None:
        """
        Record a connection failure.

        Args:
            server_name: Name of the server.
        """
        if not self._config.enabled:
            return

        circuit = self._get_circuit(server_name)
        now = time.time()

        circuit.failure_count += 1
        circuit.last_failure_time = now

        if circuit.state == CircuitState.HALF_OPEN:
            # Failed during recovery test, reopen the circuit
            circuit.state = CircuitState.OPEN
            circuit.open_until = now + self._config.recovery_timeout
            circuit.success_count = 0
            circuit.last_state_change = now
            logger.warning(
                f"Circuit breaker for {server_name}: HALF_OPEN -> OPEN "
                f"(recovery failed, retry in {self._config.recovery_timeout}s)"
            )

        elif circuit.state == CircuitState.CLOSED:
            if circuit.failure_count >= self._config.failure_threshold:
                # Trip the circuit
                circuit.state = CircuitState.OPEN
                circuit.open_until = now + self._config.recovery_timeout
                circuit.last_state_change = now
                logger.warning(
                    f"Circuit breaker for {server_name}: CLOSED -> OPEN "
                    f"({circuit.failure_count} consecutive failures, "
                    f"blocking for {self._config.recovery_timeout}s)"
                )

    def get_status(self, server_name: str) -> dict:
        """
        Get the current status of a circuit breaker.

        Args:
            server_name: Name of the server.

        Returns:
            Dict with state, failure_count, and other info.
        """
        circuit = self._get_circuit(server_name)
        now = time.time()

        return {
            "state": circuit.state.name,
            "failure_count": circuit.failure_count,
            "success_count": circuit.success_count,
            "last_failure_time": circuit.last_failure_time,
            "time_since_last_failure": now - circuit.last_failure_time if circuit.last_failure_time else None,
            "open_until": circuit.open_until if circuit.state == CircuitState.OPEN else None,
            "seconds_until_retry": max(0, circuit.open_until - now) if circuit.state == CircuitState.OPEN else None,
        }

    def reset(self, server_name: str) -> None:
        """
        Reset a circuit breaker to closed state.

        Args:
            server_name: Name of the server.
        """
        if server_name in self._circuits:
            self._circuits[server_name] = CircuitStatus()
            logger.info(f"Circuit breaker for {server_name} reset to CLOSED")

    def reset_all(self) -> None:
        """Reset all circuit breakers."""
        self._circuits.clear()
        logger.info("All circuit breakers reset")
