"""Pydantic configuration models with validation."""

from __future__ import annotations

from datetime import time
from typing import Dict, List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


# Supported mining algorithms
SUPPORTED_ALGORITHMS: List[str] = ["sha256", "randomx", "zksnark"]


def get_difficulty_buffer(pool_difficulty: float, buffer_percent: float = 0.05) -> float:
    """
    Calculate the difficulty buffer as a percentage of pool difficulty.

    Args:
        pool_difficulty: The current pool difficulty.
        buffer_percent: Buffer percentage (0.05 = 5%). Default 5%.

    Returns:
        Buffer value to add to pool difficulty.
    """
    return pool_difficulty * buffer_percent


def parse_time_string(v: str) -> time:
    """
    Parse a time string into a time object.

    Handles formats: HH:MM, HH:MM:SS, and special "24:00" for end of day.
    """
    v = v.strip()
    if not v:
        raise ValueError("Time value cannot be empty")

    # Handle "24:00" as end of day (convert to 23:59:59)
    if v == "24:00":
        return time(23, 59, 59)

    parts = v.split(":")
    if len(parts) < 1 or len(parts) > 3:
        raise ValueError(
            f"Invalid time format '{v}': expected HH:MM or HH:MM:SS"
        )

    try:
        hour = int(parts[0])
        minute = int(parts[1]) if len(parts) > 1 else 0
        second = int(parts[2]) if len(parts) > 2 else 0
    except ValueError:
        raise ValueError(
            f"Invalid time format '{v}': hour, minute, and second must be integers"
        )

    # Validate ranges
    if not (0 <= hour <= 23):
        raise ValueError(
            f"Invalid hour {hour} in time '{v}': must be 0-23"
        )
    if not (0 <= minute <= 59):
        raise ValueError(
            f"Invalid minute {minute} in time '{v}': must be 0-59"
        )
    if not (0 <= second <= 59):
        raise ValueError(
            f"Invalid second {second} in time '{v}': must be 0-59"
        )

    return time(hour, minute, second)


# Sentinel value for end-of-day (24:00)
END_OF_DAY: time = time(23, 59, 59)


class StratumServerConfig(BaseModel):
    """Configuration for a single stratum server with schedule times."""

    name: str = Field(..., description="Unique identifier for this server")
    enabled: bool = Field(default=True, description="Whether this server is enabled")
    host: str = Field(..., description="Server hostname or IP")
    port: int = Field(default=3333, ge=1, le=65535, description="Server port")
    username: str = Field(..., description="Mining pool username/wallet")
    password: str = Field(default="x", description="Mining pool password")
    ssl: bool = Field(default=False, description="Use SSL/TLS connection")
    connect_timeout: int = Field(default=30, ge=1, description="Connection timeout in seconds")
    retry_interval: int = Field(default=5, ge=1, description="Seconds between reconnection attempts")

    # Schedule times - when this server should be active
    start: Optional[time] = Field(default=None, description="Start time (HH:MM) for this server's schedule")
    end: Optional[time] = Field(default=None, description="End time (HH:MM) for this server's schedule")

    # Per-server difficulty overrides (optional - uses global defaults if not set)
    buffer_percent: Optional[float] = Field(
        default=None, ge=0.01, le=0.50,
        description="Override buffer percentage for this server (default: use global setting)"
    )
    min_difficulty: Optional[float] = Field(
        default=None, ge=0.001,
        description="Minimum difficulty to send to miners for this server"
    )
    max_difficulty: Optional[float] = Field(
        default=None, ge=1.0,
        description="Maximum difficulty to send to miners for this server"
    )

    @field_validator("start", "end", mode="before")
    @classmethod
    def parse_time(cls, v):
        """Parse time from string format."""
        if v is None:
            return None
        if isinstance(v, str):
            return parse_time_string(v)
        return v

    @model_validator(mode="after")
    def validate_schedule_times(self) -> "StratumServerConfig":
        """Validate that schedule times are consistent."""
        # If one is set, both must be set
        if (self.start is None) != (self.end is None):
            raise ValueError(
                f"Server '{self.name}': both start and end times must be specified, or neither"
            )
        # Reject start == end (only covers 1 second)
        if self.start is not None and self.start == self.end:
            raise ValueError(
                f"Server '{self.name}': start and end cannot be the same ({self.start}). "
                f"A schedule must cover a range of time."
            )
        return self

    @property
    def has_schedule(self) -> bool:
        """Check if this server has schedule times configured."""
        return self.start is not None and self.end is not None

    @property
    def is_end_of_day(self) -> bool:
        """Check if end time represents end of day (24:00)."""
        return self.end == END_OF_DAY

    def contains_time(self, t: time) -> bool:
        """
        Check if a given time falls within this server's schedule.

        End time is exclusive: [start, end) - includes start, excludes end.
        This allows adjacent schedules like 00:00-12:00 and 12:00-24:00
        where 12:00 belongs only to the second server.
        """
        if not self.has_schedule:
            return False

        # Handle end-of-day case (24:00 should include everything from start to midnight)
        if self.is_end_of_day:
            return self.start <= t

        # Handle normal case (start < end) - end is exclusive
        if self.start < self.end:
            return self.start <= t < self.end

        # Handle midnight crossover (start > end, e.g., 22:00-02:00) - end is exclusive
        return t >= self.start or t < self.end

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Validate server name is a safe identifier."""
        if not v or not v.strip():
            raise ValueError("Server name cannot be empty")
        v = v.strip()
        if len(v) > 64:
            raise ValueError("Server name must be 64 characters or less")
        # Allow alphanumeric, underscore, hyphen (can start with letter or digit)
        import re
        if not re.match(r"^[a-zA-Z0-9][a-zA-Z0-9_-]*$", v):
            raise ValueError(
                "Server name must start with a letter or digit and contain only "
                "alphanumeric characters, underscores, and hyphens"
            )
        return v

    @field_validator("username", "password")
    @classmethod
    def validate_credentials(cls, v: str) -> str:
        """
        Validate username/password don't contain control characters.

        Pool credentials are sent over JSON-RPC, so they must be safe for
        JSON encoding. Control characters could break parsing or enable
        log injection attacks.
        """
        if not v:
            return v
        # Check for control characters (ASCII 0-31 except common whitespace)
        for char in v:
            if ord(char) < 32 and char not in ('\t', '\n', '\r'):
                raise ValueError(
                    f"Username/password cannot contain control characters (found \\x{ord(char):02x})"
                )
        # Limit length to prevent DoS
        if len(v) > 256:
            raise ValueError("Username/password must be 256 characters or less")
        return v


class GlobalProxyConfig(BaseModel):
    """Global proxy settings shared across all algorithms."""

    # Connection limits
    max_connections: int = Field(default=1024, ge=1, description="Maximum concurrent connections per algorithm")

    # Miner connection timeouts
    # 180s (3 min) - TCP keepalive detects dead connections in ~90s; this catches idle miners
    miner_read_timeout: int = Field(default=180, ge=10, description="Miner read timeout in seconds")
    miner_write_timeout: int = Field(default=30, ge=1, description="Miner write timeout in seconds")

    # Upstream settings
    # 300s (5 min) - pools typically send mining.notify every ~30s for new transactions
    upstream_idle_timeout: int = Field(default=300, ge=60, description="Seconds without upstream messages before reconnecting")
    # 900s (15 min) - how long to retry connecting to a new server during scheduled switch
    # Old server stays active during this time, so mining continues uninterrupted
    server_switch_timeout: int = Field(default=900, ge=60, description="Seconds to retry connecting to new server during switch")

    # TCP keepalive settings
    tcp_keepalive: bool = Field(default=True, description="Enable TCP keepalive on connections")
    keepalive_idle: int = Field(default=60, ge=10, description="Seconds before sending keepalive probes")
    keepalive_interval: int = Field(default=10, ge=1, description="Seconds between keepalive probes")
    # 3 failed probes = 60 + (3 * 10) = 90 seconds to detect dead connection
    keepalive_count: int = Field(default=3, ge=1, description="Number of failed probes before connection is dead")

    # Retry and shutdown settings
    share_submit_retries: int = Field(default=3, ge=1, le=10, description="Number of times to retry failed share submissions")
    pending_shares_timeout: int = Field(default=10, ge=1, description="Timeout waiting for pending shares in seconds")


class ValidationConfig(BaseModel):
    """Configuration for share validation."""

    reject_duplicates: bool = Field(
        default=True, description="Reject duplicate share submissions"
    )
    reject_stale: bool = Field(
        default=True, description="Reject shares for stale/unknown jobs"
    )
    # 1000 shares is typically several minutes of mining at normal difficulty
    share_cache_size: int = Field(
        default=1000, ge=100, description="Maximum recent shares to track per session"
    )
    # 300s (5 min) - shares older than this are unlikely to be resubmitted
    share_cache_ttl: int = Field(
        default=300, ge=60, description="Share cache TTL in seconds"
    )
    # 10 jobs covers typical block intervals; pools rarely have more active jobs
    job_cache_size: int = Field(
        default=10, ge=2, description="Maximum jobs to track per session"
    )


class DifficultyConfig(BaseModel):
    """Configuration for difficulty management and smoothing."""

    # Cooldown between difficulty changes (prevents oscillation)
    change_cooldown: int = Field(
        default=30, ge=5, le=300,
        description="Minimum seconds between difficulty changes"
    )

    # Time-based decay toward pool difficulty
    decay_enabled: bool = Field(
        default=True,
        description="Enable gradual decay toward pool difficulty when above target"
    )
    decay_interval: int = Field(
        default=300, ge=60, le=3600,
        description="Seconds between decay checks"
    )
    decay_percent: float = Field(
        default=0.05, ge=0.01, le=0.25,
        description="Percentage to decay toward pool difficulty each interval (0.05 = 5%)"
    )

    # Progressive floor-reset (more gradual than fixed 50%/75%)
    floor_trigger_ratio: float = Field(
        default=0.50, ge=0.25, le=0.75,
        description="Trigger floor-reset when pool drops below this ratio of miner difficulty"
    )
    floor_reset_ratio: float = Field(
        default=0.75, ge=0.50, le=0.95,
        description="Reset to this ratio of current miner difficulty"
    )

    # Share rate monitoring - detects difficulty issues and adjusts accordingly
    share_rate_window: int = Field(
        default=300, ge=60, le=1800,
        description="Window in seconds to track share rate"
    )
    share_rate_min_shares: int = Field(
        default=3, ge=2, le=20,
        description="Minimum shares needed before analyzing rate"
    )
    share_rate_low_multiplier: float = Field(
        default=0.5, ge=0.1, le=0.9,
        description="If rate drops to this fraction of baseline, lower difficulty"
    )

    # Adaptive buffer - learns optimal buffer from rejection patterns
    buffer_start: float = Field(
        default=0.05, ge=0.01, le=0.20,
        description="Starting buffer percentage (0.05 = 5%)"
    )
    buffer_min: float = Field(
        default=0.02, ge=0.01, le=0.20,
        description="Minimum buffer percentage"
    )
    buffer_max: float = Field(
        default=0.20, ge=0.05, le=0.50,
        description="Maximum buffer percentage"
    )
    buffer_increase_step: float = Field(
        default=0.01, ge=0.005, le=0.05,
        description="Buffer increase step on low-diff rejection"
    )
    buffer_decrease_interval: int = Field(
        default=3600, ge=300, le=86400,
        description="Seconds without rejection before decreasing buffer"
    )


class CircuitBreakerConfig(BaseModel):
    """Configuration for circuit breaker pattern on pool connections."""

    enabled: bool = Field(
        default=True,
        description="Enable circuit breaker for failed pool connections"
    )
    failure_threshold: int = Field(
        default=5, ge=2, le=20,
        description="Number of consecutive failures before opening circuit"
    )
    recovery_timeout: int = Field(
        default=60, ge=10, le=600,
        description="Seconds to wait before trying a failed pool again (half-open state)"
    )
    success_threshold: int = Field(
        default=2, ge=1, le=10,
        description="Successful connections in half-open state to close circuit"
    )


class AlgorithmProxyConfig(BaseModel):
    """Per-algorithm proxy configuration (bind settings)."""

    enabled: bool = Field(default=False, description="Whether this algorithm's proxy is enabled")
    bind_host: str = Field(default="0.0.0.0", description="Address to bind to")
    bind_port: int = Field(default=3333, ge=1, le=65535, description="Port to listen on")

    @field_validator("bind_port")
    @classmethod
    def warn_privileged_port(cls, v: int) -> int:
        """Warn if using a privileged port."""
        if v < 1024:
            import warnings
            warnings.warn(
                f"Port {v} is a privileged port (< 1024) and requires "
                f"root/administrator privileges to bind",
                UserWarning,
                stacklevel=2,
            )
        return v


class ProxyConfig(BaseModel):
    """Configuration for the proxy server with per-algorithm settings."""

    # Global settings shared by all algorithms
    global_: GlobalProxyConfig = Field(default_factory=GlobalProxyConfig, alias="global")

    # Validation settings
    validation: ValidationConfig = Field(default_factory=ValidationConfig)

    # Difficulty management settings
    difficulty: DifficultyConfig = Field(default_factory=DifficultyConfig)

    # Circuit breaker settings
    circuit_breaker: CircuitBreakerConfig = Field(default_factory=CircuitBreakerConfig)

    # Per-algorithm proxy settings
    sha256: AlgorithmProxyConfig = Field(
        default_factory=lambda: AlgorithmProxyConfig(enabled=True, bind_port=3333)
    )
    randomx: AlgorithmProxyConfig = Field(
        default_factory=lambda: AlgorithmProxyConfig(enabled=False, bind_port=3334)
    )
    zksnark: AlgorithmProxyConfig = Field(
        default_factory=lambda: AlgorithmProxyConfig(enabled=False, bind_port=3335)
    )

    @model_validator(mode="after")
    def validate_unique_ports(self) -> "ProxyConfig":
        """Ensure enabled algorithms have unique ports."""
        enabled_ports: dict[int, str] = {}
        for algo in SUPPORTED_ALGORITHMS:
            algo_config = getattr(self, algo)
            if algo_config.enabled:
                if algo_config.bind_port in enabled_ports:
                    raise ValueError(
                        f"Port {algo_config.bind_port} is used by both "
                        f"'{enabled_ports[algo_config.bind_port]}' and '{algo}'"
                    )
                enabled_ports[algo_config.bind_port] = algo
        return self

    def get_enabled_algorithms(self) -> List[str]:
        """Get list of enabled algorithm names."""
        return [algo for algo in SUPPORTED_ALGORITHMS if getattr(self, algo).enabled]


class LoggingConfig(BaseModel):
    """Configuration for logging."""

    level: str = Field(default="INFO", description="Log level")
    file: Optional[str] = Field(default=None, description="Log file path")
    rotation: str = Field(default="50 MB", description="Log rotation size")
    retention: int = Field(default=10, ge=1, description="Number of rotated files to keep")
    format: str = Field(
        default="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
        description="Log message format",
    )

    @field_validator("level")
    @classmethod
    def validate_level(cls, v: str) -> str:
        """Validate log level."""
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        v_upper = v.upper()
        if v_upper not in valid_levels:
            raise ValueError(f"Invalid log level: {v}. Must be one of {valid_levels}")
        return v_upper


class Config(BaseModel):
    """Main configuration model with per-algorithm organization."""

    proxy: ProxyConfig = Field(default_factory=ProxyConfig)

    # Servers organized by algorithm (each server includes its schedule times)
    servers: Dict[str, List[StratumServerConfig]] = Field(
        default_factory=lambda: {"sha256": []},
        description="Servers organized by algorithm (sha256, randomx, zksnark)"
    )

    logging: LoggingConfig = Field(default_factory=LoggingConfig)

    @model_validator(mode="after")
    def validate_config(self) -> "Config":
        """Validate the entire configuration."""
        self._validate_algorithm_keys()
        self._validate_enabled_algorithms_have_servers()
        self._validate_unique_server_names()
        self._validate_schedules()
        return self

    def _validate_algorithm_keys(self) -> None:
        """Ensure only valid algorithm keys are used."""
        for algo in self.servers.keys():
            if algo not in SUPPORTED_ALGORITHMS:
                raise ValueError(
                    f"Unknown algorithm '{algo}' in servers. "
                    f"Supported: {SUPPORTED_ALGORITHMS}"
                )

    def _validate_enabled_algorithms_have_servers(self) -> None:
        """Ensure enabled algorithms have at least one enabled server."""
        for algo in self.proxy.get_enabled_algorithms():
            servers = self.servers.get(algo, [])
            enabled_servers = [s for s in servers if s.enabled]
            if not enabled_servers:
                raise ValueError(
                    f"Algorithm '{algo}' is enabled but has no enabled servers configured"
                )

    def _validate_unique_server_names(self) -> None:
        """Ensure all server names are unique within each algorithm."""
        for algo, servers in self.servers.items():
            names = [s.name for s in servers]
            if len(names) != len(set(names)):
                duplicates = [n for n in names if names.count(n) > 1]
                raise ValueError(
                    f"Duplicate server names in {algo}: {set(duplicates)}"
                )

    def _get_minute_ranges(
        self, algorithm: str, include_server: bool = False
    ) -> List[tuple]:
        """
        Convert server schedules to minute-based ranges for an algorithm.

        Handles midnight crossover by splitting into two ranges.

        Args:
            algorithm: The algorithm to get ranges for.
            include_server: If True, include server name in tuples.

        Returns:
            Sorted list of (start_mins, end_mins) or (start_mins, end_mins, server) tuples.
        """
        ranges = []
        for server in self.servers.get(algorithm, []):
            if not server.enabled or not server.has_schedule:
                continue

            start_mins = server.start.hour * 60 + server.start.minute
            end_mins = server.end.hour * 60 + server.end.minute
            if server.end == END_OF_DAY:
                end_mins = 24 * 60  # End of day

            if include_server:
                if end_mins > start_mins:
                    ranges.append((start_mins, end_mins, server.name))
                else:
                    # Midnight crossover - split into two ranges
                    ranges.append((start_mins, 24 * 60, server.name))
                    ranges.append((0, end_mins, server.name))
            else:
                if end_mins > start_mins:
                    ranges.append((start_mins, end_mins))
                else:
                    # Midnight crossover - split into two ranges
                    ranges.append((start_mins, 24 * 60))
                    ranges.append((0, end_mins))

        # Sort by start time
        ranges.sort(key=lambda x: x[0])
        return ranges

    def _validate_no_overlap(self, algorithm: str) -> None:
        """Ensure no server schedules overlap for an algorithm."""
        ranges = self._get_minute_ranges(algorithm, include_server=True)

        # Check for overlaps
        for i in range(len(ranges) - 1):
            current_end = ranges[i][1]
            next_start = ranges[i + 1][0]
            if current_end > next_start:
                raise ValueError(
                    f"Overlapping schedules in {algorithm}: "
                    f"server '{ranges[i][2]}' ends at minute {current_end} "
                    f"but server '{ranges[i + 1][2]}' starts at minute {next_start}"
                )

    def _validate_complete_coverage(self, algorithm: str) -> None:
        """Ensure server schedules cover all 24 hours without gaps for an algorithm."""
        ranges = self._get_minute_ranges(algorithm, include_server=False)

        # Check for gaps (schedule must start at 0 and end at 24*60)
        if not ranges:
            raise ValueError(
                f"No servers with schedules configured for '{algorithm}'. "
                f"Add start/end times to at least one server."
            )

        if ranges[0][0] != 0:
            raise ValueError(
                f"Schedule gap in {algorithm}: no server configured from 00:00 to "
                f"{ranges[0][0] // 60:02d}:{ranges[0][0] % 60:02d}"
            )

        # Check for gaps between consecutive ranges
        for i in range(len(ranges) - 1):
            current_end = ranges[i][1]
            next_start = ranges[i + 1][0]
            if current_end < next_start:
                raise ValueError(
                    f"Schedule gap in {algorithm}: no server configured from "
                    f"{current_end // 60:02d}:{current_end % 60:02d} to "
                    f"{next_start // 60:02d}:{next_start % 60:02d}"
                )

        # Check that schedule ends at midnight
        if ranges[-1][1] != 24 * 60:
            raise ValueError(
                f"Schedule gap in {algorithm}: no server configured from "
                f"{ranges[-1][1] // 60:02d}:{ranges[-1][1] % 60:02d} to 24:00"
            )

    def _validate_schedules(self) -> None:
        """Validate server schedules for all enabled algorithms."""
        for algo in self.proxy.get_enabled_algorithms():
            # Check that at least one enabled server has a schedule
            scheduled_servers = [
                s for s in self.servers.get(algo, [])
                if s.enabled and s.has_schedule
            ]
            if not scheduled_servers:
                raise ValueError(
                    f"Algorithm '{algo}' is enabled but has no servers with schedules. "
                    f"Add start/end times to at least one server."
                )
            self._validate_no_overlap(algo)
            self._validate_complete_coverage(algo)

    def get_server_by_name(self, algorithm: str, name: str) -> Optional[StratumServerConfig]:
        """Get a server configuration by name within an algorithm."""
        for server in self.servers.get(algorithm, []):
            if server.name == name:
                return server
        return None

    def get_server_names(self, algorithm: str) -> List[str]:
        """Get list of all server names for an algorithm."""
        return [s.name for s in self.servers.get(algorithm, [])]

    def get_enabled_servers(self, algorithm: str) -> List[StratumServerConfig]:
        """Get list of enabled servers for an algorithm."""
        return [s for s in self.servers.get(algorithm, []) if s.enabled]

    def get_scheduled_servers(self, algorithm: str) -> List[StratumServerConfig]:
        """Get list of enabled servers with schedules for an algorithm."""
        return [
            s for s in self.servers.get(algorithm, [])
            if s.enabled and s.has_schedule
        ]

    def get_server_for_time(self, algorithm: str, t: time) -> Optional[StratumServerConfig]:
        """Get the server that should be active at a given time."""
        for server in self.get_scheduled_servers(algorithm):
            if server.contains_time(t):
                return server
        return None
