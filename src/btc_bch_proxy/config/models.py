"""Pydantic configuration models with validation."""

from __future__ import annotations

from datetime import time
from typing import ClassVar, List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class StratumServerConfig(BaseModel):
    """Configuration for a single stratum server."""

    name: str = Field(..., description="Unique identifier for this server")
    host: str = Field(..., description="Server hostname or IP")
    port: int = Field(default=3333, ge=1, le=65535, description="Server port")
    username: str = Field(..., description="Mining pool username/wallet")
    password: str = Field(default="x", description="Mining pool password")
    ssl: bool = Field(default=False, description="Use SSL/TLS connection")
    timeout: int = Field(default=30, ge=1, description="Connection timeout in seconds")
    retry_interval: int = Field(default=5, ge=1, description="Seconds between reconnection attempts")
    # 240 retries * 5 second interval = 20 minute retry window before failover
    max_retries: int = Field(default=240, ge=1, description="Maximum reconnection attempts")

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


class TimeFrame(BaseModel):
    """A time frame during which a specific server is used."""

    start: time = Field(..., description="Start time (HH:MM)")
    end: time = Field(..., description="End time (HH:MM)")
    server: str = Field(..., description="Server name to use during this timeframe")

    # Sentinel value for end-of-day (24:00)
    END_OF_DAY: ClassVar[time] = time(23, 59, 59)

    @model_validator(mode="after")
    def validate_timeframe(self) -> "TimeFrame":
        """Validate that the timeframe covers a meaningful duration."""
        # Reject start == end because it only covers 1 second, leaving 59 seconds
        # of each minute without coverage. This is almost certainly a config error.
        if self.start == self.end:
            raise ValueError(
                f"Timeframe start and end cannot be the same ({self.start}). "
                f"A timeframe must cover a range of time."
            )
        return self

    @field_validator("start", "end", mode="before")
    @classmethod
    def parse_time(cls, v):
        """Parse time from string format."""
        if isinstance(v, str):
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
        return v

    @property
    def is_end_of_day(self) -> bool:
        """Check if end time represents end of day (24:00)."""
        return self.end == self.END_OF_DAY

    def contains(self, t: time) -> bool:
        """
        Check if a given time falls within this timeframe.

        End time is exclusive: [start, end) - includes start, excludes end.
        This allows adjacent timeframes like 00:00-12:00 and 12:00-24:00
        where 12:00 belongs only to the second timeframe.
        """
        # Handle end-of-day case (24:00 should include everything from start to midnight)
        if self.is_end_of_day:
            return self.start <= t

        # Note: start == end case is rejected by model_validator, so we don't handle it here

        # Handle normal case (start < end) - end is exclusive
        if self.start < self.end:
            return self.start <= t < self.end

        # Handle midnight crossover (start > end, e.g., 22:00-02:00) - end is exclusive
        return t >= self.start or t < self.end

    def duration_seconds(self) -> int:
        """Calculate duration of this timeframe in seconds."""
        start_secs = self.start.hour * 3600 + self.start.minute * 60 + self.start.second
        end_secs = self.end.hour * 3600 + self.end.minute * 60 + self.end.second

        if end_secs >= start_secs:
            return end_secs - start_secs
        # Midnight crossover
        return (24 * 3600 - start_secs) + end_secs


class ProxyConfig(BaseModel):
    """Configuration for the proxy server itself."""

    bind_host: str = Field(default="0.0.0.0", description="Address to bind to")
    # 3333 is the standard stratum mining protocol port
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
    max_connections: int = Field(default=100, ge=1, description="Maximum concurrent connections")
    connection_timeout: int = Field(default=60, ge=1, description="Connection timeout in seconds")
    # 600s (10 min) - miners may go quiet between jobs; too short causes unnecessary disconnects
    miner_read_timeout: int = Field(default=600, ge=10, description="Miner read timeout in seconds")
    send_timeout: int = Field(default=30, ge=1, description="Send to miner timeout in seconds")
    pending_shares_timeout: int = Field(default=10, ge=1, description="Timeout waiting for pending shares in seconds")
    tcp_keepalive: bool = Field(default=True, description="Enable TCP keepalive on connections")
    keepalive_idle: int = Field(default=60, ge=10, description="Seconds before sending keepalive probes")
    keepalive_interval: int = Field(default=10, ge=1, description="Seconds between keepalive probes")
    # 3 failed probes = 60 + (3 * 10) = 90 seconds to detect dead connection
    keepalive_count: int = Field(default=3, ge=1, description="Number of failed probes before connection is dead")
    share_submit_retries: int = Field(default=3, ge=1, le=10, description="Number of times to retry failed share submissions")
    # 300s (5 min) - pools typically send mining.notify every ~30s for new transactions
    upstream_health_timeout: int = Field(default=300, ge=60, description="Seconds without upstream messages before reconnecting")


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


class FailoverConfig(BaseModel):
    """Configuration for failover behavior."""

    retry_timeout_minutes: int = Field(
        default=20, ge=1, description="Minutes to retry primary before failover"
    )


class ValidationConfig(BaseModel):
    """Configuration for share validation."""

    reject_duplicates: bool = Field(
        default=True, description="Reject duplicate share submissions"
    )
    reject_stale: bool = Field(
        default=True, description="Reject shares for stale/unknown jobs"
    )
    validate_difficulty: bool = Field(
        default=False, description="Validate share meets difficulty target (requires hash computation)"
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


class Config(BaseModel):
    """Main configuration model."""

    proxy: ProxyConfig = Field(default_factory=ProxyConfig)
    servers: List[StratumServerConfig] = Field(..., min_length=1)
    schedule: List[TimeFrame] = Field(..., min_length=1)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    failover: FailoverConfig = Field(default_factory=FailoverConfig)
    validation: ValidationConfig = Field(default_factory=ValidationConfig)

    @model_validator(mode="after")
    def validate_config(self) -> "Config":
        """Validate the entire configuration."""
        self._validate_unique_server_names()
        self._validate_server_references()
        self._validate_no_overlap()
        self._validate_complete_coverage()
        return self

    def _validate_unique_server_names(self) -> None:
        """Ensure all server names are unique."""
        names = [s.name for s in self.servers]
        if len(names) != len(set(names)):
            duplicates = [n for n in names if names.count(n) > 1]
            raise ValueError(f"Duplicate server names: {set(duplicates)}")

    def _validate_server_references(self) -> None:
        """Ensure all schedule entries reference valid servers."""
        server_names = {s.name for s in self.servers}
        for tf in self.schedule:
            if tf.server not in server_names:
                raise ValueError(
                    f"Schedule references unknown server '{tf.server}'. "
                    f"Available servers: {server_names}"
                )

    def _validate_no_overlap(self) -> None:
        """Ensure no timeframes overlap."""
        # Convert timeframes to minute ranges for easier comparison
        ranges = []
        for tf in self.schedule:
            start_mins = tf.start.hour * 60 + tf.start.minute
            end_mins = tf.end.hour * 60 + tf.end.minute
            if tf.end == time(23, 59, 59):
                end_mins = 24 * 60  # End of day

            if end_mins > start_mins:
                ranges.append((start_mins, end_mins, tf.server))
            else:
                # Midnight crossover - split into two ranges
                ranges.append((start_mins, 24 * 60, tf.server))
                ranges.append((0, end_mins, tf.server))

        # Sort by start time
        ranges.sort(key=lambda x: x[0])

        # Check for overlaps
        for i in range(len(ranges) - 1):
            current_end = ranges[i][1]
            next_start = ranges[i + 1][0]
            if current_end > next_start:
                raise ValueError(
                    f"Overlapping timeframes detected: "
                    f"server '{ranges[i][2]}' ends at minute {current_end} "
                    f"but server '{ranges[i + 1][2]}' starts at minute {next_start}"
                )

    def _validate_complete_coverage(self) -> None:
        """Ensure schedule covers all 24 hours without gaps."""
        # Convert timeframes to minute ranges
        ranges = []
        for tf in self.schedule:
            start_mins = tf.start.hour * 60 + tf.start.minute
            end_mins = tf.end.hour * 60 + tf.end.minute
            if tf.end == time(23, 59, 59):
                end_mins = 24 * 60  # End of day

            if end_mins > start_mins:
                ranges.append((start_mins, end_mins))
            else:
                # Midnight crossover - split into two ranges
                ranges.append((start_mins, 24 * 60))
                ranges.append((0, end_mins))

        # Sort by start time
        ranges.sort(key=lambda x: x[0])

        # Check for gaps (schedule must start at 0 and end at 24*60)
        if not ranges:
            raise ValueError("Schedule is empty - no timeframes defined")

        if ranges[0][0] != 0:
            raise ValueError(
                f"Schedule gap: no server configured from 00:00 to "
                f"{ranges[0][0] // 60:02d}:{ranges[0][0] % 60:02d}"
            )

        # Check for gaps between consecutive ranges
        for i in range(len(ranges) - 1):
            current_end = ranges[i][1]
            next_start = ranges[i + 1][0]
            if current_end < next_start:
                raise ValueError(
                    f"Schedule gap: no server configured from "
                    f"{current_end // 60:02d}:{current_end % 60:02d} to "
                    f"{next_start // 60:02d}:{next_start % 60:02d}"
                )

        # Check that schedule ends at midnight
        if ranges[-1][1] != 24 * 60:
            raise ValueError(
                f"Schedule gap: no server configured from "
                f"{ranges[-1][1] // 60:02d}:{ranges[-1][1] % 60:02d} to 24:00"
            )

    def get_server_by_name(self, name: str) -> Optional[StratumServerConfig]:
        """Get a server configuration by name."""
        for server in self.servers:
            if server.name == name:
                return server
        return None

    def get_server_names(self) -> List[str]:
        """Get list of all server names."""
        return [s.name for s in self.servers]
