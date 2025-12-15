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
    max_retries: int = Field(default=240, ge=1, description="Maximum reconnection attempts")


class TimeFrame(BaseModel):
    """A time frame during which a specific server is used."""

    start: time = Field(..., description="Start time (HH:MM)")
    end: time = Field(..., description="End time (HH:MM)")
    server: str = Field(..., description="Server name to use during this timeframe")

    # Sentinel value for end-of-day (24:00)
    END_OF_DAY: ClassVar[time] = time(23, 59, 59)

    @field_validator("start", "end", mode="before")
    @classmethod
    def parse_time(cls, v):
        """Parse time from string format."""
        if isinstance(v, str):
            # Handle "24:00" as end of day (convert to 23:59:59)
            if v == "24:00":
                return time(23, 59, 59)
            parts = v.split(":")
            hour = int(parts[0])
            minute = int(parts[1]) if len(parts) > 1 else 0
            second = int(parts[2]) if len(parts) > 2 else 0
            return time(hour, minute, second)
        return v

    @property
    def is_end_of_day(self) -> bool:
        """Check if end time represents end of day (24:00)."""
        return self.end == self.END_OF_DAY

    def contains(self, t: time) -> bool:
        """Check if a given time falls within this timeframe."""
        # Handle end-of-day case (24:00 should include everything up to midnight)
        if self.is_end_of_day and self.start <= t:
            return True

        # Handle normal case (start < end)
        if self.start <= self.end:
            return self.start <= t <= self.end
        # Handle midnight crossover (start > end, e.g., 22:00-02:00)
        return t >= self.start or t <= self.end

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
    bind_port: int = Field(default=3333, ge=1, le=65535, description="Port to listen on")
    max_connections: int = Field(default=100, ge=1, description="Maximum concurrent connections")
    connection_timeout: int = Field(default=60, ge=1, description="Connection timeout in seconds")
    miner_read_timeout: int = Field(default=600, ge=10, description="Miner read timeout in seconds")
    send_timeout: int = Field(default=30, ge=1, description="Send to miner timeout in seconds")
    pending_shares_timeout: int = Field(default=10, ge=1, description="Timeout waiting for pending shares in seconds")
    tcp_keepalive: bool = Field(default=True, description="Enable TCP keepalive on connections")
    keepalive_idle: int = Field(default=60, ge=10, description="Seconds before sending keepalive probes")
    keepalive_interval: int = Field(default=10, ge=1, description="Seconds between keepalive probes")
    keepalive_count: int = Field(default=3, ge=1, description="Number of failed probes before connection is dead")
    share_submit_retries: int = Field(default=3, ge=1, le=10, description="Number of times to retry failed share submissions")
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
    share_cache_size: int = Field(
        default=1000, ge=100, description="Maximum recent shares to track per session"
    )
    share_cache_ttl: int = Field(
        default=300, ge=60, description="Share cache TTL in seconds"
    )
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

    def get_server_by_name(self, name: str) -> Optional[StratumServerConfig]:
        """Get a server configuration by name."""
        for server in self.servers:
            if server.name == name:
                return server
        return None

    def get_server_names(self) -> List[str]:
        """Get list of all server names."""
        return [s.name for s in self.servers]
