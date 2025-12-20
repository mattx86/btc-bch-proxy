"""Command-line interface for the stratum proxy."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import click
from loguru import logger

from crypto_stratum_proxy import __version__


def find_config_file() -> Optional[Path]:
    """
    Find the configuration file in common locations.

    Returns:
        Path to config file or None.
    """
    search_paths = [
        Path("config.yaml"),
        Path("config.yml"),
        Path.home() / ".config" / "crypto-stratum-proxy" / "config.yaml",
        Path("/etc/crypto-stratum-proxy/config.yaml"),
    ]

    if sys.platform == "win32":
        search_paths.append(
            Path.home() / "AppData" / "Local" / "crypto-stratum-proxy" / "config.yaml"
        )

    for path in search_paths:
        if path.exists():
            return path

    return None


@click.group()
@click.version_option(version=__version__, prog_name="crypto-stratum-proxy")
def main():
    """Crypto Stratum Proxy with time-based server routing.

    Supports multiple mining algorithms: SHA-256 (BTC/BCH), RandomX (XMR), zkSNARK (ALEO).
    """
    pass


@main.command()
@click.option(
    "-c",
    "--config",
    "config_path",
    type=click.Path(exists=True, path_type=Path),
    help="Path to configuration file",
)
@click.option(
    "-f",
    "--foreground",
    is_flag=True,
    help="Run in foreground (don't daemonize)",
)
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False),
    default=None,
    help="Override log level from config",
)
def start(config_path: Optional[Path], foreground: bool, log_level: Optional[str]):
    """Start the stratum proxy."""
    from crypto_stratum_proxy.config.loader import ConfigError, load_config
    from crypto_stratum_proxy.daemon import DaemonError, DaemonManager
    from crypto_stratum_proxy.logging.setup import setup_logging

    # Find config file
    if config_path is None:
        config_path = find_config_file()
        if config_path is None:
            click.echo("Error: No configuration file found", err=True)
            click.echo("Please specify a config file with -c/--config", err=True)
            sys.exit(1)

    click.echo(f"Using configuration: {config_path}")

    # Load and validate configuration
    try:
        config = load_config(config_path)
    except ConfigError as e:
        click.echo(f"Configuration error: {e}", err=True)
        sys.exit(1)

    # Override log level if specified
    if log_level:
        config.logging.level = log_level.upper()

    # Setup logging for CLI output
    setup_logging(config.logging)

    # Create daemon manager
    daemon = DaemonManager(config, config_path=str(config_path))

    # Check if already running
    if daemon.is_running():
        pid = daemon.get_pid()
        click.echo(f"Error: Proxy already running with PID {pid}", err=True)
        sys.exit(1)

    try:
        if foreground:
            click.echo("Starting proxy in foreground mode...")
            daemon.run_foreground()
        else:
            click.echo("Starting proxy in background...")
            pid = daemon.run_background()
            click.echo(f"Proxy started with PID {pid}")
    except DaemonError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except KeyboardInterrupt:
        click.echo("\nShutdown requested...")
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)


@main.command()
@click.option(
    "-c",
    "--config",
    "config_path",
    type=click.Path(exists=True, path_type=Path),
    help="Path to configuration file",
)
def stop(config_path: Optional[Path]):
    """Stop the running proxy."""
    from crypto_stratum_proxy.config.loader import ConfigError, load_config
    from crypto_stratum_proxy.daemon import DaemonManager

    # Find config file
    if config_path is None:
        config_path = find_config_file()
        if config_path is None:
            click.echo("Error: No configuration file found", err=True)
            sys.exit(1)

    # Load configuration
    try:
        config = load_config(config_path)
    except ConfigError as e:
        click.echo(f"Configuration error: {e}", err=True)
        sys.exit(1)

    daemon = DaemonManager(config)

    if not daemon.is_running():
        click.echo("Proxy is not running")
        return

    click.echo("Stopping proxy...")
    if daemon.stop():
        click.echo("Proxy stopped successfully")
    else:
        click.echo("Failed to stop proxy", err=True)
        sys.exit(1)


@main.command()
@click.option(
    "-c",
    "--config",
    "config_path",
    type=click.Path(exists=True, path_type=Path),
    help="Path to configuration file",
)
def status(config_path: Optional[Path]):
    """Check the proxy status."""
    from crypto_stratum_proxy.config.loader import ConfigError, load_config
    from crypto_stratum_proxy.daemon import DaemonManager

    # Find config file
    if config_path is None:
        config_path = find_config_file()
        if config_path is None:
            click.echo("Error: No configuration file found", err=True)
            sys.exit(1)

    # Load configuration
    try:
        config = load_config(config_path)
    except ConfigError as e:
        click.echo(f"Configuration error: {e}", err=True)
        sys.exit(1)

    daemon = DaemonManager(config)

    if daemon.is_running():
        pid = daemon.get_pid()
        click.echo(f"Proxy is running (PID: {pid})")
        click.echo(f"PID file: {daemon.pid_file_path}")
    else:
        click.echo("Proxy is not running")


@main.command()
@click.argument("config_path", type=click.Path(exists=True, path_type=Path))
def validate(config_path: Path):
    """Validate a configuration file."""
    from crypto_stratum_proxy.config.loader import validate_config
    from crypto_stratum_proxy.config.models import END_OF_DAY

    is_valid, message = validate_config(config_path)

    if is_valid:
        click.echo(f"✓ {message}")

        # Show additional info
        from crypto_stratum_proxy.config.loader import load_config

        config = load_config(config_path)

        # Show enabled algorithms
        enabled_algos = config.proxy.get_enabled_algorithms()
        click.echo(f"\nEnabled algorithms: {', '.join(enabled_algos)}")

        for algo in enabled_algos:
            algo_config = getattr(config.proxy, algo)
            click.echo(f"\n[{algo.upper()}] Listening on {algo_config.bind_host}:{algo_config.bind_port}")

            click.echo("  Servers:")
            for server in config.servers.get(algo, []):
                status = "enabled" if server.enabled else "disabled"
                if server.has_schedule:
                    start_str = server.start.strftime("%H:%M")
                    end_str = "24:00" if server.end == END_OF_DAY else server.end.strftime("%H:%M")
                    schedule_info = f", schedule: {start_str}-{end_str}"
                else:
                    schedule_info = ""
                click.echo(f"    - {server.name}: {server.host}:{server.port} ({status}{schedule_info})")
    else:
        click.echo(f"✗ {message}", err=True)
        sys.exit(1)


@main.command()
def init():
    """Create a sample configuration file."""
    import shutil

    try:
        package_dir = Path(__file__).parent.parent.parent
        example_config = package_dir / "config.example.yaml"

        if not example_config.exists():
            create_sample_config()
        else:
            dest_path = Path("config.yaml")
            if dest_path.exists():
                if not click.confirm(f"{dest_path} already exists. Overwrite?"):
                    click.echo("Skipping config file creation.")
                    return
            shutil.copy(example_config, dest_path)
            click.echo(f"Created {dest_path}")
    except Exception:
        create_sample_config()


def create_sample_config():
    """Create a sample configuration file from scratch."""
    config_content = """# Multi-Coin Stratum Proxy Configuration
# Supported algorithms: sha256 (BTC/BCH), randomx (XMR), zksnark (ALEO)

proxy:
  # Global settings shared by all algorithms
  global:
    # Connection limits
    max_connections: 1024           # Maximum concurrent miner connections

    # Miner connection timeouts
    miner_read_timeout: 180         # Miner read timeout (seconds, 3 min)
    miner_write_timeout: 30         # Miner write timeout (seconds)

    # Upstream settings
    upstream_idle_timeout: 300      # Seconds without upstream messages before reconnecting (5 min)
    server_switch_timeout: 900      # Seconds to retry connecting to new server during switch (15 min)

    # TCP keepalive settings
    tcp_keepalive: true             # Enable TCP keepalive on connections
    keepalive_idle: 60              # Seconds before sending keepalive probes
    keepalive_interval: 10          # Seconds between keepalive probes
    keepalive_count: 3              # Failed probes before connection is dead

    # Retry and shutdown settings
    share_submit_retries: 3         # Retries for failed share submissions
    pending_shares_timeout: 10      # Timeout waiting for pending shares (seconds)

  # Share validation settings
  validation:
    reject_duplicates: true         # Reject duplicate share submissions
    reject_stale: true              # Reject shares for stale/unknown jobs
    share_cache_size: 1000          # Max recent shares to track per session
    share_cache_ttl: 300            # Share cache TTL in seconds
    job_cache_size: 10              # Max jobs to track per session

  # Difficulty management settings
  difficulty:
    change_cooldown: 30             # Minimum seconds between difficulty changes
    # Time-based decay toward pool difficulty
    decay_enabled: true             # Enable gradual decay toward pool difficulty
    decay_interval: 300             # Seconds between decay checks (5 min)
    decay_percent: 0.05             # Decay percentage per interval (5%)
    # Floor-reset when pool difficulty drops significantly
    floor_trigger_ratio: 0.50       # Trigger floor-reset when pool < 50% of miner difficulty
    floor_reset_ratio: 0.75         # Reset to 75% of current miner difficulty
    # Share rate monitoring - detects and adjusts for difficulty issues
    share_rate_window: 300          # Window in seconds to track share rate
    share_rate_min_shares: 3        # Minimum shares needed before analyzing rate
    share_rate_low_multiplier: 0.5  # Lower difficulty if rate drops to this fraction of baseline
    # Adaptive buffer - learns optimal buffer from rejection patterns
    buffer_start: 0.05              # Starting buffer percentage (5%)
    buffer_min: 0.02                # Minimum buffer percentage (2%)
    buffer_max: 0.20                # Maximum buffer percentage (20%)
    buffer_increase_step: 0.01      # Buffer increase step on rejection (1%)
    buffer_decrease_interval: 3600  # Seconds without rejection before decreasing buffer

  # Circuit breaker - stop trying failed pools temporarily
  circuit_breaker:
    enabled: true                   # Enable circuit breaker pattern
    failure_threshold: 5            # Failures before opening circuit
    recovery_timeout: 60            # Seconds to wait before retry
    success_threshold: 2            # Successes needed to close circuit

  # SHA-256 proxy (Bitcoin, Bitcoin Cash, DigiByte)
  sha256:
    enabled: true
    bind_host: "0.0.0.0"
    bind_port: 3333

  # RandomX proxy (Monero)
  randomx:
    enabled: false
    bind_host: "0.0.0.0"
    bind_port: 3334

  # zkSNARK proxy (ALEO)
  zksnark:
    enabled: false
    bind_host: "0.0.0.0"
    bind_port: 3335

# Servers organized by algorithm (each server includes its schedule times)
servers:
  sha256:
    # stratum1 for morning/night hours (00:00 to 11:59)
    - name: "stratum1"
      enabled: true
      host: "pool1.example.com"
      port: 3333
      username: "wallet_address.worker1"
      password: "x"
      ssl: false
      connect_timeout: 30
      retry_interval: 5
      start: "00:00"                # Schedule start time (HH:MM)
      end: "12:00"                  # Schedule end time (HH:MM, use 24:00 for midnight)
      # Per-server difficulty overrides (optional)
      # buffer_percent: 0.10        # Override global buffer (e.g., 10% for this pool)
      # min_difficulty: 1000        # Minimum difficulty for this pool
      # max_difficulty: 1000000     # Maximum difficulty for this pool

    # stratum2 for afternoon/evening hours (12:00 to 23:59)
    - name: "stratum2"
      enabled: true
      host: "pool2.example.com"
      port: 3333
      username: "wallet_address.worker2"
      password: "x"
      ssl: false
      connect_timeout: 30
      retry_interval: 5
      start: "12:00"
      end: "24:00"

  # RandomX servers (Monero) - enable proxy.randomx to use
  # randomx:
  #   # xmr1 for morning/night hours (00:00 to 11:59)
  #   - name: "xmr1"
  #     enabled: true
  #     host: "xmr-pool1.example.com"
  #     port: 3333
  #     username: "wallet_address.worker1"
  #     password: "x"
  #     ssl: false
  #     connect_timeout: 30
  #     retry_interval: 5
  #     start: "00:00"
  #     end: "12:00"
  #
  #   # xmr2 for afternoon/evening hours (12:00 to 23:59)
  #   - name: "xmr2"
  #     enabled: true
  #     host: "xmr-pool2.example.com"
  #     port: 3333
  #     username: "wallet_address.worker2"
  #     password: "x"
  #     ssl: false
  #     connect_timeout: 30
  #     retry_interval: 5
  #     start: "12:00"
  #     end: "24:00"

  # zkSNARK servers (ALEO) - enable proxy.zksnark to use
  # zksnark:
  #   # zksnark1 for morning/night hours (00:00 to 11:59)
  #   - name: "zksnark1"
  #     enabled: true
  #     host: "aleo-pool1.example.com"
  #     port: 3335
  #     username: "wallet_address.worker1"
  #     password: "x"
  #     ssl: false
  #     connect_timeout: 30
  #     retry_interval: 5
  #     start: "00:00"
  #     end: "12:00"
  #
  #   # zksnark2 for afternoon/evening hours (12:00 to 23:59)
  #   - name: "zksnark2"
  #     enabled: true
  #     host: "aleo-pool2.example.com"
  #     port: 3335
  #     username: "wallet_address.worker2"
  #     password: "x"
  #     ssl: false
  #     connect_timeout: 30
  #     retry_interval: 5
  #     start: "12:00"
  #     end: "24:00"

logging:
  level: "INFO"                   # DEBUG, INFO, WARNING, ERROR, CRITICAL
  file: null                      # Log file path (null for console only)
  rotation: "50 MB"               # Log rotation size
  retention: 10                   # Keep N rotated files
  format: "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}"

# Difficulty management features:
# - Buffer: Adds configurable % to pool difficulty (prevents vardiff duplicates)
# - Highest-seen tracking: Difficulty only increases normally (never decreases)
# - Cooldown: Minimum time between difficulty changes (prevents oscillation)
# - Time-based decay: Gradually lowers toward pool difficulty when above target
# - Floor-reset: Resets when pool drops significantly below miner difficulty
# - Low-diff rejection: Raises to rejected difficulty + buffer if higher
# - Share rate monitoring: Detect if difficulty is too high (low share rate) or too low
# - Adaptive buffer: Learn optimal buffer from observed rejection patterns
# - Per-server overrides: Configure different buffer/min/max per pool
#
# Connection resilience features:
# - Circuit breaker: Temporarily stop trying failed pools to reduce log spam
"""

    dest_path = Path("config.yaml")
    if dest_path.exists():
        if not click.confirm(f"{dest_path} already exists. Overwrite?"):
            return

    dest_path.write_text(config_content)
    click.echo(f"Created {dest_path}")
    click.echo("Edit this file to configure your stratum servers and schedule.")


if __name__ == "__main__":
    main()
