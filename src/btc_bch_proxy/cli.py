"""Command-line interface for the stratum proxy."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import click
from loguru import logger

from btc_bch_proxy import __version__


def find_config_file() -> Optional[Path]:
    """
    Find the configuration file in common locations.

    Returns:
        Path to config file or None.
    """
    search_paths = [
        Path("config.yaml"),
        Path("config.yml"),
        Path.home() / ".config" / "btc-bch-proxy" / "config.yaml",
        Path("/etc/btc-bch-proxy/config.yaml"),
    ]

    if sys.platform == "win32":
        search_paths.append(
            Path.home() / "AppData" / "Local" / "btc-bch-proxy" / "config.yaml"
        )

    for path in search_paths:
        if path.exists():
            return path

    return None


@click.group()
@click.version_option(version=__version__, prog_name="btc-bch-proxy")
def main():
    """Bitcoin/Bitcoin Cash Stratum Proxy with time-based server routing."""
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
    from btc_bch_proxy.config.loader import ConfigError, load_config
    from btc_bch_proxy.daemon import DaemonError, DaemonManager
    from btc_bch_proxy.logging.setup import setup_logging

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
    from btc_bch_proxy.config.loader import ConfigError, load_config
    from btc_bch_proxy.daemon import DaemonManager

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
    from btc_bch_proxy.config.loader import ConfigError, load_config
    from btc_bch_proxy.daemon import DaemonManager

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
    from btc_bch_proxy.config.loader import validate_config

    is_valid, message = validate_config(config_path)

    if is_valid:
        click.echo(f"✓ {message}")

        # Show additional info
        from btc_bch_proxy.config.loader import load_config

        config = load_config(config_path)

        click.echo("\nServers:")
        for server in config.servers:
            click.echo(f"  - {server.name}: {server.host}:{server.port}")

        click.echo("\nSchedule:")
        for frame in config.schedule:
            click.echo(f"  - {frame.start} - {frame.end}: {frame.server}")

        click.echo(f"\nProxy: {config.proxy.bind_host}:{config.proxy.bind_port}")
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
    config_content = """# Bitcoin/Bitcoin Cash Stratum Proxy Configuration

proxy:
  bind_host: "0.0.0.0"           # Listen on all interfaces
  bind_port: 3333                 # Port for miners to connect
  max_connections: 100            # Maximum concurrent miner connections
  connection_timeout: 60          # Miner connection timeout (seconds)
  miner_read_timeout: 600         # Miner read timeout in seconds (10 minutes)
  send_timeout: 30                # Send to miner timeout (seconds)
  pending_shares_timeout: 10      # Timeout waiting for pending shares (seconds)
  tcp_keepalive: true             # Enable TCP keepalive on connections
  keepalive_idle: 60              # Seconds before sending keepalive probes
  keepalive_interval: 10          # Seconds between keepalive probes
  keepalive_count: 3              # Failed probes before connection is dead
  share_submit_retries: 3         # Retries for failed share submissions
  upstream_health_timeout: 300    # Seconds without upstream messages before reconnecting

servers:
  - name: "stratum1"
    host: "pool1.example.com"
    port: 3333
    username: "wallet_address.worker1"
    password: "x"
    ssl: false
    timeout: 30
    retry_interval: 5

  - name: "stratum2"
    host: "pool2.example.com"
    port: 3333
    username: "wallet_address.worker2"
    password: "x"
    ssl: false
    timeout: 30
    retry_interval: 5

schedule:
  # stratum1 for morning/night hours (00:00 to 11:59)
  - start: "00:00"
    end: "12:00"
    server: "stratum1"

  # stratum2 for afternoon/evening hours (12:00 to 23:59)
  - start: "12:00"
    end: "24:00"
    server: "stratum2"

logging:
  level: "INFO"                   # DEBUG, INFO, WARNING, ERROR
  file: null                      # Log file path (null for console only)
  rotation: "50 MB"               # Log rotation size
  retention: 10                   # Keep N rotated files
  format: "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}"

# Failover settings
failover:
  retry_timeout_minutes: 20       # Retry primary server for this long before failover

# Share validation settings
validation:
  reject_duplicates: true         # Reject duplicate share submissions
  reject_stale: true              # Reject shares for stale/unknown jobs
  validate_difficulty: false      # Validate share hash meets difficulty (CPU intensive)
  share_cache_size: 1000          # Max recent shares to track per session
  share_cache_ttl: 300            # Share cache TTL in seconds
  job_cache_size: 10              # Max jobs to track per session

# Per-worker settings (optional)
# Workers are identified by the username the miner uses to connect to the proxy.
# Difficulty override is only applied if it is > the pool's difficulty.
# workers:
#   - username: "miner1"          # Username the miner uses to connect
#     difficulty: 50000000         # Preferred difficulty for this worker
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
