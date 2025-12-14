"""Cross-platform daemon management for Windows and Linux."""

from __future__ import annotations

import asyncio
import os
import signal
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from loguru import logger

if TYPE_CHECKING:
    from btc_bch_proxy.config.models import Config


class DaemonError(Exception):
    """Daemon management error."""

    pass


class PidFile:
    """PID file management."""

    def __init__(self, path: str):
        """
        Initialize PID file manager.

        Args:
            path: Path to the PID file.
        """
        self.path = Path(path)

    def write(self, pid: Optional[int] = None) -> None:
        """
        Write the current process ID to the PID file.

        Args:
            pid: Process ID to write (defaults to current process).
        """
        pid = pid or os.getpid()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(str(pid))
        logger.debug(f"Wrote PID {pid} to {self.path}")

    def read(self) -> Optional[int]:
        """
        Read the PID from the file.

        Returns:
            Process ID or None if file doesn't exist.
        """
        if not self.path.exists():
            return None
        try:
            return int(self.path.read_text().strip())
        except (ValueError, OSError):
            return None

    def remove(self) -> None:
        """Remove the PID file."""
        if self.path.exists():
            self.path.unlink()
            logger.debug(f"Removed PID file {self.path}")

    def is_running(self) -> bool:
        """
        Check if the process in the PID file is running.

        Returns:
            True if process is running.
        """
        pid = self.read()
        if pid is None:
            return False

        try:
            if sys.platform == "win32":
                # Windows: try to open the process
                import ctypes

                kernel32 = ctypes.windll.kernel32
                PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
                handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
                if handle:
                    kernel32.CloseHandle(handle)
                    return True
                return False
            else:
                # Unix: send signal 0 to check if process exists
                os.kill(pid, 0)
                return True
        except (OSError, PermissionError):
            return False


class DaemonManager:
    """
    Cross-platform daemon/background process management.

    Handles:
    - Running as a background process
    - PID file management
    - Signal handling for graceful shutdown
    - Process status checking
    """

    def __init__(self, config: Config, pid_file_path: Optional[str] = None):
        """
        Initialize the daemon manager.

        Args:
            config: Application configuration.
            pid_file_path: Path to PID file (default: from config or platform default).
        """
        self.config = config

        # Determine PID file path
        if pid_file_path:
            self._pid_path = pid_file_path
        elif sys.platform == "win32":
            self._pid_path = os.path.join(
                os.environ.get("LOCALAPPDATA", "C:\\ProgramData"),
                "btc-bch-proxy",
                "proxy.pid",
            )
        else:
            self._pid_path = "/var/run/btc-bch-proxy/proxy.pid"

        self._pid_file = PidFile(self._pid_path)
        self._stop_event = asyncio.Event()

    @property
    def pid_file_path(self) -> str:
        """Get the PID file path."""
        return self._pid_path

    def is_running(self) -> bool:
        """Check if the daemon is currently running."""
        return self._pid_file.is_running()

    def get_pid(self) -> Optional[int]:
        """Get the PID of the running daemon."""
        if self.is_running():
            return self._pid_file.read()
        return None

    def run_foreground(self) -> None:
        """Run the proxy in the foreground (blocking)."""
        logger.info("Running in foreground mode")

        # Write PID file
        self._pid_file.write()

        try:
            # Setup signal handlers
            self._setup_signals()

            # Run the proxy
            asyncio.run(self._run_main_loop())
        finally:
            self._pid_file.remove()

    def run_background(self) -> int:
        """
        Run the proxy as a background process.

        Returns:
            PID of the background process.
        """
        if self.is_running():
            raise DaemonError(
                f"Daemon already running with PID {self._pid_file.read()}"
            )

        if sys.platform == "win32":
            return self._run_background_windows()
        else:
            return self._run_background_unix()

    def _run_background_windows(self) -> int:
        """Start background process on Windows."""
        import subprocess

        # Create startup info to hide console window
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = subprocess.SW_HIDE

        # Build command to run the proxy
        python = sys.executable
        args = [
            python,
            "-m",
            "btc_bch_proxy",
            "start",
            "--foreground",
        ]

        # Add config file if specified
        # Note: Config path should be passed when calling this

        # Start detached process
        process = subprocess.Popen(
            args,
            startupinfo=startupinfo,
            creationflags=subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
        )

        logger.info(f"Started background process with PID {process.pid}")
        return process.pid

    def _run_background_unix(self) -> int:
        """Start background process on Unix (double-fork)."""
        # First fork
        pid = os.fork()
        if pid > 0:
            # Parent - wait briefly for child to start
            return pid

        # Child - become session leader
        os.setsid()

        # Second fork to prevent acquiring a controlling terminal
        pid = os.fork()
        if pid > 0:
            # First child exits
            os._exit(0)

        # Grandchild - this is the daemon process

        # Change working directory
        os.chdir("/")

        # Close standard file descriptors
        sys.stdin.close()
        sys.stdout.close()
        sys.stderr.close()

        # Redirect to /dev/null
        devnull = os.open(os.devnull, os.O_RDWR)
        os.dup2(devnull, 0)
        os.dup2(devnull, 1)
        os.dup2(devnull, 2)

        # Write PID file
        self._pid_file.write()

        try:
            # Setup signal handlers
            self._setup_signals()

            # Run the proxy
            asyncio.run(self._run_main_loop())
        finally:
            self._pid_file.remove()

        os._exit(0)

    def stop(self) -> bool:
        """
        Stop the running daemon.

        Returns:
            True if daemon was stopped successfully.
        """
        pid = self._pid_file.read()
        if pid is None:
            logger.warning("No PID file found")
            return False

        if not self._pid_file.is_running():
            logger.warning(f"Process {pid} is not running")
            self._pid_file.remove()
            return False

        logger.info(f"Stopping daemon with PID {pid}")

        try:
            if sys.platform == "win32":
                # Windows: use terminate
                import ctypes

                kernel32 = ctypes.windll.kernel32
                PROCESS_TERMINATE = 0x0001
                handle = kernel32.OpenProcess(PROCESS_TERMINATE, False, pid)
                if handle:
                    kernel32.TerminateProcess(handle, 0)
                    kernel32.CloseHandle(handle)
            else:
                # Unix: send SIGTERM
                os.kill(pid, signal.SIGTERM)

            # Wait for process to exit
            import time

            for _ in range(50):  # Wait up to 5 seconds
                if not self._pid_file.is_running():
                    self._pid_file.remove()
                    logger.info("Daemon stopped successfully")
                    return True
                time.sleep(0.1)

            # Force kill if still running
            logger.warning("Daemon did not stop gracefully, forcing...")
            if sys.platform != "win32":
                os.kill(pid, signal.SIGKILL)

            self._pid_file.remove()
            return True

        except (OSError, PermissionError) as e:
            logger.error(f"Failed to stop daemon: {e}")
            return False

    def _setup_signals(self) -> None:
        """Setup signal handlers for graceful shutdown."""
        if sys.platform != "win32":
            # Unix signal handlers
            loop = asyncio.get_event_loop()

            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(
                    sig,
                    lambda: asyncio.create_task(self._handle_signal()),
                )
        else:
            # Windows: handle CTRL+C
            signal.signal(signal.SIGINT, lambda s, f: asyncio.create_task(self._handle_signal()))
            signal.signal(signal.SIGTERM, lambda s, f: asyncio.create_task(self._handle_signal()))

    async def _handle_signal(self) -> None:
        """Handle shutdown signal."""
        logger.info("Received shutdown signal")
        self._stop_event.set()

    async def _run_main_loop(self) -> None:
        """Main application loop."""
        from btc_bch_proxy.logging.setup import setup_logging
        from btc_bch_proxy.proxy.server import run_proxy

        # Setup logging
        setup_logging(self.config.logging)

        logger.info("Starting BTC/BCH Stratum Proxy")
        logger.info(f"Configured servers: {', '.join(self.config.get_server_names())}")

        # Run the proxy
        await run_proxy(self.config, self._stop_event)

        logger.info("Proxy shutdown complete")
