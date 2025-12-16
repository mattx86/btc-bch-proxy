"""Cross-platform daemon management for Windows and Linux."""

from __future__ import annotations

import asyncio
import os
import signal
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

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

        Uses atomic write (temp file + rename) to prevent race conditions.

        Args:
            pid: Process ID to write (defaults to current process).
        """
        import tempfile

        pid = pid or os.getpid()
        self.path.parent.mkdir(parents=True, exist_ok=True)

        # Write to a temp file first, then atomically rename
        # This prevents race conditions where another process reads a partial file
        fd, temp_path = tempfile.mkstemp(
            dir=self.path.parent,
            prefix=".pid_",
            suffix=".tmp",
        )
        try:
            os.write(fd, str(pid).encode())
            os.close(fd)
            fd = -1  # Mark as closed

            # On Windows, we need to remove the target first if it exists
            if sys.platform == "win32" and self.path.exists():
                self.path.unlink()

            Path(temp_path).rename(self.path)
            logger.debug(f"Wrote PID {pid} to {self.path}")
        finally:
            if fd >= 0:
                try:
                    os.close(fd)
                except OSError:
                    pass  # Ignore close errors in cleanup
            # Clean up temp file if rename failed (may not exist if rename succeeded)
            try:
                Path(temp_path).unlink()
            except FileNotFoundError:
                pass  # Already cleaned up (rename succeeded)
            except OSError as cleanup_err:
                # Log at debug level - this is non-critical cleanup
                logger.debug(f"Failed to clean up temp PID file {temp_path}: {cleanup_err}")

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

    @staticmethod
    def _is_process_running(pid: int) -> bool:
        """
        Check if a specific PID is running.

        Args:
            pid: Process ID to check.

        Returns:
            True if process is running.
        """
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

    def is_running(self) -> bool:
        """
        Check if the process in the PID file is running.

        Returns:
            True if process is running.
        """
        pid = self.read()
        if pid is None:
            return False
        return self._is_process_running(pid)


class DaemonManager:
    """
    Cross-platform daemon/background process management.

    Handles:
    - Running as a background process
    - PID file management
    - Signal handling for graceful shutdown
    - Process status checking
    """

    def __init__(
        self,
        config: Config,
        pid_file_path: Optional[str] = None,
        config_path: Optional[str] = None,
    ):
        """
        Initialize the daemon manager.

        Args:
            config: Application configuration.
            pid_file_path: Path to PID file (default: from config or platform default).
            config_path: Path to configuration file (used for background process).
        """
        self.config = config
        self._config_path = config_path

        # Determine PID file path
        if pid_file_path:
            self._pid_path = self._validate_pid_path(pid_file_path)
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

    def _validate_pid_path(self, path: str) -> str:
        """
        Validate and normalize the PID file path.

        Args:
            path: The PID file path to validate.

        Returns:
            Normalized absolute path.

        Raises:
            DaemonError: If the path is invalid.
        """
        # Convert to Path and resolve to absolute
        pid_path = Path(path).resolve()

        # Ensure it's a file path, not a directory
        if pid_path.is_dir():
            raise DaemonError(f"PID file path is a directory: {pid_path}")

        # Ensure the filename is reasonable
        if not pid_path.name or pid_path.name.startswith("."):
            raise DaemonError(f"Invalid PID file name: {pid_path.name}")

        # Ensure the filename ends with .pid for clarity
        if not pid_path.suffix == ".pid":
            logger.warning(f"PID file path does not end with .pid: {pid_path}")

        return str(pid_path)

    @property
    def pid_file_path(self) -> str:
        """Get the PID file path."""
        return self._pid_path

    def is_running(self) -> bool:
        """Check if the daemon is currently running."""
        return self._pid_file.is_running()

    def get_pid(self) -> Optional[int]:
        """
        Get the PID of the running daemon.

        Note: Reads PID once then checks if that specific PID is running
        to avoid TOCTOU race and redundant file reads.
        """
        pid = self._pid_file.read()
        if pid is None:
            return None
        # Verify the process is actually running (use static method to avoid re-reading)
        if PidFile._is_process_running(pid):
            return pid
        return None

    def run_foreground(self) -> None:
        """Run the proxy in the foreground (blocking)."""
        logger.info("Running in foreground mode")

        # Write PID file
        self._pid_file.write()

        try:
            # Run the proxy (signal handlers are set up inside the async loop)
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
        if self._config_path:
            args.extend(["--config", str(self._config_path)])

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
        """
        Start background process on Unix using double-fork daemonization.

        Platform: Unix/Linux/macOS only. Uses os.fork() and Unix-specific paths.

        The double-fork technique:
        1. First fork: Parent returns, child continues
        2. setsid(): Child becomes session leader (detaches from terminal)
        3. Second fork: Prevents reacquiring a controlling terminal
        4. chdir("/"): Avoid holding directory mounts open
        5. Redirect stdin/stdout/stderr to /dev/null
        """
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

        # Close standard file descriptors (may already be closed in some environments)
        for stream in (sys.stdin, sys.stdout, sys.stderr):
            try:
                stream.close()
            except Exception:
                pass  # Already closed or inaccessible

        # Redirect to /dev/null
        try:
            devnull = os.open(os.devnull, os.O_RDWR)
            os.dup2(devnull, 0)
            os.dup2(devnull, 1)
            os.dup2(devnull, 2)
        except OSError:
            pass  # devnull redirection failed, continue anyway

        # Write PID file
        self._pid_file.write()

        try:
            # Run the proxy (signal handlers are set up inside the async loop)
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
            if sys.platform == "win32":
                # Windows: use TerminateProcess for force kill
                import ctypes
                kernel32 = ctypes.windll.kernel32
                PROCESS_TERMINATE = 0x0001
                handle = kernel32.OpenProcess(PROCESS_TERMINATE, False, pid)
                if handle:
                    kernel32.TerminateProcess(handle, 1)
                    kernel32.CloseHandle(handle)
            else:
                os.kill(pid, signal.SIGKILL)

            self._pid_file.remove()
            return True

        except (OSError, PermissionError) as e:
            logger.error(f"Failed to stop daemon: {e}")
            return False

    def _setup_signals(self) -> None:
        """Setup signal handlers for graceful shutdown."""
        if sys.platform != "win32":
            # Unix signal handlers - use get_running_loop() since we're in async context
            loop = asyncio.get_running_loop()

            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(
                    sig,
                    self._stop_event.set,
                )
        else:
            # Windows: use synchronous signal handlers that set the event
            # Note: Windows doesn't support add_signal_handler, so we use signal.signal
            # The handler must be synchronous, so we set the event via call_soon_threadsafe
            loop = asyncio.get_running_loop()

            # Track if signal was received (for the watcher task)
            self._signal_received = False

            def sync_signal_handler(signum: int, frame: Any) -> None:
                # Mark signal as received for the watcher task
                self._signal_received = True
                # Also try to set the event via call_soon_threadsafe
                try:
                    loop.call_soon_threadsafe(self._stop_event.set)
                except RuntimeError:
                    # Loop may be closed, try direct set as fallback
                    self._stop_event.set()

            signal.signal(signal.SIGINT, sync_signal_handler)
            signal.signal(signal.SIGTERM, sync_signal_handler)

            # Start a watcher task that periodically checks for signals
            # This ensures the event loop yields frequently enough for signal handling
            asyncio.create_task(self._windows_signal_watcher())

    async def _windows_signal_watcher(self) -> None:
        """
        Periodically check for signals on Windows.

        On Windows, signal handlers only run when Python executes bytecode.
        During I/O waits (like serve_forever()), signals may not be processed.
        This task yields periodically to ensure signal handlers can run.
        """
        while not self._stop_event.is_set():
            # Check if signal was received by the sync handler
            if getattr(self, '_signal_received', False):
                logger.info("Received shutdown signal (Ctrl+C)")
                self._stop_event.set()
                break
            # Yield briefly to allow signal processing
            await asyncio.sleep(0.1)

    async def _handle_signal(self) -> None:
        """Handle shutdown signal."""
        logger.info("Received shutdown signal")
        self._stop_event.set()

    async def _run_main_loop(self) -> None:
        """Main application loop."""
        from btc_bch_proxy.logging.setup import setup_logging
        from btc_bch_proxy.proxy.server import run_proxy

        # Setup signal handlers (must be done inside async context)
        self._setup_signals()

        # Setup logging
        setup_logging(self.config.logging)

        logger.info("Starting BTC/BCH Stratum Proxy")
        logger.info(f"Configured servers: {', '.join(self.config.get_server_names())}")

        # Run the proxy
        await run_proxy(self.config, self._stop_event)

        logger.info("Proxy shutdown complete")
