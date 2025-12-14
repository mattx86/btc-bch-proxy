"""Time-based routing logic for stratum server selection."""

from __future__ import annotations

import asyncio
from datetime import datetime, time, timedelta
from typing import TYPE_CHECKING, Callable, List, Optional

from loguru import logger

if TYPE_CHECKING:
    from btc_bch_proxy.config.models import Config, StratumServerConfig, TimeFrame


class TimeBasedRouter:
    """
    Determines which upstream stratum server to use based on the current time.

    Handles:
    - Time-based server selection from schedule
    - Calculation of next switch time
    - Callbacks for server switches
    - Failover to alternate servers when primary is unavailable
    """

    def __init__(self, config: Config):
        """
        Initialize the router.

        Args:
            config: Main configuration object.
        """
        self.config = config
        self._switch_callbacks: List[Callable[[str], asyncio.Future]] = []
        self._current_server: Optional[str] = None
        self._failover_active: bool = False
        self._failover_server: Optional[str] = None

    def get_current_server(self) -> str:
        """
        Get the server name that should be used for the current time.

        Returns:
            Name of the server to use.

        Raises:
            RuntimeError: If no server is configured for the current time.
        """
        # If failover is active, use the failover server
        if self._failover_active and self._failover_server:
            return self._failover_server

        now = datetime.now().time()
        for frame in self.config.schedule:
            if frame.contains(now):
                return frame.server

        raise RuntimeError(
            f"No server configured for current time {now}. "
            f"Check your schedule configuration."
        )

    def get_scheduled_server(self) -> str:
        """
        Get the scheduled server (ignoring failover state).

        Returns:
            Name of the scheduled server.
        """
        now = datetime.now().time()
        for frame in self.config.schedule:
            if frame.contains(now):
                return frame.server
        raise RuntimeError(f"No server configured for current time {now}")

    def get_current_timeframe(self) -> Optional[TimeFrame]:
        """
        Get the current active timeframe.

        Returns:
            Current TimeFrame or None.
        """
        now = datetime.now().time()
        for frame in self.config.schedule:
            if frame.contains(now):
                return frame
        return None

    def get_next_switch_time(self) -> datetime:
        """
        Calculate when the next server switch will occur.

        Returns:
            Datetime of the next scheduled switch.
        """
        now = datetime.now()
        current_time = now.time()

        # Find the current timeframe
        current_frame = self.get_current_timeframe()
        if not current_frame:
            # Shouldn't happen if config is valid
            return now + timedelta(hours=1)

        # The next switch is at the end of the current timeframe
        end_time = current_frame.end

        # Calculate datetime for the end time
        if end_time == time(23, 59, 59):
            # End of day - next switch is at midnight
            next_switch = datetime.combine(now.date(), time(0, 0, 0)) + timedelta(days=1)
        elif end_time > current_time:
            # End time is later today
            next_switch = datetime.combine(now.date(), end_time)
        else:
            # End time is tomorrow (midnight crossover)
            next_switch = datetime.combine(now.date() + timedelta(days=1), end_time)

        return next_switch

    def get_next_server(self) -> str:
        """
        Get the server that will be active after the next switch.

        Returns:
            Name of the next scheduled server.
        """
        next_time = self.get_next_switch_time() + timedelta(seconds=1)
        test_time = next_time.time()

        for frame in self.config.schedule:
            if frame.contains(test_time):
                return frame.server

        # Fallback to first server
        return self.config.schedule[0].server

    def get_server_config(self, server_name: str) -> Optional[StratumServerConfig]:
        """
        Get the configuration for a server by name.

        Args:
            server_name: Name of the server.

        Returns:
            Server configuration or None.
        """
        return self.config.get_server_by_name(server_name)

    def get_failover_server(self, current_server: str) -> Optional[str]:
        """
        Get a failover server when the current one is unavailable.

        Returns the next server in the schedule after the current one.

        Args:
            current_server: Name of the currently failing server.

        Returns:
            Name of a failover server or None if none available.
        """
        server_names = self.config.get_server_names()

        # Find the index of the current server
        try:
            current_idx = server_names.index(current_server)
        except ValueError:
            # Unknown server, use first server
            return server_names[0] if server_names else None

        # Try the next server in the list (wrap around)
        next_idx = (current_idx + 1) % len(server_names)

        # Don't return the same server
        if server_names[next_idx] == current_server:
            return None

        return server_names[next_idx]

    def activate_failover(self, failover_server: str) -> None:
        """
        Activate failover mode to use an alternate server.

        Args:
            failover_server: Name of the server to fail over to.
        """
        logger.warning(f"Activating failover to server: {failover_server}")
        self._failover_active = True
        self._failover_server = failover_server

    def deactivate_failover(self) -> None:
        """Deactivate failover mode and return to scheduled server."""
        if self._failover_active:
            logger.info("Deactivating failover, returning to scheduled server")
            self._failover_active = False
            self._failover_server = None

    @property
    def is_failover_active(self) -> bool:
        """Check if failover mode is currently active."""
        return self._failover_active

    def register_switch_callback(
        self, callback: Callable[[str], asyncio.Future]
    ) -> None:
        """
        Register a callback to be called when servers switch.

        Args:
            callback: Async function that takes the new server name.
        """
        self._switch_callbacks.append(callback)

    def unregister_switch_callback(
        self, callback: Callable[[str], asyncio.Future]
    ) -> None:
        """
        Unregister a switch callback.

        Args:
            callback: Callback to remove.
        """
        if callback in self._switch_callbacks:
            self._switch_callbacks.remove(callback)

    async def notify_switch(self, new_server: str) -> None:
        """
        Notify all registered callbacks of a server switch.

        Args:
            new_server: Name of the new server.
        """
        logger.info(f"Notifying {len(self._switch_callbacks)} callbacks of switch to {new_server}")
        for callback in self._switch_callbacks:
            try:
                await callback(new_server)
            except Exception as e:
                logger.error(f"Error in switch callback: {e}")

    async def run_scheduler(self, stop_event: asyncio.Event) -> None:
        """
        Background task to monitor time and trigger server switches.

        Args:
            stop_event: Event to signal shutdown.
        """
        logger.info("Starting time-based scheduler")

        while not stop_event.is_set():
            try:
                # Get current and next server
                scheduled_server = self.get_scheduled_server()

                # Check if we need to deactivate failover (scheduled server changed)
                if self._failover_active and self._current_server != scheduled_server:
                    self.deactivate_failover()

                current_server = self.get_current_server()

                # Check if server changed
                if self._current_server != current_server:
                    logger.info(f"Server switch: {self._current_server} -> {current_server}")
                    self._current_server = current_server
                    await self.notify_switch(current_server)

                # Calculate sleep time until next switch
                next_switch = self.get_next_switch_time()
                sleep_seconds = (next_switch - datetime.now()).total_seconds()

                # Cap sleep time to check periodically (every 60 seconds max)
                sleep_seconds = min(max(sleep_seconds, 1), 60)

                logger.debug(
                    f"Next switch at {next_switch}, sleeping {sleep_seconds:.1f}s"
                )

                # Wait with cancellation support
                try:
                    await asyncio.wait_for(
                        stop_event.wait(),
                        timeout=sleep_seconds
                    )
                    break  # Stop event was set
                except asyncio.TimeoutError:
                    pass  # Normal timeout, continue loop

            except Exception as e:
                logger.error(f"Scheduler error: {e}")
                await asyncio.sleep(5)

        logger.info("Time-based scheduler stopped")
