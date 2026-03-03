import logging
import queue
import threading
from typing import Optional

import ray
from ray.train.v2._internal.constants import GET_ACTOR_TIMEOUT_S
from ray.train.v2._internal.state.util import is_actor_alive
from ray.util.placement_group import PlacementGroup, remove_placement_group

logger = logging.getLogger(__name__)


class PlacementGroupCleaner:
    """Detached helper that ensures PG cleanup if Ray Train Controller exits ungracefully.

    This actor should be created with lifetime='detached' to avoid being
    fate-shared with the Train controller.
    """

    def __init__(self, check_interval_s: float = 1.0):
        self._check_interval_s = check_interval_s
        self._pg_queue: queue.Queue = queue.Queue()
        self._stop_event = threading.Event()
        self._controller_actor_id: Optional[str] = None
        self._monitor_thread: Optional[threading.Thread] = None
        self._get_actor_timeout_s = GET_ACTOR_TIMEOUT_S
        self._exiting: bool = False

    def register_controller_and_placement_group(
        self, controller_actor_id: str, placement_group: PlacementGroup
    ):
        self._controller_actor_id = controller_actor_id
        logger.debug(
            "PlacementGroupCleaner registered controller %s with placement group %s",
            controller_actor_id,
            placement_group.id,
        )
        # Send placement group update to the monitor thread via queue
        self._pg_queue.put(placement_group)

    def start_monitoring(self):
        """Start monitoring the controller and placement group."""
        if self._monitor_thread is not None and self._monitor_thread.is_alive():
            # Thread already running, just return True
            logger.debug("Monitor thread already running")
            return True

        self._monitor_thread = threading.Thread(
            target=self._monitor_loop,
            name="PlacementGroupCleanerMonitor",
            daemon=True,
        )
        self._monitor_thread.start()
        logger.debug("PlacementGroupCleaner started monitoring in background thread")
        return True

    def _monitor_loop(self):
        """Monitor controller; remove PG when controller is gone.

        This runs continuously until controller dies or stop() is called.
        Uses a queue to receive placement group updates.
        """
        curr_placement_group: Optional[PlacementGroup] = None
        while not self._stop_event.is_set():
            # Check for new placement group updates from queue
            try:
                pg = self._pg_queue.get(timeout=self._check_interval_s)
                curr_placement_group = pg
                logger.debug(f"Updated current placement group to {pg.id}")
            except queue.Empty:
                pass  # continue to monitor current placement group

            # Skip monitoring if no placement group registered
            if not curr_placement_group:
                continue

            # Check if controller is still alive
            try:
                alive = is_actor_alive(
                    actor_id=self._controller_actor_id,
                    timeout=self._get_actor_timeout_s,
                )
            except ray.util.state.exception.RayStateApiException:
                logger.warning(
                    "Failed to query Ray Train Controller actor state. "
                    "State API may be temporarily unavailable. Continuing to monitor."
                )
                continue

            # Cleanup if controller is dead
            if not alive:
                self._cleanup_placement_group(curr_placement_group)
                break

        # Exit the actor after cleanup since controller is dead
        self._exit()
        self._monitor_thread = None

    def _cleanup_placement_group(self, placement_group: PlacementGroup):
        """Clean up the current placement group if it hasn't been removed."""
        if self._is_placement_group_removed(placement_group):
            logger.debug(
                "Controller actor died but placement group already removed; "
                "skipping cleanup."
            )
            return

        logger.warning(
            f"Detected that the Ray Train controller actor ({self._controller_actor_id}) is dead. "
            f"Cleaning up placement group = [{placement_group.id}] created by this run."
        )
        try:
            remove_placement_group(placement_group)
        except Exception as e:
            logger.warning(f"Failed to clean up placement group: {e}")
            return

        logger.debug(
            f"Placement group = [{placement_group.id}] cleaned up successfully"
        )

    def _stop_monitor_thread(self):
        """Stop the monitor thread and wait for it to exit.

        Returns:
            bool: True if the thread was stopped, False if there was no active thread.
        """
        if self._monitor_thread is None or not self._monitor_thread.is_alive():
            return False

        # Signal stop and wait for thread to exit
        self._stop_event.set()
        join_timeout = max(2.0, self._check_interval_s * 2)
        self._monitor_thread.join(timeout=join_timeout)
        if self._monitor_thread.is_alive():
            logger.warning(
                "Monitor thread did not exit within %.2f seconds", join_timeout
            )
            return False

        self._monitor_thread = None
        return True

    def stop(self):
        """Request the cleaner to stop monitoring and exit."""
        self._stop_monitor_thread()
        self._exit()

    def _is_placement_group_removed(self, placement_group: PlacementGroup) -> bool:
        """Check if a placement group has been removed."""
        try:
            table = ray.util.placement_group_table(placement_group)
        except Exception as e:
            logger.warning(
                f"Failed to query placement group table: {e}. "
                "Assuming placement group is not removed."
            )
            return False
        if "state" not in table:
            return True
        return table["state"] == "REMOVED"

    def _exit(self):
        """Exit the actor."""
        if self._exiting:
            return
        self._exiting = True
        try:
            ray.actor.exit_actor()
        except Exception as e:
            # If exit fails for any reason, just log it.
            logger.warning(f"Failed to exit actor: {e}")
