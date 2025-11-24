import logging
import threading
import time
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
        self._stopped: bool = False
        self._controller_actor_id: Optional[str] = None
        self._placement_group: Optional[PlacementGroup] = None
        self._monitor_thread: Optional[threading.Thread] = None
        self._get_actor_timeout_s = GET_ACTOR_TIMEOUT_S
        self._exiting: bool = False

    def register_controller_and_placement_group(
        self, controller_actor_id: str, placement_group: PlacementGroup
    ):
        self._controller_actor_id = controller_actor_id
        self._placement_group = placement_group
        logger.debug(
            "PlacementGroupCleaner registered controller %s with placement group %s",
            controller_actor_id,
            placement_group.id,
        )

    def start_monitoring(self):
        """Start monitoring the controller and placement group."""
        if not self._controller_actor_id or not self._placement_group:
            logger.warning(
                "Cannot start monitoring: controller or placement group missing"
            )
            return False

        monitor_thread = self._active_monitor_thread()
        if monitor_thread is not None:
            raise RuntimeError(
                "Cannot start monitoring: monitor thread reference is not None."
            )

        if self._stopped:
            logger.warning("Cannot start monitoring: stop already requested")
            return False

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
        """
        while not self._stopped and self._controller_actor_id:
            try:
                alive = is_actor_alive(
                    actor_id=self._controller_actor_id,
                    timeout=self._get_actor_timeout_s,
                )
            except ray.util.state.exception.RayStateApiException:
                logger.exception(
                    "Failed to query Ray Train Controller actor state. Attempting cleanup."
                )
                break

            if not alive:
                placement_group = self._placement_group
                if placement_group is None:
                    logger.debug(
                        "Controller actor died but no placement group is currently tracked; "
                        "skipping cleanup."
                    )
                    break
                if not self._is_placement_group_removed():
                    logger.warning(
                        f"Detected that the Ray Train controller actor ({self._controller_actor_id}) is dead. "
                        "Cleaning up placement group created by this run."
                    )
                    try:
                        logger.debug(
                            f"Cleaning up placement group: {placement_group.id}"
                        )
                        remove_placement_group(placement_group)
                        logger.debug("Placement group cleanup successful")
                    except Exception as e:
                        logger.warning(f"Failed to clean up placement group: {e}")
                else:
                    logger.debug(
                        "Controller actor died but placement group already removed; "
                        "skipping cleanup."
                    )
                break

            time.sleep(self._check_interval_s)

        self._monitor_thread = None

        if self._stopped:
            # Stop was requested; clear the flag so future monitoring can start.
            self._stopped = False
            return

        self._exit()

    def _stop_monitor_thread(self):
        """Stop the monitor thread and wait for it to exit.

        Returns:
            bool: True if the thread was stopped, False if there was no active thread.
        """
        monitor_thread = self._active_monitor_thread()
        if monitor_thread is None:
            return False

        self._stopped = True
        join_timeout = max(2.0, self._check_interval_s * 2)
        monitor_thread.join(timeout=join_timeout)
        if monitor_thread.is_alive():
            logger.warning(
                "Monitor thread did not exit within %.2f seconds", join_timeout
            )
            # Stop request failed; clear the flag so future restarts are allowed.
            self._stopped = False
            return False

        if self._monitor_thread is monitor_thread:
            self._monitor_thread = None
        return True

    def stop_monitoring(self):
        """Stop monitoring the current placement group without exiting the actor.

        This is called when a worker group shuts down gracefully, so we stop
        monitoring the old placement group. The cleaner actor remains alive
        to monitor possible future placement groups.
        """
        if not self._stop_monitor_thread():
            logger.debug("No active monitoring to stop")
            return

        logger.debug("Stopping monitoring for placement group shutdown")
        # Reset stopped flag so we can start monitoring again later
        self._stopped = False
        # Clear the monitored placement group
        self._placement_group = None

    def stop(self):
        """Request the cleaner to stop monitoring and exit."""
        self._stopped = True
        self._stop_monitor_thread()
        self._exit()

    def _is_placement_group_removed(self) -> bool:
        """Check if a placement group has been removed."""
        if not self._placement_group:
            return True
        try:
            table = ray.util.placement_group_table(self._placement_group)
            if "state" not in table:
                return True
            return table["state"] == "REMOVED"
        except Exception as e:
            logger.warning(
                f"Failed to query placement group table: {e}. "
                "Assuming placement group is not removed."
            )
            return False

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

    def _active_monitor_thread(self) -> Optional[threading.Thread]:
        """Return the running monitor thread, clearing stale references."""
        monitor_thread = self._monitor_thread
        if monitor_thread is None:
            return None
        if monitor_thread.is_alive():
            return monitor_thread
        if self._monitor_thread is monitor_thread:
            self._monitor_thread = None
        return None
