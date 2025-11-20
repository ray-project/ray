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
        self._monitoring: bool = False
        self._monitor_thread: Optional[threading.Thread] = None
        self._get_actor_timeout_s = GET_ACTOR_TIMEOUT_S
        self._exiting: bool = False

    def register_controller_and_placement_group(
        self, controller_actor_id: str, placement_group: PlacementGroup
    ):
        self._controller_actor_id = controller_actor_id
        self._placement_group = placement_group
        logger.info(
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

        if self._monitoring:
            logger.warning("Already monitoring")
            return False

        if self._stopped:
            logger.warning("Cannot start monitoring: stop already requested")
            return False

        self._monitoring = True
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop,
            name="PlacementGroupCleanerMonitor",
            daemon=True,
        )
        self._monitor_thread.start()
        logger.info("PlacementGroupCleaner started monitoring in background thread")
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
                if not alive and not self._is_placement_group_removed():
                    logger.warning(
                        f"Detected that the Ray Train controller actor ({self._controller_actor_id}) is dead. "
                        "Cleaning up placement group created by this run."
                    )
                    break
                time.sleep(self._check_interval_s)
            except Exception:
                logger.exception(
                    "Failed to query controller state. Attempting cleanup."
                )
                break

        if not self._stopped and not self._is_placement_group_removed():
            logger.info(f"Cleaning up placement group: {self._placement_group.id}")
            try:
                remove_placement_group(self._placement_group)
                logger.info("Placement group cleanup successful")
            except Exception as e:
                logger.warning(f"Failed to clean up placement group: {e}")

        self._monitoring = False
        self._monitor_thread = None

        if not self._stopped:
            self._exit()

    def stop(self):
        """Request the cleaner to stop monitoring and exit."""
        self._stopped = True
        logger.info("PlacementGroupCleaner stop requested")
        monitor_thread = self._monitor_thread
        if monitor_thread and monitor_thread.is_alive():
            join_timeout = max(2.0, self._check_interval_s * 2)
            monitor_thread.join(timeout=join_timeout)
            if monitor_thread.is_alive():
                logger.warning(
                    "Monitor thread did not exit within %.2f seconds", join_timeout
                )
        self._monitoring = False
        self._monitor_thread = None
        self._exit()

    def _is_placement_group_removed(self) -> bool:
        """Check if a placement group has been removed."""
        if not self._placement_group:
            return True
        table = ray.util.placement_group_table(self._placement_group)
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
