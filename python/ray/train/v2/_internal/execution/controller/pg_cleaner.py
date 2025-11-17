import logging
import math
import time
from typing import Optional

import ray
from ray.util.placement_group import PlacementGroup, remove_placement_group
from ray.train.v2._internal.state.util import is_actor_alive

logger = logging.getLogger(__name__)


@ray.remote(num_cpus=0, max_concurrency=2)
class PlacementGroupCleaner:
    """Detached helper that ensures PG cleanup if controller dies ungracefully.

    This actor should be created with lifetime='detached' to avoid being
    fate-shared with the Train controller (or its parent Tune trial actor).
    
    Workflow:
    1. Controller launches this as a detached actor on startup
    2. Controller calls register_placement_group() when creating a new worker group
    3. This actor watches the controller actor's state via the control plane
    4. If the controller dies, this actor cleans up the placement group
    5. On graceful shutdown, controller calls stop() and this actor exits
    """

    def __init__(self, check_interval_s: float = 1.0):
        self._check_interval_s = check_interval_s
        self._stopped: bool = False
        self._controller_actor_id: Optional[str] = None
        self._placement_group: Optional[PlacementGroup] = None
        self._monitoring: bool = False
        self._control_plane_timeout_s = max(1, math.ceil(self._check_interval_s))

    def register_controller(self, controller_actor_id: str):
        """Register the controller actor id to monitor via the control plane."""
        self._controller_actor_id = controller_actor_id
        logger.info("PlacementGroupCleaner registered controller actor id")

    def register_placement_group(self, placement_group: PlacementGroup):
        """Register a placement group to clean up if controller dies.
        
        Args:
            placement_group: The placement group to monitor and clean up.
        """
        self._placement_group = placement_group
        logger.info(f"PlacementGroupCleaner registered placement group: {placement_group.id}")

    def start_monitoring(self):
        """Start monitoring the controller and placement group.
        
        This should be called after both controller and placement_group are registered.
        Runs asynchronously in the background.
        """
        if not self._controller_actor_id:
            logger.warning("Cannot start monitoring: controller not registered")
            return False
            
        if self._monitoring:
            logger.warning("Already monitoring")
            return False
            
        self._monitoring = True
        logger.info("PlacementGroupCleaner started monitoring")
        
        self._monitor_loop()
        return True

    def _monitor_loop(self):
        """Monitor controller; remove PG when controller is gone.
        
        This runs continuously until controller dies or stop() is called.
        """
        while not self._stopped and self._controller_actor_id:
            try:
                alive = is_actor_alive(
                    actor_id=self._controller_actor_id,
                    timeout=self._control_plane_timeout_s,
                )
                if not alive:
                    logger.warning(
                        "Controller actor reported dead via control plane. "
                        "Attempting cleanup."
                    )
                    break
                time.sleep(self._check_interval_s)
            except Exception as e:
                # Control plane query failed – assume dead and attempt cleanup.
                logger.warning(
                    f"Failed to query controller state: {e}. Attempting cleanup."
                )
                break

        # Controller is gone or we were asked to stop: attempt cleanup.
        if not self._stopped and self._placement_group:
            logger.info(f"Cleaning up placement group: {self._placement_group.id}")
            try:
                remove_placement_group(self._placement_group)
                logger.info("Placement group cleanup successful")
            except Exception as e:
                # Best-effort cleanup — ignore errors (already removed, etc.)
                logger.warning(f"Failed to clean up placement group: {e}")
        
        # Exit the actor so it doesn't linger after cleanup.
        self._exit()

    def stop(self):
        """Request the cleaner to stop monitoring and exit."""
        self._stopped = True
        logger.info("PlacementGroupCleaner stop requested")
        self._exit()

    def _exit(self):
        """Exit the actor."""
        try:
            ray.actor.exit_actor()
        except Exception as e:
            # If exit fails for any reason, just log it.
            logger.warning(f"Failed to exit actor: {e}")


