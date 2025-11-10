import time
from typing import Optional

import ray
from ray.actor import ActorHandle
from ray.util.placement_group import PlacementGroup, remove_placement_group


@ray.remote(num_cpus=0)
class PlacementGroupReaper:
    """Detached helper that ensures PG cleanup if controller dies ungracefully.

    This actor should be created with lifetime='detached' to avoid being
    fate-shared with the Train controller (or its parent Tune trial actor).
    """

    def __init__(self, check_interval_s: float = 1.0):
        self._check_interval_s = check_interval_s
        self._stopped: bool = False

    def stop(self):
        """Request the reaper to stop."""
        self._stopped = True
        

    def run(self, controller: ActorHandle, placement_group: PlacementGroup) -> bool:
        """Monitor controller; remove PG when controller is gone.

        Returns True when cleanup attempt has been made or reaper stopped.
        """
        # Keep pinging the controller; if it errors, attempt to remove PG.
        while not self._stopped:
            try:
                # Lightweight liveness probe. Will raise if controller is dead.
                ray.get(controller.ping.remote(), timeout=1.0)
            except Exception:
                break
            time.sleep(self._check_interval_s)

        # Controller is gone or we were asked to stop: attempt cleanup.
        try:
            remove_placement_group(placement_group)
        except Exception:
            # Best-effort cleanup — ignore errors (already removed, etc.)
            pass
        # Exit the actor so it doesn't linger after cleanup.
        try:
            ray.actor.exit_actor()
        except Exception:
            # If exit fails for any reason, just return.
            pass
        return True


