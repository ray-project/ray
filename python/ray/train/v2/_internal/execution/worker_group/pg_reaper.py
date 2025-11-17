import logging
import math
import time
from typing import Optional

import ray
from ray.util.placement_group import PlacementGroup, remove_placement_group

from ray.train.v2._internal.state.util import is_actor_alive

logger = logging.getLogger(__name__)


@ray.remote(num_cpus=0, max_concurrency=2)
class PlacementGroupReaper:
    """Detached helper that ensures PG cleanup if controller dies ungracefully.

    This actor should be created with lifetime='detached' to avoid being
    fate-shared with the Train controller (or its parent Tune trial actor).
    """

    def __init__(self, check_interval_s: float = 1.0):
        self._check_interval_s = check_interval_s
        self._stopped: bool = False
        self._control_plane_timeout_s = max(1, math.ceil(self._check_interval_s))

    def stop(self):
        """Request the reaper to stop."""
        self._stopped = True
        

    def run(
        self, controller_actor_id: Optional[str], placement_group: PlacementGroup
    ) -> bool:
        """Monitor controller; remove PG when controller is gone.

        Returns True when cleanup attempt has been made or reaper stopped.
        """
        if not controller_actor_id:
            logger.warning("Controller actor id not provided; skipping reaper run.")
            return False

        # Keep checking the controller from the control plane; attempt cleanup when dead.
        while not self._stopped:
            try:
                alive = is_actor_alive(
                    actor_id=controller_actor_id,
                    timeout=self._control_plane_timeout_s,
                )
                if not alive:
                    logger.warning(
                        "Controller actor reported dead via control plane. "
                        "Reaper initiating cleanup."
                    )
                    break
                time.sleep(self._check_interval_s)
            except Exception as exc:
                logger.warning(
                    f"Failed querying controller state via control plane: {exc}. "
                    "Reaper will attempt cleanup."
                )
                break

        # Controller is gone or we were asked to stop: attempt cleanup.
        try:
            remove_placement_group(placement_group)
        except Exception as exc:
            # Best-effort cleanup — ignore errors (already removed, etc.)
            logger.debug(
                "PlacementGroupReaper failed to remove placement group: %s", exc
            )

        # Exit the actor so it doesn't linger after cleanup.
        self._exit()
        return True

    def _exit(self):
        try:
            ray.actor.exit_actor()
        except Exception as exc:
            logger.debug("PlacementGroupReaper failed to exit cleanly: %s", exc)


