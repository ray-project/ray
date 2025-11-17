import logging
from typing import TYPE_CHECKING, Optional

import ray
from ray.train.v2._internal.execution.callback import (
    ControllerCallback,
    WorkerGroupCallback,
)
from ray.train.v2._internal.execution.controller.pg_cleaner import PlacementGroupCleaner

if TYPE_CHECKING:
    from ray.train.v2._internal.execution.context import TrainRunContext
    from ray.train.v2._internal.execution.worker_group import WorkerGroup

logger = logging.getLogger(__name__)


class PlacementGroupCleanerCallback(ControllerCallback, WorkerGroupCallback):
    """Callback that manages a PlacementGroupCleaner for the training controller.

    This callback ensures that placement groups are cleaned up even if the controller
    dies ungracefully (e.g., due to preemption or system failures).

    Workflow:
    1. When the controller starts, this launches a detached PlacementGroupCleaner actor
    2. When a worker group is created, the placement group is registered with the cleaner
    3. The cleaner monitors the controller's liveness and cleans up the PG if it dies
    4. On graceful shutdown, the cleaner is stopped and exits
    """

    def __init__(self, check_interval_s: float = 1.0):
        """Initialize the callback.

        Args:
            check_interval_s: How often (in seconds) the cleaner should check
                if the controller is still alive.
        """
        self._check_interval_s = check_interval_s
        self._cleaner = None
        self._controller_actor_id: Optional[str] = None

    def after_controller_start(self, train_run_context: "TrainRunContext"):
        """Launch the detached PlacementGroupCleaner actor.

        This is called when the controller starts, before the control loop begins.
        """
        try:
            # Launch the cleaner as a detached actor so it survives controller death
            self._cleaner = PlacementGroupCleaner.options(
                name=f"pg_cleaner_{train_run_context.run_id}",
                namespace="train",
                lifetime="detached",
                get_if_exists=False,
            ).remote(check_interval_s=self._check_interval_s)

            try:
                runtime_context = ray.get_runtime_context()
                self._controller_actor_id = runtime_context.get_actor_id()
            except Exception as e:
                logger.warning(
                    f"Failed to get controller actor id: {e}. "
                    "PG cleaner will not be able to monitor controller liveness."
                )
                return

            if not self._controller_actor_id:
                logger.warning(
                    "Controller actor id is unavailable. "
                    "PG cleaner will not monitor controller liveness."
                )
                return

            # Register the controller with the cleaner
            ray.get(
                self._cleaner.register_controller.remote(self._controller_actor_id)
            )

            logger.info(
                f"PlacementGroupCleaner launched for run_id={train_run_context.run_id}"
            )
        except Exception as e:
            logger.warning(
                f"Failed to launch PlacementGroupCleaner: {e}. "
                "Placement groups may not be cleaned up if controller dies ungracefully."
            )
            self._cleaner = None

    def after_worker_group_start(self, worker_group: "WorkerGroup"):
        """Register the worker group's placement group with the cleaner.

        This is called after a worker group is successfully started.
        """
        if not self._cleaner:
            logger.debug("No PlacementGroupCleaner to register placement group with")
            return

        try:
            worker_group_state = worker_group.get_worker_group_state()
            placement_group = worker_group_state.placement_group

            # Register the placement group with the cleaner
            ray.get(self._cleaner.register_placement_group.remote(placement_group))

            # Start monitoring (non-blocking). Best-effort; we don't wait for result.
            self._cleaner.start_monitoring.remote()

            logger.info(
                f"Registered placement group {placement_group.id} with cleaner"
            )
        except Exception as e:
            logger.warning(
                f"Failed to register placement group with cleaner: {e}. "
                "Placement group may not be cleaned up if controller dies ungracefully."
            )

    def before_controller_shutdown(self):
        """Stop the PlacementGroupCleaner gracefully.

        This is called before the controller exits normally.
        The cleaner will stop monitoring and exit without cleaning up the PG.
        """
        if not self._cleaner:
            return

        try:
            # Stop the cleaner gracefully (it won't clean up the PG)
            ray.get(self._cleaner.stop.remote(), timeout=5.0)
            logger.info("PlacementGroupCleaner stopped gracefully")
        except Exception as e:
            logger.warning(f"Failed to stop PlacementGroupCleaner gracefully: {e}")
        finally:
            self._cleaner = None

