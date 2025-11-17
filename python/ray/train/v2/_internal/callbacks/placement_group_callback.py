import logging
from typing import TYPE_CHECKING, Optional

import ray
from ray.train.v2._internal.execution.callback import (
    ControllerCallback,
    WorkerGroupCallback,
)
from ray.train.v2._internal.execution.controller.placement_group_cleaner import (
    PlacementGroupCleaner,
)

if TYPE_CHECKING:
    from ray.train.v2._internal.execution.context import TrainRunContext
    from ray.train.v2._internal.execution.worker_group import WorkerGroup

logger = logging.getLogger(__name__)


class PlacementGroupCleanerCallback(ControllerCallback, WorkerGroupCallback):
    """Callback that manages a PlacementGroupCleaner for the training controller.

    This callback ensures that placement groups are cleaned up even if the controller
    dies ungracefully.
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
            cleaner_actor_cls = ray.remote(num_cpus=0, max_concurrency=2)(
                PlacementGroupCleaner
            )
            self._cleaner = cleaner_actor_cls.options(
                lifetime="detached",
                get_if_exists=False,
            ).remote(check_interval_s=self._check_interval_s)

            try:
                core_context = ray.runtime_context.get_runtime_context()
                self._controller_actor_id = core_context.get_actor_id()
            except Exception as e:
                logger.warning(
                    f"Failed to get controller actor id: {e}. "
                    "PlacementGroupCleaner will not be able to monitor controller liveness."
                )
                return

            if not self._controller_actor_id:
                logger.warning(
                    "Controller actor id is unavailable. "
                    "PG cleaner will not monitor controller liveness."
                )
                return

            ray.get(self._cleaner.register_controller.remote(self._controller_actor_id))

            logger.info(
                f"PlacementGroupCleaner launched for run_id={train_run_context.run_id}"
            )
        except Exception as e:
            logger.warning(
                f"Failed to launch PlacementGroupCleaner: {e}. "
                "Placement groups may not be cleaned up if controller exits ungracefully."
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

            # Placement group for worker group is created after worker group start.
            # Only one placement group for worker group exists at a time.
            ray.get(self._cleaner.register_placement_group.remote(placement_group))

            self._cleaner.start_monitoring.remote()

            logger.info(
                f"Registered placement group {placement_group.id} with PlacementGroupCleaner."
            )
        except Exception as e:
            logger.warning(
                f"Failed to register placement group with cleaner: {e}. "
                "Placement group may not be cleaned up if controller dies ungracefully."
            )

    def before_controller_shutdown(self):
        if not self._cleaner:
            return

        try:
            # Stop the cleaner gracefully (it won't clean up the PG)
            ray.get(self._cleaner.stop.remote(), timeout=1.0)
        except Exception as e:
            logger.warning(f"Failed to stop PlacementGroupCleaner gracefully: {e}")
        finally:
            self._cleaner = None
