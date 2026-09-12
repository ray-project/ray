import logging
from typing import TYPE_CHECKING, Optional

import ray
from ray._common.constants import HEAD_NODE_RESOURCE_NAME
from ray.exceptions import RayActorError
from ray.train.v2._internal.constants import GET_ACTOR_TIMEOUT_S
from ray.train.v2._internal.execution.callback import (
    ControllerCallback,
    WorkerGroupCallback,
)
from ray.train.v2._internal.execution.controller.placement_group_cleaner import (
    PLACEMENT_GROUP_CLEANER_NAME,
    PLACEMENT_GROUP_CLEANER_NAMESPACE,
    PlacementGroupCleaner,
)

if TYPE_CHECKING:
    from ray.train.v2._internal.execution.context import TrainRunContext
    from ray.train.v2._internal.execution.worker_group import (
        WorkerGroup,
        WorkerGroupContext,
    )
    from ray.util.placement_group import PlacementGroup

logger = logging.getLogger(__name__)


class PlacementGroupCleanerCallback(ControllerCallback, WorkerGroupCallback):
    """Callback that manages a PlacementGroupCleaner for the training controller.

    This callback ensures that placement groups are cleaned up even if the controller
    dies ungracefully.
    """

    def __init__(
        self,
        check_interval_s: float = 1.0,
        get_actor_timeout_s: float = GET_ACTOR_TIMEOUT_S,
        stop_timeout: Optional[float] = None,
    ):
        """Initialize the callback.

        Args:
            check_interval_s: How often (in seconds) the cleaner should check
                if the controller is still alive.
            get_actor_timeout_s: How long to wait when calling the get actor state api.
            stop_timeout: How long to wait for the cleaner to stop.
        """
        self._check_interval_s = check_interval_s
        self._get_actor_timeout_s = get_actor_timeout_s
        self._stop_timeout = stop_timeout
        if self._stop_timeout is None:
            self._stop_timeout = max(
                2.0, self._check_interval_s * 2 + self._get_actor_timeout_s
            )
        self._cleaner: Optional[PlacementGroupCleaner] = None
        self._controller_actor_id: Optional[str] = None
        self._registered_placement_group: Optional["PlacementGroup"] = None

    def after_controller_start(self, train_run_context: "TrainRunContext"):
        """Get the shared PlacementGroupCleaner actor and register this controller."""

        core_context = ray.runtime_context.get_runtime_context()
        self._controller_actor_id = core_context.get_actor_id()
        try:
            # This named detached actor is shared across jobs. Keeping it alive when
            # the registry is empty avoids a create-versus-exit race between jobs.
            cleaner_actor_cls = ray.remote(num_cpus=0)(PlacementGroupCleaner)
            self._cleaner = cleaner_actor_cls.options(
                name=PLACEMENT_GROUP_CLEANER_NAME,
                namespace=PLACEMENT_GROUP_CLEANER_NAMESPACE,
                lifetime="detached",
                get_if_exists=True,
                resources={HEAD_NODE_RESOURCE_NAME: 0.001},
                scheduling_strategy="DEFAULT",
                max_restarts=-1,
                max_task_retries=-1,
            ).remote(
                check_interval_s=self._check_interval_s,
                get_actor_timeout_s=self._get_actor_timeout_s,
                stop_timeout=self._stop_timeout,
            )

            registered = ray.get(
                self._cleaner.register_controller.remote(self._controller_actor_id)
            )
            if not registered:
                raise RuntimeError(
                    "The shared PlacementGroupCleaner already observed this "
                    "controller as dead."
                )

            logger.debug(
                "Registered run_id=%s with the shared PlacementGroupCleaner",
                train_run_context.run_id,
            )
        except Exception as e:
            logger.warning(
                f"Failed to launch PlacementGroupCleaner: {e}. "
                "Placement groups may not be cleaned up if controller exits ungracefully."
            )
            self._cleaner = None
            return

        self._cleaner.start_monitoring.remote()

    def after_worker_group_start(self, worker_group: "WorkerGroup"):
        """Register the worker group's placement group with the cleaner.

        This is called after a worker group is successfully started.
        """
        if not self._cleaner or not self._controller_actor_id:
            logger.warning(
                "PlacementGroupCleaner not available. "
                "Placement groups may not be cleaned up if controller exits ungracefully."
            )
            return
        worker_group_state = worker_group.get_worker_group_state()
        placement_group = worker_group_state.placement_group_handle.placement_group

        try:
            registered = ray.get(
                self._cleaner.register_placement_group.remote(
                    self._controller_actor_id, placement_group
                )
            )
            if not registered:
                raise RuntimeError(
                    "PlacementGroupCleaner already observed the controller as dead."
                )
        except Exception as e:
            logger.warning(
                f"Failed to register placement group with cleaner: {e}. "
                "Placement group may not be cleaned up if controller dies ungracefully."
            )
            return

        self._registered_placement_group = placement_group
        logger.debug(
            f"Registered placement group {placement_group.id} with PlacementGroupCleaner."
        )

    def after_worker_group_shutdown(self, worker_group_context: "WorkerGroupContext"):
        self._unregister_placement_group()

    def after_worker_group_abort(self, worker_group_context: "WorkerGroupContext"):
        if self._unregister_placement_group():
            self._unregister_controller()

    def _unregister_placement_group(self) -> bool:
        placement_group = self._registered_placement_group
        if (
            not self._cleaner
            or not self._controller_actor_id
            or placement_group is None
        ):
            return True

        try:
            ray.get(
                self._cleaner.unregister_placement_group.remote(
                    self._controller_actor_id, placement_group
                ),
                timeout=self._stop_timeout,
            )
        except Exception:
            # Keep the durable registration on failure so the cleaner can still
            # remove or prune the placement group if the controller later dies.
            logger.exception(
                "Failed to unregister placement group from PlacementGroupCleaner."
            )
            return False
        self._registered_placement_group = None
        return True

    def _unregister_controller(self):
        if not self._cleaner or not self._controller_actor_id:
            return

        try:
            ray.get(
                self._cleaner.unregister_controller.remote(self._controller_actor_id),
                timeout=self._stop_timeout,
            )
        except RayActorError:
            logger.debug(
                "PlacementGroupCleaner exited before controller deregistration "
                "completed; ignoring."
            )
        except Exception:
            logger.exception("Failed to unregister controller from cleaner.")
        finally:
            self._cleaner = None
            self._registered_placement_group = None

    async def before_controller_shutdown(self):
        self._stop_cleaner()

    def _stop_cleaner(self):
        # Worker-group shutdown has already completed before this hook runs, so
        # it is safe to drop this controller's durable registration only after
        # its placement group registration has been removed successfully.
        if self._unregister_placement_group():
            self._unregister_controller()

    def before_controller_abort(self):
        # Keep the durable registration until worker-group abort has completed.
        # If abort fails, the cleaner must retain the PGs and clean them after
        # this controller exits.
        return
