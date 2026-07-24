import abc
import logging
from dataclasses import dataclass
from typing import Dict, Optional

from ray.train.v2._internal.execution.callback import ControllerCallback
from ray.train.v2._internal.execution.context import TrainRunContext
from ray.train.v2._internal.execution.scaling_policy.autoscaling_coordinator_client import (  # noqa: E501
    TrainAutoscalingCoordinatorClient,
)
from ray.train.v2._internal.execution.worker_group import (
    WorkerGroupPollStatus,
    WorkerGroupState,
)
from ray.train.v2.api.config import ScalingConfig

logger = logging.getLogger(__name__)


@dataclass
class ScalingDecision:
    pass


@dataclass
class NoopDecision(ScalingDecision):
    pass


@dataclass
class ResizeDecision(ScalingDecision):
    num_workers: int
    resources_per_worker: Dict[str, float]


class ScalingPolicy(abc.ABC, ControllerCallback):
    """A policy that determines when and how to scale a worker group.

    This can be used to implement elasticity and fault tolerance.

    Recovery decisions are made when workers are in an inactive or unhealthy state.
    Upscale decisions are optional and are made when workers are healthy.

    Note: When adding new scaling policies, revisit the shared defaults- particularly if:
    - AutoscalingCoordinator integration is not needed or a different interface
      becomes available
    - Timeout/expiry constants need to diverge between policies
    - _get_num_workers_for_resource_request() needs variable worker counts
    - Controller lifecycle behavior diverges
    """

    # TODO: Restructure these APIs to consider different TrainControllerStates
    # instead of just running and non-running worker groups.

    def __init__(self, scaling_config: ScalingConfig):
        self.scaling_config = scaling_config
        # Due to multiple train dataset runs, the requester_id
        # isn't set until the run is started.
        self._requester_id: Optional[str] = None
        self._coordinator_client: Optional[TrainAutoscalingCoordinatorClient] = None

    @abc.abstractmethod
    def make_decision_for_non_running_worker_group(self) -> ScalingDecision:
        """Makes a scaling decision when the worker group is initializing
        or recovering from an error."""
        raise NotImplementedError

    @abc.abstractmethod
    def make_decision_for_running_worker_group(
        self,
        worker_group_state: WorkerGroupState,
        worker_group_status: WorkerGroupPollStatus,
    ) -> ScalingDecision:
        """Makes a scaling decision when monitoring healthy, running workers."""
        raise NotImplementedError

    @abc.abstractmethod
    def _get_num_workers_for_resource_request(self) -> int:
        """Return the number of workers to request resources for."""
        raise NotImplementedError

    # ---------------------------------------------------
    # Methods for interacting with AutoscalingCoordinator
    # ---------------------------------------------------

    def _maybe_send_resource_request(self):
        """Send a resource request to AutoscalingCoordinator,
        if AUTOSCALING_REQUESTS_INTERVAL_S has passed since the last send."""
        assert self._coordinator_client is not None
        self._coordinator_client.maybe_send_resource_request(
            self.scaling_config,
            self._get_num_workers_for_resource_request(),
        )

    def _send_resource_request(self):
        """Register training resources with the AutoscalingCoordinator."""
        assert self._coordinator_client is not None
        self._coordinator_client.send_resource_request(
            self.scaling_config,
            self._get_num_workers_for_resource_request(),
        )

    def _cancel_resource_request(self):
        """Cancel the resource request to AutoscalingCoordinator.

        No-ops if the coordinator client was never created (i.e. the controller
        was aborted before ``after_controller_start`` ran).
        """
        if self._coordinator_client is None:
            return
        self._coordinator_client.cancel_resource_request()

    @property
    def _autoscaling_coordinator(self):
        assert self._coordinator_client is not None
        return self._coordinator_client._autoscaling_coordinator

    # --------------------------
    # ControllerCallback
    # --------------------------

    def after_controller_start(self, train_run_context: TrainRunContext):
        """Register training resources with the AutoscalingCoordinator."""
        self._requester_id = f"train-{train_run_context.run_id}"
        self._coordinator_client = TrainAutoscalingCoordinatorClient(self._requester_id)
        resources_per_worker = self.scaling_config._resources_per_worker_not_none
        num_workers = self._get_num_workers_for_resource_request()
        label_selectors = self.scaling_config._label_selector_per_worker(num_workers)
        if label_selectors:
            logger.info(
                f"Requesting resources: {resources_per_worker} * {num_workers} "
                f"with label_selectors={label_selectors}"
            )
        else:
            logger.info(f"Requesting resources: {resources_per_worker} * {num_workers}")
        self._send_resource_request()

    async def before_controller_shutdown(self):
        """Cancel the resource request when the controller shuts down."""
        self._cancel_resource_request()

    def before_controller_abort(self):
        """Cancel the resource request when the controller is aborted."""
        self._cancel_resource_request()
