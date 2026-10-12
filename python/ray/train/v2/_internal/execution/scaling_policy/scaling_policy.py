import abc
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional

from ray.data._internal.cluster_autoscaler.base_autoscaling_coordinator import (
    LabelSelector,
    ReservedResources,
)
from ray.train._internal.autoscaling_coordinator_client import (
    RequesterId,
    _reserved_resources_to_bundle_label_selectors,
    build_train_resource_request,
)
from ray.train.v2._internal.execution.callback import ControllerCallback
from ray.train.v2._internal.execution.context import TrainRunContext
from ray.train.v2._internal.execution.scaling_policy.autoscaling_coordinator_client import (  # noqa: E501
    TrainAutoscalingCoordinatorClient,
)
from ray.train.v2._internal.execution.worker_group import (
    WorkerGroupPollStatus,
    WorkerGroupState,
)
from ray.train.v2._internal.util import time_monotonic
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
    # One node-pinning label selector per worker, built from the same
    # reservation snapshot that chose `num_workers`. `None` means the worker
    # group is not pinned (see `ScalingPolicy._should_pin_to_reservation`).
    label_selectors: Optional[List[LabelSelector]] = None


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

    # Minimum interval in seconds between logs about waiting for reserved resources.
    WAITING_FOR_RESERVATION_LOG_INTERVAL_S = 30

    def __init__(self, scaling_config: ScalingConfig):
        self.scaling_config = scaling_config
        self._latest_waiting_for_reservation_log_time = float("-inf")
        # Due to multiple train dataset runs, the requester_id
        # isn't set until the run is started.
        self._requester_id: Optional[RequesterId] = None
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
        resources, label_selectors = build_train_resource_request(
            scaling_config=self.scaling_config,
            num_workers=self._get_num_workers_for_resource_request(),
        )
        self._coordinator_client.maybe_send_resource_request(
            resources=resources,
            label_selectors=label_selectors,
            strategy=self.scaling_config.placement_strategy,
        )

    def _send_resource_request(self):
        """Register training resources with the AutoscalingCoordinator."""
        assert self._coordinator_client is not None
        resources, label_selectors = build_train_resource_request(
            scaling_config=self.scaling_config,
            num_workers=self._get_num_workers_for_resource_request(),
        )
        self._coordinator_client.send_resource_request(
            resources=resources,
            label_selectors=label_selectors,
            strategy=self.scaling_config.placement_strategy,
        )

    def _cancel_resource_request(self):
        """Cancel the resource request to AutoscalingCoordinator.

        No-ops if the coordinator client was never created (i.e. the controller
        was aborted before ``after_controller_start`` ran).
        """
        if self._coordinator_client is None:
            return
        self._coordinator_client.cancel_resource_request()

    def _get_reserved_resources(self, recompute: bool = False) -> ReservedResources:
        """Get reserved resources from the AutoscalingCoordinator.

        Returns a cached value unless ``recompute`` is True.
        """
        assert self._coordinator_client is not None
        return self._coordinator_client.get_reserved_resources(recompute=recompute)

    def _should_pin_to_reservation(self) -> bool:
        """Whether workers should be pinned to the coordinator's reservation.

        Skipped when:
        - Workers request no resources: nothing to reserve, and they fit
          anywhere, so there is nothing to wait for.
        - TPU: `SlicePlacementGroup` does its own reservation and ignores
          `label_selector`, so pins would only add latency.
        - A `label_selector` is set: the coordinator picks nodes by resource
          fit and never matches `label_selectors` against node labels, so its
          pins can name a node that violates the selector.

        Selectors returned by controller callbacks are only known at worker
        group start, so the controller drops these pins in that case.
        """
        return (
            not self.scaling_config.use_tpu
            and sum(self.scaling_config._resources_per_worker_not_none.values()) > 0
            and not self.scaling_config.label_selector
        )

    def _get_reserved_label_selectors(
        self, reserved_resources: ReservedResources, num_workers: int
    ) -> Optional[List[LabelSelector]]:
        """Convert a reservation snapshot into one node pin per worker.

        Returns ``None`` if the snapshot cannot place ``num_workers`` workers
        in a layout that satisfies the requested placement strategy.
        """
        if not reserved_resources:
            return None
        label_selectors = _reserved_resources_to_bundle_label_selectors(
            reserved_resources=reserved_resources,
            resources_per_worker=self.scaling_config._resources_per_worker_not_none,
            trainer_resources=self.scaling_config._trainer_resources_not_none,
        )
        if len(label_selectors) < num_workers:
            logger.debug(
                "Reserved capacity covers %s workers but %s are required; "
                "not ready to pin the worker group yet.",
                len(label_selectors),
                num_workers,
            )
            return None
        label_selectors = label_selectors[:num_workers]

        # The reservation has to actually satisfy the requested placement, or
        # pinning to it would quietly violate the strategy the user asked for
        # -- e.g. STRICT_SPREAD workers all landing on one node. The
        # coordinator honors both strategies, so a mismatch means its view of
        # the cluster moved; wait for it to settle rather than pin to a layout
        # that contradicts the placement group we are about to create.
        placement_strategy = self.scaling_config.placement_strategy
        num_distinct_nodes = len(
            {frozenset(selector.items()) for selector in label_selectors}
        )
        expected_distinct_nodes = {
            "STRICT_PACK": 1,
            "STRICT_SPREAD": num_workers,
        }.get(placement_strategy)
        if (
            expected_distinct_nodes is not None
            and num_distinct_nodes != expected_distinct_nodes
        ):
            logger.debug(
                "Reserved capacity spans %s nodes but %s requires %s; "
                "not ready to pin the worker group yet.",
                num_distinct_nodes,
                placement_strategy,
                expected_distinct_nodes,
            )
            return None

        return label_selectors

    def _count_reserved_workers(self, reserved_resources: ReservedResources) -> int:
        """Number of worker slots in a reservation snapshot."""
        if not reserved_resources:
            return 0
        return len(
            _reserved_resources_to_bundle_label_selectors(
                reserved_resources=reserved_resources,
                resources_per_worker=self.scaling_config._resources_per_worker_not_none,
                trainer_resources=self.scaling_config._trainer_resources_not_none,
            )
        )

    def _maybe_log_waiting_for_reservation(self, num_reserved: int, num_required: int):
        """Periodically log that the policy is waiting on the coordinator."""
        now = time_monotonic()
        if (
            now - self._latest_waiting_for_reservation_log_time
            < self.WAITING_FOR_RESERVATION_LOG_INTERVAL_S
        ):
            return
        self._latest_waiting_for_reservation_log_time = now
        logger.info(
            f"Waiting for reserved resources: {num_reserved}/{num_required} "
            "workers are covered by the current reservation."
        )

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
