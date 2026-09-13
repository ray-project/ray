import copy
import math
import threading
from typing import TYPE_CHECKING, List, Optional, Tuple

from ray.data._internal.cluster_autoscaler.base_autoscaling_coordinator import (
    LabelSelector,
    RequesterId,
    ReservedResources,
    ResourceDict,
)

if TYPE_CHECKING:
    from ray.air.config import ScalingConfig


# Relative slack absorbed before flooring a reserved amount into worker slots.
# Large enough to cover accumulated float error over thousands of bundles, far
# smaller than any real fraction of a worker.
_SLOT_EPSILON = 1e-6


def build_train_resource_request(
    scaling_config: "ScalingConfig",
    num_workers: int,
) -> Tuple[List[ResourceDict], Optional[List[LabelSelector]]]:
    """Build coordinator bundles for worker resources and optional trainer resources.

    Trainer bundles are included when ``scaling_config._trainer_resources_not_none``
    is non-empty (Ray Train V1). Ray Train V2 overrides this to ``{}``. Returns a 2 items:
    one for the resources for [coordinator, worker1, ..., workerN] and a label selector
    that dictates the locations of the workers (NOT coordinator)
    """
    resources_per_worker = scaling_config._resources_per_worker_not_none
    worker_bundles = [resources_per_worker] * num_workers
    trainer_resources = scaling_config._trainer_resources_not_none
    if trainer_resources:
        resources = [trainer_resources] + worker_bundles
    else:
        resources = worker_bundles

    label_selectors = scaling_config._label_selector_per_worker(num_workers)
    if trainer_resources and label_selectors is not None:
        label_selectors = [{}] + label_selectors
    assert label_selectors is None or len(resources) == len(label_selectors)
    return resources, label_selectors


def _subtract_trainer_resources_from_reserved(
    reserved_resources: ReservedResources,
    trainer_resources: ResourceDict,
) -> ReservedResources:
    """Remove trainer bundle reservation from per-node totals before counting workers.

    The coordinator merges trainer and worker bundles into the same per-node totals,
    so the trainer's share has to come back off before the remainder can be divided
    into worker slots.

    ``reserved_resources`` is keyed in the order the coordinator placed bundles, and
    the trainer is submitted as the first bundle (see ``build_train_resource_request``),
    so the trainer's node is the first key that can satisfy it. Do not sort here: the
    coordinator scans nodes largest-first, not by ``node_id``, so subtracting from the
    alphabetically-first node instead takes the resources off a node the trainer never
    landed on. Integer division then rounds a worker slot away and the caller sees
    fewer slots than were actually reserved.
    """
    trainer_resources = {k: v for k, v in trainer_resources.items() if v > 0}
    if not trainer_resources:
        return reserved_resources

    adjusted = copy.deepcopy(reserved_resources)
    for node_resources in adjusted.values():
        if all(node_resources.get(k, 0.0) >= v for k, v in trainer_resources.items()):
            for k, v in trainer_resources.items():
                node_resources[k] -= v
            break
    return adjusted


def _floor_slots(reserved: float, per_worker: float) -> int:
    """How many whole ``per_worker`` slots fit in ``reserved``, tolerant of float drift.

    The coordinator builds a node's reserved total by adding the per-worker bundle
    once per worker, so the total is not exactly ``n * per_worker`` for fractional
    resources -- five ``0.1`` GPU bundles sum to a float whose ``// 0.1`` is 4, not 5.
    A single slot lost that way makes a fully reserved worker group look unready
    forever, so nudge by a relative epsilon before flooring.
    """
    return math.floor(reserved / per_worker + _SLOT_EPSILON)


def _reserved_resources_to_bundle_label_selectors(
    reserved_resources: ReservedResources,
    resources_per_worker: ResourceDict,
    trainer_resources: Optional[ResourceDict] = None,
) -> List[LabelSelector]:
    """Convert per-node reserved resources to a per-bundle list of label selectors.

    Each entry pins one resource bundle to its reserved node.
    """
    import ray._raylet

    assert (
        sum(resources_per_worker.values()) > 0
    ), "resources per worker should have at least one non-zero"

    worker_reserved_resources = _subtract_trainer_resources_from_reserved(
        reserved_resources=reserved_resources,
        trainer_resources=trainer_resources or {},
    )

    label_selectors: List[LabelSelector] = []
    for node_id, node_resources in worker_reserved_resources.items():
        num_workers_on_node = min(
            _floor_slots(node_resources.get(r, 0.0), resources_per_worker[r])
            for r in resources_per_worker
            if resources_per_worker[r] > 0
        )
        label_selectors.extend(
            [{ray._raylet.RAY_NODE_ID_KEY: node_id}] * num_workers_on_node
        )
    return label_selectors


class TrainV1ResourceReservation:
    """Context manager that registers train resources for the duration of a run.
    NOTE: Only used by the train V1
    """

    def __init__(
        self,
        requester_id: RequesterId,
        scaling_config: "ScalingConfig",
        num_workers: int,
    ):
        from ray.train.v2._internal.execution.scaling_policy.autoscaling_coordinator_client import (  # noqa: E501
            AUTOSCALING_REQUESTS_GET_TIMEOUT_S,
            AUTOSCALING_REQUESTS_INTERVAL_S,
            TrainAutoscalingCoordinatorClient,
        )

        self._client = TrainAutoscalingCoordinatorClient(requester_id)
        self._scaling_config = scaling_config
        self._num_workers = num_workers
        self._refresh_interval_s = AUTOSCALING_REQUESTS_INTERVAL_S
        # 1 second more, since a send_resource_request could stall
        # up to AUTOSCALING_REQUESTS_GET_TIMEOUT_S seconds
        self._refresh_join_timeout_s = AUTOSCALING_REQUESTS_GET_TIMEOUT_S + 1
        self._stop_event = threading.Event()
        self._refresh_thread: Optional[threading.Thread] = None

    def _send_request(self) -> None:
        """Send (or refresh) this run's resource request to the coordinator."""
        resources, label_selectors = build_train_resource_request(
            scaling_config=self._scaling_config,
            num_workers=self._num_workers,
        )
        self._client.send_resource_request(
            resources=resources,
            label_selectors=label_selectors,
        )

    def __enter__(self) -> "TrainV1ResourceReservation":
        self._send_request()
        self._refresh_thread = threading.Thread(
            target=self._refresh_loop,
            daemon=True,
            name="train-resource-reservation",
        )
        self._refresh_thread.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._stop_event.set()
        if self._refresh_thread is not None:
            self._refresh_thread.join(timeout=self._refresh_join_timeout_s)
        self._client.cancel_resource_request()

    def _refresh_loop(self) -> None:
        while not self._stop_event.wait(self._refresh_interval_s):
            self._send_request()
