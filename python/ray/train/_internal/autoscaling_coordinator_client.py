import threading
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from ray.data._internal.cluster_autoscaler.base_autoscaling_coordinator import (
    ResourceDict,
)

if TYPE_CHECKING:
    from ray.air.config import ScalingConfig


def build_train_resource_request(
    scaling_config: "ScalingConfig",
    num_workers: int,
) -> Tuple[List[ResourceDict], Optional[List[Dict[str, str]]]]:
    """Build coordinator bundles for worker resources and optional trainer resources.

    Trainer bundles are included when ``scaling_config._trainer_resources_not_none``
    is non-empty (Ray Train V1). Ray Train V2 overrides this to ``{}``.
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
    return resources, label_selectors


class TrainV1ResourceReservation:
    """Context manager that registers train resources for the duration of a run.
    NOTE: Only used by the train V1
    """

    def __init__(
        self,
        requester_id: str,
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
        # 1 seconda more, since a send_resource_request could stall
        # up to AUTOSCALING_REQUESTS_GET_TIMEOUT_S seconds
        self._refresh_join_timeout_s = AUTOSCALING_REQUESTS_GET_TIMEOUT_S + 1
        self._stop_event = threading.Event()
        self._refresh_thread: Optional[threading.Thread] = None

    def __enter__(self) -> "TrainV1ResourceReservation":
        self._client.send_resource_request(self._scaling_config, self._num_workers)
        self._refresh_thread = threading.Thread(
            target=self._refresh_loop, daemon=True, name="train-resource-reservation"
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
            self._client.send_resource_request(self._scaling_config, self._num_workers)
