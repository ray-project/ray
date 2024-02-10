from typing import List
from ray.autoscaler.v2.instance_manager import (
    InstanceUpdatedSubscriber,
)
from ray.autoscaler._private.gcs_client import GcsClient
from ray.core.generated.instance_manager_pb2 import (
    InstanceUpdateEvent,
    Instance,
    TerminationRequest,
)
from ray.core.generated.autoscaler_pb2 import DrainNodeRequest, DrainNodeReason


class RayStopper(InstanceUpdatedSubscriber):
    """RayStopper is responsible for stopping ray on instances."""

    def __init__(self, gcs_client: GcsClient) -> None:
        self._gcs_client = gcs_client

    def notify(self, events: List[InstanceUpdateEvent]) -> None:
        for event in events:
            if event.new_instance_status == Instance.RAY_STOPPED:
                self._stop_ray(event)

    def _stop_ray(self, event: InstanceUpdateEvent) -> None:
        termination_request = event.termination_request
        ray_node_id = termination_request.ray_node_id

        if termination_request.cause == TerminationRequest.Cause.IDLE:
            reason = DrainNodeReason.DRAIN_NODE_REASON_IDLE_TERMINATION
            reason_str = "Idle termination for node for {} seconds.".format(
                termination_request.idle_time_ms / 1000
            )
        elif (
            termination_request.cause == TerminationRequest.Cause.MAX_NUM_NODE_PER_TYPE
        ):
            reason = DrainNodeReason.DRAIN_NODE_REASON_PREEMPTION
            reason_str

        drain_node_request = DrainNodeRequest(
            node_id=ray_node_id.encode("utf-8"),
            reason=reason,
        )
