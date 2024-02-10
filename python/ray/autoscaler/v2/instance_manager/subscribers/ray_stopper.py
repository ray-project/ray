from concurrent.futures import ThreadPoolExecutor
import logging
from typing import List
from ray.autoscaler.v2.instance_manager.instance_manager import (
    InstanceUpdatedSubscriber,
)
from ray._raylet import GcsClient
from ray.core.generated.instance_manager_pb2 import (
    InstanceUpdateEvent,
    Instance,
    TerminationRequest,
)
from ray.core.generated.autoscaler_pb2 import DrainNodeRequest, DrainNodeReason

logger = logging.getLogger(__name__)


class RayStopper(InstanceUpdatedSubscriber):
    """RayStopper is responsible for stopping ray on instances."""

    def __init__(self, gcs_client: GcsClient) -> None:
        self._gcs_client = gcs_client
        self._executor = ThreadPoolExecutor(max_workers=1)

    def notify(self, events: List[InstanceUpdateEvent]) -> None:
        for event in events:
            if event.new_instance_status == Instance.RAY_STOPPED:
                self._executor.submit(self._stop_ray, event)

    def _stop_ray(self, event: InstanceUpdateEvent) -> None:
        termination_request = event.termination_request
        ray_node_id = termination_request.ray_node_id

        if termination_request.cause == TerminationRequest.Cause.IDLE:
            reason = DrainNodeReason.DRAIN_NODE_REASON_IDLE_TERMINATION
            reason_str = "Idle termination of node for {} seconds.".format(
                termination_request.idle_time_ms / 1000
            )
        elif (
            termination_request.cause == TerminationRequest.Cause.MAX_NUM_NODE_PER_TYPE
        ):
            reason = DrainNodeReason.DRAIN_NODE_REASON_PREEMPTION
            reason_str = "Preempted due to max number of nodes per type={}.".format(
                termination_request.max_num_nodes_per_type
            )
        elif termination_request.cause == TerminationRequest.Cause.MAX_NUM_NODES:
            reason = DrainNodeReason.DRAIN_NODE_REASON_PREEMPTION
            reason_str = "Preempted due to max number of nodes={}.".format(
                termination_request.max_num_nodes
            )

        reply = self._gcs_client.drain_node(
            node_id=ray_node_id.encode("utf-8"),
            reason=reason,
            reason_message=reason_str,
            # TODO: we could probably add a deadline here that's derived
            # from the stuck instance reconcilation configs.
            deadline_timestamp_ms=0,
        )
        if reply.is_accepted:
            logger.info("Stopping ray on {}: {}".format(ray_node_id, reason_str))
        else:
            logger.warning(
                "Failed to stop ray on {}: {}".format(ray_node_id, reason_str)
            )
