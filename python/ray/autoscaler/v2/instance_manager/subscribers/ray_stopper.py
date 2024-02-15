import logging
from concurrent.futures import ThreadPoolExecutor
from typing import List
from ray._private.utils import hex_to_binary

from ray._raylet import GcsClient
from ray.autoscaler.v2.instance_manager.instance_manager import (
    InstanceUpdatedSubscriber,
)
from ray.core.generated.autoscaler_pb2 import DrainNodeReason
from ray.core.generated.instance_manager_pb2 import (
    Instance,
    InstanceUpdateEvent,
    TerminationRequest,
)

logger = logging.getLogger(__name__)


class RayStopper(InstanceUpdatedSubscriber):
    """RayStopper is responsible for stopping ray on instances.

    It will drain the ray node if it's for idle termination.
    For other terminations, it will stop the ray node. (e.g. scale down, etc.)

    If any failures happen when stopping/draining the node, we will not retry
    and rely on the reconciler to handle the failure.

    TODO: we could also surface the errors back to the reconciler for
    quicker failure detection.

    """

    def __init__(self, gcs_client: GcsClient) -> None:
        self._gcs_client = gcs_client
        self._executor = ThreadPoolExecutor(max_workers=1)

    def notify(self, events: List[InstanceUpdateEvent]) -> None:
        for event in events:
            if event.new_instance_status == Instance.RAY_STOPPING:
                fut = self._executor.submit(self._stop_or_drain_ray, event)
                fut.add_done_callback(lambda f: logger.info(f.result()))

    def _stop_or_drain_ray(self, event: InstanceUpdateEvent) -> None:
        """
        Stops or drains the ray node based on the termination request.
        """
        if not event.HasField("termination_request"):
            # This is a no-op if the event doesn't have a termination request.
            # This happens when we sync the ray node's DRAINING status to the
            # instance manager.
            return

        termination_request = event.termination_request
        ray_node_id = termination_request.ray_node_id

        if termination_request.cause == TerminationRequest.Cause.IDLE:
            reason = DrainNodeReason.DRAIN_NODE_REASON_IDLE_TERMINATION
            reason_str = "Idle termination of node for {} seconds.".format(
                termination_request.idle_duration_ms / 1000
            )
            self._drain_ray_node(self._gcs_client, ray_node_id, reason, reason_str)
            return

        # If it's not an idle termination, we stop the ray node.
        self._stop_ray_node(self._gcs_client, ray_node_id)

    @staticmethod
    def _drain_ray_node(
        gcs_client: GcsClient,
        ray_node_id: str,
        reason: DrainNodeReason,
        reason_str: str,
    ):
        """
        Drains the ray node.

        Args:
            gcs_client: The gcs client to use.
            ray_node_id: The ray node id to drain.
            reason: The reason to drain the node.
            reason_str: The reason message to drain the node.
        """
        is_accepted = gcs_client.drain_node(
            node_id=ray_node_id,
            reason=reason,
            reason_message=reason_str,
            # TODO: we could probably add a deadline here that's derived
            # from the stuck instance reconcilation configs.
            deadline_timestamp_ms=0,
        )
        if is_accepted:
            logger.info("Draining ray on {}: {}".format(ray_node_id, reason_str))
        else:
            # We could later add a retry mechanism here, as of now, the reconciler
            # should figure out that the stop fails with timeouts, and will
            # shutdown the underlying cloud instance.
            logger.warning(
                "Failed to drain ray on {}: {}".format(ray_node_id, reason_str)
            )

    @staticmethod
    def _stop_ray_node(
        gcs_client: GcsClient,
        ray_node_id: str,
    ):
        """
        Stops the ray node.

        Args:
            gcs_client: The gcs client to use.
            ray_node_id: The ray node id to stop.
        """
        drained = gcs_client.drain_nodes(node_ids=[hex_to_binary(ray_node_id)])

        if len(drained) > 0:
            logger.info("Stopping ray on {}".format(ray_node_id))
        else:
            # We could later add a retry mechanism here, as of now, the reconciler
            # should figure out that the stop fails with timeouts, and will
            # shutdown the underlying cloud instance.
            logger.warning("Failed to stop ray on {}".format(ray_node_id))
