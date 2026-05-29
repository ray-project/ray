import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from queue import Queue
from typing import List

from ray._common.utils import hex_to_binary
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


@dataclass(frozen=True)
class RayStopError:
    # Instance manager's instance id.
    im_instance_id: str


class RayStopper(InstanceUpdatedSubscriber):
    """RayStopper is responsible for stopping ray on instances.

    It will drain the ray node if it's for idle termination.
    For other terminations, it will stop the ray node. (e.g. scale down, etc.)

    If any failures happen when stopping/draining the node, we will not retry
    and rely on the reconciler to handle the failure.

    TODO: we could also surface the errors back to the reconciler for
    quicker failure detection.

    """

    def __init__(self, gcs_client: GcsClient, error_queue: Queue) -> None:
        self._gcs_client = gcs_client
        self._error_queue = error_queue
        self._executor = ThreadPoolExecutor(max_workers=1)

    def notify(self, events: List[InstanceUpdateEvent]) -> None:
        for event in events:
            if event.new_instance_status == Instance.RAY_STOP_REQUESTED:
                fut = self._executor.submit(self._stop_or_drain_ray, event)

                def _log_on_error(fut):
                    try:
                        fut.result()
                    except Exception:
                        logger.exception("Error stopping/drain ray.")

                fut.add_done_callback(_log_on_error)

    def _stop_or_drain_ray(self, event: InstanceUpdateEvent) -> None:
        """
        Stops or drains the ray node based on the termination request.
        """
        assert event.HasField("termination_request"), "Termination request is required."
        termination_request = event.termination_request
        ray_node_id = termination_request.ray_node_id
        instance_id = event.instance_id

        if termination_request.cause == TerminationRequest.Cause.IDLE:
            reason = DrainNodeReason.DRAIN_NODE_REASON_IDLE_TERMINATION
            reason_str = "Termination of node that's idle for {} seconds.".format(
                termination_request.idle_duration_ms / 1000
            )
            self._drain_ray_node(
                self._gcs_client,
                self._error_queue,
                ray_node_id,
                instance_id,
                reason,
                reason_str,
            )
            return

        # If it's not an idle termination, we stop the ray node.
        self._stop_ray_node(
            self._gcs_client, self._error_queue, ray_node_id, instance_id
        )

    @staticmethod
    def _drain_ray_node(
        gcs_client: GcsClient,
        error_queue: Queue,
        ray_node_id: str,
        instance_id: str,
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
        try:
            accepted, reject_msg_str = gcs_client.drain_node(
                node_id=ray_node_id,
                reason=reason,
                reason_message=reason_str,
                # TODO: we could probably add a deadline here that's derived
                # from the stuck instance reconciliation configs.
                deadline_timestamp_ms=0,
            )
            logger.info(
                f"Drained ray on {ray_node_id}(success={accepted}, "
                f"msg={reject_msg_str})"
            )
            if not accepted:
                error_queue.put_nowait(RayStopError(im_instance_id=instance_id))
        except Exception:
            logger.exception(f"Error draining ray on {ray_node_id}")
            error_queue.put_nowait(RayStopError(im_instance_id=instance_id))

    @staticmethod
    def _stop_ray_node(
        gcs_client: GcsClient,
        error_queue: Queue,
        ray_node_id: str,
        instance_id: str,
    ):
        """
        Stops the ray node.

        Args:
            gcs_client: The gcs client to use.
            ray_node_id: The ray node id to stop.
        """
        try:
            drained = gcs_client.drain_nodes(node_ids=[hex_to_binary(ray_node_id)])
            success = len(drained) > 0
            logger.info(
                f"Stopping ray on {ray_node_id}(instance={instance_id}): "
                f"success={success})"
            )

            if not success:
                error_queue.put_nowait(RayStopError(im_instance_id=instance_id))
        except Exception:
            logger.exception(
                f"Error stopping ray on {ray_node_id}(instance={instance_id})"
            )
            error_queue.put_nowait(RayStopError(im_instance_id=instance_id))
