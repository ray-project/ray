import logging
import uuid
from collections import defaultdict
from typing import List

from ray.autoscaler.v2.instance_manager.instance_manager import (
    InstanceUpdatedSubscriber,
)
from ray.autoscaler.v2.instance_manager.node_provider import ICloudInstanceProvider
from ray.core.generated.instance_manager_pb2 import Instance, InstanceUpdateEvent

logger = logging.getLogger(__name__)


class CloudInstanceUpdater(InstanceUpdatedSubscriber):
    """CloudInstanceUpdater is responsible for launching
    new instances and terminating cloud instances

    It requests the cloud instance provider to launch new instances when
    there are new instance requests (with REQUESTED status change).

    It requests the cloud instance provider to terminate instances when
    there are new instance terminations (with TERMINATING status change).

    The cloud instance APIs are async and non-blocking.
    """

    def __init__(
        self,
        cloud_provider: ICloudInstanceProvider,
    ) -> None:
        self._cloud_provider = cloud_provider

    def notify(self, events: List[InstanceUpdateEvent]) -> None:
        new_requests = [
            event for event in events if event.new_instance_status == Instance.REQUESTED
        ]
        new_terminations = [
            event
            for event in events
            if event.new_instance_status == Instance.TERMINATING
        ]
        self._launch_new_instances(new_requests)
        self._terminate_instances(new_terminations)

    def _terminate_instances(self, new_terminations: List[InstanceUpdateEvent]):
        """
        Terminate cloud instances through cloud provider.

        Args:
            new_terminations: List of new instance terminations.
        """
        if not new_terminations:
            logger.debug("No instances to terminate.")
            return

        # Terminate the instances.
        cloud_instance_ids = [event.cloud_instance_id for event in new_terminations]

        # This is an async call.
        self._cloud_provider.terminate(
            ids=cloud_instance_ids, request_id=str(uuid.uuid4())
        )

    def _launch_new_instances(self, new_requests: List[InstanceUpdateEvent]):
        """
        Launches new instances by requesting the cloud provider.

        Args:
            new_requests: List of new instance requests.

        """
        if not new_requests:
            logger.debug("No instances to launch.")
            return

        # Group new requests by launch request id.
        requests_by_launch_request_id = defaultdict(list)

        for event in new_requests:
            assert (
                event.launch_request_id
            ), "Launch request id should have been set by the reconciler"
            requests_by_launch_request_id[event.launch_request_id].append(event)

        for launch_request_id, events in requests_by_launch_request_id.items():
            request_shape = defaultdict(int)
            for event in events:
                request_shape[event.instance_type] += 1
            # Make requests to the cloud provider.
            self._cloud_provider.launch(
                shape=request_shape, request_id=launch_request_id
            )
