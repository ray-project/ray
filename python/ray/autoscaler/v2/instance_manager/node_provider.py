import logging
import math
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from queue import Queue
from typing import Any, Dict, List, Optional

from ray.autoscaler._private.constants import (
    AUTOSCALER_MAX_CONCURRENT_LAUNCHES,
    AUTOSCALER_MAX_LAUNCH_BATCH,
)
from ray.autoscaler._private.util import hash_launch_conf
from ray.autoscaler.node_provider import NodeProvider as NodeProviderV1
from ray.autoscaler.tags import (
    NODE_KIND_HEAD,
    NODE_KIND_UNMANAGED,
    NODE_KIND_WORKER,
    STATUS_UNINITIALIZED,
    TAG_RAY_LAUNCH_CONFIG,
    TAG_RAY_LAUNCH_REQUEST,
    TAG_RAY_NODE_KIND,
    TAG_RAY_NODE_NAME,
    TAG_RAY_NODE_STATUS,
    TAG_RAY_USER_NODE_TYPE,
)
from ray.autoscaler.v2.instance_manager.config import IConfigReader
from ray.autoscaler.v2.schema import NodeType
from ray.core.generated.instance_manager_pb2 import NodeKind

logger = logging.getLogger(__name__)

# Type Alias. This is a **unique identifier** for a cloud instance in the cluster.
# The provider should guarantee that this id is unique across the cluster,
# such that:
#   - When a cloud instance is created and running, no other cloud instance in the
#     cluster has the same id.
#   - When a cloud instance is terminated, no other cloud instance in the cluster will
#     be assigned the same id later.
CloudInstanceId = str


@dataclass
class CloudInstance:
    """
    A class that represents a cloud instance in the cluster, with necessary metadata
    of the cloud instance.
    """

    # The cloud instance id.
    cloud_instance_id: CloudInstanceId
    # The node type of the cloud instance.
    node_type: NodeType
    # The node kind, i.e head or worker.
    node_kind: NodeKind
    # If the cloud instance is already running.
    is_running: bool
    # Update request id from which the cloud instance is launched.
    # This could be None if the cloud instance couldn't be associated with requests
    # by the cloud provider: e.g. cloud provider doesn't support per-instance
    # extra metadata.
    # This is fine for now since the reconciler should be able to know how
    # to handle cloud instances w/o request ids.
    # TODO: make this a required field.
    request_id: Optional[str] = None


class CloudInstanceProviderError(Exception):
    """
    An base error class that represents an error that happened in the cloud instance
    provider.
    """

    # The timestamp of the error occurred in nanoseconds.
    timestamp_ns: int

    def __init__(self, msg, timestamp_ns) -> None:
        super().__init__(msg)
        self.timestamp_ns = timestamp_ns


class LaunchNodeError(CloudInstanceProviderError):
    # The node type that failed to launch.
    node_type: NodeType
    # Number of nodes that failed to launch.
    count: int
    # A unique id that identifies from which update request the error originates.
    request_id: str

    def __init__(
        self,
        node_type: NodeType,
        count: int,
        request_id: str,
        timestamp_ns: int,
        details: str = "",
        cause: Optional[Exception] = None,
    ) -> None:
        msg = (
            f"Failed to launch {count} nodes of type {node_type} with "
            f"request id {request_id}: {details}"
        )
        super().__init__(msg, timestamp_ns=timestamp_ns)
        self.node_type = node_type
        self.count = count
        self.request_id = request_id
        if cause:
            self.__cause__ = cause

    def __repr__(self) -> str:
        return (
            f"LaunchNodeError(node_type={self.node_type}, count={self.count}, "
            f"request_id={self.request_id}): {self.__cause__}"
        )


class TerminateNodeError(CloudInstanceProviderError):
    # The cloud instance id of the node that failed to terminate.
    cloud_instance_id: CloudInstanceId
    # A unique id that identifies from which update request the error originates.
    request_id: str

    def __init__(
        self,
        cloud_instance_id: CloudInstanceId,
        request_id: str,
        timestamp_ns: int,
        details: str = "",
        cause: Optional[Exception] = None,
    ) -> None:
        msg = (
            f"Failed to terminate node {cloud_instance_id} with "
            f"request id {request_id}: {details}"
        )
        super().__init__(msg, timestamp_ns=timestamp_ns)
        self.cloud_instance_id = cloud_instance_id
        self.request_id = request_id
        if cause:
            self.__cause__ = cause

    def __repr__(self) -> str:
        return (
            f"TerminateNodeError(cloud_instance_id={self.cloud_instance_id}, "
            f"request_id={self.request_id}): {self.__cause__}"
        )


class ICloudInstanceProvider(ABC):
    """
    The interface for a cloud instance provider.

    This interface is a minimal interface that should be implemented by the
    various cloud instance providers (e.g. AWS, and etc).

    The cloud instance provider is responsible for managing the cloud instances in the
    cluster. It provides the following main functionalities:
        - Launch new cloud instances.
        - Terminate existing running instances.
        - Get the non-terminated cloud instances in the cluster.
        - Poll the errors that happened for the updates to the cloud instance provider.

    Below properties of the cloud instance provider are assumed with this interface:

    1. Eventually consistent
    The cloud instance provider is expected to be eventually consistent with the
    cluster state. For example, when a cloud instance is request to be terminated
    or launched, the provider may not immediately reflect the change in its state.
    However, the provider is expected to eventually reflect the change in its state.

    2. Asynchronous
    The provider could also be asynchronous, where the termination/launch
    request may not immediately return the result of the request.

    3. Unique cloud instance ids
    Cloud instance ids are expected to be unique across the cluster.

    4. Idempotent updates
    For the update APIs (e.g. ensure_min_nodes, terminate), the provider may use the
    request ids to provide idempotency.

    Usage:
        ```
            provider: ICloudInstanceProvider = ...

            # Update the cluster with a desired shape.
            provider.launch(
                shape={
                    "worker_nodes": 10,
                    "ray_head": 1,
                },
                request_id="1",
            )

            # Get the non-terminated nodes of the cloud instance provider.
            running = provider.get_non_terminated()

            # Poll the errors
            errors = provider.poll_errors()

            # Terminate nodes.
            provider.terminate(
                ids=["cloud_instance_id_1", "cloud_instance_id_2"],
                request_id="2",
            )

            # Process the state of the provider.
            ...
        ```
    """

    @abstractmethod
    def get_non_terminated(self) -> Dict[CloudInstanceId, CloudInstance]:
        """Get the non-terminated cloud instances in the cluster.

        Returns:
            A dictionary of the non-terminated cloud instances in the cluster.
            The key is the cloud instance id, and the value is the cloud instance.
        """
        pass

    @abstractmethod
    def terminate(self, ids: List[CloudInstanceId], request_id: str) -> None:
        """
        Terminate the cloud instances asynchronously.

        This method is expected to be idempotent, i.e. if the same request id is used
        to terminate the same cloud instances, this should be a no-op if
        the cloud instances are already terminated or being terminated.

        Args:
            ids: the cloud instance ids to terminate.
            request_id: a unique id that identifies the request.
        """
        pass

    @abstractmethod
    def launch(
        self,
        shape: Dict[NodeType, int],
        request_id: str,
    ) -> None:
        """Launch the cloud instances asynchronously.

        Args:
            shape: A map from node type to number of nodes to launch.
            request_id: a unique id that identifies the update request.
        """
        pass

    @abstractmethod
    def poll_errors(self) -> List[CloudInstanceProviderError]:
        """
        Poll the errors that happened since the last poll.

        This method would also clear the errors that happened since the last poll.

        Returns:
            The errors that happened since the last poll.
        """
        pass


@dataclass(frozen=True)
class CloudInstanceLaunchRequest:
    """
    The arguments to launch a node.
    """

    # The node type to launch.
    node_type: NodeType
    # Number of nodes to launch.
    count: int
    # A unique id that identifies the request.
    request_id: str


@dataclass(frozen=True)
class CloudInstanceTerminateRequest:
    """
    The arguments to terminate a node.
    """

    # The cloud instance id of the node to terminate.
    cloud_instance_id: CloudInstanceId
    # A unique id that identifies the request.
    request_id: str


class NodeProviderAdapter(ICloudInstanceProvider):
    """
    Warps a NodeProviderV1 to a ICloudInstanceProvider.

    TODO(rickyx):
    The current adapter right now consists of two sets of APIs:
    - v1: the old APIs that are used by the autoscaler, where
    we forward the calls to the NodeProviderV1.
    - v2: the new APIs that are used by the autoscaler v2, this is
    defined in the ICloudInstanceProvider interface.

    We should eventually remove the v1 APIs and only use the v2 APIs.
    It's currently left as a TODO since changing the v1 APIs would
    requires a lot of changes in the cluster launcher codebase.
    """

    def __init__(
        self,
        v1_provider: NodeProviderV1,
        config_reader: IConfigReader,
        max_launch_batch_per_type: int = AUTOSCALER_MAX_LAUNCH_BATCH,
        max_concurrent_launches: int = AUTOSCALER_MAX_CONCURRENT_LAUNCHES,
    ) -> None:
        """
        Args:
            v1_provider: The v1 node provider to wrap.
            config_reader: The config reader to read the autoscaling config.
            max_launch_batch_per_type: The maximum number of nodes to launch per
                node type in a single batch.
            max_concurrent_launches: The maximum number of concurrent launches.
        """

        super().__init__()
        self._v1_provider = v1_provider
        self._config_reader = config_reader
        # Executor to async launching and terminating nodes.
        self._main_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="ray::NodeProviderAdapter"
        )

        # v1 legacy rate limiting on the node provider launch calls.
        self._max_launch_batch_per_type = max_launch_batch_per_type
        max_batches = math.ceil(
            max_concurrent_launches / float(max_launch_batch_per_type)
        )
        self._node_launcher_executors = ThreadPoolExecutor(
            max_workers=max_batches,
            thread_name_prefix="ray::NodeLauncherPool",
        )

        # Queue to retrieve new errors occur in the multi-thread executors
        # temporarily.
        self._errors_queue = Queue()

    def get_non_terminated(self) -> Dict[CloudInstanceId, CloudInstance]:
        nodes = {}

        cloud_instance_ids = self._v1_non_terminated_nodes({})
        # Filter out nodes that are not running.
        # This is efficient since the provider is expected to cache the
        # running status of the nodes.
        for cloud_instance_id in cloud_instance_ids:
            node_tags = self._v1_node_tags(cloud_instance_id)
            node_kind_tag = node_tags.get(TAG_RAY_NODE_KIND, NODE_KIND_UNMANAGED)
            if node_kind_tag == NODE_KIND_UNMANAGED:
                # Filter out unmanaged nodes.
                continue
            elif node_kind_tag == NODE_KIND_WORKER:
                node_kind = NodeKind.WORKER
            elif node_kind_tag == NODE_KIND_HEAD:
                node_kind = NodeKind.HEAD
            else:
                raise ValueError(f"Invalid node kind: {node_kind_tag}")

            nodes[cloud_instance_id] = CloudInstance(
                cloud_instance_id=cloud_instance_id,
                node_type=node_tags.get(TAG_RAY_USER_NODE_TYPE, ""),
                is_running=self._v1_is_running(cloud_instance_id),
                request_id=node_tags.get(TAG_RAY_LAUNCH_REQUEST, ""),
                node_kind=node_kind,
            )

        return nodes

    def poll_errors(self) -> List[CloudInstanceProviderError]:
        errors = []
        while not self._errors_queue.empty():
            errors.append(self._errors_queue.get_nowait())
        return errors

    def launch(
        self,
        shape: Dict[NodeType, int],
        request_id: str,
    ) -> None:
        self._main_executor.submit(self._do_launch, shape, request_id)

    def terminate(self, ids: List[CloudInstanceId], request_id: str) -> None:
        self._main_executor.submit(self._do_terminate, ids, request_id)

    ###########################################
    # Private APIs
    ###########################################

    def _do_launch(
        self,
        shape: Dict[NodeType, int],
        request_id: str,
    ) -> None:
        """
        Launch the cloud instances by calling into the v1 base node provider.

        Args:
            shape: The requested to launch node type and number of nodes.
            request_id: The request id that identifies the request.
        """
        for node_type, count in shape.items():
            # Keep submitting the launch requests to the launch pool in batches.
            while count > 0:
                to_launch = min(count, self._max_launch_batch_per_type)
                self._node_launcher_executors.submit(
                    self._launch_nodes_by_type,
                    node_type,
                    to_launch,
                    request_id,
                )
                count -= to_launch

    def _do_terminate(self, ids: List[CloudInstanceId], request_id: str) -> None:
        """
        Terminate the cloud instances by calling into the v1 base node provider.

        If errors happen during the termination, the errors will be put into the
        errors queue.

        Args:
            ids: The cloud instance ids to terminate.
            request_id: The request id that identifies the request.
        """

        try:
            self._v1_terminate_nodes(ids)
        except Exception as e:
            for id in ids:
                error = TerminateNodeError(id, request_id, int(time.time_ns()))
                error.__cause__ = e
                self._errors_queue.put(error)

    def _launch_nodes_by_type(
        self,
        node_type: NodeType,
        count: int,
        request_id: str,
    ) -> None:
        """
        Launch nodes of the given node type.

        Args:
            node_type: The node type to launch.
            count: Number of nodes to launch.
            request_id: A unique id that identifies the request.

        Raises:
            ValueError: If the node type is invalid.
            LaunchNodeError: If the launch failed and raised by the underlying provider.
        """
        # Check node type is valid.
        try:
            config = self._config_reader.get_cached_autoscaling_config()
            launch_config = config.get_cloud_node_config(node_type)
            resources = config.get_node_resources(node_type)
            labels = config.get_node_labels(node_type)

            # This is to be compatible with the v1 node launcher.
            # See more in https://github.com/ray-project/ray/blob/6f5a189bc463e52c51a70f8aea41fb2950b443e8/python/ray/autoscaler/_private/node_launcher.py#L78-L85 # noqa
            # TODO: this should be synced with what's stored in the IM, it should
            # probably be made as a metadata field in the cloud instance. This is
            # another incompatibility with KubeRay.
            launch_hash = hash_launch_conf(launch_config, config.get_config("auth", {}))
            node_tags = {
                TAG_RAY_NODE_NAME: "ray-{}-worker".format(
                    config.get_config("cluster_name", "")
                ),
                TAG_RAY_NODE_KIND: NODE_KIND_WORKER,
                TAG_RAY_NODE_STATUS: STATUS_UNINITIALIZED,
                TAG_RAY_LAUNCH_CONFIG: launch_hash,
                TAG_RAY_LAUNCH_REQUEST: request_id,
                TAG_RAY_USER_NODE_TYPE: node_type,
            }

            logger.info("Launching {} nodes of type {}.".format(count, node_type))
            self._v1_provider.create_node_with_resources_and_labels(
                launch_config, node_tags, count, resources, labels
            )
            logger.info("Launched {} nodes of type {}.".format(count, node_type))
        except Exception as e:
            error = LaunchNodeError(node_type, count, request_id, int(time.time_ns()))
            error.__cause__ = e
            self._errors_queue.put(error)

    ###########################################
    # V1 Legacy APIs
    ###########################################
    """
    Below are the necessary legacy APIs from the V1 node provider.
    These are needed as of now to provide the needed features
    for V2 node provider.
    The goal is to eventually remove these APIs and only use the
    V2 APIs by modifying the individual node provider to inherit
    from ICloudInstanceProvider.
    """

    def _v1_terminate_nodes(
        self, ids: List[CloudInstanceId]
    ) -> Optional[Dict[str, Any]]:
        return self._v1_provider.terminate_nodes(ids)

    def _v1_non_terminated_nodes(
        self, tag_filters: Dict[str, str]
    ) -> List[CloudInstanceId]:
        return self._v1_provider.non_terminated_nodes(tag_filters)

    def _v1_is_running(self, node_id: CloudInstanceId) -> bool:
        return self._v1_provider.is_running(node_id)

    def _v1_post_process(self) -> None:
        self._v1_provider.post_process()

    def _v1_node_tags(self, node_id: CloudInstanceId) -> Dict[str, str]:
        return self._v1_provider.node_tags(node_id)

    def _v1_safe_to_scale(self) -> bool:
        return self._v1_provider.safe_to_scale()
