import copy
import logging
import math
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from queue import Queue
from typing import Any, Dict, List, Optional, Tuple, Union

from ray.autoscaler._private.constants import (
    AUTOSCALER_MAX_CONCURRENT_TERMINATING,
    AUTOSCALER_MAX_CONCURRENT_TYPES_TO_LAUNCH,
)
from ray.autoscaler._private.node_launcher import BaseNodeLauncher
from ray.autoscaler._private.util import hash_launch_conf
from ray.autoscaler.node_provider import NodeProvider as NodeProviderV1
from ray.autoscaler.tags import (
    NODE_KIND_WORKER,
    STATUS_UNINITIALIZED,
    TAG_RAY_LAUNCH_CONFIG,
    TAG_RAY_LAUNCH_REQUEST,
    TAG_RAY_NODE_KIND,
    TAG_RAY_NODE_NAME,
    TAG_RAY_NODE_STATUS,
    TAG_RAY_USER_NODE_TYPE,
)
from ray.autoscaler.v2.schema import NodeType
from ray.autoscaler.v2.instance_manager.config import AutoscalingConfig, IConfigReader

logger = logging.getLogger(__name__)

# Type Alias. This is a **unique identifier** for a cloud node in the cluster.
# The node provider should guarantee that this id is unique across the cluster,
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
    # Update request id from which the cloud instance is launched.
    request_id: str
    # If the cloud instance is running.
    is_running: bool


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
    ) -> None:
        msg = f"Failed to launch {count} nodes of type {node_type} with request id {request_id}."
        super().__init__(msg, timestamp_ns=timestamp_ns)
        self.node_type = node_type
        self.count = count
        self.request_id = request_id

    def __repr__(self) -> str:
        return f"LaunchNodeError(node_type={self.node_type}, count={self.count}, request_id={self.request_id}): {self.__cause__}"


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
    ) -> None:
        msg = (
            f"Failed to terminate node {cloud_instance_id} with request id {request_id}"
        )
        super().__init__(msg, timestamp_ns=timestamp_ns)
        self.cloud_instance_id = cloud_instance_id
        self.request_id = request_id

    def __repr__(self) -> str:
        return f"TerminateNodeError(cloud_instance_id={self.cloud_instance_id}, request_id={self.request_id}): {self.__cause__}"


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
class LaunchArgs:
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
class TerminateArgs:
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
        max_concurrent_types_to_launch: int = AUTOSCALER_MAX_CONCURRENT_TYPES_TO_LAUNCH,
        max_concurrent_to_terminate: int = AUTOSCALER_MAX_CONCURRENT_TERMINATING,
        sync_launch: bool = False,
        sync_terminate: bool = False,
    ) -> None:
        super().__init__()
        self._v1_provider = v1_provider
        # Executor to async launching and terminating nodes.
        self._main_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="ray::NodeProviderAdapter"
        )
        self._node_launcher_executors = ThreadPoolExecutor(
            max_workers=max_concurrent_types_to_launch,
            thread_name_prefix="ray::NodeLauncherPool",
        )
        self._node_terminator_executors = ThreadPoolExecutor(
            max_workers=max_concurrent_to_terminate,
            thread_name_prefix="ray::NodeTerminatorPool",
        )
        self._config_reader = config_reader

        # Whether to sync launch and terminate.
        self._sync_launch = sync_launch
        self._sync_terminate = sync_terminate

        # Queues to retrieve new errors occur in the multi-thread executors
        # temporarily.
        self._launch_q = Queue()
        self._terminate_q = Queue()
        self._errors_q = Queue()

    def get_non_terminated(self) -> Dict[CloudInstanceId, CloudInstance]:
        # Get the current state of the node provider.
        nodes = {}

        cloud_instance_ids = self._v1_non_terminated_nodes({})
        # Filter out nodes that are not running.
        # This is efficient since the provider is expected to cache the
        # running status of the nodes.
        for cloud_instance_id in cloud_instance_ids:
            node_tags = self._v1_node_tags(cloud_instance_id)
            nodes[cloud_instance_id] = CloudInstance(
                cloud_instance_id=cloud_instance_id,
                node_type=node_tags.get(TAG_RAY_USER_NODE_TYPE, None),
                is_running=self._v1_is_running(cloud_instance_id),
                request_id=node_tags.get(TAG_RAY_LAUNCH_REQUEST, None),
            )

        return nodes

    def poll_errors(self) -> List[CloudInstanceProviderError]:
        errors = []
        while not self._errors_q.empty():
            errors.append(self._errors_q.get())
        return errors

    def launch(
        self,
        shape: Dict[NodeType, int],
        request_id: str,
    ) -> None:
        self._launch_q.put_nowait((shape, request_id))
        fut = self._main_executor.submit(self._process)
        if self._sync_launch:
            fut.result()

    def terminate(self, ids: List[CloudInstanceId], request_id: str) -> None:
        self._terminate_q.put_nowait((ids, request_id))
        fut = self._main_executor.submit(self._process)
        if self._sync_terminate:
            fut.result()

    ###########################################
    # Private APIs
    ###########################################

    def _process(self) -> None:
        """
        Process the launch and terminate requests in the queue.
        """
        if not self._v1_safe_to_scale():
            return
        futs_to_launch_args = self._process_launch()
        futs_to_terminate_args = self._process_terminate()

        # Wait for launch and terminate to finish.
        for fut in as_completed(futs_to_launch_args.keys()):
            self._wait_for_future(fut, arg=futs_to_launch_args[fut])

        for fut in as_completed(futs_to_terminate_args.keys()):
            self._wait_for_future(fut, arg=futs_to_terminate_args[fut])

        self._post_process(
            futs_to_launch_args.values(), futs_to_terminate_args.values()
        )

    def _wait_for_future(
        self, fut: Future, arg: Union[LaunchArgs, TerminateArgs]
    ) -> None:
        """
        Wait for the future to finish and handle the errors.
        """
        try:
            fut.result()
        except (TerminateNodeError, LaunchNodeError) as e:
            self._errors_q.put(e)
        except Exception as e:
            error = self._make_error(e, arg)
            self._errors_q.put(error)

    @staticmethod
    def _make_error(
        e: Exception, arg: Union[LaunchArgs, TerminateArgs]
    ) -> CloudInstanceProviderError:
        """
        Make an error from the exception and the arguments.
        """
        if isinstance(arg, LaunchArgs):
            launch_error = LaunchNodeError(
                node_type=arg.node_type,
                count=arg.count,
                request_id=arg.request_id,
                timestamp_ns=int(time.time_ns()),
            )
            launch_error.__cause__ = e
            return launch_error
        elif isinstance(arg, TerminateArgs):
            terminate_error = TerminateNodeError(
                cloud_instance_id=arg.cloud_instance_id,
                request_id=arg.request_id,
                timestamp_ns=int(time.time_ns()),
            )
            terminate_error.__cause__ = e
            return terminate_error
        else:
            logger.error(f"Unknown arg type {arg}")
            raise e

    def _post_process(
        self, launch_args: List[LaunchArgs], terminate_args: List[TerminateArgs]
    ) -> None:
        """
        Post process the provider.
        """
        try:
            self._v1_post_process()
        except Exception as e:
            # If post process failed, we will treat all launch and terminate requests
            # as failed. While this may introduce false negatives, we will have the
            # autoscaler IM to reconcile those (e.g. by shutting down the extra nodes
            # that were already launched but thought to be failed, or by terminating
            # a node that was already terminated, which is fine).
            for arg in launch_args:
                self._errors_q.put(self._make_error(e, arg))
            for arg in terminate_args:
                self._errors_q.put(self._make_error(e, arg))

    def _process_launch(self) -> Dict[Future, LaunchArgs]:
        """
        Process the launch requests in the launch queue.
        """
        futs_to_launch_args = {}
        config = self._config_reader.get_autoscaling_config()
        while not self._launch_q.empty():
            to_launch_shape, request_id = self._launch_q.get()
            # Submit to the launch pool.
            for node_type, count in to_launch_shape.items():
                futs_to_launch_args[
                    self._node_launcher_executors.submit(
                        self._launch_nodes_by_type,
                        node_type,
                        count,
                        request_id,
                        config,
                    )
                ] = LaunchArgs(node_type, count, request_id)

        return futs_to_launch_args

    def _process_terminate(self) -> Dict[Future, TerminateArgs]:
        """
        Process the terminate requests in the terminate queue.
        """
        futs_to_terminate_args = {}
        while not self._terminate_q.empty():
            to_terminate_ids, request_id = self._terminate_q.get()
            # Submit to the terminate pool.
            for cloud_instance_id in to_terminate_ids:
                futs_to_terminate_args[
                    self._node_terminator_executors.submit(
                        self._v1_terminate_node,
                        cloud_instance_id,
                    )
                ] = TerminateArgs(cloud_instance_id, request_id)

        return futs_to_terminate_args

    def _launch_nodes_by_type(
        self,
        node_type: NodeType,
        count: int,
        request_id: str,
        config: AutoscalingConfig,
    ) -> None:
        # Check node type is valid.
        if node_type not in config.get_node_type_configs():
            raise ValueError(f"Invalid node type {node_type}")

        launch_config = config.get_cloud_node_config(node_type)
        resources = config.get_node_resources(node_type)
        labels = config.get_node_labels(node_type)

        # This is to be compatible with the v1 node launcher.
        # See more in https://github.com/ray-project/ray/blob/6f5a189bc463e52c51a70f8aea41fb2950b443e8/python/ray/autoscaler/_private/node_launcher.py#L78-L85 # noqa
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

        self._v1_provider.create_node_with_resources_and_labels(
            launch_config, node_tags, count, resources, labels
        )

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

    def _v1_terminate_node(self, node_id: CloudInstanceId) -> Optional[Dict[str, Any]]:
        return self._v1_provider.terminate_node(node_id)

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
