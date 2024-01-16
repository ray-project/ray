import logging
import math
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from queue import Queue
from typing import Any, Dict, List, Optional

from ray.autoscaler._private.constants import (
    AUTOSCALER_MAX_CONCURRENT_LAUNCHES,
    AUTOSCALER_MAX_CONCURRENT_TERMINATING,
    AUTOSCALER_MAX_LAUNCH_BATCH,
)
from ray.autoscaler._private.node_launcher import BaseNodeLauncher
from ray.autoscaler.node_provider import NodeProvider as NodeProviderV1
from ray.autoscaler.tags import TAG_RAY_USER_NODE_TYPE
from ray.autoscaler.v2.schema import NodeType
from ray.autoscaler.v2.instance_manager.config import AutoscalingConfig

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


@dataclass
class CloudInstanceProviderError:
    """
    An base error class that represents an error that happened in the cloud instance
    provider.
    """

    # The exception that caused the error.
    exception: Optional[Exception]
    # The details of the error.
    details: Optional[str]
    # The timestamp of the error occurred in nanoseconds.
    timestamp_ns: int


@dataclass
class LaunchNodeError(CloudInstanceProviderError):
    # The node type that failed to launch.
    node_type: NodeType
    # Number of nodes that failed to launch.
    count: int
    # A unique id that identifies from which update request the error originates.
    request_id: str


@dataclass
class TerminateNodeError(CloudInstanceProviderError):
    # The cloud instance id of the node that failed to terminate.
    cloud_instance_id: CloudInstanceId
    # From which update request the error originates.
    request_id: str


class ICloudInstanceProvider(ABC):
    """
    The interface for a cloud instance provider.

    This interface is a minimal interface that should be implemented by the
    various cloud instance providers (e.g. AWS, and etc).

    The cloud instance provider is responsible for managing the cloud instances in the
    cluster. It provides the following main functionalities:
        - Launch new cloud instances.
        - Terminate existing running instances.
        - Get the running cloud instances in the cluster.
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

            # Get the running nodes of the cloud instance provider.
            running = provider.get_running()

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
    def get_running(self) -> Dict[CloudInstanceId, CloudInstance]:
        """Get the running cloud instances in the cluster.

        Returns:
            A dictionary of the running cloud instances in the cluster.
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
        provider: NodeProviderV1,
        node_launcher: BaseNodeLauncher,
        autoscaling_config: AutoscalingConfig,
        max_concurrent_launches: int = AUTOSCALER_MAX_CONCURRENT_LAUNCHES,
        max_launch_batch: int = AUTOSCALER_MAX_LAUNCH_BATCH,
        max_concurrent_terminating: int = AUTOSCALER_MAX_CONCURRENT_TERMINATING,
    ) -> None:
        super().__init__()
        self._provider = provider
        self._node_launcher = node_launcher
        # Executor to async launching and terminating nodes.
        self._main_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="ray::NodeProviderAdapter"
        )
        self._node_launcher_executors = ThreadPoolExecutor(
            max_workers=math.ceil(
                max_concurrent_launches / float(max_launch_batch)
                if max_launch_batch > 0
                else 1
            ),
            thread_name_prefix="ray::NodeLauncherPool",
        )
        self._node_terminator_executors = ThreadPoolExecutor(
            max_workers=max_concurrent_terminating,
            thread_name_prefix="ray::NodeTerminatorPool",
        )
        self._config = autoscaling_config

        # Queues to retrieve new errors occur in the multi-thread executors
        # temporarily.
        self._update_q = Queue()
        self._launch_errors_q = Queue()
        self._termination_errors_q = Queue()

    def get_state(self) -> CloudNodeProviderState:
        # Get new errors
        launch_errors = []
        termination_errors = []

        while not self._launch_errors_q.empty():
            launch_errors.append(self._launch_errors_q.get_nowait())

        while not self._termination_errors_q.empty():
            termination_errors.append(self._termination_errors_q.get_nowait())

        # Get the current state of the node provider.
        running_nodes = {}

        cloud_instance_ids = self._v1_non_terminated_nodes({})
        # Filter out nodes that are not running.
        # This is efficient since the provider is expected to cache the
        # running status of the nodes.
        for cloud_instance_id in cloud_instance_ids:
            if not self._v1_is_running(cloud_instance_id):
                continue

            node_tags = self._v1_node_tags(cloud_instance_id)
            running_nodes[cloud_instance_id] = CloudInstance(
                cloud_instance_id=cloud_instance_id,
                node_type=node_tags.get(TAG_RAY_USER_NODE_TYPE, None),
            )

        return CloudNodeProviderState(
            running_nodes=running_nodes,
            launch_errors=launch_errors,
            termination_errors=termination_errors,
        )

    def update(
        self,
        id: str,
        target_running_nodes: Dict[NodeType, int],
        to_terminate: Optional[List[CloudInstanceId]] = None,
        wait: bool = False,
    ) -> None:
        if to_terminate is None:
            to_terminate = []

        self._update_q.put_nowait((id, target_running_nodes, to_terminate))

        fut = self._main_executor.submit(self._update)
        if wait:
            fut.result()

    def _update(self) -> None:
        all_to_terminate = {}
        target_running_nodes = None
        update_id = None
        assert not self._update_q.empty()
        while not self._update_q.empty():
            # We will only consider the latest update for launching nodes since
            # we only want to launch nodes for the latest update request.
            update_id, target_running_nodes, to_terminate = self._update_q.get()

            # We accumulate the to_terminate list from all the update requests.
            # This is to ensure that we terminate all the nodes that are requested
            # to be terminated.
            for node_id in to_terminate:
                all_to_terminate[node_id] = update_id

        assert target_running_nodes is not None

        to_launch = self._compute_to_launch(target_running_nodes, to_terminate)

        logger.debug(
            f"Launching nodes: {dict(to_launch)}; Terminating nodes: {all_to_terminate}"
        )

        futs_to_updates = {}
        # Launch nodes in the thread pool.
        for node_type, count in to_launch.items():
            futs_to_updates[
                self._node_launcher_executors.submit(
                    self._launch_nodes_by_type,
                    node_type,
                    count,
                )
            ] = ("launch", (node_type, count, id))

        # Terminate nodes in the thread pool.
        for cloud_instance_id, update_id in all_to_terminate.items():
            futs_to_updates[
                self._node_terminator_executors.submit(
                    self._v1_terminate_node,
                    cloud_instance_id,
                )
            ] = ("terminate", (cloud_instance_id, update_id))

        # Wait for all the futures to finish.
        for fut in as_completed(futs_to_updates):
            update_type, update_args = futs_to_updates[fut]
            try:
                fut.result()
            except Exception as e:
                if update_type == "launch":
                    node_type, count, update_id = update_args
                    self._launch_errors_q.put_nowait(
                        LaunchNodeError(
                            exception=e,
                            details=str(e),
                            timestamp_ns=time.time_ns(),
                            node_type=node_type,
                            count=count,
                            update_id=update_id,
                        )
                    )
                elif update_type == "terminate":
                    cloud_instance_id, update_id = update_args
                    self._termination_errors_q.put_nowait(
                        TerminateNodeError(
                            exception=e,
                            details=str(e),
                            timestamp_ns=time.time_ns(),
                            cloud_instance_id=cloud_instance_id,
                            update_id=update_id,
                        )
                    )
        self._v1_post_process()

    ###########################################
    # Private APIs
    ###########################################

    def _compute_to_launch(
        self,
        target_running_nodes: Dict[NodeType, int],
        to_terminate: List[CloudInstanceId],
    ) -> Dict[NodeType, int]:
        """
        Compute the number of nodes to launch for each node type.

        """
        terminating_nodes = set(to_terminate)
        terminating_nodes_by_type = defaultdict(int)
        for cloud_instance_id in terminating_nodes:
            node_tags = self._v1_node_tags(cloud_instance_id)
            node_type = node_tags.get(TAG_RAY_USER_NODE_TYPE, None)
            if node_type:
                terminating_nodes_by_type[node_type] += 1
            else:
                logger.warning(
                    f"Node {cloud_instance_id} does not have a node type tag."
                )

        non_terminated_nodes = self._v1_non_terminated_nodes({})
        non_terminated_nodes_by_type = defaultdict(int)
        for cloud_instance_id in non_terminated_nodes:
            node_tags = self._v1_node_tags(cloud_instance_id)
            node_type = node_tags.get(TAG_RAY_USER_NODE_TYPE, None)
            if node_type:
                non_terminated_nodes_by_type[node_type] += 1
            else:
                logger.warning(
                    f"Node {cloud_instance_id} does not have a node type tag."
                )

        # Compute the number of nodes to launch for each node type.
        to_launch = defaultdict(int)
        for node_type, target_count in target_running_nodes.items():
            non_terminated_count = non_terminated_nodes_by_type[node_type]
            terminating_count = terminating_nodes_by_type[node_type]

            if non_terminated_count - terminating_count < target_count:
                to_launch[node_type] = target_count - (
                    non_terminated_count - terminating_count
                )
        return to_launch

    def _launch_nodes_by_type(self, node_type: NodeType, count: int) -> None:
        # Launch nodes by type.
        self._node_launcher.launch_node(
            config=self._config.get_raw_config_mutable(),
            count=count,
            node_type=node_type,
            raise_exception=True,
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
        return self._provider.terminate_node(node_id)

    def _v1_non_terminated_nodes(
        self, tag_filters: Dict[str, str]
    ) -> List[CloudInstanceId]:
        return self._provider.non_terminated_nodes(tag_filters)

    def _v1_is_running(self, node_id: CloudInstanceId) -> bool:
        return self._provider.is_running(node_id)

    def _v1_post_process(self) -> None:
        self._provider.post_process()

    def _v1_node_tags(self, node_id: CloudInstanceId) -> Dict[str, str]:
        return self._provider.node_tags(node_id)
