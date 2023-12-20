import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
import math
from queue import Queue
from typing import Any, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from ray.autoscaler._private.constants import AUTOSCALER_MAX_CONCURRENT_LAUNCHES, AUTOSCALER_MAX_CONCURRENT_TERMINATING, AUTOSCALER_MAX_LAUNCH_BATCH

from ray.autoscaler._private.node_launcher import BaseNodeLauncher
from ray.autoscaler.node_provider import NodeProvider as NodeProviderV1
from ray.autoscaler.tags import TAG_RAY_USER_NODE_TYPE
from ray.autoscaler.v2.instance_manager.config import NodeProviderConfig
from ray.autoscaler.v2.schema import NodeType
from ray.core.generated.instance_manager_pb2 import Instance

logger = logging.getLogger(__name__)

# Type Alias. This is a **unique identifier** for a cloud node in the cluster.
# The node provider should guarantee that this id is unique across the cluster,
# such that:
#   - When a cloud node is created and running, no other cloud node in the
#     cluster has the same id.
#   - When a cloud node is terminated, no other cloud node in the cluster should
#     be assigned the same id later.
CloudInstanceId = str


@dataclass
class CloudInstance:
    # The cloud instance id.
    cloud_instance_id: CloudInstanceId
    # The node type of the cloud instance.
    node_type: NodeType


@dataclass
class CloudNodeProviderError:
    """
    An error class that represents an error that happened in the cloud node provider.
    """

    # The exception that caused the error.
    exception: Optional[Exception] = None
    # The details of the error.
    details: Optional[str] = None
    # The timestamp of the error in nanoseconds.
    timestamp_ns: int

@dataclass
class LaunchNodeError(CloudNodeProviderError):
    # The node type that failed to launch.
    node_type: NodeType
    # Number of nodes that failed to launch.
    count: int
    # From which update request the error originates.
    update_id: str


@dataclass
class TerminateNodeError(CloudNodeProviderError):
    # The cloud instance id of the node that failed to terminate.
    cloud_instance_id: CloudInstanceId
    # From which update request the error originates.
    update_id: str


@dataclass
class CloudNodeProviderState:
    """
    The state of a cloud node provider.
    """
    # The terminated cloud nodes in the cluster.
    terminated: List[CloudInstanceId] = field(default_factory=list)
    # The cloud nodes that are currently running.
    running: Dict[CloudInstanceId, CloudInstance] = field(default_factory=dict)
    # Errors that have happened when launching nodes.
    launch_errors: List[LaunchNodeError] = field(default_factory=list)
    # Errors that have happened when terminating nodes.
    termination_errors: List[TerminateNodeError] = field(default_factory=list)


class ICloudNodeProvider(ABC):
    """
    The interface for a cloud node provider.

    This interface is a minimal interface that should be implemented by the
    various cloud node providers (e.g. AWS, and etc).

    The cloud node provider is responsible for managing the cloud nodes in the
    cluster. It provides the following main functionalities:
        - Launch new cloud nodes.
        - Terminate cloud nodes.
        - Get the running cloud nodes in the cluster.

    Below properties of the cloud node provider are assumed with this interface:

    1. Eventually consistent
    The cloud node provider is expected to be eventually consistent with the
    cluster state. For example, when a node is request to be terminated/launched,
    the node provider may not immediately reflect the change in its state.

    2. Asynchronous
    The node provider could also be asynchronous, where the termination/launch
    request may not immediately return the result of the request.

    3. Unique cloud node ids
    Cloud node ids are expected to be unique across the cluster.

    Usage:
        ```
            cloud_node_provider: ICloudNodeProvider = ...

            # Update the cluster with a designed shape.
            cloud_node_provider.update(
                id="update_1",
                target_running_nodes={
                    "worker_nodes": 10,
                    "ray_head": 1,
                },
                to_terminate=["node_1", "node_2"],
            )

            # Poll the state of the cloud node provider.
            state = cloud_node_provider.get_state()

            # Process the state of the cloud node provider.
        ```
    """

    @abstractmethod
    def get_state(self, errors_since_ns: Optional[int]) -> CloudNodeProviderState:
        """Get the current state of the cloud node provider.

        Args:
            errors_since_ns: retrieve errors that happened since this timestamp.

        Returns:
            The current state of the cloud node provider.
        """
        pass

    @abstractmethod
    def update(self, id: str, target_running_nodes: Dict[NodeType, int], to_terminate: List[CloudInstanceId]) -> None:
        """Update the cloud node provider state by launching
         or terminating cloud nodes.

        Args:
            id: the id of the update request.
            target_running_nodes: the target cluster shape (number of running nodes by type).
            to_terminate: the nodes to terminate.
        """
        pass


# class NodeProviderAdapter(ICloudNodeProvider):
#     """
#     Warps a NodeProviderV1 to a ICloudNodeProvider.
# 
#     TODO(rickyx):
#     The current adapter right now consists of two sets of APIs:
#     - v1: the old APIs that are used by the autoscaler, where
#     we forward the calls to the NodeProviderV1.
#     - v2: the new APIs that are used by the autoscaler v2, this is
#     defined in the ICloudNodeProvider interface.
# 
#     We should eventually remove the v1 APIs and only use the v2 APIs.
#     It's currently left as a TODO since changing the v1 APIs would
#     requires a lot of changes in the cluster launcher codebase.
#     """
# 
#     def __init__(
#         self,
#         provider: NodeProviderV1,
#         node_launcher: BaseNodeLauncher,
#         instance_config_provider: NodeProviderConfig,
#         max_concurrent_launches: int = AUTOSCALER_MAX_CONCURRENT_LAUNCHES,
#         max_launch_batch: int = AUTOSCALER_MAX_LAUNCH_BATCH,
#         max_concurrent_terminating: int = AUTOSCALER_MAX_CONCURRENT_TERMINATING,
#         launch_timeout_s: Optional[int] = None,
#         terminate_timeout_s: Optional[int] = None,
#     ) -> None:
#         super().__init__()
#         self._provider = provider
#         self._node_launcher = node_launcher
#         self._config = instance_config_provider
#         self._node_launcher_executors = ThreadPoolExecutor(
#             max_workers=math.ceil(max_concurrent_launches / float(max_launch_batch) if max_launch_batch > 0 else 1),
#             thread_name_prefix="ray::NodeLauncherPool",
#         )
#         self._node_terminator_executors = ThreadPoolExecutor(
#             max_workers=max_concurrent_terminating,
#         )
#         self._launch_timeout_s = launch_timeout_s
#         self._terminate_timeout_s = terminate_timeout_s
# 
#         self._launch_errors = Queue()
#         self._termination_errors = Queue() 
# 
# 
#     def get_state(self) -> CloudNodeProviderState:
#         pass
# 
# 
#     def update(self, request: UpdateCloudNodeProviderRequest) -> None:
#         to_launch = self._compute_to_launch(request)
#         req_id = request.id
# 
#         # Launch nodes in the thread pool.
#         for node_type, count in to_launch.items():
#             self._node_launcher_executors.submit(
#                 self._launch_nodes_by_type,
#                 node_type,
#                 count,
#                 id,
#             )
# 
# 
#     def get_running_nodes(self) -> List[CloudInstance]:
#         """
#         Get cloud nodes in the cluster that are running.
# 
#         Overrides ICloudNodeProvider.get_running_nodes()
#         """
#         # TODO: make node provider returns running nodes directly
#         # instead of pending/terminating nodes.
#         cloud_instance_ids = self._v1_non_terminated_nodes({})
#         # Filter out nodes that are not running.
#         # This is efficient since the provider is expected to cache the
#         # running status of the nodes.
#         cloud_instances = []
#         for cloud_instance_id in cloud_instance_ids:
#             if not self._v1_is_running(cloud_instance_id):
#                 continue
# 
#             node_tags = self._v1_node_tags(cloud_instance_id)
#             cloud_instances.append(
#                 CloudInstance(
#                     cloud_instance_id=cloud_instance_id,
#                     node_type=node_tags.get(TAG_RAY_USER_NODE_TYPE, None),
#                 )
#             )
#         return cloud_instances
# 
#     def update(
#         self, request: UpdateCloudNodeProviderRequest
#     ) -> UpdateCloudNodeProviderReply:
#         """
#         Update the cluster with new nodes/terminating nodes.
# 
#         Overrides ICloudNodeProvider.update()
#         """
#         reply = UpdateCloudNodeProviderReply()
# 
#         # Launch nodes
#         self._launch_nodes(request, reply)
# 
#         # Terminated nodes.
#         self._terminate_nodes(request, reply)
# 
#         # Finalize the updates for async node providers.
#         self._v1_post_process()
# 
#         return reply
# 
#     ###########################################
#     # Private APIs
#     ###########################################
# 
#     def _compute_to_launch(
#             self, req: UpdateCloudNodeProviderRequest
#     ) -> Dict[NodeType, int]:
#         """
#         Compute the number of nodes to launch for each node type.
# 
#         """
#         terminating_nodes = set(req.to_terminate)
#         terminating_nodes_by_type = defaultdict(int)
#         for cloud_instance_id in terminating_nodes:
#             node_tags = self._v1_node_tags(cloud_instance_id)
#             node_type = node_tags.get(TAG_RAY_USER_NODE_TYPE, None)
#             if node_type:
#                 terminating_nodes_by_type[node_type] += 1
#             else:
#                 logger.warning(
#                     f"Node {cloud_instance_id} does not have a node type tag."
#                 )
# 
# 
#         non_terminated_nodes = self._v1_non_terminated_nodes({})
#         non_terminated_nodes_by_type = defaultdict(int)
#         for cloud_instance_id in non_terminated_nodes:
#             node_tags = self._v1_node_tags(cloud_instance_id)
#             node_type = node_tags.get(TAG_RAY_USER_NODE_TYPE, None)
#             if node_type:
#                 non_terminated_nodes_by_type[node_type] += 1
#             else:
#                 logger.warning(
#                     f"Node {cloud_instance_id} does not have a node type tag."
#                 )
#         
#         # Compute the number of nodes to launch for each node type.
#         to_launch = defaultdict(int)
#         for node_type, target_count in req.target_running_nodes.items():
#             non_terminated_count = non_terminated_nodes_by_type[node_type]
#             terminating_count = terminating_nodes_by_type[node_type]
# 
#             if non_terminated_count - terminating_count < target_count:
#                 to_launch[node_type] = target_count - (non_terminated_count - terminating_count)
# 
#         return to_launch
#     
# 
#     def _launch_nodes_by_type(
#         self, node_type: NodeType, count: int, id: str
#     ) -> None:
#         # TODO: embed the node launcher logic here.
#         try:
#             self._node_launcher.launch_node(
#                 self._config.get_raw_config_mutable(),
#                 count,
#                 node_type,
#                 raise_exception=True,
#             )
#         except Exception as e:
#             self._launch_errors.put_nowait(
#                 LaunchNodeError(
#                     exception=e,
#                     details=str(e),
#                     timestamp_ns=time.time_ns(),
#                     node_type=node_type,
#                     count=count,
#                     update_id=id,
#                 )
#             )
#             raise e
# 
# 
#     def _launch_nodes(
#         self, req: UpdateCloudNodeProviderRequest, reply: UpdateCloudNodeProviderReply
#     ) -> None:
#         """
#         Launch nodes.
# 
#         Args:
#             req: the request to launch nodes.
#             reply: the reply to the request. It will be populated with the launched
#             nodes and launch failures.
#         """
# 
#         to_launch = self._compute_to_launch(req)
# 
#         futures_to_launch_args = {}
# 
#         for node_type, count in to_launch.items():
#             futures_to_launch_args[self._node_launcher_executors.submit(
#                 self._launch_nodes_by_type,
#                 node_type,
#                 count,
#             )] = (node_type, count)
# 
#         # We are not waiting for any of the futures to complete if the launch
#         # timeout is 0, indicating immediate return. 
#         if self._launch_timeout_s == 0:
#             reply.num_launching = {node_type: count for node_type, count in to_launch.items()}
#             return
# 
#         # NOTE: launch_timeout_s could be None to indicate wait forever. 
#         for fut in as_completed(futures_to_launch_args, timeout=self._launch_timeout_s):
#             node_type, count = futures_to_launch_args[fut]
#             try:
#                 fut.result()
#             except Exception as e:
#                 # TODO(rickyx):
#                 # This currently assumes that the node launcher will always
#                 # launch all nodes successfully or none of them.
#                 # We should eventually make the node launcher return
#                 # the nodes that are successfully launched when errors happen.
#                 logger.error(f"Failed to launch {count} nodes of {node_type}: {e}")
#                 reply.num_launching[node_type] = 0
#                 reply.launch_failures[node_type] = UpdateCloudNodeProviderException(
#                     exception=e
#                 )
#             else:
#                 reply.num_launching[node_type] = count 
# 
#     
# 
#     def _terminate_nodes(
#         self, req: UpdateCloudNodeProviderRequest, reply: UpdateCloudNodeProviderReply
#     ) -> None:
#         """
#         Terminate nodes.
# 
#         Args:
#             req: the request to terminate nodes.
#             reply: the reply to the request. It will be populated with the terminating
#             nodes.
#         """
#         futures_to_terminating_ids = {}
#         for cloud_instance_id in req.to_terminate:
#             futures_to_terminating_ids[self._node_terminator_executors.submit(
#                 self._v1_terminate_node,
#                 self,
#                 cloud_instance_id,
#             )] = cloud_instance_id
# 
#         if self._terminate_timeout_s == 0:
#             reply.terminating = req.to_terminate
#             return
#         
#         # NOTE: terminate_timeout_s could be None to indicate wait forever.
#         for fut in as_completed(futures_to_terminating_ids, timeout=self._terminate_timeout_s):
#             cloud_instance_id = futures_to_terminating_ids[fut]
#             try:
#                 fut.result()
#             except Exception as e:
#                 logger.error(f"Failed to terminate node {cloud_instance_id}: {e}")
#                 reply.terminate_failures[
#                     cloud_instance_id
#                 ] = UpdateCloudNodeProviderException(exception=e)
#             else:
#                 reply.terminating.append(cloud_instance_id)
# 
#     ###########################################
#     # V1 Legacy APIs
#     ###########################################
#     """
#     Below are the necessary legacy APIs from the V1 node provider.
#     These are needed as of now to provide the needed features
#     for V2 node provider.
#     The goal is to eventually remove these APIs and only use the
#     V2 APIs by modifying the individual node provider to inherit
#     from ICloudNodeProvider.
#     """
# 
#     def _v1_terminate_node(self, node_id: CloudInstanceId) -> Optional[Dict[str, Any]]:
#         return self._provider.terminate_node(node_id)
# 
#     def _v1_non_terminated_nodes(
#         self, tag_filters: Dict[str, str]
#     ) -> List[CloudInstanceId]:
#         return self._provider.non_terminated_nodes(tag_filters)
# 
#     def _v1_is_running(self, node_id: CloudInstanceId) -> bool:
#         return self._provider.is_running(node_id)
# 
#     def _v1_post_process(self) -> None:
#         self._provider.post_process()
# 
#     def _v1_node_tags(self, node_id: CloudInstanceId) -> Dict[str, str]:
#         return self._provider.node_tags(node_id)
