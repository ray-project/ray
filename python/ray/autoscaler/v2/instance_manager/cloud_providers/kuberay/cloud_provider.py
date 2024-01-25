import copy
import json
import logging
import os
import threading
import time
from abc import ABC, ABCMeta, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

from ray.autoscaler._private.kuberay.node_provider import (
    KUBERAY_KIND_HEAD,
    KUBERAY_LABEL_KEY_KIND,
    KUBERAY_LABEL_KEY_TYPE,
    KUBERAY_TYPE_HEAD,
    IKubernetesHttpApiClient,
    _worker_group_index,
    _worker_group_max_replicas,
    _worker_group_replicas,
    load_k8s_secrets,
    worker_delete_patch,
    worker_replica_patch,
)
from ray.autoscaler.tags import NODE_KIND_HEAD, NODE_KIND_WORKER
from ray.autoscaler.v2.instance_manager.config import IConfigReader
from ray.autoscaler.v2.instance_manager.node_provider import (
    CloudInstance,
    CloudInstanceId,
    CloudInstanceProviderError,
    ICloudInstanceProvider,
    LaunchNodeError,
    TerminateNodeError,
)
from ray.autoscaler.v2.schema import NodeKind, NodeStatus, NodeType

# TODO(rickyx):
# Is this the right way to get the pod name?
RAY_HEAD_POD_NAME = os.getenv("RAY_HEAD_POD_NAME")

logger = logging.getLogger(__name__)


@dataclass
class ScaleRequest:
    """Stores desired scale computed by the autoscaler.

    Attributes:
        desired_num_workers: Map of worker NodeType to desired number of workers of
            that type.
        workers_to_delete: List of ids of nodes that should be removed.
    """

    desired_num_workers: Dict[NodeType, int] = field(default_factory=dict)
    workers_to_delete: Dict[NodeType, List[CloudInstanceId]] = field(
        default_factory=dict
    )
    worker_groups_to_clear_delete: Set[NodeType] = field(default_factory=set)
    worker_groups_with_pending_deletes: Set[NodeType] = field(default_factory=set)


class KuberayProvider(ICloudInstanceProvider):
    def __init__(
        self,
        cluster_name: str,
        namespace: str,
        k8s_api_client: IKubernetesHttpApiClient,
    ):
        self._cluster_name = cluster_name
        self._namespace = namespace
        self._requests = set()

        self._k8s_api_client = k8s_api_client

        self._launch_errors_queue = []
        self._terminate_errors_queue = []

        # Below are states that are fetched from the Kuberay API server.
        self._ray_cluster = None
        self._cached_instances: Dict[CloudInstanceId, CloudInstance]

    def get_non_terminated(self) -> Dict[CloudInstanceId, CloudInstance]:
        self._sync_with_api_server()
        return copy.deepcopy(self._cached_instances)

    def terminate(self, ids: List[CloudInstanceId], request_id: str) -> None:
        """Terminates the specified instances."""
        if request_id in self._requests:
            # This request is already processed.
            return
        self._requests.add(request_id)

        scale_request = self._get_scale_request(to_launch={}, to_delete_instances=ids)
        if scale_request.worker_groups_with_pending_deletes:
            errors_msg = (
                "There are workers to be deleted from: "
                f"{scale_request.worker_groups_with_pending_deletes}. "
                "Waiting for them to be created before deleting workers."
            )
            logger.warning(errors_msg)
            self._add_terminate_errors(
                ids,
                request_id,
                details=errors_msg,
            )
            return

        try:
            self._submit_scale_request(scale_request)
        except Exception as e:
            logger.exception(f"Error terminating nodes: {e}")
            self._add_terminate_errors(ids, request_id, str(e), e)

    def launch(self, shape: Dict[NodeType, int], request_id: str) -> None:
        # Add the desired number of workers to the scale request.
        if request_id in self._requests:
            # This request is already processed.
            return
        self._requests.add(request_id)

        scale_request = self._get_scale_request(to_launch=shape, to_delete_instances=[])

        if scale_request.worker_groups_with_pending_deletes:
            error_msg = (
                "There are workers to be deleted from: "
                f"{scale_request.worker_groups_with_pending_deletes}. "
                "Waiting for them to be deleted before creating new workers."
            )
            logger.warning(error_msg)
            self._add_launch_errors(
                shape,
                request_id,
                details=error_msg,
            )
            return

        try:
            self._submit_scale_request(scale_request)
        except Exception as e:
            logger.exception(f"Error launching nodes: {e}")
            self._add_launch_errors(shape, request_id, str(e), e)

    def poll_errors(self) -> List[CloudInstanceProviderError]:
        errors = []
        errors += self._launch_errors_queue
        errors += self._terminate_errors_queue
        self._launch_errors_queue = []
        self._terminate_errors_queue = []
        return errors

    ############################
    # Private
    ############################

    def _get_scale_request(
        self, to_launch: Dict[NodeType, int], to_delete_instances: List[CloudInstanceId]
    ) -> ScaleRequest:

        # Update the cached states.
        self._sync_with_api_server()
        ray_cluster = self.ray_cluster
        cur_instances = self.instances

        # Check if there are still workers to be deleted, and safe to scale.
        (
            worker_groups_with_pending_deletes,
            worker_groups_without_pending_deletes,
        ) = self._get_workers_groups_with_deletes(
            ray_cluster, set(cur_instances.keys())
        )

        if len(worker_groups_with_pending_deletes) > 0:
            # There are still workers to be deleted.
            return ScaleRequest(
                worker_groups_with_pending_deletes=worker_groups_with_pending_deletes,
            )

        # Calculate the desired number of workers.
        num_workers_dict = defaultdict(int)
        for instance_id, instance in cur_instances.items():
            if instance.node_kind == NodeKind.HEAD:
                # Only track workers.
                continue
            num_workers_dict[instance.node_type] += 1

        # Add to launch
        for node_type, count in to_launch.items():
            num_workers_dict[node_type] += count

        # Remove deleting workers.
        for to_delete_id in to_delete_instances:
            to_delete_instance = cur_instances.get(to_delete_id, None)
            if to_delete_instance is None:
                # This instance has already been deleted.
                continue
            num_workers_dict[to_delete_instance.node_type] -= 1
            assert num_workers_dict[to_delete_instance.node_type] >= 0

        # Group the to delete workers by node type.
        to_delete_instances_by_type = defaultdict(list)
        for instance_id in to_delete_instances:
            instance = cur_instances.get(instance_id, None)
            if instance is None:
                # This instance has already been deleted.
                continue

            if instance.node_kind == NodeKind.HEAD:
                # Not possible to delete head node.
                continue

            to_delete_instances_by_type[instance.node_type].append(instance)

        scale_request = ScaleRequest(
            desired_num_workers=num_workers_dict,
            workers_to_delete=to_delete_instances_by_type,
            worker_groups_to_clear_delete=worker_groups_without_pending_deletes,
        )

        return scale_request

    def _submit_scale_request(self, scale_request: ScaleRequest) -> None:
        """Submits a scale request to the Kuberay API server."""
        # Get the current ray cluster spec.
        patch_payload = []

        raycluster = self.ray_cluster

        # Collect patches for replica counts.
        for node_type, target_replicas in scale_request.desired_num_workers.items():
            group_index = _worker_group_index(raycluster, node_type)
            group_max_replicas = _worker_group_max_replicas(raycluster, group_index)
            # Cap the replica count to maxReplicas.
            if group_max_replicas is not None and group_max_replicas < target_replicas:
                logger.warning(
                    "Autoscaler attempted to create "
                    + "more than maxReplicas pods of type {}.".format(node_type)
                )
                target_replicas = group_max_replicas
            # Check if we need to change the target count.
            if target_replicas == _worker_group_replicas(raycluster, group_index):
                # No patch required.
                continue
            # Need to patch replica count. Format the patch and add it to the payload.
            patch = worker_replica_patch(group_index, target_replicas)
            patch_payload.append(patch)

        # Maps node_type to nodes to delete for that group.
        for (
            node_type,
            workers_to_delete_of_type,
        ) in scale_request.workers_to_delete.items():
            group_index = _worker_group_index(raycluster, node_type)
            worker_ids_to_delete = [
                worker.cloud_instance_id for worker in workers_to_delete_of_type
            ]
            patch = worker_delete_patch(group_index, worker_ids_to_delete)
            patch_payload.append(patch)

        # Clear the workersToDelete field for the worker groups that have been deleted.
        for node_type in scale_request.worker_groups_to_clear_delete:
            if node_type in scale_request.workers_to_delete:
                # This node type is still being deleted.
                continue
            group_index = _worker_group_index(raycluster, node_type)
            patch = worker_delete_patch(group_index, [])
            patch_payload.append(patch)

        if len(patch_payload) == 0:
            # No patch required.
            return

        self._patch(f"rayclusters/{self._cluster_name}", patch_payload)

    def _add_launch_errors(
        self,
        shape: Dict[NodeType, int],
        request_id: str,
        details: str,
        e: Optional[Exception] = None,
    ) -> None:
        for node_type, count in shape.items():
            self._launch_errors_queue.append(
                LaunchNodeError(
                    node_type=node_type,
                    details=details,
                    timestamp_ns=time.time_ns(),
                    count=count,
                    request_id=request_id,
                    exception=e,
                )
            )

    def _add_terminate_errors(
        self,
        ids: List[CloudInstanceId],
        request_id: str,
        details: str,
        e: Optional[Exception] = None,
    ) -> None:
        for id in ids:
            self._terminate_errors_queue.append(
                TerminateNodeError(
                    cloud_instance_id=id,
                    details=details,
                    timestamp_ns=time.time_ns(),
                    request_id=request_id,
                    exception=e,
                )
            )

    def _sync_with_api_server(self) -> None:
        """Fetches the RayCluster resource from the Kuberay API server."""
        self._ray_cluster = self._get(f"rayclusters/{self._cluster_name}")
        self._cached_instances = self._fetch_instances()

    @property
    def ray_cluster(self) -> Dict[str, Any]:
        return self._ray_cluster

    @property
    def instances(self) -> Dict[CloudInstanceId, CloudInstance]:
        return self._cached_instances

    @staticmethod
    def _get_workers_groups_with_deletes(
        ray_cluster_spec: Dict[str, Any], node_set: Set[CloudInstanceId]
    ) -> Tuple[Set[NodeType], Set[NodeType]]:

        worker_groups_with_pending_deletes = set()
        worker_groups_with_finished_deletes = set()

        worker_groups = ray_cluster_spec["spec"].get("workerGroupSpecs", [])
        for worker_group in worker_groups:
            workersToDelete = worker_group.get("scaleStrategy", {}).get(
                "workersToDelete", []
            )
            if not workersToDelete:
                # No workers to delete in this group.
                continue

            node_type = worker_group["groupName"]
            for worker in workersToDelete:
                if worker in node_set:
                    worker_groups_with_pending_deletes.add(node_type)
                    break

            # All to delete in the worker spec have been deleted.
            worker_groups_with_finished_deletes.add(node_type)

        return worker_groups_with_pending_deletes, worker_groups_with_finished_deletes

    def _fetch_instances(self) -> Dict[CloudInstanceId, CloudInstance]:
        """Queries K8s for pods in the RayCluster. Converts that pod data into a
        map of pod name to Ray NodeData, as required by BatchingNodeProvider.
        """
        # Get the pods resource version.
        # Specifying a resource version in list requests is important for scalability:
        # https://kubernetes.io/docs/reference/using-api/api-concepts/#semantics-for-get-and-list
        resource_version = self._get_pods_resource_version()
        if resource_version:
            logger.info(
                f"Listing pods for RayCluster {self._cluster_name}"
                f" in namespace {self._namespace}"
                f" at pods resource version >= {resource_version}."
            )

        # Filter pods by cluster_name.
        label_selector = requests.utils.quote(f"ray.io/cluster={self._cluster_name}")

        resource_path = f"pods?labelSelector={label_selector}"
        if resource_version:
            resource_path += (
                f"&resourceVersion={resource_version}"
                + "&resourceVersionMatch=NotOlderThan"
            )

        pod_list = self._get(resource_path)
        fetched_resource_version = pod_list["metadata"]["resourceVersion"]
        logger.info(
            f"Fetched pod data at resource version" f" {fetched_resource_version}."
        )

        # Extract node data from the pod list.
        cloud_instances = {}
        for pod in pod_list["items"]:
            # Kubernetes sets metadata.deletionTimestamp immediately after admitting a
            # request to delete an object. Full removal of the object may take some time
            # after the deletion timestamp is set. See link for details:
            # https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-deletion
            if "deletionTimestamp" in pod["metadata"]:
                # Ignore pods marked for termination.
                continue
            pod_name = pod["metadata"]["name"]
            cloud_instances[pod_name] = self._cloud_instance_from_pod(pod)
        return cloud_instances

    @staticmethod
    def _cloud_instance_from_pod(pod: Dict[str, Any]) -> CloudInstance:
        """Converts a Ray pod extracted from K8s into Ray NodeData.
        NodeData is processed by BatchingNodeProvider.
        """
        labels = pod["metadata"]["labels"]
        if labels[KUBERAY_LABEL_KEY_KIND] == KUBERAY_KIND_HEAD:
            kind = NodeKind.HEAD
            type = KUBERAY_TYPE_HEAD
        else:
            kind = NodeKind.WORKER
            type = labels[KUBERAY_LABEL_KEY_TYPE]

        status = KuberayProvider._get_node_status(pod)
        cloud_instance_id = pod["metadata"]["name"]
        return CloudInstance(
            cloud_instance_id=cloud_instance_id,
            node_type=type,
            node_kind=kind,
            status=status,
        )

    @staticmethod
    def _get_node_status(pod) -> NodeStatus:
        """Convert pod state to Ray autoscaler node status."""
        if (
            "containerStatuses" not in pod["status"]
            or not pod["status"]["containerStatuses"]
        ):
            return NodeStatus.PENDING

        state = pod["status"]["containerStatuses"][0]["state"]

        if "pending" in state or "waiting" in state:
            return NodeStatus.PENDING
        if "running" in state:
            return NodeStatus.RUNNING
        if "terminated" in state:
            return NodeStatus.TERMINATED
        return NodeStatus.UNKNOWN

    def _get(self, remote_path: str) -> Dict[str, Any]:
        return self._k8s_api_client.get(remote_path)

    def _patch(self, remote_path: str, payload: List[Dict[str, Any]]) -> Dict[str, Any]:
        return self._k8s_api_client.patch(remote_path, payload)

    def _get_pods_resource_version(self) -> str:
        """
        Extract a recent pods resource version by reading the head pod's
        metadata.resourceVersion of the response.
        """
        if not RAY_HEAD_POD_NAME:
            return None
        pod_resp = self._get(f"pods/{RAY_HEAD_POD_NAME}")
        return pod_resp["metadata"]["resourceVersion"]
