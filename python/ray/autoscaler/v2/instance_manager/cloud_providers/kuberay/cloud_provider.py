import copy
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

# TODO(rickyx): We should eventually remove these imports
# when we deprecate the v1 kuberay node provider.
from ray.autoscaler._private.kuberay.node_provider import (
    KUBERAY_KIND_HEAD,
    KUBERAY_KIND_WORKER,
    KUBERAY_LABEL_KEY_KIND,
    KUBERAY_LABEL_KEY_TYPE,
    RAY_HEAD_POD_NAME,
    IKubernetesHttpApiClient,
    KubernetesHttpApiClient,
    _worker_group_index,
    _worker_group_max_replicas,
    _worker_group_replicas,
    worker_delete_patch,
    worker_replica_patch,
)
from ray.autoscaler.v2.instance_manager.node_provider import (
    CloudInstance,
    CloudInstanceId,
    CloudInstanceProviderError,
    ICloudInstanceProvider,
    LaunchNodeError,
    NodeKind,
    TerminateNodeError,
)
from ray.autoscaler.v2.schema import NodeType

logger = logging.getLogger(__name__)


class KubeRayProvider(ICloudInstanceProvider):
    """
    This class is a thin wrapper around the Kubernetes API client. It modifies
    the RayCluster resource spec on the Kubernetes API server to scale the cluster:

    It launches new instances/nodes by submitting patches to the Kubernetes API
    to update the RayCluster CRD.
    """

    def __init__(
        self,
        cluster_name: str,
        provider_config: Dict[str, Any],
        k8s_api_client: Optional[IKubernetesHttpApiClient] = None,
    ):
        """
        Args:
            cluster_name: The name of the RayCluster resource.
            provider_config: The namespace of the RayCluster.
            k8s_api_client: The client to the Kubernetes API server.
                This could be used to mock the Kubernetes API server for testing.
        """
        self._cluster_name = cluster_name
        self._namespace = provider_config["namespace"]

        self._k8s_api_client = k8s_api_client or KubernetesHttpApiClient(
            namespace=self._namespace
        )

        # Below are states that are cached locally.
        self._requests = set()
        self._launch_errors_queue = []
        self._terminate_errors_queue = []

        # Below are states that are fetched from the Kubernetes API server.
        self._ray_cluster = None
        self._cached_instances: Dict[CloudInstanceId, CloudInstance]

    @dataclass
    class ScaleRequest:
        """Represents a scale request that contains the current states and go-to states
        for the ray cluster.

        This class will be converted to patches to be submitted to the Kubernetes API
        server:
        - For launching new instances, it will adjust the `replicas` field in the
            workerGroupSpecs.
        - For terminating instances, it will adjust the `workersToDelete` field in the
            workerGroupSpecs.

        """

        # The desired number of workers for each node type.
        desired_num_workers: Dict[NodeType, int] = field(default_factory=dict)
        # The workers to delete for each node type.
        workers_to_delete: Dict[NodeType, List[CloudInstanceId]] = field(
            default_factory=dict
        )
        # The worker groups with empty workersToDelete field.
        # This is needed since we will also need to clear the workersToDelete field
        # for the worker groups that have finished deletes.
        worker_groups_without_pending_deletes: Set[NodeType] = field(
            default_factory=set
        )
        # The worker groups that still have workers to be deleted.
        worker_groups_with_pending_deletes: Set[NodeType] = field(default_factory=set)

    ################################
    # Interface for ICloudInstanceProvider
    ################################

    def get_non_terminated(self) -> Dict[CloudInstanceId, CloudInstance]:
        self._sync_with_api_server()
        return copy.deepcopy(dict(self._cached_instances))

    def terminate(self, ids: List[CloudInstanceId], request_id: str) -> None:
        if request_id in self._requests:
            # This request is already processed.
            logger.warning(f"Request {request_id} is already processed for: {ids}")
            return
        self._requests.add(request_id)
        logger.info("Terminating worker pods: {}".format(ids))

        scale_request = self._initialize_scale_request(
            to_launch={}, to_delete_instances=ids
        )
        if scale_request.worker_groups_with_pending_deletes:
            errors_msg = (
                "There are workers to be deleted from: "
                f"{scale_request.worker_groups_with_pending_deletes}. "
                "Waiting for them to be deleted before adding new workers "
                " to be deleted"
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
            logger.exception(f"Error terminating nodes: {scale_request}")
            self._add_terminate_errors(ids, request_id, details=str(e), e=e)

    def launch(self, shape: Dict[NodeType, int], request_id: str) -> None:
        if request_id in self._requests:
            # This request is already processed.
            return
        self._requests.add(request_id)

        scale_request = self._initialize_scale_request(
            to_launch=shape, to_delete_instances=[]
        )

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
            logger.exception(f"Error launching nodes: {scale_request}")
            self._add_launch_errors(shape, request_id, details=str(e), e=e)

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

    def _initialize_scale_request(
        self, to_launch: Dict[NodeType, int], to_delete_instances: List[CloudInstanceId]
    ) -> "KubeRayProvider.ScaleRequest":
        """
        Initialize the scale request based on the current state of the cluster and
        the desired state (to launch, to delete).

        Args:
            to_launch: The desired number of workers to launch for each node type.
            to_delete_instances: The instances to delete.

        Returns:
            The scale request.
        """

        # Update the cached states.
        self._sync_with_api_server()
        ray_cluster = self.ray_cluster
        cur_instances = self.instances

        # Get the worker groups that have pending deletes and the worker groups that
        # have finished deletes, and the set of workers included in the workersToDelete
        # field of any worker group.
        (
            worker_groups_with_pending_deletes,
            worker_groups_without_pending_deletes,
            worker_to_delete_set,
        ) = self._get_workers_delete_info(ray_cluster, set(cur_instances.keys()))

        # Calculate the desired number of workers by type.
        num_workers_dict = defaultdict(int)
        worker_groups = ray_cluster["spec"].get("workerGroupSpecs", [])
        for worker_group in worker_groups:
            node_type = worker_group["groupName"]
            # Handle the case where users manually increase `minReplicas`
            # to scale up the number of worker Pods. In this scenario,
            # `replicas` will be smaller than `minReplicas`.
            num_workers_dict[node_type] = max(
                worker_group["replicas"], worker_group["minReplicas"]
            )

        # Add to launch nodes.
        for node_type, count in to_launch.items():
            num_workers_dict[node_type] += count

        to_delete_instances_by_type = defaultdict(list)
        # Update the number of workers with to_delete_instances
        # and group them by type.
        for to_delete_id in to_delete_instances:
            to_delete_instance = cur_instances.get(to_delete_id, None)
            if to_delete_instance is None:
                # This instance has already been deleted.
                continue

            if to_delete_instance.node_kind == NodeKind.HEAD:
                # Not possible to delete head node.
                continue

            if to_delete_instance.cloud_instance_id in worker_to_delete_set:
                # If the instance is already in the workersToDelete field of
                # any worker group, skip it.
                continue

            num_workers_dict[to_delete_instance.node_type] -= 1
            assert num_workers_dict[to_delete_instance.node_type] >= 0
            to_delete_instances_by_type[to_delete_instance.node_type].append(
                to_delete_instance
            )

        scale_request = KubeRayProvider.ScaleRequest(
            desired_num_workers=num_workers_dict,
            workers_to_delete=to_delete_instances_by_type,
            worker_groups_without_pending_deletes=worker_groups_without_pending_deletes,
            worker_groups_with_pending_deletes=worker_groups_with_pending_deletes,
        )

        return scale_request

    def _submit_scale_request(
        self, scale_request: "KubeRayProvider.ScaleRequest"
    ) -> None:
        """Submits a scale request to the Kubernetes API server.

        This method will convert the scale request to patches and submit the patches
        to the Kubernetes API server.

        Args:
            scale_request: The scale request.

        Raises:
            Exception: An exception is raised if the Kubernetes API server returns an
                error.
        """
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
        for node_type in scale_request.worker_groups_without_pending_deletes:
            if node_type in scale_request.workers_to_delete:
                # This node type is still being deleted.
                continue
            group_index = _worker_group_index(raycluster, node_type)
            patch = worker_delete_patch(group_index, [])
            patch_payload.append(patch)

        if len(patch_payload) == 0:
            # No patch required.
            return

        logger.info(f"Submitting a scale request: {scale_request}")
        self._patch(f"rayclusters/{self._cluster_name}", patch_payload)

    def _add_launch_errors(
        self,
        shape: Dict[NodeType, int],
        request_id: str,
        details: str,
        e: Optional[Exception] = None,
    ) -> None:
        """
        Adds launch errors to the error queue.

        Args:
            shape: The shape of the nodes that failed to launch.
            request_id: The request id of the launch request.
            details: The details of the error.
            e: The exception that caused the error.
        """
        for node_type, count in shape.items():
            self._launch_errors_queue.append(
                LaunchNodeError(
                    node_type=node_type,
                    timestamp_ns=time.time_ns(),
                    count=count,
                    request_id=request_id,
                    details=details,
                    cause=e,
                )
            )

    def _add_terminate_errors(
        self,
        ids: List[CloudInstanceId],
        request_id: str,
        details: str,
        e: Optional[Exception] = None,
    ) -> None:
        """
        Adds terminate errors to the error queue.

        Args:
            ids: The ids of the nodes that failed to terminate.
            request_id: The request id of the terminate request.
            details: The details of the error.
            e: The exception that caused the error.
        """
        for id in ids:
            self._terminate_errors_queue.append(
                TerminateNodeError(
                    cloud_instance_id=id,
                    timestamp_ns=time.time_ns(),
                    request_id=request_id,
                    details=details,
                    cause=e,
                )
            )

    def _sync_with_api_server(self) -> None:
        """Fetches the RayCluster resource from the Kubernetes API server."""
        self._ray_cluster = self._get(f"rayclusters/{self._cluster_name}")
        self._cached_instances = self._fetch_instances()

    @property
    def ray_cluster(self) -> Dict[str, Any]:
        return copy.deepcopy(self._ray_cluster)

    @property
    def instances(self) -> Dict[CloudInstanceId, CloudInstance]:
        return copy.deepcopy(self._cached_instances)

    @staticmethod
    def _get_workers_delete_info(
        ray_cluster_spec: Dict[str, Any], node_set: Set[CloudInstanceId]
    ) -> Tuple[Set[NodeType], Set[NodeType], Set[CloudInstanceId]]:
        """
        Gets the worker groups that have pending deletes and the worker groups that
        have finished deletes.

        Returns:
            worker_groups_with_pending_deletes: The worker groups that have pending
                deletes.
            worker_groups_with_finished_deletes: The worker groups that have finished
                deletes.
            worker_to_delete_set: A set of Pods that are included in the workersToDelete
                field of any worker group.
        """

        worker_groups_with_pending_deletes = set()
        worker_groups_with_deletes = set()
        worker_to_delete_set = set()

        worker_groups = ray_cluster_spec["spec"].get("workerGroupSpecs", [])
        for worker_group in worker_groups:
            workersToDelete = worker_group.get("scaleStrategy", {}).get(
                "workersToDelete", []
            )
            if not workersToDelete:
                # No workers to delete in this group.
                continue

            node_type = worker_group["groupName"]
            worker_groups_with_deletes.add(node_type)

            for worker in workersToDelete:
                worker_to_delete_set.add(worker)
                if worker in node_set:
                    worker_groups_with_pending_deletes.add(node_type)
                    break

        worker_groups_with_finished_deletes = (
            worker_groups_with_deletes - worker_groups_with_pending_deletes
        )
        return (
            worker_groups_with_pending_deletes,
            worker_groups_with_finished_deletes,
            worker_to_delete_set,
        )

    def _fetch_instances(self) -> Dict[CloudInstanceId, CloudInstance]:
        """
        Fetches the pods from the Kubernetes API server and convert them to Ray
        CloudInstance.

        Returns:
            A dict of CloudInstanceId to CloudInstance.
        """
        # Get the pods resource version.
        # Specifying a resource version in list requests is important for scalability:
        # https://kubernetes.io/docs/reference/using-api/api-concepts/#semantics-for-get-and-list
        resource_version = self._get_head_pod_resource_version()
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
            cloud_instance = self._cloud_instance_from_pod(pod)
            if cloud_instance:
                cloud_instances[pod_name] = cloud_instance
        return cloud_instances

    @staticmethod
    def _cloud_instance_from_pod(pod: Dict[str, Any]) -> Optional[CloudInstance]:
        """
        Convert a pod to a Ray CloudInstance.

        Args:
            pod: The pod resource dict.
        """
        labels = pod["metadata"]["labels"]
        if labels[KUBERAY_LABEL_KEY_KIND] == KUBERAY_KIND_HEAD:
            kind = NodeKind.HEAD
            type = labels[KUBERAY_LABEL_KEY_TYPE]
        elif labels[KUBERAY_LABEL_KEY_KIND] == KUBERAY_KIND_WORKER:
            kind = NodeKind.WORKER
            type = labels[KUBERAY_LABEL_KEY_TYPE]
        else:
            # Other ray nodes types defined by KubeRay.
            # e.g. this could also be `redis-cleanup`
            # We will not track these nodes.
            return None

        # TODO: we should prob get from the pod's env var (RAY_CLOUD_INSTANCE_ID)
        # directly.
        cloud_instance_id = pod["metadata"]["name"]
        return CloudInstance(
            cloud_instance_id=cloud_instance_id,
            node_type=type,
            node_kind=kind,
            is_running=KubeRayProvider._is_running(pod),
        )

    @staticmethod
    def _is_running(pod) -> bool:
        """Convert pod state to Ray NodeStatus

        A cloud instance is considered running if the pod is in the running state,
        else it could be pending/containers-terminated.

        When it disappears from the list, it is considered terminated.
        """
        if (
            "containerStatuses" not in pod["status"]
            or not pod["status"]["containerStatuses"]
        ):
            return False

        state = pod["status"]["containerStatuses"][0]["state"]
        if "running" in state:
            return True

        return False

    def _get(self, remote_path: str) -> Dict[str, Any]:
        """Get a resource from the Kubernetes API server."""
        return self._k8s_api_client.get(remote_path)

    def _patch(self, remote_path: str, payload: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Patch a resource on the Kubernetes API server."""
        return self._k8s_api_client.patch(remote_path, payload)

    def _get_head_pod_resource_version(self) -> str:
        """
        Extract a recent pods resource version by reading the head pod's
        metadata.resourceVersion of the response.
        """
        if not RAY_HEAD_POD_NAME:
            return None
        pod_resp = self._get(f"pods/{RAY_HEAD_POD_NAME}")
        return pod_resp["metadata"]["resourceVersion"]
