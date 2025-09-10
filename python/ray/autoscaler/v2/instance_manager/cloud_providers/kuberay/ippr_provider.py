import asyncio
import logging
import re
import time
import json
import jsonschema
import ray._private.utils

from functools import lru_cache
from decimal import Decimal
from typing import Any, Dict, List, Optional
from ray._common.network_utils import build_address
from ray._raylet import GcsClient, NodeID
from ray.core.generated import gcs_pb2, node_manager_pb2, node_manager_pb2_grpc
from ray.autoscaler._private.kuberay.node_provider import (
    KUBERAY_LABEL_KEY_TYPE,
    KUBERAY_LABEL_KEY_KIND,
    KUBERAY_KIND_HEAD,
    KUBERAY_KIND_WORKER,
    IKubernetesHttpApiClient,
    replace_patch,
)
from ray.autoscaler._private.kuberay.utils import parse_quantity
from ray.autoscaler.v2.schema import (
    IPPRStatus,
    IPPRSpecs,
    IPPRSpecsSchema,
    IPPRGroupSpec,
)


logger = logging.getLogger(__name__)


class KubeRayIPPRProvider:
    """Implements in-place pod resize (IPPR) operations for KubeRay pods.

    This provider is responsible for:
    - Validating and materializing IPPR specs from the RayCluster annotation
      (``ray.io/ippr``) into typed structures (``IPPRSpecs``/``IPPRGroupSpec``).
    - Tracking per-pod resize status (``IPPRStatus``) from Kubernetes pods and
      computing the desired resize actions.
    - Issuing Kubernetes Pod Resize API requests and keeping a shadow annotation
      (``ray.io/ippr-status``) to track progress and temporary caps.
    - Synchronizing successful resource changes with the Raylet so Ray's local
      resource view matches Kubernetes.

    Attributes:
        _gcs_client: Ray GCS client used to fetch Raylet node information.
        _k8s_api_client: Kubernetes HTTP client for patching pods.
        _ippr_specs: Validated per-group IPPR specs (limits and timeouts).
        _ippr_statuses: Latest per-pod IPPR statuses indexed by pod name.
        _container_resources: Snapshot of container resource requests/limits
            from both pod spec and pod status, per pod name, used to compute
            patch diffs.
    """

    def __init__(
        self,
        gcs_client: GcsClient,
        k8s_api_client: IKubernetesHttpApiClient,
    ):
        """Create a new IPPR provider.

        Args:
            gcs_client: Ray GCS client for resolving Raylet addresses.
            k8s_api_client: Kubernetes HTTP client to issue patch requests.
        """
        self._gcs_client = gcs_client
        self._k8s_api_client = k8s_api_client
        self._ippr_specs: IPPRSpecs = IPPRSpecs(groups={})
        self._ippr_statuses: Dict[str, IPPRStatus] = {}
        self._container_resources: Dict[str, Any] = {}

    def validate_and_set_ippr_specs(
        self, ray_cluster: Optional[Dict[str, Any]]
    ) -> None:
        """Validate and load IPPR specs from a RayCluster CR.

        Reads the ``ray.io/ippr`` annotation, validates it against
        ``IPPRSpecsSchema``, and converts it to typed ``IPPRSpecs`` with per-group
        ``IPPRGroupSpec`` entries. Minimal resources are derived from the group's
        pod template; maximums and timeout come from the annotation.

        Args:
            ray_cluster: The RayCluster custom resource as a dict. If missing or
                lacking the annotation, this method is a no-op.

        Raises:
            ValueError: If the Ray pod template is incompatible with IPPR (e.g.,
                missing required requests, using unsupported resizePolicy restarts,
                or conflicting ``rayStartParams``).
        """
        if not ray_cluster:
            return

        specs_str = ray_cluster["metadata"].get("annotations", {}).get("ray.io/ippr")
        if not specs_str:
            return

        specs_raw = json.loads(specs_str)
        jsonschema.validate(instance=specs_raw, schema=IPPRSpecsSchema)

        def _get_ippr_group_spec(
            group_spec: Dict[str, Any], worker_group_spec: Dict[str, Any]
        ) -> IPPRGroupSpec:
            # Disallow per-pod overrides that conflict with IPPR's dynamic sizing.
            ray_start_params = worker_group_spec.get("rayStartParams", {})
            if "num-cpus" in ray_start_params or "memory" in ray_start_params:
                raise ValueError(
                    "should not have 'num-cpus' or 'memory' in rayStartParams if IPPR is used"
                )
            container_spec = worker_group_spec["template"]["spec"]["containers"][0]
            pod_spec_requests = container_spec.get("resources", {}).get("requests", {})
            # Pod template must declare baseline CPU/memory requests for IPPR.
            if "cpu" not in pod_spec_requests or "memory" not in pod_spec_requests:
                raise ValueError(
                    "should have 'cpu' and 'memory' in resource requests if IPPR is used"
                )

            for policy in container_spec.get("resizePolicy", []):
                resource_name = policy.get("resourceName")
                if resource_name != "cpu" and resource_name != "memory":
                    continue
                restart = policy.get("restartPolicy")
                # IPPR requires NotRequired so that K8s won't restart the container
                # during in-place resource updates.
                if restart is not None and restart != "NotRequired":
                    raise ValueError("IPPR only supports restartPolicy=NotRequired")

            pod_spec_limits = container_spec.get("resources", {}).get("limits", {})
            spec_cpu = float(
                parse_quantity(
                    pod_spec_limits.get("cpu") or pod_spec_requests.get("cpu")
                )
            )
            spec_memory = int(
                parse_quantity(
                    pod_spec_limits.get("memory") or pod_spec_requests.get("memory")
                )
            )
            return IPPRGroupSpec(
                min_cpu=spec_cpu,
                min_memory=spec_memory,
                max_cpu=float(parse_quantity(group_spec.get("max-cpu", 0))),
                max_memory=int(parse_quantity(group_spec.get("max-memory", 0))),
                resize_timeout=int(group_spec.get("resize-timeout", 0)),
            )

        # Validate and build typed spec per group
        worker_groups = {
            worker_group_spec["groupName"]: worker_group_spec
            for worker_group_spec in ray_cluster["spec"].get("workerGroupSpecs", [])
        }
        worker_groups["headgroup"] = ray_cluster["spec"]["headGroupSpec"]

        groups = {
            group_name: _get_ippr_group_spec(group_spec, worker_groups[group_name])
            for group_name, group_spec in specs_raw.get("groups", {}).items()
            if group_name in worker_groups
        }

        self._ippr_specs = IPPRSpecs(groups=groups)

    def sync_with_raylets(self) -> None:
        """Propagate completed K8s resizes to Raylets.

        For any pod whose K8s resize has completed, update the corresponding Raylet's local resource
        instances via gRPC and clear the pending timestamp on the pod's
        ``ray.io/ippr-status`` annotation.
        """
        for ippr_status in self._ippr_statuses.values():
            if not ippr_status.need_sync_with_raylet():
                continue
            try:
                raylet_addr = _get_raylet_address(
                    self._gcs_client, ippr_status.raylet_id
                )
                if not raylet_addr:
                    raise RuntimeError(
                        f"Raylet address not found for pod {ippr_status.cloud_instance_id}"
                    )
                _resize_raylet_resources(
                    raylet_addr,
                    ippr_status.current_cpu,
                    ippr_status.current_memory,
                )
                self._patch_ippr_status(ippr_status, resized_at=None)
                logger.info(f"Pod {ippr_status.cloud_instance_id} resized successfully")
            except Exception as e:
                logger.error(
                    f"Failed to resize pod {ippr_status.cloud_instance_id}: {e}"
                )

    def sync_ippr_status_from_pods(self, pods: List[Dict[str, Any]]) -> None:
        """Refresh internal IPPR statuses and container resources from pods.

        Parses pods to produce up-to-date ``IPPRStatus`` objects and stores
        relevant request/limit snapshots for later patch computations.

        Args:
            pods: List of Kubernetes Pod resources for the Ray cluster.
        """
        self._ippr_statuses = {}
        self._container_resources = {}
        for pod in pods:
            if "deletionTimestamp" in pod["metadata"]:
                continue

            labels = pod["metadata"]["labels"]
            if (
                labels[KUBERAY_LABEL_KEY_KIND] != KUBERAY_KIND_HEAD
                and labels[KUBERAY_LABEL_KEY_KIND] != KUBERAY_KIND_WORKER
            ):
                continue

            ippr_group_spec = self._ippr_specs.groups.get(
                labels[KUBERAY_LABEL_KEY_TYPE]
            )
            ippr_status, container_resource = _get_ippr_status_from_pod(
                ippr_group_spec, pod
            )
            if ippr_status:
                self._ippr_statuses[ippr_status.cloud_instance_id] = ippr_status
            if container_resource:
                self._container_resources[
                    ippr_status.cloud_instance_id
                ] = container_resource

    def do_ippr_requests(self, resizes: List[IPPRStatus]) -> None:
        """Issue Kubernetes Pod Resize requests for the given targets.

        If downsizing, attempt to first adjust the Raylet's local resources via
        gRPC to avoid temporary overcommit in Ray's scheduler.

        Args:
            resizes: List of IPPRStatus with desired resources and metadata.
        """
        for resize in resizes:
            logger.info(
                f"Resizing pod {resize.cloud_instance_id} to cpu={resize.desired_cpu} memory={resize.desired_memory} from cpu={resize.current_cpu} memory={resize.current_memory}"
            )
            if (
                resize.desired_cpu < resize.current_cpu
                or resize.desired_memory < resize.current_memory
            ):
                # For downsizing, update Raylet first.
                try:
                    raylet_addr = _get_raylet_address(
                        self._gcs_client, resize.raylet_id
                    )
                    if not raylet_addr:
                        raise RuntimeError(
                            f"Raylet address not found for pod {resize.cloud_instance_id}"
                        )
                    updated = _resize_raylet_resources(
                        raylet_addr,
                        resize.desired_cpu,
                        resize.desired_memory,
                    )
                    resize.desired_cpu = float(updated.total_resources["CPU"])
                    resize.desired_memory = int(updated.total_resources["memory"])
                except Exception as e:
                    logger.error(
                        f"Skip failed downsizing on pod {resize.cloud_instance_id}: {e}"
                    )
                    continue
            self._patch_ippr_resize(resize)

    def get_ippr_specs(self) -> IPPRSpecs:
        """Return the current validated IPPR specs."""
        return self._ippr_specs

    def get_ippr_statuses(self) -> Dict[str, IPPRStatus]:
        """Return the latest per-pod IPPR statuses keyed by pod name."""
        return self._ippr_statuses

    def _patch_ippr_resize(self, resize: IPPRStatus) -> None:
        patch = self._make_ippr_patch(resize)
        self._k8s_api_client.patch(
            "pods/{}/resize".format(resize.cloud_instance_id), patch
        )
        self._patch_ippr_status(resize, resized_at=int(time.time()))

    def _patch_ippr_status(self, resize: IPPRStatus, resized_at: Optional[int]) -> None:
        # Keep the state in a pod annotation so we can correlate in-progress
        # and completed resizes across/beyond scheduler/provider iterations.
        self._k8s_api_client.patch(
            "pods/{}".format(resize.cloud_instance_id),
            {
                "metadata": {
                    "annotations": {
                        "ray.io/ippr-status": json.dumps(
                            {
                                "raylet-id": resize.raylet_id,
                                "resized-at": resized_at,
                                "suggested-max-cpu": resize.suggested_max_cpu,
                                "suggested-max-memory": resize.suggested_max_memory,
                            }
                        )
                    }
                }
            },
            content_type="application/strategic-merge-patch+json",
        )

    def _make_ippr_patch(self, resize: IPPRStatus) -> List[Dict[str, Any]]:
        patch = []
        path_prefix = "/spec/containers/0/resources"
        container_resource = self._container_resources[resize.cloud_instance_id]
        # When limits are present, preserve the existing gap (limits - requests)
        # by adjusting requests proportionally so QoS doesn't change.
        if container_resource["status"]["limits"].get("cpu"):
            # Gap between status limits and requests for CPU.
            diff = parse_quantity(
                container_resource["status"]["limits"].get("cpu")
            ) - parse_quantity(container_resource["status"]["requests"].get("cpu"))
            patch.append(replace_patch(f"{path_prefix}/limits/cpu", resize.desired_cpu))
            patch.append(
                replace_patch(
                    f"{path_prefix}/requests/cpu",
                    float(Decimal(str(resize.desired_cpu)) - diff),
                )
            )
        else:
            # No limits configured: adjust requests only.
            patch.append(
                replace_patch(f"{path_prefix}/requests/cpu", resize.desired_cpu)
            )
        if container_resource["status"]["limits"].get("memory"):
            # Gap between status limits and requests for memory.
            diff = int(
                parse_quantity(container_resource["status"]["limits"].get("memory"))
            ) - int(
                parse_quantity(container_resource["status"]["requests"].get("memory"))
            )
            patch.append(
                replace_patch(
                    f"{path_prefix}/limits/memory",
                    # Ensure the memory limit never drop below the pod spec's limit.
                    # Kubernetes does not allow lower memory limit than the current spec.
                    max(
                        resize.desired_memory,
                        int(
                            parse_quantity(
                                container_resource["spec"]["limits"].get("memory", 0)
                            )
                        ),
                    ),
                )
            )
            patch.append(
                replace_patch(
                    f"{path_prefix}/requests/memory",
                    resize.desired_memory - diff,
                )
            )
        else:
            # No limits configured: adjust requests only.
            patch.append(
                replace_patch(
                    f"{path_prefix}/requests/memory",
                    resize.desired_memory,
                )
            )
        return patch


async def _get_node_info(
    gcs_client: GcsClient, raylet_id: str
) -> Optional[Dict[NodeID, gcs_pb2.GcsNodeInfo]]:
    return await gcs_client.async_get_all_node_info(NodeID.from_hex(raylet_id))


@lru_cache(maxsize=2**10)
def _get_raylet_address(gcs_client: GcsClient, raylet_id: str) -> Optional[str]:
    # Cache the mapping from raylet_id to address to avoid repeated GCS calls
    # during a scaling loop.
    node_info_dict = asyncio.run(_get_node_info(gcs_client, raylet_id))
    if not node_info_dict:
        return None
    _, node_info = next(iter(node_info_dict.items()))
    raylet_addr = build_address(
        node_info.node_manager_address, node_info.node_manager_port
    )
    return raylet_addr


def _resize_raylet_resources(
    raylet_addr: str, cpu: float, memory: float
) -> node_manager_pb2.ResizeLocalResourceInstancesReply:
    # Update Raylet's local view so scheduling decisions are consistent with
    # the pod's resources when K8s accepts the resize.
    channel = ray._private.utils.init_grpc_channel(raylet_addr, asynchronous=False)
    raylet_client = node_manager_pb2_grpc.NodeManagerServiceStub(channel)
    request = node_manager_pb2.ResizeLocalResourceInstancesRequest()
    request.resources["CPU"] = cpu
    request.resources["memory"] = memory
    return raylet_client.ResizeLocalResourceInstances(request)


def _get_ippr_status_from_pod(
    ippr_group_spec: Optional[IPPRGroupSpec],
    pod: Dict[str, Any],
) -> tuple[Optional[IPPRStatus], Optional[Dict[str, Any]]]:
    """Build IPPRStatus and container resource snapshots from a Pod.

    Returns a tuple of (ippr_status, container_resource_snapshot). The snapshot
    contains both spec and status requests/limits used to construct resize
    patches that preserve the current QoS gap.
    """
    if not ippr_group_spec:
        return (None, None)

    pod_spec_requests = (
        pod["spec"]["containers"][0].get("resources", {}).get("requests", {})
    )
    pod_spec_limits = (
        pod["spec"]["containers"][0].get("resources", {}).get("limits", {})
    )
    container_status = {}
    other_container_resources = []
    for status in pod.get("status", {}).get("containerStatuses", []):
        if status["name"] == pod["spec"]["containers"][0]["name"]:
            container_status = status
        else:
            # We need to substract other containers' resources when adjusting the
            # the new resource requests based on the capactity report from the Kubelet.
            requests = status.get("resources", {}).get("requests")
            if requests:
                other_container_resources.append(requests)
    pod_status_requests = container_status.get("resources", {}).get(
        "requests", pod_spec_requests
    )
    pod_status_limits = container_status.get("resources", {}).get(
        "limits", pod_spec_limits
    )

    ippr_status = IPPRStatus(
        cloud_instance_id=pod["metadata"]["name"],
        spec=ippr_group_spec,
        desired_cpu=float(
            parse_quantity(pod_spec_limits.get("cpu") or pod_spec_requests.get("cpu"))
        ),
        desired_memory=int(
            parse_quantity(
                pod_spec_limits.get("memory") or pod_spec_requests.get("memory")
            )
        ),
        current_cpu=float(
            parse_quantity(
                pod_status_limits.get("cpu") or pod_status_requests.get("cpu")
            )
        ),
        current_memory=int(
            parse_quantity(
                pod_status_limits.get("memory") or pod_status_requests.get("memory")
            )
        ),
    )

    pod_ippr_status_json = (
        pod["metadata"].get("annotations", {}).get("ray.io/ippr-status")
    )
    if pod_ippr_status_json:
        pod_ippr_status = json.loads(pod_ippr_status_json)
        ippr_status.raylet_id = pod_ippr_status.get("raylet-id")
        ippr_status.resized_at = pod_ippr_status.get("resized-at")
        ippr_status.suggested_max_cpu = pod_ippr_status.get("suggested-max-cpu")
        ippr_status.suggested_max_memory = pod_ippr_status.get("suggested-max-memory")

    for condition in pod.get("status", {}).get("conditions", []):
        if condition["type"] == "PodResizePending" and condition["status"] == "True":
            ippr_status.resized_message = condition.get("message")
            ippr_status.resized_status = condition.get("reason", "").lower()
            match = None
            if ippr_status.resized_message and ippr_status.resized_status == "deferred":
                match = re.search(
                    r"Node didn't have enough resource: (cpu|memory), requested: (\d+), used: (\d+), capacity: (\d+)",
                    ippr_status.resized_message,
                )
            elif (
                ippr_status.resized_message
                and ippr_status.resized_status == "infeasible"
            ):
                match = re.search(
                    r"Node didn't have enough capacity: (cpu|memory), requested: (\d+), ()capacity: (\d+)",
                    ippr_status.resized_message,
                )
            if match:
                # Example (resize to max-cpu = 8, max-memory = 20Gi):
                #   Initial pod status:
                #     - CPU limits set to 4 cores
                #     - CPU requests set to 1 core (gap = 3 cores)
                #     - Mem limits set to 8Gi
                #     - Mem requests set to 2Gi (gap = 6Gi)
                #   Initial resize request (desired values picked by autoscaler):
                #     - desired_cpu=8 cores
                #     - desired_memory=20Gi
                #   Actual resize patch:
                #     - CPU limits set 8 cores (upsize from 4)
                #     - CPU requests set to 8 - 3  = 5 cores (upsize from 1, keep the gap 3 cores)
                #     - Mem limits set to 20Gi (upsize from 8Gi)
                #     - Mem requests set to 20 − 6 = 14Gi    (upsize from 2Gi, keep the gap 6Gi)
                #   Kubelet reports (deferred):
                #     - CPU: used=6, capacity=9 → remaining_cpu = 3 cores
                #     - Mem: used=4Gi, capacity=10Gi  → remaining_mem = 6Gi
                #   Targets while preserving gaps:
                #     - Next CPU requests = 3 cores; Next Mem requests = 6Gi
                #   Suggestions used in patch:
                #     - suggested_max_cpu    = remaining_cpu + cpu_gap = 3 + 3  = 6 cores
                #     - suggested_max_memory = remaining_mem + mem_gap = 6Gi + 6Gi = 12Gi
                #   Patch outcome (requests = suggested − gap):
                #     - CPU limits set to 6 cores
                #     - CPU requests set to 6 - 3 cores (gap = 3 cores)
                #     - Mem limits set to max(12Gi, 20Gi) = 20Gi (we can't lower the limit)
                #     - Mem requests set to 12Gi - 6Gi = 6Gi (gap = 6Gi)
                used = int(match.group(3) or "0")
                capacity = int(match.group(4))
                max_request = capacity - used
                if match.group(1) == "cpu":
                    diff = parse_quantity(
                        pod_status_limits.get("cpu") or pod_status_requests.get("cpu")
                    ) - parse_quantity(pod_status_requests.get("cpu"))
                    other_container_cpu_requests = sum(
                        parse_quantity(requests.get("cpu", "0"))
                        for requests in other_container_resources
                    )
                    ippr_status.suggested_max_cpu = float(
                        Decimal(str(max_request / 1000))
                        + diff
                        - other_container_cpu_requests
                    )
                else:
                    diff = parse_quantity(
                        pod_status_limits.get("memory")
                        or pod_status_requests.get("memory")
                    ) - parse_quantity(pod_status_requests.get("memory"))
                    other_container_memory_requests = sum(
                        parse_quantity(requests.get("memory", "0"))
                        for requests in other_container_resources
                    )
                    ippr_status.suggested_max_memory = int(
                        Decimal(str(max_request))
                        + diff
                        - other_container_memory_requests
                    )
                if ippr_status.queue_resize_request(
                    desired_cpu=ippr_status.suggested_max_cpu,
                    desired_memory=ippr_status.suggested_max_memory,
                ):
                    logger.info(
                        f"Apply resize suggestions for {ippr_status.cloud_instance_id} to cpu={ippr_status.suggested_max_cpu} memory={ippr_status.suggested_max_memory}"
                    )
            break
        elif (
            condition["type"] == "PodResizeInProgress" and condition["status"] == "True"
        ):
            ippr_status.resized_message = condition.get("message")
            ippr_status.resized_status = "inprogress"
            if condition.get("reason") == "Error":
                ippr_status.resized_status = "error"
            break

    if ippr_status.is_failed() or ippr_status.is_timeout():
        if ippr_status.queue_resize_request(
            desired_cpu=ippr_status.current_cpu,
            desired_memory=ippr_status.current_memory,
        ):
            logger.info(
                f"Revert failed or stuck IPPR for {ippr_status.cloud_instance_id} with status {ippr_status}"
            )

    return (
        ippr_status,
        {
            "spec": {
                "requests": pod_spec_requests,
                "limits": pod_spec_limits,
            },
            "status": {
                "requests": pod_status_requests,
                "limits": pod_status_limits,
            },
        },
    )
