import asyncio
import logging
import re
import time
import jsonschema
import json
import ray._private.utils

from functools import lru_cache
from typing import Any, Dict, List, Optional
from ray._common.network_utils import build_address
from ray._raylet import GcsClient, NodeID
from ray.core.generated import gcs_pb2, node_manager_pb2, node_manager_pb2_grpc
from ray.autoscaler._private.kuberay.node_provider import (
    KUBERAY_LABEL_KEY_TYPE,
    KUBERAY_LABEL_KEY_KIND,
    IKubernetesHttpApiClient,
    replace_patch,
)
from ray.autoscaler._private.kuberay.utils import parse_quantity
from ray.autoscaler.v2.instance_manager.cloud_providers.kuberay.cloud_provider import (
    KUBERAY_KIND_HEAD,
    KUBERAY_KIND_WORKER,
)
from ray.autoscaler.v2.schema import IPPRStatus


ippr_schema = {
    "type": "object",
    "properties": {
        "groups": {
            "type": "object",
            "additionalProperties": {
                "type": "object",
                "properties": {
                    "max-cpu": {"type": ["string", "number"]},
                    "max-memory": {"type": ["string", "number"]},
                    "resize-timeout": {"type": "integer"},
                },
                "required": ["max-cpu", "max-memory", "resize-timeout"],
            },
        }
    },
}

logger = logging.getLogger(__name__)


class KubeRayIPPRProvider:
    def __init__(
        self,
        gcs_client: GcsClient,
        k8s_api_client: IKubernetesHttpApiClient,
    ):
        self._gcs_client = gcs_client
        self._k8s_api_client = k8s_api_client
        self._ippr_spec: Dict[str, Any] = {}
        self._ippr_statuses: Dict[str, IPPRStatus] = {}
        self._container_resources: Dict[str, Any] = {}

    def validate_and_set_ippr_spec(self, ray_cluster: Optional[Dict[str, Any]]) -> None:
        if not ray_cluster:
            return

        spec_str = ray_cluster["metadata"].get("annotations", {}).get("ray.io/ippr")
        if not spec_str:
            return

        spec = json.loads(spec_str)
        jsonschema.validate(instance=spec, schema=ippr_schema)

        def validate_worker_group_spec(worker_group_spec: Dict[str, Any]) -> None:
            ray_start_params = worker_group_spec.get("rayStartParams", {})
            if "num-cpus" in ray_start_params or "memory" in ray_start_params:
                raise ValueError(
                    "should not have 'num-cpus' or 'memory' in rayStartParams if IPPR is used"
                )
            container_spec = worker_group_spec["template"]["spec"]["containers"][0]
            resource_requests = container_spec.get("resources", {}).get("requests", {})
            if "cpu" not in resource_requests or "memory" not in resource_requests:
                raise ValueError(
                    "should have 'cpu' and 'memory' in resource requests if IPPR is used"
                )

            for policy in container_spec.get("resizePolicy", []):
                resource_name = policy.get("resourceName")
                if resource_name != "cpu" and resource_name != "memory":
                    continue
                restart = policy.get("restartPolicy")
                if restart is not None and restart != "NotRequired":
                    raise ValueError("IPPR only supports restartPolicy=NotRequired")

        for group_name in self._ippr_spec.get("groups", {}).keys():
            if group_name == "headgroup":
                validate_worker_group_spec(ray_cluster["spec"]["headGroupSpec"])
                continue
            for worker_group_spec in ray_cluster["spec"].get("workerGroupSpecs", []):
                if worker_group_spec["groupName"] == group_name:
                    validate_worker_group_spec(worker_group_spec)
                    break

        self._ippr_spec = spec

    def sync_with_raylets(self) -> None:
        for ippr_status in self._ippr_statuses.values():
            if (
                ippr_status.resized_at is not None
                and ippr_status.resized_status is None
                and ippr_status.desired_cpu == ippr_status.current_cpu
                and ippr_status.desired_memory == ippr_status.current_memory
            ):
                raylet_addr = _get_raylet_address(
                    self._gcs_client, ippr_status.raylet_id
                )
                if not raylet_addr:
                    continue
                try:
                    _resize_raylet_resources(
                        raylet_addr,
                        ippr_status.current_cpu,
                        ippr_status.current_memory,
                    )
                    self._patch_ippr_status(ippr_status, resized_at=None)
                    logger.info(
                        f"Pod {ippr_status.cloud_instance_id} resized successfully"
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to resize pod {ippr_status.cloud_instance_id}: {e}"
                    )

    def sync_ippr_status_from_pods(self, pods: List[Dict[str, Any]]) -> None:
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

            ippr_group_spec = self._ippr_spec.get("groups", {}).get(
                labels[KUBERAY_LABEL_KEY_TYPE]
            )
            ippr_status, container_resource = _set_ippr_status_for_pod(
                ippr_group_spec, pod
            )
            if ippr_status:
                self._ippr_statuses[ippr_status.cloud_instance_id] = ippr_status
            if container_resource:
                self._container_resources[
                    ippr_status.cloud_instance_id
                ] = container_resource

    def do_ippr_requests(self, resizes: List[IPPRStatus]) -> None:
        for resize in resizes:
            logger.info(
                f"Resizing pod {resize.cloud_instance_id} to cpu={resize.desired_cpu} memory={resize.desired_memory} from cpu={resize.current_cpu} memory={resize.current_memory}"
            )
            if (
                resize.desired_cpu > resize.current_cpu
                or resize.desired_memory > resize.current_memory
            ):
                self._patch_ippr_resize(resize)
            else:
                raylet_addr = _get_raylet_address(self._gcs_client, resize.raylet_id)
                if not raylet_addr:
                    continue
                try:
                    self._resize_raylet_resources(
                        raylet_addr,
                        resize.desired_cpu,
                        resize.desired_memory,
                    )
                except Exception as e:
                    logger.error(
                        f"Skip failed downsizing on pod {resize.cloud_instance_id}: {e}"
                    )
                    continue
                self._patch_ippr_resize(resize)

    def get_ippr_capacities(self) -> Dict[str, Dict[str, float]]:
        return {
            group_name: {
                "CPU": float(parse_quantity(group_spec.get("max-cpu", 0))),
                "memory": float(parse_quantity(group_spec.get("max-memory", 0))),
            }
            for group_name, group_spec in self._ippr_spec.get("groups", {}).items()
        }

    def get_ippr_statuses(self) -> Dict[str, IPPRStatus]:
        return self._ippr_statuses

    def _patch_ippr_resize(self, resize: IPPRStatus) -> None:
        patch = self._get_ippr_patch(resize)
        self._k8s_api_client.patch(
            "pods/{}/resize".format(resize.cloud_instance_id), patch
        )
        self._patch_ippr_status(resize, resized_at=int(time.time()))

    def _patch_ippr_status(self, resize: IPPRStatus, resized_at: Optional[int]) -> None:
        self._k8s_api_client.patch(
            "pods/{}".format(resize.cloud_instance_id),
            {
                "metadata": {
                    "annotations": {
                        "ray.io/ippr-status": json.dumps(
                            {
                                "raylet-id": resize.raylet_id,
                                "min-cpu": resize.min_cpu,
                                "min-memory": resize.min_memory,
                                "resized-at": resized_at,
                            }
                        )
                    }
                }
            },
            content_type="application/strategic-merge-patch+json",
        )

    def _get_ippr_patch(self, resize: IPPRStatus) -> List[Dict[str, Any]]:
        patch = []
        path_prefix = "/spec/containers/0/resources"
        container_resource = self._container_resources[resize.cloud_instance_id]
        if container_resource["status"]["limits"].get("cpu"):
            patch.append(replace_patch(f"{path_prefix}/limits/cpu", resize.desired_cpu))
            patch.append(
                replace_patch(
                    f"{path_prefix}/requests/cpu",
                    float(
                        parse_quantity(
                            container_resource["status"]["requests"].get("cpu", 0)
                        )
                    )
                    + (resize.desired_cpu - resize.current_cpu),
                )
            )
        else:
            patch.append(
                replace_patch(f"{path_prefix}/requests/cpu", resize.desired_cpu)
            )
        if container_resource["status"]["limits"].get("memory"):
            patch.append(
                replace_patch(
                    f"{path_prefix}/limits/memory",
                    max(
                        resize.desired_memory,
                        float(
                            parse_quantity(
                                container_resource["spec"]["limits"].get("memory")
                            )
                        ),
                    ),
                )
            )
            patch.append(
                replace_patch(
                    f"{path_prefix}/requests/memory",
                    float(
                        parse_quantity(
                            container_resource["status"]["requests"].get("memory", 0)
                        )
                    )
                    + (resize.desired_memory - resize.current_memory),
                )
            )
        else:
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
    node_info_dict = asyncio.run(_get_node_info(gcs_client, raylet_id))
    if not node_info_dict:
        return None
    _, node_info = next(iter(node_info_dict.items()))
    raylet_addr = build_address(
        node_info.node_manager_address, node_info.node_manager_port
    )
    return raylet_addr


def _resize_raylet_resources(raylet_addr: str, cpu: float, memory: float):
    channel = ray._private.utils.init_grpc_channel(raylet_addr, asynchronous=False)
    raylet_client = node_manager_pb2_grpc.NodeManagerServiceStub(channel)
    request = node_manager_pb2.ResizeLocalResourceInstancesRequest()
    request.resources["CPU"] = cpu
    request.resources["memory"] = memory
    return raylet_client.ResizeLocalResourceInstances(request)


def _set_ippr_status_for_pod(
    ippr_group_spec: Dict[str, Any],
    pod: Dict[str, Any],
) -> tuple[Optional[IPPRStatus], Optional[Dict[str, Any]]]:
    if not ippr_group_spec:
        return (None, None)

    container_status = {}
    for status in pod.get("status", {}).get("containerStatuses", []):
        if status["name"] == pod["spec"]["containers"][0]["name"]:
            container_status = status
            break

    pod_spec_requests = (
        pod["spec"]["containers"][0].get("resources", {}).get("requests", {})
    )
    pod_spec_limits = (
        pod["spec"]["containers"][0].get("resources", {}).get("limits", {})
    )
    pod_status_requests = container_status.get("resources", {}).get("requests", {})
    pod_status_limits = container_status.get("resources", {}).get("limits", {})

    spec_cpu = float(
        parse_quantity(pod_spec_limits.get("cpu") or pod_spec_requests.get("cpu", 0))
    )
    spec_memory = float(
        parse_quantity(
            pod_spec_limits.get("memory") or pod_spec_requests.get("memory", 0)
        )
    )

    ippr_status = IPPRStatus(
        cloud_instance_id=pod["metadata"]["name"],
        min_cpu=spec_cpu,
        max_cpu=float(parse_quantity(ippr_group_spec.get("max-cpu", 0))),
        min_memory=spec_memory,
        max_memory=float(parse_quantity(ippr_group_spec.get("max-memory", 0))),
        desired_cpu=spec_cpu,
        desired_memory=spec_memory,
        current_cpu=float(
            parse_quantity(
                pod_status_limits.get("cpu")
                or pod_status_requests.get("cpu")
                or spec_cpu
            )
        ),
        current_memory=float(
            parse_quantity(
                pod_status_limits.get("memory")
                or pod_status_requests.get("memory")
                or spec_memory
            )
        ),
        resize_timeout=ippr_group_spec.get("resize-timeout", 0),
    )

    pod_ippr_status_json = (
        pod["metadata"].get("annotations", {}).get("ray.io/ippr-status")
    )
    if pod_ippr_status_json:
        pod_ippr_status = json.loads(pod_ippr_status_json)
        ippr_status.raylet_id = pod_ippr_status.get("raylet-id")
        ippr_status.min_cpu = float(
            parse_quantity(pod_ippr_status.get("min-cpu", ippr_status.min_cpu))
        )
        ippr_status.min_memory = float(
            parse_quantity(pod_ippr_status.get("min-memory", ippr_status.min_memory))
        )
        ippr_status.resized_at = pod_ippr_status.get("resized-at")

    for condition in pod.get("status", {}).get("conditions", []):
        if condition["type"] == "PodResizePending" and condition["status"] == "True":
            ippr_status.resized_message = condition.get("message")
            ippr_status.resized_status = condition.get("reason", "").lower()
            match = re.search(
                r"Node didn't have enough resource: (cpu|memory), requested: (\d+), used: (\d+), capacity: (\d+)",
                ippr_status.resized_message or "",
            )
            if match:
                # used doesn't include the part used by the current pod
                used = int(match.group(3))
                capacity = int(match.group(4))
                # the remaining resource that can be used
                remaining = float(capacity - used)
                if match.group(1) == "cpu":
                    diff = spec_cpu - (remaining / 1000)
                    ippr_status.suggested_cpu = ippr_status.desired_cpu - diff
                else:
                    diff = spec_memory - remaining
                    ippr_status.suggested_memory = ippr_status.desired_memory - diff
            break
        elif (
            condition["type"] == "PodResizeInProgress" and condition["status"] == "True"
        ):
            ippr_status.resized_message = condition.get("message")
            ippr_status.resized_status = "inprogress"
            if condition.get("reason") == "Error":
                ippr_status.resized_status = "error"
            break
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
