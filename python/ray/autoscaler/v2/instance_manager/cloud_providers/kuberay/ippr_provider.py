import json
import logging
import re
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional

import jsonschema

from ray._raylet import GcsClient
from ray.autoscaler._private.kuberay.node_provider import (
    KUBERAY_KIND_HEAD,
    KUBERAY_KIND_WORKER,
    KUBERAY_LABEL_KEY_KIND,
    KUBERAY_LABEL_KEY_TYPE,
    IKubernetesHttpApiClient,
    replace_patch,
)
from ray.autoscaler._private.kuberay.utils import parse_quantity
from ray.autoscaler.v2.schema import (
    IPPRGroupSpec,
    IPPRSpecs,
    IPPRSpecsSchema,
    IPPRStatus,
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
        """Propagate completed K8s resizes to Raylets via GCS.

        For any pod whose K8s resize has completed, update the corresponding Raylet's local resource
        instances via GCS gRPC and clear the pending timestamp on the pod's
        ``ray.io/ippr-status`` annotation.
        """
        for ippr_status in self._ippr_statuses.values():
            if not ippr_status.need_sync_with_raylet():
                continue
            try:
                self._gcs_client.resize_raylet_resource_instances(
                    ippr_status.raylet_id,
                    {
                        "CPU": ippr_status.current_cpu,
                        "memory": ippr_status.current_memory,
                    },
                )
                self._patch_ippr_status(ippr_status, resizing_at=None)
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

        if not self._ippr_specs.groups:
            return

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
            if ippr_status and container_resource:
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
                    updated_total_resources = (
                        self._gcs_client.resize_raylet_resource_instances(
                            resize.raylet_id,
                            {
                                "CPU": resize.desired_cpu,
                                "memory": resize.desired_memory,
                            },
                        )
                    )
                    if (
                        "CPU" not in updated_total_resources
                        or "memory" not in updated_total_resources
                    ):
                        raise RuntimeError(
                            f"CPU or memory not found in the response of resizing raylet resources: {updated_total_resources}"
                        )
                    resize.desired_cpu = float(updated_total_resources["CPU"])
                    resize.desired_memory = int(updated_total_resources["memory"])
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
        self._patch_ippr_status(resize, resizing_at=int(time.time()))

    def _patch_ippr_status(
        self, resize: IPPRStatus, resizing_at: Optional[int]
    ) -> None:
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
                                "resizing-at": resizing_at,
                                "suggested-max-cpu": resize.suggested_max_cpu,
                                "suggested-max-memory": resize.suggested_max_memory,
                                "last-failed-at": resize.last_failed_at,
                                "last-failed-reason": resize.last_failed_reason,
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
                    resize.desired_memory,
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

    pod_spec_requests = (
        pod["spec"]["containers"][0].get("resources", {}).get("requests", {})
    )
    pod_spec_limits = (
        pod["spec"]["containers"][0].get("resources", {}).get("limits", {})
    )
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

    ippr_status = _restore_ippr_status_from_annotation(ippr_status, pod)
    ippr_status = _apply_resize_conditions(
        ippr_status=ippr_status,
        pod=pod,
        pod_status_requests=pod_status_requests,
        pod_status_limits=pod_status_limits,
        other_container_resources=other_container_resources,
    )
    ippr_status = _handle_failed_or_timed_out_ippr(ippr_status)

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


def _restore_ippr_status_from_annotation(
    ippr_status: IPPRStatus, pod: Dict[str, Any]
) -> IPPRStatus:
    """Restore previously persisted IPPR status fields from pod annotations."""
    pod_ippr_status_json = (
        pod["metadata"].get("annotations", {}).get("ray.io/ippr-status")
    )
    if not pod_ippr_status_json:
        return ippr_status

    pod_ippr_status = json.loads(pod_ippr_status_json)
    ippr_status.raylet_id = pod_ippr_status.get("raylet-id")
    ippr_status.resizing_at = pod_ippr_status.get("resizing-at")
    ippr_status.suggested_max_cpu = pod_ippr_status.get("suggested-max-cpu")
    ippr_status.suggested_max_memory = pod_ippr_status.get("suggested-max-memory")
    ippr_status.last_failed_at = pod_ippr_status.get("last-failed-at")
    ippr_status.last_failed_reason = pod_ippr_status.get("last-failed-reason")
    return ippr_status


def _apply_resize_conditions(
    ippr_status: IPPRStatus,
    pod: Dict[str, Any],
    pod_status_requests: Dict[str, Any],
    pod_status_limits: Dict[str, Any],
    other_container_resources: List[Dict[str, Any]],
) -> IPPRStatus:
    """Parse pod resize conditions and queue any follow-up suggestions."""
    for condition in pod.get("status", {}).get("conditions", []):
        if condition["type"] == "PodResizePending" and condition["status"] == "True":
            ippr_status.resizing_message = condition.get("message")
            ippr_status.resizing_status = condition.get("reason", "").lower()
            ippr_status = _apply_resize_suggestion(
                ippr_status=ippr_status,
                pod_status_requests=pod_status_requests,
                pod_status_limits=pod_status_limits,
                other_container_resources=other_container_resources,
            )
            break

        if condition["type"] == "PodResizeInProgress" and condition["status"] == "True":
            ippr_status.resizing_message = condition.get("message")
            ippr_status.resizing_status = "inprogress"
            if condition.get("reason") == "Error":
                ippr_status.resizing_status = "error"
            break

    return ippr_status


def _apply_resize_suggestion(
    ippr_status: IPPRStatus,
    pod_status_requests: Dict[str, Any],
    pod_status_limits: Dict[str, Any],
    other_container_resources: List[Dict[str, Any]],
) -> IPPRStatus:
    """Parse Kubelet capacity reports and queue a suggested follow-up resize."""
    report = None
    if ippr_status.resizing_message and ippr_status.resizing_status == "deferred":
        report = re.search(
            r"Node didn't have enough resource: (cpu|memory), requested: (\d+), used: (\d+), capacity: (\d+)",
            ippr_status.resizing_message,
        )
    elif ippr_status.resizing_message and ippr_status.resizing_status == "infeasible":
        report = re.search(
            r"Node didn't have enough capacity: (cpu|memory), requested: (\d+), ()capacity: (\d+)",
            ippr_status.resizing_message,
        )

    if not report:
        return ippr_status

    # Example (resize to max-cpu = 8, max-memory = 20Gi):
    #   Initial pod status:
    #     - CPU limit is 4 cores
    #     - CPU request is 1 core (gap = 3 cores)
    #     - Mem limit is 8Gi
    #     - Mem request is 2Gi (gap = 6Gi)
    #   Initial resize request will be:
    #     - desired_cpu=8 cores
    #     - desired_memory=20Gi
    #   The actual resize patch will be:
    #     - CPU limit is 8 cores (upsize from 4)
    #     - CPU request is 8 - 3 = 5 cores (upsize from 1, keep the gap 3 cores)
    #     - Mem limit is 20Gi (upsize from 8Gi)
    #     - Mem request is 20 − 6 = 14Gi   (upsize from 2Gi, keep the gap 6Gi)
    #   If Kubelet reports (the deferred case):
    #     - CPU: used=5, capacity=9 → remaining_cpu = 4 cores
    #     - Mem: used=6Gi, capacity=10Gi  → remaining_mem = 4Gi
    #   The suggestions used in the next patch will be:
    #     - suggested_max_cpu    = remaining_cpu + cpu_gap = 4 + 3  = 7 cores
    #     - suggested_max_memory = remaining_mem + mem_gap = 4Gi + 6Gi = 10Gi
    #   The actual resize patch will be:
    #     - CPU limit is 7 cores
    #     - CPU request is 7 - 3 = 4 cores (aligned with the kubelet's report, and keep the gap 3 cores)
    #     - Mem limit is 10Gi
    #     - Mem request is 10Gi - 6Gi = 4Gi (aligned with the kubelet's report, and keep the gap 6Gi)
    used = int(report.group(3) or "0")
    capacity = int(report.group(4))
    max_request = capacity - used
    if report.group(1) == "cpu":
        ippr_status = _apply_cpu_resize_suggestion(
            ippr_status=ippr_status,
            max_request=max_request,
            pod_status_requests=pod_status_requests,
            pod_status_limits=pod_status_limits,
            other_container_resources=other_container_resources,
        )
    else:
        ippr_status = _apply_memory_resize_suggestion(
            ippr_status=ippr_status,
            max_request=max_request,
            pod_status_requests=pod_status_requests,
            pod_status_limits=pod_status_limits,
            other_container_resources=other_container_resources,
        )
    return ippr_status


def _apply_cpu_resize_suggestion(
    ippr_status: IPPRStatus,
    max_request: int,
    pod_status_requests: Dict[str, Any],
    pod_status_limits: Dict[str, Any],
    other_container_resources: List[Dict[str, Any]],
) -> IPPRStatus:
    """Apply a CPU resize suggestion derived from a Kubelet capacity report."""
    diff = parse_quantity(
        pod_status_limits.get("cpu") or pod_status_requests.get("cpu")
    ) - parse_quantity(pod_status_requests.get("cpu"))
    other_container_cpu_requests = sum(
        parse_quantity(requests.get("cpu", "0"))
        for requests in other_container_resources
    )
    ippr_status.suggested_max_cpu = float(
        Decimal(str(max_request / 1000)) + diff - other_container_cpu_requests
    )
    if ippr_status.queue_resize_request(desired_cpu=ippr_status.suggested_max_cpu):
        logger.info(
            f"Apply resize suggestions for {ippr_status.cloud_instance_id} to cpu={ippr_status.suggested_max_cpu}"
        )
    return ippr_status


def _apply_memory_resize_suggestion(
    ippr_status: IPPRStatus,
    max_request: int,
    pod_status_requests: Dict[str, Any],
    pod_status_limits: Dict[str, Any],
    other_container_resources: List[Dict[str, Any]],
) -> IPPRStatus:
    """Apply a memory resize suggestion derived from a Kubelet capacity report."""
    diff = parse_quantity(
        pod_status_limits.get("memory") or pod_status_requests.get("memory")
    ) - parse_quantity(pod_status_requests.get("memory"))
    other_container_memory_requests = sum(
        parse_quantity(requests.get("memory", "0"))
        for requests in other_container_resources
    )
    ippr_status.suggested_max_memory = int(
        Decimal(str(max_request)) + diff - other_container_memory_requests
    )
    if ippr_status.queue_resize_request(
        desired_memory=ippr_status.suggested_max_memory,
    ):
        logger.info(
            f"Apply resize suggestions for {ippr_status.cloud_instance_id} to memory={ippr_status.suggested_max_memory}"
        )
    return ippr_status


def _handle_failed_or_timed_out_ippr(ippr_status: IPPRStatus) -> IPPRStatus:
    """Record terminal IPPR failures and queue a revert to current resources."""
    if not (ippr_status.is_failed() or ippr_status.is_timeout()):
        return ippr_status

    if ippr_status.last_failed_at is None:
        if ippr_status.is_failed():
            ippr_status.record_failure(
                reason=ippr_status.resizing_message or "Pod resize failed"
            )
        else:
            ippr_status.record_failure(reason="Pod resize timed out")

    if ippr_status.queue_resize_request(
        desired_cpu=ippr_status.current_cpu,
        desired_memory=ippr_status.current_memory,
    ):
        logger.info(
            f"Revert failed or stuck IPPR for {ippr_status.cloud_instance_id} to cpu={ippr_status.current_cpu} memory={ippr_status.current_memory}"
        )
    return ippr_status
