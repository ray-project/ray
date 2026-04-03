import json
import logging
from typing import Any, Dict, Optional, Union

import jsonschema

from ray._raylet import GcsClient
from ray.autoscaler._private.kuberay.node_provider import IKubernetesHttpApiClient
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
        pod template; maximums and timeout come from the annotation. If the
        annotation is removed, clear any previously loaded IPPR specs.

        Args:
            ray_cluster: The RayCluster custom resource as a dict. If missing or
                lacking the annotation, this method is a no-op.

        Raises:
            ValueError: If the Ray pod template is incompatible with IPPR (e.g.,
                missing required requests, using unsupported resizePolicy restarts,
                or conflicting ``rayStartParams``).

        Example:
            import json

            ray_cluster = {
                "metadata": {
                    "name": "example-raycluster",
                    "annotations": {
                        "ray.io/ippr": json.dumps(
                            {
                                "groups": {
                                    "headgroup": {
                                        "max-cpu": "4",
                                        "max-memory": "8Gi",
                                        "resize-timeout": 300,
                                    },
                                    "small-workers": {
                                        "max-cpu": 2,
                                        "max-memory": "4Gi",
                                        "resize-timeout": 120,
                                    },
                                }
                            }
                        ),
                    },
                },
                "spec": {
                    "headGroupSpec": {
                        "rayStartParams": {},
                        "template": {
                            "spec": {
                                "containers": [
                                    {
                                        "name": "ray-head",
                                        "resources": {
                                            "requests": {
                                                "cpu": "1",
                                                "memory": "2Gi",
                                            },
                                            "limits": {
                                                "cpu": "2",
                                                "memory": "4Gi",
                                            },
                                        },
                                        "resizePolicy": [
                                            {
                                                "resourceName": "cpu",
                                                "restartPolicy": "NotRequired",
                                            },
                                            {
                                                "resourceName": "memory",
                                                "restartPolicy": "NotRequired",
                                            },
                                        ],
                                    }
                                ],
                            }
                        },
                    },
                    "workerGroupSpecs": [
                        {
                            "groupName": "small-workers",
                            "rayStartParams": {},
                            "template": {
                                "spec": {
                                    "containers": [
                                        {
                                            "name": "ray-worker",
                                            "resources": {
                                                "requests": {
                                                    "cpu": "500m",
                                                    "memory": "1Gi",
                                                },
                                            },
                                        }
                                    ],
                                }
                            },
                        }
                    ],
                },
            }
            provider.validate_and_set_ippr_specs(ray_cluster)
        """
        if not ray_cluster:
            return

        specs_str = ray_cluster["metadata"].get("annotations", {}).get("ray.io/ippr")
        if not specs_str:
            self._ippr_specs = IPPRSpecs(groups={})
            return

        ippr_specs_raw = json.loads(specs_str)
        jsonschema.validate(instance=ippr_specs_raw, schema=IPPRSpecsSchema)

        # Validate and build typed spec per group
        worker_groups = {
            worker_group_spec["groupName"]: worker_group_spec
            for worker_group_spec in ray_cluster["spec"].get("workerGroupSpecs", [])
        }
        worker_groups["headgroup"] = ray_cluster["spec"]["headGroupSpec"]

        groups = {
            group_name: _build_ippr_group_spec(group_spec, worker_groups[group_name])
            for group_name, group_spec in ippr_specs_raw.get("groups", {}).items()
            if group_name in worker_groups
        }

        self._ippr_specs = IPPRSpecs(groups=groups)

    def sync_with_raylets(self) -> None:
        """Propagate completed K8s resizes to Raylets via GCS.

        For any pod whose K8s resize has completed, update the corresponding Raylet's local resource
        instances via GCS gRPC and clear the pending timestamp on the pod's
        ``ray.io/ippr-status`` annotation.


        Three situations we can have exceptions are:
        1. K8s API is not available.
        2. GCS is not available.
        3. Raylet is not available.
        If a raylet is truly dead, its pod will also be deleted eventually.
        All of the above exceptions can only be resolved in the future reconcile loops.
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
                ippr_status.resizing_at = None
                logger.info(f"Pod {ippr_status.cloud_instance_id} resized successfully")
            except Exception as e:
                logger.error(
                    f"Failed to resize pod {ippr_status.cloud_instance_id}: {e}"
                )

    def get_ippr_specs(self) -> IPPRSpecs:
        """Return the current validated IPPR specs."""
        return self._ippr_specs

    def get_ippr_statuses(self) -> Dict[str, IPPRStatus]:
        """Return the latest per-pod IPPR statuses keyed by pod name."""
        return self._ippr_statuses

    def _patch_ippr_status(
        self, resize: IPPRStatus, resizing_at: Optional[int]
    ) -> None:
        """Save the IPPR status to the pod annotation ``ray.io/ippr-status``.
        The annotation is used to track the IPPR status of the pod across reconcile loops.

        Args:
            resize: The IPPR status to save.
            resizing_at: Timestamp while a resize is in progress; pass ``None``
                to clear after the resize completes (e.g. from ``sync_with_raylets``).
        """
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


def _build_ippr_group_spec(
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
            "should have 'cpu' and 'memory' in resource requests as the resources lower bounds if IPPR is used"
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

    # pod_spec_limits are the initial resource limits specified for the pod.
    # we use it together with pod_spec_requests to derive the lower bounds for IPPR.
    pod_spec_limits = container_spec.get("resources", {}).get("limits", {})
    return IPPRGroupSpec(
        min_cpu=_resource_value(pod_spec_requests, pod_spec_limits, "cpu", float),
        min_memory=_resource_value(pod_spec_requests, pod_spec_limits, "memory", int),
        max_cpu=float(parse_quantity(group_spec.get("max-cpu"))),
        max_memory=int(parse_quantity(group_spec.get("max-memory"))),
        resize_timeout=int(group_spec.get("resize-timeout")),
    )


def _resource_value(
    requests: Dict[str, Any],
    limits: Dict[str, Any],
    resource_name: str,
    value_type: Union[type[float], type[int]],
) -> Union[float, int]:
    return value_type(
        parse_quantity(limits.get(resource_name) or requests.get(resource_name))
    )
