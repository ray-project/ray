from contextlib import suppress
import logging
import math
import requests
from typing import Any, Dict, Optional

import json

from ray.autoscaler._private.kuberay import node_provider
from ray.autoscaler._private.util import validate_config

logger = logging.getLogger(__name__)


# Logical group name for the KubeRay head group.
# Used as the name of the "head node type" by the autoscaler.
_HEAD_GROUP_NAME = "head-group"

_GPU_WARNING_LOGGED = False


class AutoscalingConfigProducer:
    """Produces an autoscaling config by reading data from the RayCluster CR.

    Used to fetch the autoscaling config at the beginning of each autoscaler iteration.

    In the context of Ray deployment on Kubernetes, the autoscaling config is an
    internal interface.

    The autoscaling config carries the strict subset of RayCluster CR data required by
    the autoscaler to make scaling decisions; in particular, the autoscaling config does
    not carry pod configuration data.

    This class is the only public object in this file.
    """

    def __init__(self, ray_cluster_name, ray_cluster_namespace):
        self._headers, self._verify = node_provider.load_k8s_secrets()
        self._ray_cr_url = node_provider.url_from_resource(
            namespace=ray_cluster_namespace, path=f"rayclusters/{ray_cluster_name}"
        )

    def __call__(self):
        ray_cr = self._fetch_ray_cr_from_k8s()
        autoscaling_config = _derive_autoscaling_config_from_ray_cr(ray_cr)
        return autoscaling_config

    def _fetch_ray_cr_from_k8s(self) -> Dict[str, Any]:
        result = requests.get(
            self._ray_cr_url, headers=self._headers, verify=self._verify
        )
        assert result.status_code == 200
        ray_cr = result.json()
        return ray_cr


def _derive_autoscaling_config_from_ray_cr(ray_cr: Dict[str, Any]) -> Dict[str, Any]:
    provider_config = _generate_provider_config(ray_cr["metadata"]["namespace"])

    available_node_types = _generate_available_node_types_from_ray_cr_spec(
        ray_cr["spec"]
    )

    # The autoscaler expects a global max workers field. We set it to the sum of
    # node type max workers.
    global_max_workers = sum(
        node_type["max_workers"] for node_type in available_node_types.values()
    )

    # Legacy autoscaling fields carry no information but are required for compatibility.
    legacy_autoscaling_fields = _generate_legacy_autoscaling_config_fields()

    autoscaling_config = {
        "provider": provider_config,
        "cluster_name": ray_cr["metadata"]["name"],
        "head_node_type": _HEAD_GROUP_NAME,
        "available_node_types": available_node_types,
        "max_workers": global_max_workers,
        # Should consider exposing `idleTimeoutMinutes` in the RayCluster CRD,
        # under an `autoscaling` field.
        "idle_timeout_minutes": 5,
        # Should consider exposing `upscalingSpeed` in the RayCluster CRD,
        # under an `autoscaling` field.
        "upscaling_speed": 1,
        **legacy_autoscaling_fields,
    }

    # Make sure the config is readable by the autoscaler.
    validate_config(autoscaling_config)

    return autoscaling_config


def _generate_provider_config(ray_cluster_namespace: str) -> Dict[str, Any]:
    """Generates the `provider` field of the autoscaling config, which carries data
    required to instantiate the KubeRay node provider.
    """
    return {
        "type": "kuberay",
        "namespace": ray_cluster_namespace,
        "disable_node_updaters": True,
        "disable_launch_config_check": True,
    }


def _generate_legacy_autoscaling_config_fields() -> Dict[str, Any]:
    """Generates legacy autoscaling config fields required for compatibiliy."""
    return {
        "file_mounts": {},
        "cluster_synced_files": [],
        "file_mounts_sync_continuously": False,
        "initialization_commands": [],
        "setup_commands": [],
        "head_setup_commands": [],
        "worker_setup_commands": [],
        "head_start_ray_commands": [],
        "worker_start_ray_commands": [],
        "auth": {},
        "head_node": {},
        "worker_nodes": {},
    }


def _generate_available_node_types_from_ray_cr_spec(
    ray_cr_spec: Dict[str, Any]
) -> Dict[str, Any]:
    """Formats autoscaler "available_node_types" field based on the Ray CR's group
    specs.
    """
    headGroupSpec = ray_cr_spec["headGroupSpec"]
    return {
        _HEAD_GROUP_NAME: _node_type_from_group_spec(headGroupSpec, is_head=True),
        **{
            worker_group_spec["groupName"]: _node_type_from_group_spec(
                worker_group_spec, is_head=False
            )
            for worker_group_spec in ray_cr_spec["workerGroupSpecs"]
        },
    }


def _node_type_from_group_spec(
    group_spec: Dict[str, Any], is_head: bool
) -> Dict[str, Any]:
    """Converts CR group spec to autoscaler node type."""
    if is_head:
        # The head node type has no workers because the head is not a worker.
        min_workers = max_workers = 0
    else:
        # `minReplicas` and `maxReplicas` are required fields for each workerGroupSpec
        min_workers = group_spec["minReplicas"]
        max_workers = group_spec["maxReplicas"]

    resources = _get_ray_resources_from_group_spec(group_spec, is_head)

    return {
        "min_workers": min_workers,
        "max_workers": max_workers,
        # `node_config` is a legacy field required for compatibility.
        # Pod config data is required by the operator but not by the autoscaler.
        "node_config": {},
        "resources": resources,
    }


def _get_ray_resources_from_group_spec(
    group_spec: Dict[str, Any], is_head: bool
) -> Dict[str, int]:
    """
    Infers Ray resources from rayStartCommands and K8s limits.
    The resources extracted are used in autoscaling calculations.

    TODO: Expose a better interface in the RayCluster CRD for Ray resource annotations.
    For now, we take the rayStartParams as the primary source of truth.
    """
    ray_start_params = group_spec["rayStartParams"]
    # This assumes the Ray container is the first.
    # TODO. Clearly warn users to put the Ray container first when using sidecars.
    k8s_resource_limits = (
        group_spec["template"]["spec"]["containers"][0]
        .get("resources", {})
        .get("limits", {})
    )
    group_name = _HEAD_GROUP_NAME if is_head else group_spec["groupName"]

    num_cpus = _get_num_cpus(ray_start_params, k8s_resource_limits, group_name)
    num_gpus = _get_num_gpus(ray_start_params, k8s_resource_limits, group_name)
    custom_resource_dict = _get_custom_resources(ray_start_params, group_name)
    memory = _get_memory(ray_start_params, k8s_resource_limits)

    # It's not allowed to use object store memory as a resource request, so we don't
    # add that to the autoscaler's resources annotations.

    resources = {}

    assert isinstance(num_cpus, int)
    resources["CPU"] = num_cpus

    if num_gpus is not None:
        resources["GPU"] = num_gpus

    if memory is not None:
        resources["memory"] = memory

    resources.update(custom_resource_dict)

    return resources


def _get_num_cpus(
    ray_start_params: Dict[str, str],
    k8s_resource_limits: Dict[str, str],
    group_name: str,
) -> int:
    if "num_cpus" in ray_start_params:
        return int(ray_start_params["num_cpus"])
    elif "cpu" in k8s_resource_limits:
        cpu_str = str(k8s_resource_limits["cpu"])
        if cpu_str[-1] == "m":
            # For example, '500m' rounds up to 1.
            return math.ceil(int(cpu_str[:-1]) / 1000)
        else:
            return int(cpu_str)
    else:
        # Getting the number of CPUs is important, so raise an error if we can't do it.
        raise ValueError(
            f"Autoscaler failed to detect `CPU` resources for group {group_name}."
            "\nSet the `--num-cpus` rayStartParam and/or "
            "the CPU resource limit for the Ray container."
        )


def _get_memory(
    ray_start_params: Dict[str, str], k8s_resource_limits: Dict[str, Any]
) -> Optional[int]:
    """Get memory resource annotation from ray_start_params, if it is set there.

    TODO, maybe: Consider container resource limits as in
    https://github.com/ray-project/ray/pull/14567/files
    """
    if "memory" in ray_start_params:
        return int(ray_start_params["memory"])
    return None


def _get_num_gpus(
    ray_start_params: Dict[str, str],
    k8s_resource_limits: Dict[str, Any],
    group_name: str,
) -> Optional[int]:
    """Read the number of GPUs from the Ray start params.

    Potential TODO: Read GPU info from the container spec, here and in the
    Ray Operator.
    """

    if "num_gpus" in ray_start_params:
        return int(ray_start_params["num_gpus"])

    # Issue a warning if GPUs are present in the container spec but not in the
    # ray start params.
    # TODO: Consider reading GPU info from container spec.
    for key in k8s_resource_limits:
        global _GPU_WARNING_LOGGED
        if "gpu" in key and not _GPU_WARNING_LOGGED:
            with suppress(Exception):
                if int(k8s_resource_limits[key]) > 0:
                    logger.warning(
                        f"Detected GPUs in container resources for group {group_name}."
                        "To ensure Ray and the autoscaler are aware of the GPUs,"
                        " set the `--num-gpus` rayStartParam."
                    )
                    _GPU_WARNING_LOGGED = True
            break

    return None


def _get_custom_resources(
    ray_start_params: Dict[str, Any], group_name: str
) -> Dict[str, int]:
    """Format custom resources based on the `resources` Ray start param.

    For the current prototype, the value of the `resources` field must
    be formatted as follows:
    '"{\"Custom1\": 1, \"Custom2\": 5}"'.

    We intend to provide a better interface soon.

    This method first converts the input to a correctly formatted
    json string and then loads that json string to a dict.
    """
    if "resources" not in ray_start_params:
        return {}
    resources_string = ray_start_params["resources"]
    try:
        # Drop the extra pair of quotes and remove the backslash escapes.
        # resources_json should be a json string.
        resources_json = resources_string[1:-1].replace("\\", "")
        # Load a dict from the json string.
        resources = json.loads(resources_json)
        assert isinstance(resources, dict)
        for key, value in resources.items():
            assert isinstance(key, str)
            assert isinstance(value, int)
    except Exception as e:
        logger.error(
            f"Error reading `resource` rayStartParam for group {group_name}."
            " For the correct format, refer to example configuration at "
            "https://github.com/ray-project/ray/blob/master/python/"
            "ray/autoscaler/kuberay/ray-cluster.complete.yaml."
        )
        raise e
    return resources
