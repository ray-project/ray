import decimal
import json
import logging
import time
from typing import Any, Dict, Optional

import requests

from ray.autoscaler._private.constants import (
    DISABLE_LAUNCH_CONFIG_CHECK_KEY,
    DISABLE_NODE_UPDATERS_KEY,
    FOREGROUND_NODE_LAUNCH_KEY,
    WORKER_LIVENESS_CHECK_KEY,
    WORKER_RPC_DRAIN_KEY,
)
from ray.autoscaler._private.kuberay import node_provider, utils
from ray.autoscaler._private.util import validate_config

logger = logging.getLogger(__name__)

AUTOSCALER_OPTIONS_KEY = "autoscalerOptions"
IDLE_SECONDS_KEY = "idleTimeoutSeconds"
UPSCALING_KEY = "upscalingMode"
UPSCALING_VALUE_AGGRESSIVE = "Aggressive"
UPSCALING_VALUE_DEFAULT = "Default"
UPSCALING_VALUE_CONSERVATIVE = "Conservative"

MAX_RAYCLUSTER_FETCH_TRIES = 5
RAYCLUSTER_FETCH_RETRY_S = 5

# Logical group name for the KubeRay head group.
# Used as the name of the "head node type" by the autoscaler.
_HEAD_GROUP_NAME = "head-group"


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
        ray_cr = self._fetch_ray_cr_from_k8s_with_retries()
        autoscaling_config = _derive_autoscaling_config_from_ray_cr(ray_cr)
        return autoscaling_config

    def _fetch_ray_cr_from_k8s_with_retries(self) -> Dict[str, Any]:
        """Fetch the RayCluster CR by querying the K8s API server.

        Retry on HTTPError for robustness, in particular to protect autoscaler
        initialization.
        """
        for i in range(1, MAX_RAYCLUSTER_FETCH_TRIES + 1):
            try:
                return self._fetch_ray_cr_from_k8s()
            except requests.HTTPError as e:
                if i < MAX_RAYCLUSTER_FETCH_TRIES:
                    logger.exception(
                        "Failed to fetch RayCluster CR from K8s. Retrying."
                    )
                    time.sleep(RAYCLUSTER_FETCH_RETRY_S)
                else:
                    raise e from None

        # This branch is inaccessible. Raise to satisfy mypy.
        raise AssertionError

    def _fetch_ray_cr_from_k8s(self) -> Dict[str, Any]:
        result = requests.get(
            self._ray_cr_url, headers=self._headers, verify=self._verify
        )
        if not result.status_code == 200:
            result.raise_for_status()
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

    # Process autoscaler options.
    autoscaler_options = ray_cr["spec"].get(AUTOSCALER_OPTIONS_KEY, {})
    if IDLE_SECONDS_KEY in autoscaler_options:
        idle_timeout_minutes = autoscaler_options[IDLE_SECONDS_KEY] / 60.0
    else:
        idle_timeout_minutes = 1.0

    if autoscaler_options.get(UPSCALING_KEY) == UPSCALING_VALUE_CONSERVATIVE:
        upscaling_speed = 1  # Rate-limit upscaling if "Conservative" is set by user.
    # This elif is redudant but included for clarity.
    elif autoscaler_options.get(UPSCALING_KEY) == UPSCALING_VALUE_DEFAULT:
        upscaling_speed = 1000  # i.e. big, no rate-limiting by default
    # This elif is redudant but included for clarity.
    elif autoscaler_options.get(UPSCALING_KEY) == UPSCALING_VALUE_AGGRESSIVE:
        upscaling_speed = 1000
    else:
        upscaling_speed = 1000

    autoscaling_config = {
        "provider": provider_config,
        "cluster_name": ray_cr["metadata"]["name"],
        "head_node_type": _HEAD_GROUP_NAME,
        "available_node_types": available_node_types,
        "max_workers": global_max_workers,
        # Should consider exposing `idleTimeoutMinutes` in the RayCluster CRD,
        # under an `autoscaling` field.
        "idle_timeout_minutes": idle_timeout_minutes,
        # Should consider exposing `upscalingSpeed` in the RayCluster CRD,
        # under an `autoscaling` field.
        "upscaling_speed": upscaling_speed,
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
        DISABLE_NODE_UPDATERS_KEY: True,
        DISABLE_LAUNCH_CONFIG_CHECK_KEY: True,
        FOREGROUND_NODE_LAUNCH_KEY: True,
        WORKER_LIVENESS_CHECK_KEY: False,
        # For the time being we are letting the autoscaler drain nodes,
        # hence the following setting is set to True (the default value).
        # This is because we are observing that with the flag set to false,
        # The GCS may not be properly notified of node downscaling.
        # TODO Solve this issue, flip the key back to false -- else we may have
        # a race condition in which the autoscaler kills the Ray container
        # Kubernetes recreates it,
        # and then KubeRay deletes the pod, killing the container again.
        WORKER_RPC_DRAIN_KEY: True,
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
    """Get CPU annotation from ray_start_params or k8s_resource_limits,
    with priority for ray_start_params.
    """
    if "num-cpus" in ray_start_params:
        return int(ray_start_params["num-cpus"])
    elif "cpu" in k8s_resource_limits:
        cpu_quantity: str = k8s_resource_limits["cpu"]
        return _round_up_k8s_quantity(cpu_quantity)
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
    """Get memory resource annotation from ray_start_params or k8s_resource_limits,
    with priority for ray_start_params.
    """
    if "memory" in ray_start_params:
        return int(ray_start_params["memory"])
    elif "memory" in k8s_resource_limits:
        memory_quantity: str = k8s_resource_limits["memory"]
        return _round_up_k8s_quantity(memory_quantity)
    return None


def _get_num_gpus(
    ray_start_params: Dict[str, str],
    k8s_resource_limits: Dict[str, Any],
    group_name: str,
) -> Optional[int]:
    """Get memory resource annotation from ray_start_params or k8s_resource_limits,
    with priority for ray_start_params.
    """

    if "num-gpus" in ray_start_params:
        return int(ray_start_params["num-gpus"])
    else:
        for key in k8s_resource_limits:
            # e.g. nvidia.com/gpu
            if key.endswith("gpu"):
                # Typically, this is a string representing an interger, e.g. "1".
                gpu_resource_quantity = k8s_resource_limits[key]
                # Convert to int, making no assumptions on the gpu_resource_quantity,
                # besides that it's valid as a K8s resource quantity.
                num_gpus = _round_up_k8s_quantity(gpu_resource_quantity)
                if num_gpus > 0:
                    # Only one GPU type supported for now, break out on first
                    # "/gpu" match.
                    return num_gpus
    return None


def _round_up_k8s_quantity(quantity: str) -> int:
    """Rounds a Kubernetes resource quantity up to the nearest integer.

    Args:
        quantity: Resource quantity as a string in the canonical K8s form.

    Returns:
        The quantity, rounded up, as an integer.
    """
    resource_decimal: decimal.Decimal = utils.parse_quantity(quantity)
    rounded = resource_decimal.to_integral_value(rounding=decimal.ROUND_UP)
    return int(rounded)


def _get_custom_resources(
    ray_start_params: Dict[str, Any], group_name: str
) -> Dict[str, int]:
    """Format custom resources based on the `resources` Ray start param.

    Currently, the value of the `resources` field must
    be formatted as follows:
    '"{\"Custom1\": 1, \"Custom2\": 5}"'.

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
