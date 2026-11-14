import copy
import platform
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Type
from unittest import mock

import pytest
import requests
import yaml

from ray._private.accelerators.tpu import (
    GKE_TPU_ACCELERATOR_LABEL,
    GKE_TPU_TOPOLOGY_LABEL,
)
from ray.autoscaler._private.kuberay.autoscaling_config import (
    EXTERNAL_PLACEMENT_NONEMPTY_KEYS,
    KUEUE_PODSET_UNCONSTRAINED_TOPOLOGY_ANNOTATION,
    RAY_MULTIHOST_TOPOLOGY_EXCLUSIVE_ANNOTATION,
    RAY_MULTIHOST_TOPOLOGY_KEY_ANNOTATION,
    AutoscalingConfigProducer,
    _derive_autoscaling_config_from_ray_cr,
    _generate_available_node_types_from_ray_cr_spec,
    _get_custom_resources,
    _get_num_tpus,
    _get_ray_resources_from_group_spec,
    _get_required_node_labels,
    _has_external_multihost_placement,
    _is_k8s_qualified_name,
    _parse_worker_group_label_refs,
    _round_up_k8s_quantity,
    is_exclusive_tpu_slice_topology_key,
)

AUTOSCALING_CONFIG_MODULE_PATH = "ray.autoscaler._private.kuberay.autoscaling_config"


def get_basic_ray_cr() -> dict:
    """Returns the example Ray CR included in the Ray documentation,
    modified to include a GPU worker group and a TPU worker group.
    """
    cr_path = str(
        Path(__file__).resolve().parents[2]
        / "autoscaler"
        / "kuberay"
        / "ray-cluster.complete.yaml"
    )
    config = yaml.safe_load(open(cr_path).read())
    gpu_group = copy.deepcopy(config["spec"]["workerGroupSpecs"][0])
    gpu_group["groupName"] = "gpu-group"
    gpu_group["template"]["spec"]["containers"][0]["resources"]["limits"].setdefault(
        "nvidia.com/gpu", 3
    )
    gpu_group["maxReplicas"] = 200
    config["spec"]["workerGroupSpecs"].append(gpu_group)
    tpu_group = copy.deepcopy(config["spec"]["workerGroupSpecs"][0])
    tpu_group["groupName"] = "tpu-group"
    tpu_group["template"]["spec"]["containers"][0]["resources"]["limits"].setdefault(
        "google.com/tpu", 4
    )
    tpu_group["template"]["spec"]["nodeSelector"] = {}
    tpu_group["template"]["spec"]["nodeSelector"][
        "cloud.google.com/gke-tpu-topology"
    ] = "2x2x2"
    tpu_group["template"]["spec"]["nodeSelector"][
        "cloud.google.com/gke-tpu-accelerator"
    ] = "tpu-v4-podslice"
    tpu_group["maxReplicas"] = 4
    tpu_group["numOfHosts"] = 2
    config["spec"]["workerGroupSpecs"].append(tpu_group)
    config["metadata"]["resourceVersion"] = "123456"
    return config


def _get_basic_autoscaling_config() -> dict:
    """The expected autoscaling derived from the example Ray CR."""
    return {
        "cluster_name": "raycluster-complete",
        "provider": {
            "disable_node_updaters": True,
            "disable_launch_config_check": True,
            "foreground_node_launch": True,
            "worker_liveness_check": False,
            "namespace": "default",
            "type": "kuberay",
        },
        "available_node_types": {
            "headgroup": {
                "labels": {},
                "max_workers": 0,
                "min_workers": 0,
                "node_config": {},
                "resources": {
                    "CPU": 1,
                    "memory": 1000000000,
                    "Custom1": 1,
                    "Custom2": 5,
                },
            },
            "small-group": {
                "labels": {},
                "max_workers": 300,
                "min_workers": 0,
                "node_config": {},
                "resources": {
                    "CPU": 1,
                    "memory": 536870912,
                    "Custom2": 5,
                    "Custom3": 1,
                },
            },
            # Same as "small-group" with a GPU resource entry added
            # and modified max_workers.
            "gpu-group": {
                "labels": {},
                "max_workers": 200,
                "min_workers": 0,
                "node_config": {},
                "resources": {
                    "CPU": 1,
                    "memory": 536870912,
                    "Custom2": 5,
                    "Custom3": 1,
                    "GPU": 3,
                },
            },
            # Same as "small-group" with a TPU resource entry added
            # and modified max_workers and node_config.
            "tpu-group": {
                "labels": {
                    "ray.io/accelerator-type": "TPU-V4",
                    "ray.io/tpu-topology": "2x2x2",
                    "ray.io/tpu-pod-type": "v4-16",
                },
                "max_workers": 8,
                "min_workers": 0,
                "node_config": {},
                "resources": {
                    "CPU": 1,
                    "memory": 536870912,
                    "Custom2": 5,
                    "Custom3": 1,
                    "TPU": 4,
                    "TPU-v4-16-head": 1,
                },
                "replica_config": {
                    "workers_per_replica": 2,
                    "domain_label_key": "ray.io/tpu-slice-name",
                    "domain_exclusive": True,
                    "worker_index_label_key": "ray.io/tpu-worker-id",
                    "head_resources": ["TPU-v4-16-head"],
                },
                "runtime_label_keys": [
                    "ray.io/tpu-slice-name",
                    "ray.io/tpu-worker-id",
                ],
            },
        },
        "auth": {},
        "cluster_synced_files": [],
        "file_mounts": {},
        "file_mounts_sync_continuously": False,
        "head_node_type": "headgroup",
        "head_setup_commands": [],
        "head_start_ray_commands": [],
        "idle_timeout_minutes": 1.0,
        "initialization_commands": [],
        "max_workers": 508,
        "setup_commands": [],
        "upscaling_speed": 1000,
        "worker_setup_commands": [],
        "worker_start_ray_commands": [],
    }


def _get_ray_cr_no_cpu_error() -> dict:
    """Incorrectly formatted Ray CR without num-cpus rayStartParam and without resource
    limits. Autoscaler should raise an error when reading this.
    """
    cr = get_basic_ray_cr()
    # Verify that the num-cpus rayStartParam is not present for the worker type.
    assert "num-cpus" not in cr["spec"]["workerGroupSpecs"][0]["rayStartParams"]
    del cr["spec"]["workerGroupSpecs"][0]["template"]["spec"]["containers"][0][
        "resources"
    ]["limits"]["cpu"]
    del cr["spec"]["workerGroupSpecs"][0]["template"]["spec"]["containers"][0][
        "resources"
    ]["requests"]["cpu"]
    return cr


def _get_no_cpu_error() -> str:
    return (
        "Autoscaler failed to detect `CPU` resources for group small-group."
        "\nSet the `--num-cpus` rayStartParam and/or "
        "the CPU resource limit for the Ray container."
    )


def _get_ray_cr_with_overrides() -> dict:
    """CR with memory, cpu, and gpu overrides from rayStartParams."""
    cr = get_basic_ray_cr()
    cr["spec"]["workerGroupSpecs"][0]["rayStartParams"]["memory"] = "300000000"
    # num-gpus rayStartParam with no gpus in container limits
    cr["spec"]["workerGroupSpecs"][0]["rayStartParams"]["num-gpus"] = "100"
    # num-gpus rayStartParam overriding gpus in container limits
    cr["spec"]["workerGroupSpecs"][1]["rayStartParams"]["num-gpus"] = "100"
    cr["spec"]["workerGroupSpecs"][0]["rayStartParams"]["num-cpus"] = "100"
    return cr


def _get_autoscaling_config_with_overrides() -> dict:
    """Autoscaling config with memory and gpu annotations."""
    config = _get_basic_autoscaling_config()
    config["available_node_types"]["small-group"]["resources"]["memory"] = 300000000
    config["available_node_types"]["small-group"]["resources"]["GPU"] = 100
    config["available_node_types"]["small-group"]["resources"]["CPU"] = 100
    config["available_node_types"]["gpu-group"]["resources"]["GPU"] = 100
    return config


def _get_ray_cr_with_autoscaler_options() -> dict:
    cr = get_basic_ray_cr()
    cr["spec"]["autoscalerOptions"] = {
        "upscalingMode": "Conservative",
        "idleTimeoutSeconds": 300,
    }
    return cr


def _get_ray_cr_with_tpu_custom_resource() -> dict:
    cr = get_basic_ray_cr()
    cr["spec"]["workerGroupSpecs"][2]["rayStartParams"][
        "resources"
    ] = '"{"TPU": 4, "Custom2": 5, "Custom3": 1}"'
    # remove google.com/tpu k8s resource Pod limit
    del cr["spec"]["workerGroupSpecs"][2]["template"]["spec"]["containers"][0][
        "resources"
    ]["limits"]["google.com/tpu"]

    return cr


def _get_ray_cr_with_tpu_k8s_resource_limit_and_custom_resource() -> dict:
    cr = get_basic_ray_cr()
    cr["spec"]["workerGroupSpecs"][2]["rayStartParams"][
        "resources"
    ] = '"{"TPU": 4, "Custom2": 5, "Custom3": 1}"'

    return cr


def _get_ray_cr_with_top_level_labels() -> dict:
    """CR with a top-level `labels` field."""
    cr = get_basic_ray_cr()
    # This top-level structured labels take priority.
    cr["spec"]["workerGroupSpecs"][0]["labels"] = {"instance-type": "mx5"}
    # Top-level labels are merged over, and override, labels derived from the
    # TPU nodeSelector.
    cr["spec"]["workerGroupSpecs"][2]["labels"] = {
        "ray.io/market-type": "spot",
        "ray.io/tpu-pod-type": "v4-16-custom",
    }

    # rayStartParams labels field should be ignored.
    cr["spec"]["workerGroupSpecs"][0]["rayStartParams"]["labels"] = "instance-type=n2"
    return cr


def _get_autoscaling_config_with_top_level_labels() -> dict:
    config = _get_basic_autoscaling_config()
    config["available_node_types"]["small-group"]["labels"] = {"instance-type": "mx5"}
    config["available_node_types"]["tpu-group"]["labels"].update(
        {"ray.io/market-type": "spot", "ray.io/tpu-pod-type": "v4-16-custom"}
    )
    return config


def _get_ray_cr_with_invalid_top_level_labels() -> dict:
    """CR with a syntactically invalid top-level `labels` field."""
    cr = get_basic_ray_cr()
    cr["spec"]["workerGroupSpecs"][0]["labels"] = {"!!invalid-key!!": "some-value"}
    return cr


def _get_ray_cr_with_top_level_resources() -> dict:
    """CR with a top-level `resources` field to test priority."""
    cr = get_basic_ray_cr()

    # The top-level resources field should take priority.
    cr["spec"]["workerGroupSpecs"][1]["resources"] = {
        "CPU": "16",
        "GPU": "8",
        "memory": "2Gi",
        "CustomResource": "99",
    }
    # These rayStartParams should be ignored.
    cr["spec"]["workerGroupSpecs"][1]["rayStartParams"]["num-cpus"] = "1"
    cr["spec"]["workerGroupSpecs"][1]["rayStartParams"]["memory"] = "100000"
    cr["spec"]["workerGroupSpecs"][1]["rayStartParams"]["num-gpus"] = "2"
    cr["spec"]["workerGroupSpecs"][1]["rayStartParams"][
        "resources"
    ] = '"{"Custom2": 1}"'
    return cr


def _get_autoscaling_config_with_top_level_resources() -> dict:
    config = _get_basic_autoscaling_config()

    config["available_node_types"]["gpu-group"]["resources"] = {
        "CPU": 16,
        "GPU": 8,
        "memory": 2147483648,
        "CustomResource": 99,
    }
    return config


def _get_ray_cr_with_top_level_tpu_resource() -> dict:
    """CR with a top-level `resources` field for the TPU custom resource."""
    cr = _get_ray_cr_with_tpu_k8s_resource_limit_and_custom_resource()

    # The top-level field should take priority.
    cr["spec"]["workerGroupSpecs"][2]["resources"] = {"TPU": "8"}
    return cr


def _get_ray_cr_with_no_tpus() -> dict:
    cr = get_basic_ray_cr()
    # remove TPU worker group
    cr["spec"]["workerGroupSpecs"].pop(2)

    return cr


def _get_ray_cr_with_only_requests() -> dict:
    """CR contains only resource requests"""
    cr = get_basic_ray_cr()

    for group in [cr["spec"]["headGroupSpec"]] + cr["spec"]["workerGroupSpecs"]:
        for container in group["template"]["spec"]["containers"]:
            container["resources"]["requests"] = container["resources"]["limits"]
            del container["resources"]["limits"]
    return cr


def _get_ray_cr_with_labels() -> dict:
    """CR with labels in rayStartParams of head and worker groups."""
    cr = get_basic_ray_cr()

    # Pass invalid labels to the head group to test error handling.
    cr["spec"]["headGroupSpec"]["rayStartParams"]["labels"] = "!!ray.io/node-group=,"
    # Pass valid labels to each of the worker groups.
    cr["spec"]["workerGroupSpecs"][0]["rayStartParams"][
        "labels"
    ] = "ray.io/availability-region=us-central2, ray.io/market-type=spot"
    cr["spec"]["workerGroupSpecs"][1]["rayStartParams"][
        "labels"
    ] = "ray.io/accelerator-type=A100"
    cr["spec"]["workerGroupSpecs"][2]["rayStartParams"][
        "labels"
    ] = "ray.io/accelerator-type=TPU-V4"
    return cr


def _get_autoscaling_config_with_labels() -> dict:
    """Autoscaling config with parsed labels for each group."""
    config = _get_basic_autoscaling_config()

    # Since we passed invalid labels to the head group `rayStartParams`,
    # we expect an empty dictionary in the autoscaling config.
    config["available_node_types"]["headgroup"]["labels"] = {}
    config["available_node_types"]["small-group"]["labels"] = {
        "ray.io/availability-region": "us-central2",
        "ray.io/market-type": "spot",
    }
    config["available_node_types"]["gpu-group"]["labels"] = {
        "ray.io/accelerator-type": "A100"
    }
    config["available_node_types"]["tpu-group"]["labels"] = {
        "ray.io/accelerator-type": "TPU-V4"
    }
    return config


def _get_autoscaling_config_with_options() -> dict:
    config = _get_basic_autoscaling_config()
    config["upscaling_speed"] = 1
    config["idle_timeout_minutes"] = 5.0
    return config


def _get_ray_cr_with_worker_group_priority() -> dict:
    """CR with a `priority` field set on a worker group."""
    cr = get_basic_ray_cr()
    cr["spec"]["workerGroupSpecs"][1]["priority"] = 10
    return cr


def _get_autoscaling_config_with_worker_group_priority() -> dict:
    config = _get_basic_autoscaling_config()
    config["available_node_types"]["gpu-group"]["priority"] = 10
    return config


def _get_tpu_group_with_no_node_selectors() -> dict[str, Any]:
    cr = get_basic_ray_cr()
    tpu_group = cr["spec"]["workerGroupSpecs"][2]
    tpu_group["template"]["spec"].pop("nodeSelector", None)
    return tpu_group


def _get_tpu_group_without_accelerator_node_selector() -> dict[str, Any]:
    cr = get_basic_ray_cr()
    tpu_group = cr["spec"]["workerGroupSpecs"][2]
    tpu_group["template"]["spec"]["nodeSelector"].pop(GKE_TPU_ACCELERATOR_LABEL, None)
    return tpu_group


def _get_tpu_group_without_topology_node_selector() -> dict[str, Any]:
    cr = get_basic_ray_cr()
    tpu_group = cr["spec"]["workerGroupSpecs"][2]
    tpu_group["template"]["spec"]["nodeSelector"].pop(GKE_TPU_TOPOLOGY_LABEL, None)
    return tpu_group


def _get_tpu_group_with_v7x_node_selectors() -> dict[str, Any]:
    cr = get_basic_ray_cr()
    tpu_group = cr["spec"]["workerGroupSpecs"][2]
    tpu_group["template"]["spec"]["nodeSelector"][GKE_TPU_TOPOLOGY_LABEL] = "2x2x2"
    tpu_group["template"]["spec"]["nodeSelector"][GKE_TPU_ACCELERATOR_LABEL] = "tpu7x"
    return tpu_group


def _get_ray_cr_with_tpu_v7x() -> dict[str, Any]:
    cr = get_basic_ray_cr()
    cr["spec"]["workerGroupSpecs"][2] = _get_tpu_group_with_v7x_node_selectors()
    return cr


def _get_autoscaling_config_with_v7x() -> dict[str, Any]:
    config = _get_basic_autoscaling_config()
    config["available_node_types"]["tpu-group"]["resources"]["TPU-v7x-16-head"] = 1
    config["available_node_types"]["tpu-group"]["resources"].pop("TPU-v4-16-head", None)
    config["available_node_types"]["tpu-group"]["replica_config"]["head_resources"] = [
        "TPU-v7x-16-head"
    ]
    config["available_node_types"]["tpu-group"]["labels"].update(
        {
            "ray.io/accelerator-type": "TPU-V7X",
            "ray.io/tpu-topology": "2x2x2",
            "ray.io/tpu-pod-type": "v7x-16",
        }
    )
    return config


def _get_tpu_group_with_v5litepod_node_selectors() -> dict[str, Any]:
    cr = get_basic_ray_cr()
    tpu_group = cr["spec"]["workerGroupSpecs"][2]
    tpu_group["template"]["spec"]["nodeSelector"][GKE_TPU_TOPOLOGY_LABEL] = "2x4"
    tpu_group["template"]["spec"]["nodeSelector"][
        GKE_TPU_ACCELERATOR_LABEL
    ] = "tpu-v5-lite-podslice"
    return tpu_group


def _get_ray_cr_with_tpu_v5litepod() -> dict[str, Any]:
    cr = get_basic_ray_cr()
    cr["spec"]["workerGroupSpecs"][2] = _get_tpu_group_with_v5litepod_node_selectors()
    return cr


def _get_autoscaling_config_with_v5litepod() -> dict[str, Any]:
    config = _get_basic_autoscaling_config()
    config["available_node_types"]["tpu-group"]["resources"]["TPU-v5litepod-8-head"] = 1
    config["available_node_types"]["tpu-group"]["resources"].pop("TPU-v4-16-head", None)
    config["available_node_types"]["tpu-group"]["replica_config"]["head_resources"] = [
        "TPU-v5litepod-8-head"
    ]
    config["available_node_types"]["tpu-group"]["labels"].update(
        {
            "ray.io/accelerator-type": "TPU-V5LITEPOD",
            "ray.io/tpu-topology": "2x4",
            "ray.io/tpu-pod-type": "v5litepod-8",
        }
    )
    return config


@pytest.mark.parametrize(
    "input,output",
    [
        # There's no particular discipline to these test cases.
        ("100m", 1),
        ("15001m", 16),
        ("2", 2),
        ("100Mi", 104857600),
        ("1G", 1000000000),
    ],
)
def test_resource_quantity(input: str, output: int):
    assert _round_up_k8s_quantity(input) == output, output


PARAM_ARGS = ",".join(
    [
        "ray_cr_in",
        "expected_config_out",
        "expected_error",
        "expected_error_message",
        "expected_log_warning",
    ]
)

TEST_DATA = (
    []
    if platform.system() == "Windows"
    else [
        pytest.param(
            get_basic_ray_cr(),
            _get_basic_autoscaling_config(),
            None,
            None,
            None,
            id="basic",
        ),
        pytest.param(
            _get_ray_cr_with_only_requests(),
            _get_basic_autoscaling_config(),
            None,
            None,
            None,
            id="only-requests",
        ),
        pytest.param(
            _get_ray_cr_no_cpu_error(),
            None,
            ValueError,
            _get_no_cpu_error(),
            None,
            id="no-cpu-error",
        ),
        pytest.param(
            _get_ray_cr_with_overrides(),
            _get_autoscaling_config_with_overrides(),
            None,
            None,
            None,
            id="overrides",
        ),
        pytest.param(
            _get_ray_cr_with_autoscaler_options(),
            _get_autoscaling_config_with_options(),
            None,
            None,
            None,
            id="autoscaler-options",
        ),
        pytest.param(
            _get_ray_cr_with_worker_group_priority(),
            _get_autoscaling_config_with_worker_group_priority(),
            None,
            None,
            None,
            id="worker-group-priority",
        ),
        pytest.param(
            _get_ray_cr_with_tpu_custom_resource(),
            _get_basic_autoscaling_config(),
            None,
            None,
            None,
            id="tpu-custom-resource",
        ),
        pytest.param(
            get_basic_ray_cr(),
            _get_basic_autoscaling_config(),
            None,
            None,
            None,
            id="tpu-k8s-resource-limit",
        ),
        pytest.param(
            _get_ray_cr_with_tpu_k8s_resource_limit_and_custom_resource(),
            _get_basic_autoscaling_config(),
            None,
            None,
            None,
            id="tpu-k8s-resource-limit-and-custom-resource",
        ),
        pytest.param(
            _get_ray_cr_with_labels(),
            _get_basic_autoscaling_config(),
            None,
            None,
            "Ignoring labels: ray.io/accelerator-type=TPU-V4 set in rayStartParams for group 'tpu-group'. Group labels are supported in the top-level Labels field starting in KubeRay v1.5",
            id="groups-with-raystartparam-labels",
        ),
        pytest.param(
            _get_ray_cr_with_top_level_labels(),
            _get_autoscaling_config_with_top_level_labels(),
            None,
            None,
            "Ignoring labels: instance-type=n2 set in rayStartParams for group 'small-group'. Group labels are supported in the top-level Labels field starting in KubeRay v1.5",
            id="groups-with-top-level-labels",
        ),
        pytest.param(
            _get_ray_cr_with_invalid_top_level_labels(),
            _get_basic_autoscaling_config(),
            ValueError,
            None,
            None,
            id="invalid-top-level-labels",
        ),
        pytest.param(
            _get_ray_cr_with_tpu_v7x(),
            _get_autoscaling_config_with_v7x(),
            None,
            None,
            None,
            id="tpu-v7x",
        ),
        pytest.param(
            _get_ray_cr_with_tpu_v5litepod(),
            _get_autoscaling_config_with_v5litepod(),
            None,
            None,
            None,
            id="tpu-v5litepod",
        ),
    ]
)


@pytest.mark.skipif(platform.system() == "Windows", reason="Not relevant.")
@pytest.mark.parametrize(PARAM_ARGS, TEST_DATA)
def test_autoscaling_config(
    ray_cr_in: Dict[str, Any],
    expected_config_out: Optional[Dict[str, Any]],
    expected_error: Optional[Type[Exception]],
    expected_error_message: Optional[str],
    expected_log_warning: Optional[str],
):
    ray_cr_in["metadata"]["namespace"] = "default"
    # Reset log_once state to ensure each test case is independent.
    from ray.util.debug import _logged

    _logged.clear()
    with mock.patch(f"{AUTOSCALING_CONFIG_MODULE_PATH}.logger") as mock_logger:
        if expected_error:
            with pytest.raises(expected_error, match=expected_error_message):
                _derive_autoscaling_config_from_ray_cr(ray_cr_in)
        else:
            assert (
                _derive_autoscaling_config_from_ray_cr(ray_cr_in) == expected_config_out
            )
            if expected_log_warning:
                mock_logger.warning.assert_called_with(expected_log_warning)
            else:
                mock_logger.warning.assert_not_called()


@pytest.mark.skipif(platform.system() == "Windows", reason="Not relevant.")
def test_cr_image_consistency():
    """Verify that the example config uses the same Ray image for all Ray pods."""
    cr = get_basic_ray_cr()

    group_specs = [cr["spec"]["headGroupSpec"]] + cr["spec"]["workerGroupSpecs"]
    # Head, CPU group, GPU group, TPU group.
    assert len(group_specs) == 4

    ray_containers = [
        group_spec["template"]["spec"]["containers"][0] for group_spec in group_specs
    ]

    # All Ray containers in the example config have "ray-" in their name.
    assert all("ray-" in ray_container["name"] for ray_container in ray_containers)

    # All Ray images are from the Ray repo.
    assert all(
        "rayproject/ray" in ray_container["image"] for ray_container in ray_containers
    )

    # All Ray images are the same.
    assert len({ray_container["image"] for ray_container in ray_containers}) == 1


@pytest.mark.parametrize("exception", [Exception, requests.HTTPError])
@pytest.mark.parametrize("num_exceptions", range(6))
def test_autoscaling_config_fetch_retries(exception, num_exceptions):
    """Validates retry logic in
    AutoscalingConfigProducer._fetch_ray_cr_from_k8s_with_retries.
    """

    class MockKubernetesHttpApiClient:
        def __init__(self):
            self.exception_counter = 0

        def get(self, *args, **kwargs):
            if self.exception_counter < num_exceptions:
                self.exception_counter += 1
                raise exception
            else:
                return {"ok-key": "ok-value"}

    class MockAutoscalingConfigProducer(AutoscalingConfigProducer):
        def __init__(self, *args, **kwargs):
            self.kubernetes_api_client = MockKubernetesHttpApiClient()
            self._ray_cr_path = "rayclusters/mock"

    config_producer = MockAutoscalingConfigProducer()
    # Patch retry backoff period.
    with mock.patch(
        "ray.autoscaler._private.kuberay.autoscaling_config.RAYCLUSTER_FETCH_RETRY_S",
        0,
    ):
        # If you hit an exception and it's not HTTPError, expect to raise.
        # If you hit >= 5 exceptions, expect to raise.
        # Otherwise, don't expect to raise.
        if (
            num_exceptions > 0 and exception != requests.HTTPError
        ) or num_exceptions >= 5:
            with pytest.raises(exception):
                config_producer._fetch_ray_cr_from_k8s_with_retries()
        else:
            out = config_producer._fetch_ray_cr_from_k8s_with_retries()
            assert out == {"ok-key": "ok-value"}


TPU_PARAM_ARGS = ",".join(
    [
        "ray_cr_in",
        "expected_num_tpus",
    ]
)
TPU_TEST_DATA = (
    []
    if platform.system() == "Windows"
    else [
        pytest.param(
            get_basic_ray_cr(),
            4,
            id="tpu-k8s-resource-limits",
        ),
        pytest.param(
            _get_ray_cr_with_tpu_custom_resource(),
            4,
            id="tpu-custom-resource",
        ),
        pytest.param(
            _get_ray_cr_with_tpu_k8s_resource_limit_and_custom_resource(),
            4,
            id="tpu--k8s-resource-limits-and-custom-resource",
        ),
        pytest.param(
            _get_ray_cr_with_no_tpus(),
            0,
            id="no-tpus-requested",
        ),
        pytest.param(
            _get_ray_cr_with_top_level_tpu_resource(),
            8,
            id="tpu-top-level-resource",
        ),
    ]
)


@pytest.mark.skipif(platform.system() == "Windows", reason="Not relevant.")
@pytest.mark.parametrize(TPU_PARAM_ARGS, TPU_TEST_DATA)
def test_get_num_tpus(ray_cr_in: Dict[str, Any], expected_num_tpus: int):
    """Verify that _get_num_tpus correctly returns the number of requested TPUs."""
    for worker_group in ray_cr_in["spec"]["workerGroupSpecs"]:
        group_resources = worker_group.get("resources", {})
        ray_start_params = worker_group["rayStartParams"]
        custom_resources = _get_custom_resources(
            group_resources, ray_start_params, worker_group["groupName"]
        )
        k8s_resources = worker_group["template"]["spec"]["containers"][0]["resources"]

        num_tpus = _get_num_tpus(group_resources, custom_resources, k8s_resources)

        if worker_group["groupName"] == "tpu-group":
            assert num_tpus == expected_num_tpus
        else:
            assert num_tpus is None


RAY_RESOURCES_PARAM_ARGS = ",".join(
    [
        "group_spec",
        "is_head",
        "expected_resources",
    ]
)
RAY_RESOURCES_TEST_DATA = (
    []
    if platform.system() == "Windows"
    else [
        pytest.param(
            get_basic_ray_cr()["spec"]["headGroupSpec"],
            True,
            {
                "CPU": 1,
                "memory": 1000000000,
                "Custom1": 1,
                "Custom2": 5,
            },
            id="head-group",
        ),
        pytest.param(
            get_basic_ray_cr()["spec"]["workerGroupSpecs"][0],
            False,
            {
                "CPU": 1,
                "memory": 536870912,
                "Custom2": 5,
                "Custom3": 1,
            },
            id="cpu-group",
        ),
        pytest.param(
            get_basic_ray_cr()["spec"]["workerGroupSpecs"][1],
            False,
            {
                "CPU": 1,
                "memory": 536870912,
                "Custom2": 5,
                "Custom3": 1,
                "GPU": 3,
            },
            id="gpu-group",
        ),
        pytest.param(
            get_basic_ray_cr()["spec"]["workerGroupSpecs"][2],
            False,
            {
                "CPU": 1,
                "memory": 536870912,
                "Custom2": 5,
                "Custom3": 1,
                "TPU": 4,
                "TPU-v4-16-head": 1,
            },
            id="tpu-group",
        ),
        pytest.param(
            _get_tpu_group_with_no_node_selectors(),
            False,
            {
                "CPU": 1,
                "memory": 536870912,
                "Custom2": 5,
                "Custom3": 1,
                "TPU": 4,
            },
            id="tpu-group-no-node-selectors",
        ),
        pytest.param(
            _get_tpu_group_without_accelerator_node_selector(),
            False,
            {
                "CPU": 1,
                "memory": 536870912,
                "Custom2": 5,
                "Custom3": 1,
                "TPU": 4,
            },
            id="tpu-group-no-accelerator-node-selector",
        ),
        pytest.param(
            _get_tpu_group_without_topology_node_selector(),
            False,
            {
                "CPU": 1,
                "memory": 536870912,
                "Custom2": 5,
                "Custom3": 1,
                "TPU": 4,
            },
            id="tpu-group-no-topology-node-selector",
        ),
        pytest.param(
            _get_tpu_group_with_v7x_node_selectors(),
            False,
            {
                "CPU": 1,
                "memory": 536870912,
                "Custom2": 5,
                "Custom3": 1,
                "TPU": 4,
                "TPU-v7x-16-head": 1,
            },
            id="tpu-group-v7x",
        ),
        pytest.param(
            _get_tpu_group_with_v5litepod_node_selectors(),
            False,
            {
                "CPU": 1,
                "memory": 536870912,
                "Custom2": 5,
                "Custom3": 1,
                "TPU": 4,
                "TPU-v5litepod-8-head": 1,
            },
            id="tpu-group-v5litepod",
        ),
    ]
)


@pytest.mark.skipif(platform.system() == "Windows", reason="Not relevant.")
@pytest.mark.parametrize(RAY_RESOURCES_PARAM_ARGS, RAY_RESOURCES_TEST_DATA)
def test_get_ray_resources_from_group_spec(
    group_spec: Dict[str, Any],
    is_head: bool,
    expected_resources: Dict[str, Any],
):
    assert _get_ray_resources_from_group_spec(group_spec, is_head) == expected_resources


@pytest.mark.skipif(platform.system() == "Windows", reason="Not relevant.")
def test_get_ray_resources_malformed_tpu_topology_skips_head_resource():
    group_spec = copy.deepcopy(get_basic_ray_cr()["spec"]["workerGroupSpecs"][2])
    group_spec["template"]["spec"]["nodeSelector"] = {
        "cloud.google.com/gke-tpu-accelerator": "tpu-v6e-slice",
        "cloud.google.com/gke-tpu-topology": "4x",
    }
    assert _get_ray_resources_from_group_spec(group_spec, False) == {
        "CPU": 1,
        "memory": 536870912,
        "Custom2": 5,
        "Custom3": 1,
        "TPU": 4,
    }


@pytest.mark.skipif(platform.system() == "Windows", reason="Not relevant.")
def test_top_level_resources_override_warnings():
    """
    Verify all override warnings are logged when a top-level `resources` field is used in
    addition to specifying those resources in the rayStartParams.
    """
    ray_cr_in = _get_ray_cr_with_top_level_resources()
    ray_cr_in["metadata"]["namespace"] = "default"

    with mock.patch(f"{AUTOSCALING_CONFIG_MODULE_PATH}.logger") as mock_logger:
        _derive_autoscaling_config_from_ray_cr(ray_cr_in)

        expected_calls = [
            mock.call(
                "'CPU' specified in both the top-level 'resources' field and in 'rayStartParams'. "
                "Using the value from 'resources': 16."
            ),
            mock.call(
                "'GPU' specified in both the top-level 'resources' field and in 'rayStartParams'. "
                "Using the value from 'resources': 8."
            ),
            mock.call(
                "'memory' specified in both the top-level 'resources' field and in 'rayStartParams'. "
                "Using the value from 'resources': 2Gi."
            ),
            mock.call(
                "custom resources specified in both the top-level 'resources' field and in 'rayStartParams'. "
                "Using the values from 'resources': {'CPU': '16', 'GPU': '8', 'memory': '2Gi', 'CustomResource': '99'}."
            ),
        ]

        # Assert that all expected calls were made, in any order.
        mock_logger.warning.assert_has_calls(expected_calls, any_order=True)
        assert mock_logger.warning.call_count == 4


@pytest.mark.skipif(platform.system() == "Windows", reason="Not relevant.")
def test_dynamic_slicing_tpu_labels_and_resources():
    """Verify GKE TPU Dynamic Slicing pod annotations, single-host TPU, and
    zero-TPU groups derive expected default labels and head resources."""
    from ray.autoscaler.v2.instance_manager.config import AutoscalingConfig

    cr = get_basic_ray_cr()
    cr["metadata"]["namespace"] = "default"
    cr["spec"]["workerGroupSpecs"][1]["numOfHosts"] = 4
    cr["spec"]["workerGroupSpecs"][1]["maxReplicas"] = 2

    # GKE TPU Dynamic Slicing worker group: topology lives on pod annotation
    # `cloud.google.com/gke-tpu-slice-topology` without `gke-tpu-topology` selector.
    tpu_ds = cr["spec"]["workerGroupSpecs"][2]
    tpu_ds["template"]["spec"]["nodeSelector"] = {
        "cloud.google.com/gke-tpu-accelerator": "tpu7x",
    }
    tpu_ds["template"].setdefault("metadata", {}).setdefault("annotations", {})[
        "cloud.google.com/gke-tpu-slice-topology"
    ] = "2x2x2"
    tpu_ds["resources"] = {"TPU": "8"}
    tpu_ds["numOfHosts"] = 2
    tpu_ds["maxReplicas"] = 3

    # Dynamic Slicing overriding a larger shared pool selector (`4x4x4` -> `2x2x2`),
    # plus a single-host TPU group (`numOfHosts=1`) and zero-TPU groups.
    tpu_override = copy.deepcopy(tpu_ds)
    tpu_override["groupName"] = "tpu-ds-override"
    tpu_override["template"]["spec"]["nodeSelector"][
        "cloud.google.com/gke-tpu-topology"
    ] = "4x4x4"

    tpu_single = copy.deepcopy(tpu_ds)
    tpu_single["groupName"] = "tpu-single"
    tpu_single["numOfHosts"] = 1
    tpu_single["template"]["metadata"]["annotations"][
        "cloud.google.com/gke-tpu-slice-topology"
    ] = "2x2x1"
    tpu_zero = copy.deepcopy(tpu_ds)
    tpu_zero["groupName"] = "tpu-zero-hosts"
    tpu_zero["numOfHosts"] = 0
    tpu_zero["template"]["metadata"] = {}
    tpu_zero["template"]["spec"]["nodeSelector"] = {
        "cloud.google.com/gke-tpu-accelerator": "tpu7x",
        "cloud.google.com/gke-tpu-topology": "2x2x1",
    }
    tpu_zero_res = copy.deepcopy(tpu_zero)
    tpu_zero_res["groupName"] = "tpu-zero-resource"
    tpu_zero_res["resources"] = {"CPU": "4", "TPU": "0"}
    tpu_zero_res["numOfHosts"] = 1
    cpu_zero_tpu = copy.deepcopy(cr["spec"]["workerGroupSpecs"][0])
    cpu_zero_tpu["groupName"] = "cpu-zero-tpu"
    cpu_zero_tpu["resources"] = {"CPU": "4", "TPU": "0"}
    cr["spec"]["workerGroupSpecs"].extend(
        [tpu_override, tpu_single, tpu_zero, tpu_zero_res, cpu_zero_tpu]
    )

    derived = _derive_autoscaling_config_from_ray_cr(cr)
    autoscaling_cfg = AutoscalingConfig(configs=derived)
    nt_cfgs = autoscaling_cfg.get_node_type_configs()

    from ray.autoscaler.v2.instance_manager.config import WorkerReplicaConfig

    for name in ("tpu-zero-resource", "cpu-zero-tpu"):
        assert not any(
            k.startswith("TPU-") and k.endswith("-head")
            for k in nt_cfgs[name].resources
        )
        assert "ray.io/accelerator-type" not in nt_cfgs[name].labels
        assert "ray.io/tpu-topology" not in nt_cfgs[name].labels
        assert "ray.io/tpu-pod-type" not in nt_cfgs[name].labels
        assert nt_cfgs[name].replica_config is None
        assert nt_cfgs[name].runtime_label_keys == ()
    assert nt_cfgs["gpu-group"].max_worker_nodes == 8
    for name in ("tpu-group", "tpu-ds-override"):
        assert nt_cfgs[name].max_worker_nodes == 6
        assert nt_cfgs[name].labels == {
            "ray.io/accelerator-type": "TPU-V7X",
            "ray.io/tpu-topology": "2x2x2",
            "ray.io/tpu-pod-type": "v7x-16",
        }
        assert nt_cfgs[name].resources["TPU-v7x-16-head"] == 1
        assert nt_cfgs[name].replica_config == WorkerReplicaConfig(
            workers_per_replica=2,
            domain_label_key="ray.io/tpu-slice-name",
            domain_exclusive=True,
            worker_index_label_key="ray.io/tpu-worker-id",
            head_resources=("TPU-v7x-16-head",),
        )
        assert nt_cfgs[name].runtime_label_keys == (
            "ray.io/tpu-slice-name",
            "ray.io/tpu-worker-id",
        )
    assert nt_cfgs["tpu-single"].max_worker_nodes == 3
    assert nt_cfgs["tpu-single"].replica_config == WorkerReplicaConfig(
        workers_per_replica=1,
        domain_label_key="ray.io/tpu-slice-name",
        domain_exclusive=True,
        worker_index_label_key="ray.io/tpu-worker-id",
        head_resources=("TPU-v7x-8-head",),
    )
    assert nt_cfgs["tpu-single"].runtime_label_keys == (
        "ray.io/tpu-slice-name",
        "ray.io/tpu-worker-id",
    )
    assert nt_cfgs["tpu-zero-hosts"].max_worker_nodes == 0
    assert nt_cfgs["tpu-zero-hosts"].replica_config is None
    assert nt_cfgs["tpu-zero-hosts"].runtime_label_keys == ()
    for name in ("tpu-single", "tpu-zero-hosts"):
        assert nt_cfgs[name].labels == {
            "ray.io/accelerator-type": "TPU-V7X",
            "ray.io/tpu-topology": "2x2x1",
            "ray.io/tpu-pod-type": "v7x-8",
        }
        assert nt_cfgs[name].resources["TPU-v7x-8-head"] == 1


@pytest.mark.skipif(platform.system() == "Windows", reason="Not relevant.")
@pytest.mark.parametrize(
    "pod_spec, expected",
    [
        pytest.param(
            {
                "nodeSelector": {
                    "cloud.google.com/gke-tpu-accelerator": "tpu7x",
                    "cloud.google.com/gke-tpu-topology": "2x2x2",
                }
            },
            {
                "cloud.google.com/gke-tpu-accelerator": "tpu7x",
                "cloud.google.com/gke-tpu-topology": "2x2x2",
            },
            id="node-selector-only",
        ),
        pytest.param(
            {
                "affinity": {
                    "nodeAffinity": {
                        "requiredDuringSchedulingIgnoredDuringExecution": {
                            "nodeSelectorTerms": [
                                {
                                    "matchExpressions": [
                                        {
                                            "key": "cloud.google.com/gke-tpu-accelerator",
                                            "operator": "In",
                                            "values": ["tpu7x"],
                                        },
                                        {
                                            "key": "topology.kubernetes.io/zone",
                                            "operator": "In",
                                            "values": ["us-east5-a"],
                                        },
                                    ]
                                },
                                {
                                    "matchExpressions": [
                                        {
                                            "key": "cloud.google.com/gke-tpu-accelerator",
                                            "operator": "In",
                                            "values": ["tpu7x"],
                                        },
                                        {
                                            "key": "topology.kubernetes.io/zone",
                                            "operator": "In",
                                            "values": ["us-east5-b"],
                                        },
                                    ]
                                },
                            ]
                        }
                    }
                }
            },
            {"cloud.google.com/gke-tpu-accelerator": "tpu7x"},
            id="multi-term-affinity-keeps-common-pin",
        ),
        pytest.param(
            {
                "nodeSelector": {"cloud.google.com/gke-tpu-accelerator": "tpu7x"},
                "affinity": {
                    "nodeAffinity": {
                        "requiredDuringSchedulingIgnoredDuringExecution": {
                            "nodeSelectorTerms": [
                                {
                                    "matchExpressions": [
                                        {
                                            "key": "cloud.google.com/gke-tpu-accelerator",
                                            "operator": "In",
                                            "values": ["tpu-v6e-slice", "tpu7x"],
                                        },
                                        {
                                            "key": "cloud.google.com/gke-tpu-topology",
                                            "operator": "In",
                                            "values": ["2x2x1", "2x2x2"],
                                        },
                                        {
                                            "key": "cloud.google.com/gke-tpu-topology",
                                            "operator": "In",
                                            "values": ["2x2x2", "4x4x4"],
                                        },
                                    ]
                                }
                            ]
                        }
                    }
                },
            },
            {
                "cloud.google.com/gke-tpu-accelerator": "tpu7x",
                "cloud.google.com/gke-tpu-topology": "2x2x2",
            },
            id="intersecting-in-expressions-pin-singleton",
        ),
        pytest.param(
            {
                "nodeSelector": {"cloud.google.com/gke-tpu-accelerator": "tpu7x"},
                "affinity": {
                    "nodeAffinity": {
                        "requiredDuringSchedulingIgnoredDuringExecution": {
                            "nodeSelectorTerms": []
                        }
                    }
                },
            },
            {},
            id="empty-node-selector-terms-unsatisfiable",
        ),
        pytest.param(
            {
                "nodeSelector": {"cloud.google.com/gke-tpu-accelerator": "tpu7x"},
                "affinity": {
                    "nodeAffinity": {
                        "requiredDuringSchedulingIgnoredDuringExecution": {
                            "nodeSelectorTerms": [{}]
                        }
                    }
                },
            },
            {},
            id="only-empty-term-unsatisfiable",
        ),
        pytest.param(
            {
                "nodeSelector": {"cloud.google.com/gke-tpu-accelerator": "tpu7x"},
                "affinity": {
                    "nodeAffinity": {
                        "requiredDuringSchedulingIgnoredDuringExecution": {
                            "nodeSelectorTerms": [
                                {
                                    "matchFields": [
                                        {
                                            "key": "metadata.name",
                                            "operator": "In",
                                            "values": ["node-0"],
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                },
            },
            {"cloud.google.com/gke-tpu-accelerator": "tpu7x"},
            id="match-fields-only-term-preserves-node-selector",
        ),
        pytest.param(
            {
                "affinity": {
                    "nodeAffinity": {
                        "requiredDuringSchedulingIgnoredDuringExecution": {
                            "nodeSelectorTerms": [
                                {
                                    "matchExpressions": [
                                        {
                                            "key": "cloud.google.com/gke-tpu-accelerator",
                                            "operator": "In",
                                            "values": ["tpu7x"],
                                        }
                                    ]
                                },
                                {
                                    "matchFields": [
                                        {
                                            "key": "metadata.name",
                                            "operator": "In",
                                            "values": ["node-0"],
                                        }
                                    ]
                                },
                            ]
                        }
                    }
                }
            },
            {},
            id="match-fields-only-second-term-breaks-uniqueness",
        ),
        pytest.param(
            {
                "nodeSelector": {"cloud.google.com/gke-tpu-accelerator": "tpu7x"},
                "affinity": {
                    "nodeAffinity": {
                        "requiredDuringSchedulingIgnoredDuringExecution": {
                            "nodeSelectorTerms": [
                                {
                                    "matchExpressions": [
                                        {
                                            "key": "cloud.google.com/gke-tpu-accelerator",
                                            "operator": "In",
                                            "values": ["tpu-v6e-slice"],
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                },
            },
            {},
            id="conflicting-in-and-node-selector-unsatisfiable",
        ),
        pytest.param(
            {
                "affinity": {
                    "nodeAffinity": {
                        "requiredDuringSchedulingIgnoredDuringExecution": {
                            "nodeSelectorTerms": [
                                {
                                    "matchExpressions": [
                                        {
                                            "key": "cloud.google.com/gke-tpu-accelerator",
                                            "operator": "In",
                                            "values": ["tpu7x", "tpu-v6e-slice"],
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                }
            },
            {},
            id="multi-valued-in-not-pinned",
        ),
        pytest.param(
            {
                "affinity": {
                    "nodeAffinity": {
                        "requiredDuringSchedulingIgnoredDuringExecution": {
                            "nodeSelectorTerms": [
                                {
                                    "matchExpressions": [
                                        {
                                            "key": "cloud.google.com/gke-tpu-accelerator",
                                            "operator": "In",
                                            "values": ["tpu7x"],
                                        }
                                    ]
                                },
                                {
                                    "matchExpressions": [
                                        {
                                            "key": "cloud.google.com/gke-tpu-accelerator",
                                            "operator": "In",
                                            "values": ["tpu-v6e-slice"],
                                        }
                                    ]
                                },
                            ]
                        }
                    }
                }
            },
            {},
            id="disjoint-terms-not-pinned",
        ),
        pytest.param(
            {
                "affinity": {
                    "nodeAffinity": {
                        "requiredDuringSchedulingIgnoredDuringExecution": {
                            "nodeSelectorTerms": [
                                {
                                    "matchExpressions": [
                                        {
                                            "key": "cloud.google.com/gke-tpu-accelerator",
                                            "operator": "In",
                                            "values": ["tpu7x"],
                                        },
                                        {
                                            "key": "cloud.google.com/gke-tpu-topology",
                                            "operator": "Exists",
                                        },
                                    ]
                                },
                                {},
                                {
                                    "matchExpressions": [
                                        {
                                            "key": "cloud.google.com/gke-tpu-accelerator",
                                            "operator": "In",
                                            "values": [],
                                        }
                                    ]
                                },
                            ]
                        }
                    }
                }
            },
            {"cloud.google.com/gke-tpu-accelerator": "tpu7x"},
            id="skips-empty-and-empty-in-terms",
        ),
        pytest.param(
            {
                "nodeSelector": {
                    "cloud.google.com/gke-tpu-accelerator": "tpu-v6e-slice",
                    "cloud.google.com/gke-tpu-topology": "4x4",
                },
                "affinity": {
                    "nodeAffinity": {
                        "preferredDuringSchedulingIgnoredDuringExecution": [
                            {
                                "weight": 100,
                                "preference": {
                                    "matchExpressions": [
                                        {
                                            "key": "cloud.google.com/gke-tpu-accelerator",
                                            "operator": "In",
                                            "values": ["tpu7x"],
                                        }
                                    ]
                                },
                            }
                        ]
                    }
                },
            },
            {
                "cloud.google.com/gke-tpu-accelerator": "tpu-v6e-slice",
                "cloud.google.com/gke-tpu-topology": "4x4",
            },
            id="preferred-affinity-ignored",
        ),
    ],
)
def test_get_required_node_labels(pod_spec, expected):
    assert _get_required_node_labels(pod_spec) == expected


@pytest.mark.skipif(platform.system() == "Windows", reason="Not relevant.")
def test_node_affinity_tpu_labels_and_resources():
    """Verify required nodeAffinity derives TPU labels and head resources,
    ignores Pod template labels, and preserves explicit overrides."""
    from ray.autoscaler.v2.instance_manager.config import (
        AutoscalingConfig,
        WorkerReplicaConfig,
    )

    cr = get_basic_ray_cr()
    cr["metadata"]["namespace"] = "default"

    tpu_affinity = copy.deepcopy(cr["spec"]["workerGroupSpecs"][2])
    tpu_affinity["groupName"] = "tpu-affinity"
    tpu_affinity["template"]["spec"].pop("nodeSelector", None)
    tpu_affinity["template"]["spec"]["affinity"] = {
        "nodeAffinity": {
            "requiredDuringSchedulingIgnoredDuringExecution": {
                "nodeSelectorTerms": [
                    {
                        "matchExpressions": [
                            {
                                "key": "cloud.google.com/gke-tpu-accelerator",
                                "operator": "In",
                                "values": ["tpu-v6e-slice"],
                            },
                            {
                                "key": "cloud.google.com/gke-tpu-topology",
                                "operator": "In",
                                "values": ["4x4"],
                            },
                        ]
                    }
                ]
            }
        }
    }
    tpu_affinity["labels"] = {"ray.io/tpu-pod-type": "custom-override"}
    tpu_affinity["resources"] = {"TPU": "4", "TPU-v6e-16-head": "2"}

    # Pod metadata.labels and worker group labels must never be read as GKE TPU inputs.
    tpu_pod_labels_only = copy.deepcopy(cr["spec"]["workerGroupSpecs"][2])
    tpu_pod_labels_only["groupName"] = "tpu-pod-labels-only"
    tpu_pod_labels_only["template"]["spec"].pop("nodeSelector", None)
    tpu_pod_labels_only["template"].setdefault("metadata", {})["labels"] = {
        "cloud.google.com/gke-tpu-accelerator": "tpu7x",
        "cloud.google.com/gke-tpu-topology": "2x2x2",
    }
    tpu_pod_labels_only["labels"] = {
        "cloud.google.com/gke-tpu-accelerator": "tpu7x",
        "cloud.google.com/gke-tpu-topology": "2x2x2",
    }

    cr["spec"]["workerGroupSpecs"].extend([tpu_affinity, tpu_pod_labels_only])
    derived = _derive_autoscaling_config_from_ray_cr(cr)
    nt_cfgs = AutoscalingConfig(configs=derived).get_node_type_configs()

    assert nt_cfgs["tpu-affinity"].labels == {
        "ray.io/accelerator-type": "TPU-V6E",
        "ray.io/tpu-topology": "4x4",
        "ray.io/tpu-pod-type": "custom-override",
    }
    assert nt_cfgs["tpu-affinity"].resources["TPU-v6e-16-head"] == 2

    assert "ray.io/accelerator-type" not in nt_cfgs["tpu-pod-labels-only"].labels
    assert "ray.io/tpu-topology" not in nt_cfgs["tpu-pod-labels-only"].labels
    assert "ray.io/tpu-pod-type" not in nt_cfgs["tpu-pod-labels-only"].labels
    assert not any(
        k.startswith("TPU-") and k.endswith("-head")
        for k in nt_cfgs["tpu-pod-labels-only"].resources
    )
    assert nt_cfgs["tpu-affinity"].replica_config == WorkerReplicaConfig(
        workers_per_replica=2,
        domain_label_key="ray.io/tpu-slice-name",
        domain_exclusive=True,
        worker_index_label_key="ray.io/tpu-worker-id",
        head_resources=("TPU-v6e-16-head",),
    )
    assert nt_cfgs["tpu-affinity"].runtime_label_keys == (
        "ray.io/tpu-slice-name",
        "ray.io/tpu-worker-id",
    )
    assert nt_cfgs["tpu-pod-labels-only"].replica_config == WorkerReplicaConfig(
        workers_per_replica=2,
    )
    assert nt_cfgs["tpu-pod-labels-only"].runtime_label_keys == ()


@pytest.mark.parametrize(
    "key, expected",
    [
        ("cloud.google.com/gke-nodepool", True),
        ("cloud.google.com/gke-tpu-slice", True),
        ("cloud.google.com/gke-tpu-partition-2x2x2-id", True),
        ("cloud.google.com/gke-tpu-slice-2x4-id", True),
        ("cloud.google.com/gke-tpu-partition-foo-id", True),
        ("cloud.google.com/gke-tpu-partition--id", False),
        ("cloud.google.com/gke-tpu-slice--id", False),
        ("cloud.google.com/gke-tpu-partition-id", False),
        ("cloud.google.com/gke-tpu-slice-id", False),
        ("cloud.google.com/gke-tpu-partition-2x2x2-ID", False),
        ("Cloud.Google.Com/gke-nodepool", False),
        ("nvidia.com/gpu.clique", False),
        ("cloud.google.com/gce-topology-block", False),
        ("topology.kubernetes.io/zone", False),
        ("", False),
        (None, False),
        (123, False),
    ],
)
def test_is_exclusive_tpu_slice_topology_key(key, expected):
    assert is_exclusive_tpu_slice_topology_key(key) is expected


def _make_label_ref(node_label_key: str, name: Optional[str] = None) -> Dict[str, Any]:
    ref: Dict[str, Any] = {
        "valueFrom": {"nodeRef": {"fieldPath": f"metadata.labels['{node_label_key}']"}}
    }
    if name is not None:
        ref["name"] = name
    return ref


@pytest.mark.skipif(platform.system() == "Windows", reason="Not relevant.")
def test_worker_group_label_refs_parsing_and_runtime_label_keys():
    from ray.autoscaler.v2.instance_manager.config import (
        AutoscalingConfig,
        WorkerReplicaConfig,
    )

    cr = get_basic_ray_cr()
    cr["metadata"]["namespace"] = "default"
    cr["metadata"]["annotations"] = {
        RAY_MULTIHOST_TOPOLOGY_KEY_ANNOTATION: "nvidia.com/gpu.clique",
    }

    # Synthetic labelRefs and multihost annotations on headGroupSpec must be ignored.
    cr["spec"]["headGroupSpec"]["labelRefs"] = [
        _make_label_ref("topology.kubernetes.io/zone")
    ]
    cr["spec"]["headGroupSpec"]["template"].setdefault("metadata", {})[
        "annotations"
    ] = {
        RAY_MULTIHOST_TOPOLOGY_KEY_ANNOTATION: "topology.kubernetes.io/zone",
    }

    # Single-host non-TPU group with labelRefs (including malformed, duplicate,
    # and static-overlapping entries): emits runtime_label_keys, omits replica_config.
    small = cr["spec"]["workerGroupSpecs"][0]
    small["numOfHosts"] = 1
    small["labels"] = {"static-key": "val"}
    small["labelRefs"] = [
        None,
        "not-a-dict",
        {},
        {"valueFrom": None},
        {"valueFrom": {"nodeRef": None}},
        {"valueFrom": {"nodeRef": {"fieldPath": 123}}},
        {"valueFrom": {"nodeRef": {"fieldPath": 'metadata.labels["double-quoted"]'}}},
        {"valueFrom": {"nodeRef": {"fieldPath": "metadata.annotations['ann']"}}},
        {"valueFrom": {"nodeRef": {"fieldPath": "metadata.labels['trailing-nl']\n"}}},
        {"valueFrom": {"nodeRef": {"fieldPath": "metadata.labels['']"}}},
        {"valueFrom": {"nodeRef": {"fieldPath": "metadata.labels['   ']"}}},
        {"valueFrom": {"nodeRef": {"fieldPath": "metadata.labels[' padded ']"}}},
        {"valueFrom": {"nodeRef": {"fieldPath": "spec.providerID"}}},
        _make_label_ref("nvidia.com/gpu.clique", name="   "),
        _make_label_ref("nvidia.com/gpu.clique", name=" padded "),
        {"name": 999, "valueFrom": {"nodeRef": {"fieldPath": "metadata.labels['k']"}}},
        _make_label_ref("topology.kubernetes.io/zone"),
        _make_label_ref("nvidia.com/gpu.clique", name=""),
        _make_label_ref("cloud.google.com/gce-topology-block", name="block-alias"),
        _make_label_ref("topology.kubernetes.io/zone"),  # duplicate delivered key
        _make_label_ref("ignored.node/key", name="static-key"),  # overlaps static label
    ]

    # Multi-host GPU group with two labelRefs aliasing the same node label:
    # both appear in runtime_label_keys; first-seen wins domain_label_key.
    gpu = cr["spec"]["workerGroupSpecs"][1]
    gpu["numOfHosts"] = 4
    gpu["maxReplicas"] = 2
    gpu["labelRefs"] = [
        _make_label_ref("nvidia.com/gpu.clique", name="clique-a"),
        _make_label_ref("nvidia.com/gpu.clique", name="clique-b"),
    ]

    # TPU group with labelRefs and pod-level topology annotation: accelerator hook
    # retains TPU domain_label_key and merges labelRefs + TPU keys in order.
    tpu_group = cr["spec"]["workerGroupSpecs"][2]
    tpu_group["template"].setdefault("metadata", {})["annotations"] = {
        RAY_MULTIHOST_TOPOLOGY_KEY_ANNOTATION: "cloud.google.com/gce-topology-block",
        RAY_MULTIHOST_TOPOLOGY_EXCLUSIVE_ANNOTATION: "false",
    }
    tpu_group["labelRefs"] = [
        _make_label_ref("cloud.google.com/gce-topology-block"),
        _make_label_ref("cloud.google.com/gke-tpu-slice-coordinates"),
    ]

    # Explicit numOfHosts: None defaults to 1.
    none_hosts = copy.deepcopy(cr["spec"]["workerGroupSpecs"][1])
    none_hosts["groupName"] = "none-hosts-group"
    none_hosts["numOfHosts"] = None
    none_hosts["maxReplicas"] = 3
    none_hosts["labelRefs"] = [_make_label_ref("topology.kubernetes.io/zone")]
    cr["spec"]["workerGroupSpecs"].append(none_hosts)

    derived = _derive_autoscaling_config_from_ray_cr(cr)
    assert "replica_config" not in derived["available_node_types"]["headgroup"]
    assert "runtime_label_keys" not in derived["available_node_types"]["headgroup"]
    assert "replica_config" not in derived["available_node_types"]["small-group"]
    assert derived["available_node_types"]["small-group"]["runtime_label_keys"] == [
        "topology.kubernetes.io/zone",
        "nvidia.com/gpu.clique",
        "block-alias",
    ]
    assert "replica_config" not in derived["available_node_types"]["none-hosts-group"]
    assert derived["available_node_types"]["none-hosts-group"]["max_workers"] == 3

    nt_cfgs = AutoscalingConfig(configs=derived).get_node_type_configs()
    assert nt_cfgs["headgroup"].replica_config is None
    assert nt_cfgs["headgroup"].runtime_label_keys == ()
    assert nt_cfgs["small-group"].replica_config is None
    assert nt_cfgs["small-group"].runtime_label_keys == (
        "topology.kubernetes.io/zone",
        "nvidia.com/gpu.clique",
        "block-alias",
    )
    assert nt_cfgs["gpu-group"].replica_config == WorkerReplicaConfig(
        workers_per_replica=4,
        domain_label_key="clique-a",
        domain_exclusive=False,
    )
    assert nt_cfgs["gpu-group"].runtime_label_keys == ("clique-a", "clique-b")
    assert nt_cfgs["tpu-group"].replica_config == WorkerReplicaConfig(
        workers_per_replica=2,
        domain_label_key="ray.io/tpu-slice-name",
        domain_exclusive=True,
        worker_index_label_key="ray.io/tpu-worker-id",
        head_resources=("TPU-v4-16-head",),
    )
    assert nt_cfgs["tpu-group"].runtime_label_keys == (
        "cloud.google.com/gce-topology-block",
        "cloud.google.com/gke-tpu-slice-coordinates",
        "ray.io/tpu-slice-name",
        "ray.io/tpu-worker-id",
    )


@pytest.mark.skipif(platform.system() == "Windows", reason="Not relevant.")
def test_multihost_topology_annotations_precedence_and_exclusivity():
    from ray.autoscaler.v2.instance_manager.config import (
        AutoscalingConfig,
        WorkerReplicaConfig,
    )

    def _derive_gpu_group(
        cluster_annotations: Optional[Dict[str, str]] = None,
        pod_annotations: Optional[Dict[str, str]] = None,
        label_refs: Optional[list] = None,
        num_of_hosts: int = 4,
    ):
        cr = get_basic_ray_cr()
        cr["metadata"]["namespace"] = "default"
        if cluster_annotations is not None:
            cr["metadata"]["annotations"] = dict(cluster_annotations)
        gpu = copy.deepcopy(cr["spec"]["workerGroupSpecs"][1])
        gpu["numOfHosts"] = num_of_hosts
        gpu["maxReplicas"] = 2
        if pod_annotations is not None:
            gpu["template"].setdefault("metadata", {})["annotations"] = dict(
                pod_annotations
            )
        if label_refs is not None:
            gpu["labelRefs"] = label_refs
        cr["spec"]["workerGroupSpecs"] = [gpu]
        derived = _derive_autoscaling_config_from_ray_cr(cr)
        nt_cfg = AutoscalingConfig(configs=derived).get_node_type_configs()["gpu-group"]
        return derived["available_node_types"]["gpu-group"], nt_cfg

    # 1. Cluster annotation only with unaliased labelRef (non-TPU clique -> exclusive False).
    raw, nt = _derive_gpu_group(
        cluster_annotations={
            RAY_MULTIHOST_TOPOLOGY_KEY_ANNOTATION: "nvidia.com/gpu.clique"
        },
        label_refs=[_make_label_ref("nvidia.com/gpu.clique")],
    )
    assert raw["replica_config"] == {
        "workers_per_replica": 4,
        "domain_label_key": "nvidia.com/gpu.clique",
        "domain_exclusive": False,
    }
    assert nt.replica_config == WorkerReplicaConfig(
        workers_per_replica=4,
        domain_label_key="nvidia.com/gpu.clique",
        domain_exclusive=False,
    )
    assert nt.runtime_label_keys == ("nvidia.com/gpu.clique",)

    # 2. Pod annotation overrides cluster annotation; aliased labelRef evaluates
    # is_exclusive_tpu_slice_topology_key on the K8s node key, NOT the alias.
    _, nt = _derive_gpu_group(
        cluster_annotations={
            RAY_MULTIHOST_TOPOLOGY_KEY_ANNOTATION: "nvidia.com/gpu.clique"
        },
        pod_annotations={
            RAY_MULTIHOST_TOPOLOGY_KEY_ANNOTATION: "cloud.google.com/gke-tpu-slice"
        },
        label_refs=[
            _make_label_ref("nvidia.com/gpu.clique"),
            _make_label_ref("cloud.google.com/gke-tpu-slice", name="custom-slice"),
        ],
    )
    assert nt.replica_config == WorkerReplicaConfig(
        workers_per_replica=4,
        domain_label_key="custom-slice",
        domain_exclusive=True,
    )

    _, nt = _derive_gpu_group(
        pod_annotations={
            RAY_MULTIHOST_TOPOLOGY_KEY_ANNOTATION: "cloud.google.com/gke-tpu-partition-2x2x2-id"
        },
        label_refs=[
            _make_label_ref(
                "cloud.google.com/gke-tpu-partition-2x2x2-id", name="slice-id"
            )
        ],
    )
    assert nt.replica_config == WorkerReplicaConfig(
        workers_per_replica=4,
        domain_label_key="slice-id",
        domain_exclusive=True,
    )

    # 3. Pod annotation set to "" shadows non-empty cluster annotation (no fallback).
    raw, nt = _derive_gpu_group(
        cluster_annotations={
            RAY_MULTIHOST_TOPOLOGY_KEY_ANNOTATION: "nvidia.com/gpu.clique"
        },
        pod_annotations={RAY_MULTIHOST_TOPOLOGY_KEY_ANNOTATION: ""},
        label_refs=[_make_label_ref("nvidia.com/gpu.clique")],
    )
    assert raw["replica_config"] == {"workers_per_replica": 4}
    assert nt.replica_config == WorkerReplicaConfig(workers_per_replica=4)
    assert nt.runtime_label_keys == ("nvidia.com/gpu.clique",)

    # 4. Topology annotation without matching labelRef emits workers_per_replica only.
    raw, nt = _derive_gpu_group(
        pod_annotations={
            RAY_MULTIHOST_TOPOLOGY_KEY_ANNOTATION: "nvidia.com/gpu.clique"
        },
        label_refs=[_make_label_ref("topology.kubernetes.io/zone")],
    )
    assert raw["replica_config"] == {"workers_per_replica": 4}
    assert nt.replica_config == WorkerReplicaConfig(workers_per_replica=4)
    assert nt.runtime_label_keys == ("topology.kubernetes.io/zone",)

    # 5. ray.io/multihost-topology-exclusive parsing (case-insensitive "true" only;
    # no whitespace trimming).
    for true_val in ("true", "True", "TRUE"):
        _, nt = _derive_gpu_group(
            pod_annotations={
                RAY_MULTIHOST_TOPOLOGY_KEY_ANNOTATION: "nvidia.com/gpu.clique",
                RAY_MULTIHOST_TOPOLOGY_EXCLUSIVE_ANNOTATION: true_val,
            },
            label_refs=[_make_label_ref("nvidia.com/gpu.clique")],
        )
        assert nt.replica_config.domain_exclusive is True

    for false_val in ("false", "False", "FALSE", "invalid", "", " true ", "1", "yes"):
        _, nt = _derive_gpu_group(
            cluster_annotations={
                RAY_MULTIHOST_TOPOLOGY_EXCLUSIVE_ANNOTATION: "true",
            },
            pod_annotations={
                RAY_MULTIHOST_TOPOLOGY_KEY_ANNOTATION: "cloud.google.com/gke-tpu-slice",
                RAY_MULTIHOST_TOPOLOGY_EXCLUSIVE_ANNOTATION: false_val,
            },
            label_refs=[_make_label_ref("cloud.google.com/gke-tpu-slice")],
        )
        # Pod presence shadows cluster "true" AND overrides exclusive TPU key default.
        assert nt.replica_config.domain_exclusive is False


@pytest.mark.skipif(platform.system() == "Windows", reason="Not relevant.")
def test_external_multihost_placement_triggers_and_scopes():
    from ray.autoscaler.v2.instance_manager.config import (
        AutoscalingConfig,
        WorkerReplicaConfig,
    )

    def _base_cr_and_gpu():
        cr = get_basic_ray_cr()
        cr["metadata"]["namespace"] = "default"
        gpu = copy.deepcopy(cr["spec"]["workerGroupSpecs"][1])
        gpu["numOfHosts"] = 4
        gpu["maxReplicas"] = 2
        gpu["template"].setdefault("metadata", {})["annotations"] = {
            RAY_MULTIHOST_TOPOLOGY_KEY_ANNOTATION: "nvidia.com/gpu.clique",
            RAY_MULTIHOST_TOPOLOGY_EXCLUSIVE_ANNOTATION: "true",
        }
        gpu["labelRefs"] = [_make_label_ref("nvidia.com/gpu.clique")]
        cr["spec"]["workerGroupSpecs"] = [gpu]
        return cr, gpu

    # Baseline without external placement: domain_label_key IS inferred.
    cr, gpu = _base_cr_and_gpu()
    assert _has_external_multihost_placement(cr["metadata"], gpu) is False
    derived = _derive_autoscaling_config_from_ray_cr(cr)
    assert derived["available_node_types"]["gpu-group"]["replica_config"] == {
        "workers_per_replica": 4,
        "domain_label_key": "nvidia.com/gpu.clique",
        "domain_exclusive": True,
    }

    # Trigger 1: schedulerName == "slice-scheduler" (and negative other scheduler).
    cr, gpu = _base_cr_and_gpu()
    gpu["template"]["spec"]["schedulerName"] = "default-scheduler"
    assert _has_external_multihost_placement(cr["metadata"], gpu) is False
    gpu["template"]["spec"]["schedulerName"] = "slice-scheduler"
    assert _has_external_multihost_placement(cr["metadata"], gpu) is True
    derived = _derive_autoscaling_config_from_ray_cr(cr)
    assert derived["available_node_types"]["gpu-group"]["replica_config"] == {
        "workers_per_replica": 4
    }
    assert derived["available_node_types"]["gpu-group"]["runtime_label_keys"] == [
        "nvidia.com/gpu.clique"
    ]

    # Trigger 2: schedulingGates contains {"name": "kueue.x-k8s.io/topology"}.
    cr, gpu = _base_cr_and_gpu()
    gpu["template"]["spec"]["schedulingGates"] = [{"name": "other/gate"}, None, "bad"]
    assert _has_external_multihost_placement(cr["metadata"], gpu) is False
    gpu["template"]["spec"]["schedulingGates"].append(
        {"name": "kueue.x-k8s.io/topology"}
    )
    assert _has_external_multihost_placement(cr["metadata"], gpu) is True

    # Trigger 3: slice.tpu.google.com/* in exactly 4 maps (NOT RayCluster.metadata).
    def _set_scope(cr_obj, gpu_obj, scope_name, key, val):
        if scope_name == "nodeSelector":
            gpu_obj["template"]["spec"].setdefault("nodeSelector", {})[key] = val
        elif scope_name == "pod_labels":
            gpu_obj["template"].setdefault("metadata", {}).setdefault("labels", {})[
                key
            ] = val
        elif scope_name == "pod_annotations":
            gpu_obj["template"].setdefault("metadata", {}).setdefault(
                "annotations", {}
            )[key] = val
        elif scope_name == "group_labels":
            gpu_obj.setdefault("labels", {})[key] = val
        elif scope_name == "cr_labels":
            cr_obj["metadata"].setdefault("labels", {})[key] = val
        elif scope_name == "cr_annotations":
            cr_obj["metadata"].setdefault("annotations", {})[key] = val
        else:
            raise AssertionError(scope_name)

    for scope in ("nodeSelector", "pod_labels", "pod_annotations", "group_labels"):
        for val in ("2x2x2", " "):
            cr, gpu = _base_cr_and_gpu()
            _set_scope(cr, gpu, scope, "slice.tpu.google.com/topology", val)
            assert _has_external_multihost_placement(cr["metadata"], gpu) is True
        # Empty string does NOT trigger.
        cr, gpu = _base_cr_and_gpu()
        _set_scope(cr, gpu, scope, "slice.tpu.google.com/topology", "")
        assert _has_external_multihost_placement(cr["metadata"], gpu) is False

    for neg_scope in ("cr_labels", "cr_annotations"):
        cr, gpu = _base_cr_and_gpu()
        _set_scope(cr, gpu, neg_scope, "slice.tpu.google.com/topology", "2x2x2")
        assert _has_external_multihost_placement(cr["metadata"], gpu) is False

    # Trigger 4a: 5 Volcano/Kueue non-empty keys across 5 maps (NOT nodeSelector).
    volcano_kueue_scopes = (
        "cr_labels",
        "cr_annotations",
        "pod_labels",
        "pod_annotations",
        "group_labels",
    )
    for key in EXTERNAL_PLACEMENT_NONEMPTY_KEYS:
        for scope in volcano_kueue_scopes:
            cr, gpu = _base_cr_and_gpu()
            _set_scope(cr, gpu, scope, key, "rack")
            assert _has_external_multihost_placement(cr["metadata"], gpu) is True
            # Empty string does NOT trigger.
            cr, gpu = _base_cr_and_gpu()
            _set_scope(cr, gpu, scope, key, "")
            assert _has_external_multihost_placement(cr["metadata"], gpu) is False
        # nodeSelector does NOT trigger.
        cr, gpu = _base_cr_and_gpu()
        _set_scope(cr, gpu, "nodeSelector", key, "rack")
        assert _has_external_multihost_placement(cr["metadata"], gpu) is False

    # kueue.x-k8s.io/queue-name alone does NOT trigger.
    for scope in volcano_kueue_scopes:
        cr, gpu = _base_cr_and_gpu()
        _set_scope(cr, gpu, scope, "kueue.x-k8s.io/queue-name", "local-queue")
        assert _has_external_multihost_placement(cr["metadata"], gpu) is False

    # Trigger 4b: kueue.x-k8s.io/podset-unconstrained-topology case-insensitive "true".
    for scope in volcano_kueue_scopes:
        for true_val in ("true", "True", "TRUE"):
            cr, gpu = _base_cr_and_gpu()
            _set_scope(
                cr,
                gpu,
                scope,
                KUEUE_PODSET_UNCONSTRAINED_TOPOLOGY_ANNOTATION,
                true_val,
            )
            assert _has_external_multihost_placement(cr["metadata"], gpu) is True
        for false_val in ("false", "False", "", "1", "yes", " true "):
            cr, gpu = _base_cr_and_gpu()
            _set_scope(
                cr,
                gpu,
                scope,
                KUEUE_PODSET_UNCONSTRAINED_TOPOLOGY_ANNOTATION,
                false_val,
            )
            assert _has_external_multihost_placement(cr["metadata"], gpu) is False
    cr, gpu = _base_cr_and_gpu()
    _set_scope(
        cr,
        gpu,
        "nodeSelector",
        KUEUE_PODSET_UNCONSTRAINED_TOPOLOGY_ANNOTATION,
        "true",
    )
    assert _has_external_multihost_placement(cr["metadata"], gpu) is False

    # Interaction: TPU multi-host with external placement (slice-scheduler +
    # slice.tpu.google.com/* + Kueue TAS) STILL emits full TPU replica_config.
    tpu_cr = get_basic_ray_cr()
    tpu_cr["metadata"]["namespace"] = "default"
    tpu_cr["metadata"]["annotations"] = {
        "kueue.x-k8s.io/podset-required-topology": "cloud.google.com/gce-topology-block"
    }
    tpu_group = tpu_cr["spec"]["workerGroupSpecs"][2]
    tpu_group["template"]["spec"]["schedulerName"] = "slice-scheduler"
    tpu_group["template"]["spec"]["nodeSelector"][
        "slice.tpu.google.com/topology"
    ] = "2x2x2"
    tpu_group["labelRefs"] = [_make_label_ref("cloud.google.com/gce-topology-block")]
    derived = _derive_autoscaling_config_from_ray_cr(tpu_cr)
    nt_cfgs = AutoscalingConfig(configs=derived).get_node_type_configs()
    assert nt_cfgs["tpu-group"].replica_config == WorkerReplicaConfig(
        workers_per_replica=2,
        domain_label_key="ray.io/tpu-slice-name",
        domain_exclusive=True,
        worker_index_label_key="ray.io/tpu-worker-id",
        head_resources=("TPU-v4-16-head",),
    )
    assert nt_cfgs["tpu-group"].runtime_label_keys == (
        "cloud.google.com/gce-topology-block",
        "ray.io/tpu-slice-name",
        "ray.io/tpu-worker-id",
    )


@pytest.mark.skipif(platform.system() == "Windows", reason="Not relevant.")
def test_direct_spec_helper_without_metadata_backwards_compat():
    cr = get_basic_ray_cr()
    node_types = _generate_available_node_types_from_ray_cr_spec(cr["spec"])
    assert node_types["tpu-group"]["replica_config"] == {
        "workers_per_replica": 2,
        "domain_label_key": "ray.io/tpu-slice-name",
        "domain_exclusive": True,
        "worker_index_label_key": "ray.io/tpu-worker-id",
        "head_resources": ["TPU-v4-16-head"],
    }
    assert node_types["tpu-group"]["runtime_label_keys"] == [
        "ray.io/tpu-slice-name",
        "ray.io/tpu-worker-id",
    ]


@pytest.mark.parametrize(
    "value, expected",
    [
        # Valid qualified names (including max boundary lengths and >63-char prefix segments)
        ("a", True),
        ("A", True),
        ("0", True),
        ("a-b_c.d", True),
        ("example.com/My_Name-1.0", True),
        ("a..b", True),
        ("a__b", True),
        ("a--b", True),
        ("a" * 63, True),
        ("a" * 253 + "/" + "b" * 63, True),
        ("a" * 64 + "." + "b" * 64 + "/name", True),
        ("ray.io/tpu-slice-name", True),
        ("cloud.google.com/gke-tpu-partition-2x2x2-id", True),
        ("topology.kubernetes.io/zone", True),
        # Invalid: whitespace and trailing newline
        ("a b", False),
        ("a\nb", False),
        ("foo\nbar", False),
        ("a\tb", False),
        (" leading", False),
        ("trailing ", False),
        ("foo\n", False),
        ("example.com/foo\n", False),
        # Invalid: slash structure
        ("", False),
        ("/", False),
        ("/foo", False),
        ("foo/", False),
        ("a/b/c", False),
        # Invalid: DNS1123 subdomain prefix
        ("FOO.com/bar", False),
        ("example_com/bar", False),
        ("a..b/c", False),
        (".a/b", False),
        ("a./b", False),
        ("-a.com/b", False),
        ("a-.com/b", False),
        ("a" * 254 + "/bar", False),
        # Invalid: name part
        ("a" * 64, False),
        ("example.com/" + "a" * 64, False),
        ("-bad", False),
        ("bad-", False),
        ("_bad", False),
        ("bad_", False),
        (".bad", False),
        ("bad.", False),
        ("bad@char", False),
        ("caf\u00e9", False),
        ("\u00e9.com/a", False),
        # Non-string values
        (None, False),
        (123, False),
    ],
)
def test_is_k8s_qualified_name(value, expected):
    assert _is_k8s_qualified_name(value) is expected


@pytest.mark.skipif(platform.system() == "Windows", reason="Not relevant.")
def test_worker_group_label_refs_k8s_qualified_name_validation():
    from ray.autoscaler.v2.instance_manager.config import (
        AutoscalingConfig,
        WorkerReplicaConfig,
    )

    # Unit checks: invalid node label keys and invalid name aliases are skipped.
    invalid_keys = [
        "a b",
        "a\nb",
        "foo\nbar",
        "a\tb",
        " leading",
        "trailing ",
        "foo\n",
        "a/b/c",
        "/foo",
        "foo/",
        "/",
        "FOO.com/bar",
        "example_com/bar",
        "a..b/c",
        ".a/b",
        "a./b",
        "-a.com/b",
        "a-.com/b",
        "a" * 254 + "/bar",
        "a" * 64,
        "example.com/" + "a" * 64,
        "-bad",
        "bad-",
        "_bad",
        "bad_",
        ".bad",
        "bad.",
        "bad@char",
        "caf\u00e9",
    ]
    invalid_refs = [_make_label_ref(key) for key in invalid_keys] + [
        _make_label_ref("topology.kubernetes.io/zone", name=key) for key in invalid_keys
    ]
    assert _parse_worker_group_label_refs({"labelRefs": invalid_refs}) == ([], {})

    # Valid boundary keys pass through intact.
    valid_boundary_keys = [
        "a",
        "0",
        "a-b_c.d",
        "example.com/My_Name-1.0",
        "a" * 63,
        "a" * 253 + "/" + "b" * 63,
        "a" * 64 + "." + "b" * 64 + "/name",
        "ray.io/tpu-slice-name",
        "cloud.google.com/gke-tpu-partition-2x2x2-id",
        "topology.kubernetes.io/zone",
    ]
    delivered, mapping = _parse_worker_group_label_refs(
        {"labelRefs": [_make_label_ref(key) for key in valid_boundary_keys]}
    )
    assert delivered == valid_boundary_keys
    assert mapping == {key: key for key in valid_boundary_keys}

    # Duplicate delivered key across distinct node labels preserves first-seen entry only.
    dup_delivered, dup_mapping = _parse_worker_group_label_refs(
        {
            "labelRefs": [
                _make_label_ref("node.a/key", name="dup"),
                _make_label_ref("node.b/key", name="dup"),
            ]
        }
    )
    assert dup_delivered == ["dup"]
    assert dup_mapping == {"node.a/key": "dup"}

    # E2E regression: invalid labelRefs never leak into runtime_label_keys or replica_config.
    cr = get_basic_ray_cr()
    cr["metadata"]["namespace"] = "default"
    gpu = copy.deepcopy(cr["spec"]["workerGroupSpecs"][1])
    gpu["numOfHosts"] = 2
    gpu["maxReplicas"] = 2
    gpu["template"].setdefault("metadata", {})["annotations"] = {
        RAY_MULTIHOST_TOPOLOGY_KEY_ANNOTATION: "a b",
    }
    gpu["labelRefs"] = [
        _make_label_ref("a b"),
        _make_label_ref("a\nb"),
        _make_label_ref("topology.kubernetes.io/zone", name="foo\nbar"),
        _make_label_ref("a/b/c"),
        _make_label_ref("/foo"),
        _make_label_ref("FOO.com/bar"),
        _make_label_ref("a" * 64),
        _make_label_ref("a" * 254 + "/bar"),
        _make_label_ref(" padded "),
    ]

    # Duplicate delivered key E2E: second distinct node label is not bound to "dup".
    dup_gpu = copy.deepcopy(cr["spec"]["workerGroupSpecs"][1])
    dup_gpu["groupName"] = "dup-gpu-group"
    dup_gpu["numOfHosts"] = 4
    dup_gpu["maxReplicas"] = 2
    dup_gpu["template"].setdefault("metadata", {})["annotations"] = {
        RAY_MULTIHOST_TOPOLOGY_KEY_ANNOTATION: "node.b/key",
    }
    dup_gpu["labelRefs"] = [
        _make_label_ref("node.a/key", name="dup"),
        _make_label_ref("node.b/key", name="dup"),
    ]

    cr["spec"]["workerGroupSpecs"] = [gpu, dup_gpu]
    derived = _derive_autoscaling_config_from_ray_cr(cr)
    nt_cfgs = AutoscalingConfig(configs=derived).get_node_type_configs()
    assert nt_cfgs["gpu-group"].runtime_label_keys == ()
    assert nt_cfgs["gpu-group"].replica_config == WorkerReplicaConfig(
        workers_per_replica=2
    )
    assert nt_cfgs["dup-gpu-group"].runtime_label_keys == ("dup",)
    assert nt_cfgs["dup-gpu-group"].replica_config == WorkerReplicaConfig(
        workers_per_replica=4
    )


@pytest.mark.skipif(platform.system() == "Windows", reason="Not relevant.")
def test_negative_num_of_hosts_and_explicit_zero_tpu_head_override():
    from ray.autoscaler.v2.instance_manager.config import (
        AutoscalingConfig,
        WorkerReplicaConfig,
    )

    cr = get_basic_ray_cr()
    cr["metadata"]["namespace"] = "default"

    # Negative numOfHosts clamps workers to 0 and skips replica_config while keeping labelRefs.
    neg_hosts = copy.deepcopy(cr["spec"]["workerGroupSpecs"][2])
    neg_hosts["groupName"] = "neg-hosts-tpu"
    neg_hosts["numOfHosts"] = -1
    neg_hosts["minReplicas"] = 1
    neg_hosts["maxReplicas"] = 4
    neg_hosts["labelRefs"] = [_make_label_ref("topology.kubernetes.io/zone")]

    # Explicit "TPU-v4-16-head": "0" suppresses head_resources while keeping TPU slice metadata.
    tpu_group = cr["spec"]["workerGroupSpecs"][2]
    tpu_group["resources"] = {"TPU": "4", "TPU-v4-16-head": "0"}

    cr["spec"]["workerGroupSpecs"].append(neg_hosts)
    derived = _derive_autoscaling_config_from_ray_cr(cr)
    raw_neg = derived["available_node_types"]["neg-hosts-tpu"]
    assert raw_neg["min_workers"] == 0
    assert raw_neg["max_workers"] == 0
    assert "replica_config" not in raw_neg

    nt_cfgs = AutoscalingConfig(configs=derived).get_node_type_configs()
    neg_nt = nt_cfgs["neg-hosts-tpu"]
    assert neg_nt.min_worker_nodes == 0
    assert neg_nt.max_worker_nodes == 0
    assert neg_nt.replica_config is None
    assert neg_nt.runtime_label_keys == ("topology.kubernetes.io/zone",)

    tpu_nt = nt_cfgs["tpu-group"]
    assert tpu_nt.resources["TPU-v4-16-head"] == 0
    assert tpu_nt.replica_config == WorkerReplicaConfig(
        workers_per_replica=2,
        domain_label_key="ray.io/tpu-slice-name",
        domain_exclusive=True,
        worker_index_label_key="ray.io/tpu-worker-id",
        head_resources=(),
    )
    assert tpu_nt.runtime_label_keys == (
        "ray.io/tpu-slice-name",
        "ray.io/tpu-worker-id",
    )


@pytest.mark.skipif(platform.system() == "Windows", reason="Not relevant.")
@pytest.mark.parametrize(
    "label_key, label_val",
    [
        ("slice.tpu.google.com/topology", "2x2x2"),
        ("kueue.x-k8s.io/podset-required-topology", "rack"),
        ("kueue.x-k8s.io/podset-unconstrained-topology", "true"),
    ],
)
def test_worker_group_labels_external_placement_e2e(label_key, label_val):
    from ray.autoscaler.v2.instance_manager.config import (
        AutoscalingConfig,
        WorkerReplicaConfig,
    )

    cr = get_basic_ray_cr()
    cr["metadata"]["namespace"] = "default"
    gpu = copy.deepcopy(cr["spec"]["workerGroupSpecs"][1])
    gpu["numOfHosts"] = 4
    gpu["maxReplicas"] = 2
    gpu["template"].setdefault("metadata", {})["annotations"] = {
        RAY_MULTIHOST_TOPOLOGY_KEY_ANNOTATION: "nvidia.com/gpu.clique",
        RAY_MULTIHOST_TOPOLOGY_EXCLUSIVE_ANNOTATION: "true",
    }
    gpu["labelRefs"] = [_make_label_ref("nvidia.com/gpu.clique")]
    gpu["labels"] = {label_key: label_val}
    cr["spec"]["workerGroupSpecs"] = [gpu]

    derived = _derive_autoscaling_config_from_ray_cr(cr)
    nt = AutoscalingConfig(configs=derived).get_node_type_configs()["gpu-group"]
    assert nt.labels[label_key] == label_val
    assert nt.replica_config == WorkerReplicaConfig(workers_per_replica=4)
    assert nt.runtime_label_keys == ("nvidia.com/gpu.clique",)


@pytest.mark.skipif(platform.system() == "Windows", reason="Not relevant.")
def test_replica_metadata_head_resources_none_and_workers_per_replica_precedence():
    from ray.autoscaler.v2.instance_manager.config import (
        AutoscalingConfig,
        WorkerReplicaConfig,
    )

    class _SingleHostNoneHeadManager:
        @staticmethod
        def get_node_group_accelerator_labels(
            resources, node_selector, annotations=None
        ):
            return None

        @staticmethod
        def get_node_group_additional_resources(
            resources, node_selector, annotations=None
        ):
            return None

        @staticmethod
        def get_node_group_replica_metadata(resources, node_selector, annotations=None):
            return {"head_resources": None}

    class _MultiHostOverrideManager:
        @staticmethod
        def get_node_group_accelerator_labels(
            resources, node_selector, annotations=None
        ):
            return None

        @staticmethod
        def get_node_group_additional_resources(
            resources, node_selector, annotations=None
        ):
            return None

        @staticmethod
        def get_node_group_replica_metadata(resources, node_selector, annotations=None):
            return {
                "domain_label_key": "custom/domain",
                "head_resources": None,
                "workers_per_replica": 999,
            }

    # Case 1: single-host group with head_resources=None pops the key and omits replica_config.
    cr1 = get_basic_ray_cr()
    cr1["metadata"]["namespace"] = "default"
    single = copy.deepcopy(cr1["spec"]["workerGroupSpecs"][1])
    single["numOfHosts"] = 1
    single["maxReplicas"] = 2
    cr1["spec"]["workerGroupSpecs"] = [single]
    with mock.patch(
        f"{AUTOSCALING_CONFIG_MODULE_PATH}.get_all_accelerator_managers",
        return_value=[_SingleHostNoneHeadManager],
    ):
        derived1 = _derive_autoscaling_config_from_ray_cr(cr1)
    raw1 = derived1["available_node_types"]["gpu-group"]
    assert "replica_config" not in raw1
    nt1 = AutoscalingConfig(configs=derived1).get_node_type_configs()["gpu-group"]
    assert nt1.replica_config is None

    # Case 2: multi-host group pops head_resources=None and keeps CR numOfHosts=4 over 999.
    cr2 = get_basic_ray_cr()
    cr2["metadata"]["namespace"] = "default"
    multi = copy.deepcopy(cr2["spec"]["workerGroupSpecs"][1])
    multi["numOfHosts"] = 4
    multi["maxReplicas"] = 2
    cr2["spec"]["workerGroupSpecs"] = [multi]
    with mock.patch(
        f"{AUTOSCALING_CONFIG_MODULE_PATH}.get_all_accelerator_managers",
        return_value=[_MultiHostOverrideManager],
    ):
        derived2 = _derive_autoscaling_config_from_ray_cr(cr2)
    raw2 = derived2["available_node_types"]["gpu-group"]
    assert "head_resources" not in raw2["replica_config"]
    nt2 = AutoscalingConfig(configs=derived2).get_node_type_configs()["gpu-group"]
    assert nt2.replica_config == WorkerReplicaConfig(
        workers_per_replica=4,
        domain_label_key="custom/domain",
        domain_exclusive=True,
        head_resources=(),
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
