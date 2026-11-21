import copy
import platform
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Type
from unittest import mock

import pytest
import requests
import yaml

from ray._private.accelerators import tpu
from ray._private.accelerators.accelerator import AcceleratorManager
from ray._private.accelerators.tpu import (
    GKE_TPU_ACCELERATOR_LABEL,
    GKE_TPU_SLICE_TOPOLOGY_ANNOTATION,
    GKE_TPU_TOPOLOGY_LABEL,
)
from ray.autoscaler._private.kuberay.autoscaling_config import (
    AutoscalingConfigProducer,
    _derive_autoscaling_config_from_ray_cr,
    _generate_available_node_types_from_ray_cr_spec,
    _get_custom_resources,
    _get_labels_from_group_spec,
    _get_num_tpus,
    _get_pod_scheduling_metadata,
    _get_ray_resources_from_group_spec,
    _get_required_node_labels,
    _parse_worker_group_label_refs,
    _round_up_k8s_quantity,
)
from ray.util.debug import reset_log_once

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
    reset_log_once()
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


def _required_node_affinity(
    terms: Any,
    node_selector: Any = None,
) -> Dict[str, Any]:
    """Build a pod spec with requiredDuringSchedulingIgnoredDuringExecution."""
    pod_spec: Dict[str, Any] = {
        "affinity": {
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": terms,
                }
            }
        }
    }
    if node_selector is not None:
        pod_spec["nodeSelector"] = node_selector
    return pod_spec


def _expr(key: str, operator: str, values: Any = None) -> Dict[str, Any]:
    """Build a Kubernetes NodeSelectorRequirement dictionary."""
    req: Dict[str, Any] = {"key": key, "operator": operator}
    if values is not None:
        req["values"] = values
    return req


@pytest.mark.skipif(platform.system() == "Windows", reason="Not relevant.")
@pytest.mark.parametrize(
    "pod_spec, expected",
    [
        pytest.param(
            {
                "nodeSelector": {
                    GKE_TPU_ACCELERATOR_LABEL: "tpu7x",
                    GKE_TPU_TOPOLOGY_LABEL: "2x2x2",
                }
            },
            {
                GKE_TPU_ACCELERATOR_LABEL: "tpu7x",
                GKE_TPU_TOPOLOGY_LABEL: "2x2x2",
            },
            id="node-selector-only",
        ),
        pytest.param(
            _required_node_affinity(
                [
                    {
                        "matchExpressions": [
                            _expr(GKE_TPU_ACCELERATOR_LABEL, "In", ["tpu7x"]),
                            _expr("topology.kubernetes.io/zone", "In", ["us-east5-a"]),
                        ]
                    },
                    {
                        "matchExpressions": [
                            _expr(GKE_TPU_ACCELERATOR_LABEL, "In", ["tpu7x"]),
                            _expr("topology.kubernetes.io/zone", "In", ["us-east5-b"]),
                        ]
                    },
                ]
            ),
            {GKE_TPU_ACCELERATOR_LABEL: "tpu7x"},
            id="multi-term-affinity-keeps-common-pin",
        ),
        pytest.param(
            _required_node_affinity(
                [
                    {
                        "matchExpressions": [
                            _expr(
                                GKE_TPU_ACCELERATOR_LABEL,
                                "In",
                                ["tpu-v6e-slice", "tpu7x"],
                            ),
                            _expr(GKE_TPU_TOPOLOGY_LABEL, "In", ["2x2x1", "2x2x2"]),
                            _expr(GKE_TPU_TOPOLOGY_LABEL, "In", ["2x2x2", "4x4x4"]),
                        ]
                    }
                ],
                node_selector={GKE_TPU_ACCELERATOR_LABEL: "tpu7x"},
            ),
            {
                GKE_TPU_ACCELERATOR_LABEL: "tpu7x",
                GKE_TPU_TOPOLOGY_LABEL: "2x2x2",
            },
            id="intersecting-in-expressions-pin-singleton",
        ),
        pytest.param(
            _required_node_affinity(
                [], node_selector={GKE_TPU_ACCELERATOR_LABEL: "tpu7x"}
            ),
            {},
            id="empty-node-selector-terms-unsatisfiable",
        ),
        pytest.param(
            _required_node_affinity(
                [{}], node_selector={GKE_TPU_ACCELERATOR_LABEL: "tpu7x"}
            ),
            {},
            id="only-empty-term-unsatisfiable",
        ),
        pytest.param(
            _required_node_affinity(
                [{"matchFields": [_expr("metadata.name", "In", ["node-0"])]}],
                node_selector={GKE_TPU_ACCELERATOR_LABEL: "tpu7x"},
            ),
            {GKE_TPU_ACCELERATOR_LABEL: "tpu7x"},
            id="match-fields-only-term-preserves-node-selector",
        ),
        pytest.param(
            _required_node_affinity(
                [{"matchFields": [_expr("metadata.name", "NotIn", ["node-0"])]}],
                node_selector={GKE_TPU_ACCELERATOR_LABEL: "tpu7x"},
            ),
            {GKE_TPU_ACCELERATOR_LABEL: "tpu7x"},
            id="match-fields-notin-single-value-valid",
        ),
        pytest.param(
            _required_node_affinity(
                [
                    {
                        "matchExpressions": [
                            _expr(GKE_TPU_ACCELERATOR_LABEL, "In", ["tpu7x"])
                        ]
                    },
                    {"matchFields": [_expr("metadata.name", "In", ["node-0"])]},
                ]
            ),
            {},
            id="match-fields-only-second-term-breaks-uniqueness",
        ),
        pytest.param(
            _required_node_affinity(
                [
                    {
                        "matchExpressions": [
                            _expr(GKE_TPU_ACCELERATOR_LABEL, "In", ["tpu-v6e-slice"])
                        ]
                    }
                ],
                node_selector={GKE_TPU_ACCELERATOR_LABEL: "tpu7x"},
            ),
            {},
            id="conflicting-in-and-node-selector-unsatisfiable",
        ),
        pytest.param(
            _required_node_affinity(
                [
                    {
                        "matchExpressions": [
                            _expr(
                                GKE_TPU_ACCELERATOR_LABEL,
                                "In",
                                ["tpu7x", "tpu-v6e-slice"],
                            )
                        ]
                    }
                ]
            ),
            {},
            id="multi-valued-in-not-pinned",
        ),
        pytest.param(
            _required_node_affinity(
                [
                    {
                        "matchExpressions": [
                            _expr(GKE_TPU_ACCELERATOR_LABEL, "In", ["tpu7x"])
                        ]
                    },
                    {
                        "matchExpressions": [
                            _expr(GKE_TPU_ACCELERATOR_LABEL, "In", ["tpu-v6e-slice"])
                        ]
                    },
                ]
            ),
            {},
            id="disjoint-terms-not-pinned",
        ),
        pytest.param(
            _required_node_affinity(
                [
                    {
                        "matchExpressions": [
                            _expr(GKE_TPU_ACCELERATOR_LABEL, "In", ["tpu7x"]),
                            _expr(GKE_TPU_TOPOLOGY_LABEL, "Exists"),
                        ]
                    },
                    {},
                    {"matchExpressions": [_expr(GKE_TPU_ACCELERATOR_LABEL, "In", [])]},
                ]
            ),
            {GKE_TPU_ACCELERATOR_LABEL: "tpu7x"},
            id="skips-empty-and-empty-in-terms",
        ),
        pytest.param(
            {
                "nodeSelector": {
                    GKE_TPU_ACCELERATOR_LABEL: "tpu-v6e-slice",
                    GKE_TPU_TOPOLOGY_LABEL: "4x4",
                },
                "affinity": {
                    "nodeAffinity": {
                        "preferredDuringSchedulingIgnoredDuringExecution": [
                            {
                                "weight": 100,
                                "preference": {
                                    "matchExpressions": [
                                        _expr(
                                            GKE_TPU_ACCELERATOR_LABEL, "In", ["tpu7x"]
                                        )
                                    ]
                                },
                            }
                        ]
                    }
                },
            },
            {
                GKE_TPU_ACCELERATOR_LABEL: "tpu-v6e-slice",
                GKE_TPU_TOPOLOGY_LABEL: "4x4",
            },
            id="preferred-affinity-ignored",
        ),
        # Non-In operator contradiction & narrowing semantics (S1 / nodeaffinity.go).
        pytest.param(
            _required_node_affinity(
                [
                    {
                        "matchExpressions": [
                            _expr(GKE_TPU_ACCELERATOR_LABEL, "NotIn", ["tpu7x"])
                        ]
                    }
                ],
                node_selector={GKE_TPU_ACCELERATOR_LABEL: "tpu7x"},
            ),
            {},
            id="notin-contradicts-node-selector",
        ),
        pytest.param(
            _required_node_affinity(
                [
                    {
                        "matchExpressions": [
                            _expr(GKE_TPU_ACCELERATOR_LABEL, "DoesNotExist")
                        ]
                    }
                ],
                node_selector={GKE_TPU_ACCELERATOR_LABEL: "tpu7x"},
            ),
            {},
            id="does-not-exist-contradicts-node-selector",
        ),
        pytest.param(
            _required_node_affinity(
                [
                    {
                        "matchExpressions": [
                            _expr(GKE_TPU_ACCELERATOR_LABEL, "In", ["tpu7x"]),
                            _expr(GKE_TPU_ACCELERATOR_LABEL, "NotIn", ["tpu7x"]),
                        ]
                    }
                ]
            ),
            {},
            id="notin-contradicts-in",
        ),
        pytest.param(
            _required_node_affinity(
                [
                    {
                        "matchExpressions": [
                            _expr(GKE_TPU_ACCELERATOR_LABEL, "In", ["tpu7x"]),
                            _expr(GKE_TPU_ACCELERATOR_LABEL, "DoesNotExist"),
                        ]
                    }
                ]
            ),
            {},
            id="does-not-exist-contradicts-in",
        ),
        pytest.param(
            _required_node_affinity(
                [
                    {
                        "matchExpressions": [
                            _expr("custom-key", "Exists"),
                            _expr("custom-key", "DoesNotExist"),
                        ]
                    }
                ],
                node_selector={GKE_TPU_ACCELERATOR_LABEL: "tpu7x"},
            ),
            {},
            id="does-not-exist-contradicts-exists",
        ),
        pytest.param(
            _required_node_affinity(
                [
                    {
                        "matchExpressions": [
                            _expr(GKE_TPU_ACCELERATOR_LABEL, "In", ["tpu7x"]),
                            _expr(GKE_TPU_TOPOLOGY_LABEL, "In", ["2x2x2"]),
                        ]
                    },
                    {
                        "matchExpressions": [
                            _expr(GKE_TPU_ACCELERATOR_LABEL, "NotIn", ["tpu7x"]),
                        ]
                    },
                ],
                node_selector={GKE_TPU_ACCELERATOR_LABEL: "tpu7x"},
            ),
            {
                GKE_TPU_ACCELERATOR_LABEL: "tpu7x",
                GKE_TPU_TOPOLOGY_LABEL: "2x2x2",
            },
            id="multi-term-skips-notin-contradicted-fallback",
        ),
        pytest.param(
            _required_node_affinity(
                [
                    {
                        "matchExpressions": [
                            _expr(GKE_TPU_ACCELERATOR_LABEL, "In", ["tpu7x"]),
                            _expr(GKE_TPU_TOPOLOGY_LABEL, "In", ["2x2x2"]),
                        ]
                    },
                    {
                        "matchExpressions": [
                            _expr(GKE_TPU_ACCELERATOR_LABEL, "DoesNotExist"),
                        ]
                    },
                ],
                node_selector={GKE_TPU_ACCELERATOR_LABEL: "tpu7x"},
            ),
            {
                GKE_TPU_ACCELERATOR_LABEL: "tpu7x",
                GKE_TPU_TOPOLOGY_LABEL: "2x2x2",
            },
            id="multi-term-skips-does-not-exist-contradicted-fallback",
        ),
        pytest.param(
            _required_node_affinity(
                [
                    {
                        "matchExpressions": [
                            _expr(
                                GKE_TPU_ACCELERATOR_LABEL,
                                "In",
                                ["tpu-v6e-slice", "tpu7x"],
                            ),
                            _expr(
                                GKE_TPU_ACCELERATOR_LABEL,
                                "NotIn",
                                ["tpu-v6e-slice"],
                            ),
                        ]
                    }
                ]
            ),
            {GKE_TPU_ACCELERATOR_LABEL: "tpu7x"},
            id="notin-narrows-multi-valued-in-to-singleton",
        ),
        pytest.param(
            _required_node_affinity(
                [
                    {
                        "matchExpressions": [
                            _expr(
                                GKE_TPU_ACCELERATOR_LABEL,
                                "In",
                                ["tpu-v4-podslice", "tpu-v6e-slice", "tpu7x"],
                            ),
                            _expr(
                                GKE_TPU_ACCELERATOR_LABEL,
                                "NotIn",
                                ["tpu-v4-podslice"],
                            ),
                        ]
                    }
                ]
            ),
            {},
            id="notin-leaving-multiple-values-stays-unpinned",
        ),
        pytest.param(
            _required_node_affinity(
                [
                    {
                        "matchExpressions": [
                            _expr(GKE_TPU_ACCELERATOR_LABEL, "In", ["tpu7x"]),
                            _expr("node-generation", "Gt", ["1"]),
                            _expr("node-generation", "Lt", ["10"]),
                        ]
                    }
                ]
            ),
            {GKE_TPU_ACCELERATOR_LABEL: "tpu7x"},
            id="valid-gt-lt-preserves-pins",
        ),
        pytest.param(
            _required_node_affinity(
                [
                    {
                        "matchExpressions": [
                            _expr("tier", "In", ["1", "5", "10"]),
                            _expr("tier", "Gt", ["2"]),
                            _expr("tier", "Lt", ["8"]),
                        ]
                    }
                ]
            ),
            {"tier": "5"},
            id="gt-lt-narrows-in-to-singleton",
        ),
        pytest.param(
            _required_node_affinity(
                [
                    {
                        "matchExpressions": [
                            _expr(GKE_TPU_ACCELERATOR_LABEL, "In", ["tpu7x"]),
                            _expr("tier", "Gt", ["5"]),
                            _expr("tier", "Lt", ["6"]),
                        ]
                    }
                ]
            ),
            {},
            id="contradictory-gt-lt-integer-range-unsatisfiable",
        ),
        pytest.param(
            _required_node_affinity(
                [
                    {
                        "matchExpressions": [
                            _expr("tier", "Gt", ["5"]),
                        ]
                    }
                ],
                node_selector={"tier": "3", GKE_TPU_ACCELERATOR_LABEL: "tpu7x"},
            ),
            {},
            id="gt-contradicts-node-selector-int",
        ),
        pytest.param(
            _required_node_affinity(
                [
                    {
                        "matchExpressions": [
                            _expr("tier", "Gt", ["5"]),
                        ]
                    }
                ],
                node_selector={"tier": "non-int", GKE_TPU_ACCELERATOR_LABEL: "tpu7x"},
            ),
            {},
            id="gt-contradicts-non-int-node-selector",
        ),
        pytest.param(
            _required_node_affinity(
                [
                    {
                        "matchExpressions": [
                            _expr("tier", "Gt", ["1"]),
                            _expr("tier", "DoesNotExist"),
                        ]
                    }
                ],
                node_selector={GKE_TPU_ACCELERATOR_LABEL: "tpu7x"},
            ),
            {},
            id="gt-contradicts-does-not-exist",
        ),
        # Malformed non-In expressions & invalid matchFields must invalidate only that term.
        pytest.param(
            _required_node_affinity(
                [
                    {
                        "matchExpressions": [
                            _expr(GKE_TPU_ACCELERATOR_LABEL, "In", ["tpu7x"])
                        ]
                    },
                    {"matchExpressions": [_expr("k", "NotIn", [])]},
                    {"matchExpressions": [_expr("k", "Exists", ["unexpected"])]},
                    {"matchExpressions": [_expr("k", "DoesNotExist", ["unexpected"])]},
                    {"matchExpressions": [_expr("k", "Gt", [])]},
                    {"matchExpressions": [_expr("k", "Gt", ["1", "2"])]},
                    {"matchExpressions": [_expr("k", "Gt", ["not-an-int"])]},
                    {"matchExpressions": [_expr("k", "Gt", ["1_000"])]},
                    {"matchExpressions": [_expr("k", "Gt", ["9" * 5000])]},
                    {"matchExpressions": [_expr("k", "Gt", ["9223372036854775808"])]},
                    {"matchExpressions": [_expr("k", "Lt", ["1.5"])]},
                    {"matchExpressions": [_expr("k", "UnknownOp", ["x"])]},
                    {"matchFields": [_expr("metadata.name", "In", [])]},
                    {"matchFields": [_expr("metadata.name", "In", ["n0", "n1"])]},
                    {"matchFields": [_expr("metadata.name", "NotIn", ["n0", "n1"])]},
                    {"matchFields": [_expr("metadata.name", "Exists")]},
                    {
                        "matchFields": [
                            _expr("metadata.name", "In", ["node-0"]),
                            _expr("metadata.name", "NotIn", ["node-0"]),
                        ]
                    },
                    {
                        "matchFields": [
                            _expr("metadata.name", "In", ["node-0"]),
                            _expr("metadata.name", "In", ["node-1"]),
                        ]
                    },
                ]
            ),
            {GKE_TPU_ACCELERATOR_LABEL: "tpu7x"},
            id="multi-term-skips-all-malformed-and-contradicted-terms",
        ),
        pytest.param(
            _required_node_affinity(
                [{"matchExpressions": [_expr("k", "UnknownOp", ["x"])]}],
                node_selector={GKE_TPU_ACCELERATOR_LABEL: "tpu7x"},
            ),
            {},
            id="sole-unknown-operator-term-unsatisfiable",
        ),
        pytest.param(
            _required_node_affinity(
                [{"matchFields": [_expr("metadata.name", "In", ["n0", "n1"])]}],
                node_selector={GKE_TPU_ACCELERATOR_LABEL: "tpu7x"},
            ),
            {},
            id="sole-invalid-match-fields-term-unsatisfiable",
        ),
        # Malformed CR input types.
        pytest.param(
            {"nodeSelector": None},
            {},
            id="null-node-selector",
        ),
        pytest.param(
            {
                "nodeSelector": {
                    GKE_TPU_ACCELERATOR_LABEL: "tpu7x",
                    "bad-int": 123,
                    456: "bad-key",
                }
            },
            {GKE_TPU_ACCELERATOR_LABEL: "tpu7x"},
            id="non-string-node-selector-entries-filtered",
        ),
        pytest.param(
            {
                "nodeSelector": {GKE_TPU_ACCELERATOR_LABEL: "tpu7x"},
                "affinity": None,
            },
            {GKE_TPU_ACCELERATOR_LABEL: "tpu7x"},
            id="null-affinity-preserves-node-selector",
        ),
        pytest.param(
            {
                "nodeSelector": {GKE_TPU_ACCELERATOR_LABEL: "tpu7x"},
                "affinity": {"nodeAffinity": "not-a-dict"},
            },
            {GKE_TPU_ACCELERATOR_LABEL: "tpu7x"},
            id="malformed-node-affinity-preserves-node-selector",
        ),
        pytest.param(
            _required_node_affinity(
                "not-a-list", node_selector={GKE_TPU_ACCELERATOR_LABEL: "tpu7x"}
            ),
            {},
            id="non-list-node-selector-terms-unsatisfiable",
        ),
        pytest.param(
            _required_node_affinity(
                [
                    "not-a-dict",
                    {"matchExpressions": "not-a-list"},
                    {"matchFields": "not-a-list"},
                    {"matchExpressions": ["not-a-dict"]},
                    {
                        "matchExpressions": [
                            _expr(GKE_TPU_ACCELERATOR_LABEL, "In", ["tpu7x"])
                        ]
                    },
                ]
            ),
            {GKE_TPU_ACCELERATOR_LABEL: "tpu7x"},
            id="skips-malformed-term-structures",
        ),
    ],
)
def test_get_required_node_labels(pod_spec, expected):
    assert _get_required_node_labels(pod_spec) == expected


@pytest.mark.skipif(platform.system() == "Windows", reason="Not relevant.")
def test_get_pod_scheduling_metadata_malformed_inputs():
    for bad_group in (
        {"template": None},
        {"template": "bad"},
        {"template": {"spec": None, "metadata": None}},
        {"template": {"spec": "bad", "metadata": {"annotations": ["not-a-dict"]}}},
    ):
        assert _get_pod_scheduling_metadata(bad_group) == ({}, {})


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
    tpu_affinity["template"]["spec"].update(
        _required_node_affinity(
            [
                {
                    "matchExpressions": [
                        _expr(GKE_TPU_ACCELERATOR_LABEL, "In", ["tpu-v6e-slice"]),
                        _expr(GKE_TPU_TOPOLOGY_LABEL, "In", ["4x4"]),
                    ]
                }
            ]
        )
    )
    tpu_affinity["labels"] = {"ray.io/tpu-pod-type": "custom-override"}
    tpu_affinity["resources"] = {"TPU": "4", "TPU-v6e-16-head": "2"}

    # Dynamic Slicing pod annotation combined with required nodeAffinity (no nodeSelector).
    tpu_ds_affinity = copy.deepcopy(cr["spec"]["workerGroupSpecs"][2])
    tpu_ds_affinity["groupName"] = "tpu-ds-affinity"
    tpu_ds_affinity["template"]["spec"].pop("nodeSelector", None)
    tpu_ds_affinity["template"]["spec"].update(
        _required_node_affinity(
            [
                {
                    "matchExpressions": [
                        _expr(GKE_TPU_ACCELERATOR_LABEL, "In", ["tpu7x"]),
                    ]
                }
            ]
        )
    )
    tpu_ds_affinity["template"].setdefault("metadata", {}).setdefault(
        "annotations", {}
    )[GKE_TPU_SLICE_TOPOLOGY_ANNOTATION] = "2x2x2"

    # rayStartParams setting {"TPU": 0} overrides K8s google.com/tpu: 4 limit.
    tpu_zero_start_param = copy.deepcopy(cr["spec"]["workerGroupSpecs"][2])
    tpu_zero_start_param["groupName"] = "tpu-zero-start-param"
    tpu_zero_start_param["rayStartParams"]["resources"] = '"{"TPU": 0}"'

    # headGroupSpec requesting TPUs derives TPU labels and head resource too.
    head = cr["spec"]["headGroupSpec"]
    head["template"]["spec"]["containers"][0]["resources"]["limits"][
        "google.com/tpu"
    ] = 4
    head["template"]["spec"]["nodeSelector"] = {
        GKE_TPU_ACCELERATOR_LABEL: "tpu-v6e-slice",
        GKE_TPU_TOPOLOGY_LABEL: "2x4",
    }

    # Pod metadata.labels and worker group labels must never be read as GKE TPU inputs.
    tpu_pod_labels_only = copy.deepcopy(cr["spec"]["workerGroupSpecs"][2])
    tpu_pod_labels_only["groupName"] = "tpu-pod-labels-only"
    tpu_pod_labels_only["template"]["spec"].pop("nodeSelector", None)
    tpu_pod_labels_only["template"].setdefault("metadata", {})["labels"] = {
        GKE_TPU_ACCELERATOR_LABEL: "tpu7x",
        GKE_TPU_TOPOLOGY_LABEL: "2x2x2",
    }
    tpu_pod_labels_only["labels"] = {
        GKE_TPU_ACCELERATOR_LABEL: "tpu7x",
        GKE_TPU_TOPOLOGY_LABEL: "2x2x2",
    }

    from unittest.mock import patch

    import ray.autoscaler._private.kuberay.autoscaling_config as ac_mod

    cr["spec"]["workerGroupSpecs"].extend(
        [tpu_affinity, tpu_ds_affinity, tpu_zero_start_param, tpu_pod_labels_only]
    )
    with patch.object(
        ac_mod,
        "_get_pod_scheduling_metadata",
        wraps=ac_mod._get_pod_scheduling_metadata,
    ) as spy_meta:
        derived = _derive_autoscaling_config_from_ray_cr(cr)
    assert spy_meta.call_count == 1 + len(cr["spec"]["workerGroupSpecs"])
    assert ac_mod._get_labels_from_group_spec(
        tpu_affinity, "tpu-affinity", resources={"TPU": 4}
    ) == {
        "ray.io/accelerator-type": "TPU-V6E",
        "ray.io/tpu-topology": "4x4",
        "ray.io/tpu-pod-type": "custom-override",
    }
    nt_cfgs = AutoscalingConfig(configs=derived).get_node_type_configs()

    assert nt_cfgs["tpu-affinity"].labels == {
        "ray.io/accelerator-type": "TPU-V6E",
        "ray.io/tpu-topology": "4x4",
        "ray.io/tpu-pod-type": "custom-override",
    }
    assert nt_cfgs["tpu-affinity"].resources["TPU-v6e-16-head"] == 2

    assert nt_cfgs["tpu-ds-affinity"].labels == {
        "ray.io/accelerator-type": "TPU-V7X",
        "ray.io/tpu-topology": "2x2x2",
        "ray.io/tpu-pod-type": "v7x-16",
    }
    assert nt_cfgs["tpu-ds-affinity"].resources["TPU-v7x-16-head"] == 1

    assert nt_cfgs["headgroup"].labels == {
        "ray.io/accelerator-type": "TPU-V6E",
        "ray.io/tpu-topology": "2x4",
        "ray.io/tpu-pod-type": "v6e-8",
    }
    assert nt_cfgs["headgroup"].resources["TPU-v6e-8-head"] == 1

    for name in ("tpu-zero-start-param", "tpu-pod-labels-only"):
        assert "ray.io/accelerator-type" not in nt_cfgs[name].labels
        assert "ray.io/tpu-topology" not in nt_cfgs[name].labels
        assert "ray.io/tpu-pod-type" not in nt_cfgs[name].labels
        assert not any(
            k.startswith("TPU-") and k.endswith("-head")
            for k in nt_cfgs[name].resources
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


@pytest.mark.skipif(platform.system() == "Windows", reason="Not relevant.")
@pytest.mark.parametrize(
    "bad_slice_topology",
    [
        pytest.param(
            "1" * 22 + "x" + "1" * 22 + "x" + "1" * 22,
            id="over-63-chars-3d",
        ),
        pytest.param("9" * 4301 + "x2x2", id="over-4300-digits-single-dim"),
        pytest.param(
            "9" * 2000 + "x" + "9" * 2000 + "x" + "9" * 2000,
            id="over-4300-digits-product",
        ),
    ],
)
def test_oversized_tpu_slice_topology_annotation_handled_safely(bad_slice_topology):
    """Oversized numeric slice-topology annotations must never crash config
    derivation or emit >63-char Ray labels."""
    reset_log_once()
    cr = get_basic_ray_cr()
    cr["metadata"]["namespace"] = "default"
    tpu_group = cr["spec"]["workerGroupSpecs"][2]
    tpu_group["template"]["spec"]["nodeSelector"] = {
        GKE_TPU_ACCELERATOR_LABEL: "tpu7x",
    }
    tpu_group["template"].setdefault("metadata", {}).setdefault("annotations", {})[
        GKE_TPU_SLICE_TOPOLOGY_ANNOTATION
    ] = bad_slice_topology

    derived = _derive_autoscaling_config_from_ray_cr(cr)
    tpu_nt = derived["available_node_types"]["tpu-group"]
    assert tpu_nt["labels"] == {}
    assert not any(
        k.startswith("TPU-") and k.endswith("-head") for k in tpu_nt["resources"]
    )


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

    # labelRefs on headGroupSpec must be ignored.
    cr["spec"]["headGroupSpec"]["labelRefs"] = [
        _make_label_ref("topology.kubernetes.io/zone")
    ]

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
    # both appear in runtime_label_keys; replica_config carries only workers_per_replica.
    gpu = cr["spec"]["workerGroupSpecs"][1]
    gpu["numOfHosts"] = 4
    gpu["maxReplicas"] = 2
    gpu["labelRefs"] = [
        _make_label_ref("nvidia.com/gpu.clique", name="clique-a"),
        _make_label_ref("nvidia.com/gpu.clique", name="clique-b"),
    ]

    # TPU group with labelRefs: accelerator hook provides TPU domain_label_key
    # and merges labelRefs + TPU runtime keys in order.
    tpu_group = cr["spec"]["workerGroupSpecs"][2]
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


@pytest.mark.skipif(platform.system() == "Windows", reason="Not relevant.")
def test_worker_group_label_refs_malformed_and_whitespace_validation():
    from ray.autoscaler.v2.instance_manager.config import (
        AutoscalingConfig,
        WorkerReplicaConfig,
    )

    # Unit checks: whitespace and empty label keys or aliases are skipped.
    invalid_keys = [
        "",
        "   ",
        "a b",
        "a\nb",
        "foo\nbar",
        "a\tb",
        " leading",
        "trailing ",
        "foo\n",
    ]
    invalid_refs = [_make_label_ref(key) for key in invalid_keys] + [
        _make_label_ref("topology.kubernetes.io/zone", name=key)
        for key in invalid_keys
        if key != ""
    ]
    assert _parse_worker_group_label_refs({"labelRefs": invalid_refs}) == []

    # Non-dict entries and non-string aliases are ignored cleanly without raising.
    malformed_refs = [
        None,
        "not-a-dict",
        {"valueFrom": None},
        {"valueFrom": {"nodeRef": "bad"}},
        {"valueFrom": {"nodeRef": {"fieldPath": 123}}},
        {"valueFrom": {"nodeRef": {"fieldPath": "metadata.name"}}},
        _make_label_ref("topology.kubernetes.io/zone", name=123),
    ]
    assert _parse_worker_group_label_refs({"labelRefs": malformed_refs}) == []

    # Valid boundary keys pass through intact.
    valid_boundary_keys = [
        "a",
        "0",
        "a-b_c.d",
        "example.com/My_Name-1.0",
        "a" * 63,
        "a" * 253 + "/" + "b" * 63,
        "ray.io/tpu-slice-name",
        "cloud.google.com/gke-tpu-partition-2x2x2-id",
        "topology.kubernetes.io/zone",
    ]
    delivered = _parse_worker_group_label_refs(
        {"labelRefs": [_make_label_ref(key) for key in valid_boundary_keys]}
    )
    assert delivered == valid_boundary_keys

    # Duplicate delivered key across distinct node labels preserves first-seen entry only.
    dup_delivered = _parse_worker_group_label_refs(
        {
            "labelRefs": [
                _make_label_ref("node.a/key", name="dup"),
                _make_label_ref("node.b/key", name="dup"),
            ]
        }
    )
    assert dup_delivered == ["dup"]

    # E2E regression: invalid labelRefs never leak into runtime_label_keys or replica_config.
    cr = get_basic_ray_cr()
    cr["metadata"]["namespace"] = "default"
    gpu = copy.deepcopy(cr["spec"]["workerGroupSpecs"][1])
    gpu["numOfHosts"] = 2
    gpu["maxReplicas"] = 2
    gpu["labelRefs"] = [
        _make_label_ref("a b"),
        _make_label_ref("a\nb"),
        _make_label_ref("topology.kubernetes.io/zone", name="foo\nbar"),
        _make_label_ref(" padded "),
        {"valueFrom": {"nodeRef": {"fieldPath": "metadata.name"}}},
        _make_label_ref("topology.kubernetes.io/zone", name=123),
    ]

    # Duplicate delivered key E2E: deduplicates to a single "dup" runtime key.
    dup_gpu = copy.deepcopy(cr["spec"]["workerGroupSpecs"][1])
    dup_gpu["groupName"] = "dup-gpu-group"
    dup_gpu["numOfHosts"] = 4
    dup_gpu["maxReplicas"] = 2
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
def test_replica_metadata_head_resources_none_and_workers_per_replica_precedence():
    from ray.autoscaler.v2.instance_manager.config import (
        AutoscalingConfig,
        WorkerReplicaConfig,
    )

    seen_workers_per_replica = []

    class _SingleHostNoneHeadManager:
        @staticmethod
        def get_node_group_accelerator_labels(
            resources, node_selector, annotations=None, group_name=None
        ):
            return None

        @staticmethod
        def get_node_group_additional_resources(
            resources, node_selector, annotations=None, group_name=None
        ):
            return None

        @staticmethod
        def get_node_group_replica_metadata(
            resources,
            node_selector,
            annotations=None,
            workers_per_replica=None,
            group_name=None,
        ):
            seen_workers_per_replica.append(workers_per_replica)
            return {"head_resources": None}

    class _MultiHostOverrideManager:
        @staticmethod
        def get_node_group_accelerator_labels(
            resources, node_selector, annotations=None, group_name=None
        ):
            return None

        @staticmethod
        def get_node_group_additional_resources(
            resources, node_selector, annotations=None, group_name=None
        ):
            return None

        @staticmethod
        def get_node_group_replica_metadata(
            resources,
            node_selector,
            annotations=None,
            workers_per_replica=None,
            group_name=None,
        ):
            seen_workers_per_replica.append(workers_per_replica)
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
    assert seen_workers_per_replica == [1, 4]


@pytest.mark.skipif(platform.system() == "Windows", reason="Not relevant.")
def test_derived_labels_validated_and_explicit_labels_not_aliased():
    """Verify derived labels pass Kubernetes label syntax validation and
    explicit group_spec['labels'] is never aliased."""
    cr = get_basic_ray_cr()
    cpu_group = cr["spec"]["workerGroupSpecs"][0]
    cpu_group["labels"] = {"pool": "cpu"}
    labels = _get_labels_from_group_spec(cpu_group, "small-group", {"CPU": 1})
    assert labels == {"pool": "cpu"}
    assert labels is not cpu_group["labels"]
    with pytest.raises(TypeError):
        _get_labels_from_group_spec(cpu_group, "small-group")  # type: ignore[call-arg]

    class InvalidLabelManager(AcceleratorManager):
        @staticmethod
        def get_node_group_accelerator_labels(
            resources, node_selector, annotations=None, group_name=None
        ):
            return {"ray.io/tpu-topology": "1" * 64}

    with mock.patch(
        f"{AUTOSCALING_CONFIG_MODULE_PATH}.get_all_accelerator_managers",
        return_value={InvalidLabelManager},
    ):
        with pytest.raises(ValueError, match="63 chars or less"):
            _get_labels_from_group_spec(cpu_group, "small-group", {"CPU": 1})


@pytest.mark.skipif(platform.system() == "Windows", reason="Not relevant.")
def test_multiple_accelerator_managers_deterministic_and_isolated():
    """Verify accelerator managers run in deterministic __name__ order, receive
    isolated resource snapshots, and warn on resource or label key collisions."""
    observed_in_zeta_res = []
    observed_in_zeta_lbl = []

    class AlphaManager(AcceleratorManager):
        @staticmethod
        def get_node_group_additional_resources(
            resources, node_selector, annotations=None, group_name=None
        ):
            resources["MUTATED_BY_ALPHA"] = 999
            return {"SHARED_RES": 1, "ALPHA_RES": 1}

        @staticmethod
        def get_node_group_accelerator_labels(
            resources, node_selector, annotations=None, group_name=None
        ):
            resources["MUTATED_BY_ALPHA_LBL"] = 999
            return {
                "ray.io/accelerator-type": "ALPHA-ACCEL",
                "ray.io/alpha-only": "yes",
            }

    class ZetaManager(AcceleratorManager):
        @staticmethod
        def get_node_group_additional_resources(
            resources, node_selector, annotations=None, group_name=None
        ):
            observed_in_zeta_res.append(dict(resources))
            return {"SHARED_RES": 2, "ZETA_RES": 1}

        @staticmethod
        def get_node_group_accelerator_labels(
            resources, node_selector, annotations=None, group_name=None
        ):
            observed_in_zeta_lbl.append(dict(resources))
            return {
                "ray.io/accelerator-type": "ZETA-ACCEL",
                "ray.io/zeta-only": "yes",
            }

    for managers in ([AlphaManager, ZetaManager], [ZetaManager, AlphaManager]):
        observed_in_zeta_res.clear()
        observed_in_zeta_lbl.clear()
        cr = get_basic_ray_cr()
        cr["metadata"]["namespace"] = "default"
        with mock.patch(
            f"{AUTOSCALING_CONFIG_MODULE_PATH}.get_all_accelerator_managers",
            return_value=managers,
        ), mock.patch(f"{AUTOSCALING_CONFIG_MODULE_PATH}.logger") as mock_logger:
            derived = _derive_autoscaling_config_from_ray_cr(cr)

        nt = derived["available_node_types"]["small-group"]
        assert nt["resources"]["SHARED_RES"] == 1
        assert nt["resources"]["ALPHA_RES"] == 1
        assert nt["resources"]["ZETA_RES"] == 1
        assert "MUTATED_BY_ALPHA" not in nt["resources"]
        assert nt["labels"] == {
            "ray.io/accelerator-type": "ALPHA-ACCEL",
            "ray.io/alpha-only": "yes",
            "ray.io/zeta-only": "yes",
        }
        assert observed_in_zeta_res
        assert all(
            "MUTATED_BY_ALPHA" not in r and "SHARED_RES" not in r
            for r in observed_in_zeta_res
        )
        assert observed_in_zeta_lbl
        assert all("MUTATED_BY_ALPHA_LBL" not in r for r in observed_in_zeta_lbl)
        warning_msgs = [
            call.args[0] % call.args[1:] if len(call.args) > 1 else call.args[0]
            for call in mock_logger.warning.call_args_list
        ]
        assert any("SHARED_RES" in m and "ZetaManager" in m for m in warning_msgs)
        assert any(
            "ray.io/accelerator-type" in m and "ZetaManager" in m for m in warning_msgs
        )


@pytest.mark.skipif(platform.system() == "Windows", reason="Not relevant.")
def test_distinct_misconfigured_tpu_groups_each_log_once_with_bounded_keys():
    """Distinct misconfigured TPU worker groups must each emit diagnostics once,
    and log_once keys must never retain unbounded raw annotation strings."""
    from ray.util import debug as ray_debug

    reset_log_once()
    cr = get_basic_ray_cr()
    cr["metadata"]["namespace"] = "default"

    group_a = copy.deepcopy(cr["spec"]["workerGroupSpecs"][2])
    group_a["groupName"] = "tpu-missing-a"
    group_a["template"]["spec"]["nodeSelector"] = {GKE_TPU_ACCELERATOR_LABEL: "tpu7x"}

    group_b = copy.deepcopy(group_a)
    group_b["groupName"] = "tpu-missing-b"

    group_huge = copy.deepcopy(group_a)
    group_huge["groupName"] = "tpu-huge-annotation"
    group_huge["template"].setdefault("metadata", {}).setdefault("annotations", {})[
        GKE_TPU_SLICE_TOPOLOGY_ANNOTATION
    ] = "bad-" + ("x" * 10000)

    cr["spec"]["workerGroupSpecs"] = [group_a, group_b, group_huge]

    with mock.patch.object(tpu.logger, "error") as mock_err, mock.patch.object(
        tpu.logger, "warning"
    ) as mock_warn:
        _derive_autoscaling_config_from_ray_cr(cr)
        assert mock_err.call_count == 2
        assert mock_warn.call_count == 1
        assert ray_debug._logged
        assert max(len(k) for k in ray_debug._logged) < 100

        # Subsequent autoscaler ticks must not re-log the same per-group errors.
        _derive_autoscaling_config_from_ray_cr(cr)
        assert mock_err.call_count == 2
        assert mock_warn.call_count == 1


@pytest.mark.skipif(platform.system() == "Windows", reason="Not relevant.")
def test_kuberay_cr_multihost_label_refs_scheduler_rescheduling_and_idle_seam():
    """End-to-end seam from KubeRay CR (numOfHosts=2 + labelRefs, non-TPU)
    through AutoscalingConfig to ResourceDemandScheduler:
    1) shared runtime_label_keys on a running replica member propagate onto
       ALLOCATED booting siblings so pinned rescheduling gangs land without launching;
    2) idle complete replicas inside a pinned shared clique stay protected while
       another replica in that clique has busy work, while other cliques scale down."""
    from ray.autoscaler.v2.instance_manager.config import AutoscalingConfig
    from ray.autoscaler.v2.scheduler import (
        ResourceDemandScheduler,
        SchedulingRequest,
    )
    from ray.autoscaler.v2.schema import AutoscalerInstance
    from ray.core.generated.autoscaler_pb2 import (
        BundleSelector,
        GangResourceRequest,
        LocalityConstraint,
        LocalityRequirement,
        NodeState,
        NodeStatus,
        ResourceRequest,
    )
    from ray.core.generated.common_pb2 import (
        LabelSelectorConstraint,
        LabelSelectorOperator,
        PlacementStrategy,
    )
    from ray.core.generated.instance_manager_pb2 import Instance, NodeKind

    clique_key = "ray.io/gpu-clique"
    cr = get_basic_ray_cr()
    cr["metadata"]["namespace"] = "default"
    gpu_wg = copy.deepcopy(cr["spec"]["workerGroupSpecs"][1])
    gpu_wg.update(
        groupName="gpu-clique",
        replicas=0,
        minReplicas=0,
        maxReplicas=4,
        numOfHosts=2,
        rayStartParams={},
        labelRefs=[
            {
                "name": clique_key,
                "valueFrom": {
                    "nodeRef": {"fieldPath": "metadata.labels['nvidia.com/gpu.clique']"}
                },
            }
        ],
    )
    gpu_wg["template"].setdefault("metadata", {})["annotations"] = {}
    gpu_wg["template"]["spec"]["nodeSelector"] = {}
    gpu_wg["template"]["spec"]["containers"][0]["resources"] = {
        "limits": {"cpu": "8", "memory": "16Gi", "nvidia.com/gpu": "4"},
        "requests": {"cpu": "8", "memory": "16Gi", "nvidia.com/gpu": "4"},
    }
    cr["spec"]["workerGroupSpecs"] = [gpu_wg]

    derived = _derive_autoscaling_config_from_ray_cr(cr)
    nt_cfgs = AutoscalingConfig(configs=derived).get_node_type_configs()
    assert nt_cfgs["gpu-clique"].runtime_label_keys == (clique_key,)
    assert nt_cfgs["gpu-clique"].workers_per_replica == 2

    def _run_inst(iid, clique, rep, *, busy=False, idle_ms=0):
        nid = iid.encode().ljust(28, b"\0")
        avail = {
            "CPU": 8.0,
            "GPU": 0.0 if busy else 4.0,
            "memory": float(16 * 1024**3),
        }
        total = {"CPU": 8.0, "GPU": 4.0, "memory": float(16 * 1024**3)}
        return AutoscalerInstance(
            im_instance=Instance(
                instance_id=iid,
                instance_type="gpu-clique",
                status=Instance.RAY_RUNNING,
                node_id=nid.hex(),
                cloud_instance_id=f"c-{iid}",
                node_kind=NodeKind.WORKER,
            ),
            ray_node=NodeState(
                node_id=nid,
                instance_id=f"c-{iid}",
                ray_node_type_name="gpu-clique",
                status=NodeStatus.RUNNING if busy else NodeStatus.IDLE,
                idle_duration_ms=idle_ms,
                total_resources=total,
                available_resources=avail,
                labels={clique_key: clique},
            ),
            cloud_instance_id=f"c-{iid}",
            cloud_instance_replica_id=rep,
        )

    def _boot_inst(iid, rep):
        return AutoscalerInstance(
            im_instance=Instance(
                instance_id=iid,
                instance_type="gpu-clique",
                status=Instance.ALLOCATED,
                cloud_instance_id=f"c-{iid}",
                node_kind=NodeKind.WORKER,
            ),
            cloud_instance_id=f"c-{iid}",
            cloud_instance_replica_id=rep,
        )

    def _pinned_gang(n_bundles, clique_val, pg_key):
        reqs = []
        for _ in range(n_bundles):
            req = ResourceRequest(resources_bundle={"CPU": 4.0, "GPU": 4.0})
            pc = req.placement_constraints.add()
            pc.anti_affinity.label_name = pg_key
            pc.anti_affinity.label_value = ""
            reqs.append(req)
        lr = LocalityRequirement(
            locality_constraint=LocalityConstraint(
                label_name=clique_key,
                placement_strategy=PlacementStrategy.STRICT_PACK,
            )
        )
        lr.label_selector.label_constraints.append(
            LabelSelectorConstraint(
                label_key=clique_key,
                operator=LabelSelectorOperator.LABEL_OPERATOR_IN,
                label_values=[clique_val],
            )
        )
        return GangResourceRequest(
            bundle_selectors=[
                BundleSelector(resource_requests=reqs, locality_requirement=lr)
            ]
        )

    # 1) Booting sibling inherits shared runtime label key from running peer.
    rep_boot = ResourceDemandScheduler().schedule(
        SchedulingRequest(
            disable_launch_config_check=True,
            node_type_configs=nt_cfgs,
            current_instances=[
                _run_inst("r0-0", "clique-0", "rep-0", busy=True),
                _boot_inst("r0-1", "rep-0"),
            ],
            gang_resource_requests=[_pinned_gang(1, "clique-0", "_pg_boot")],
        )
    )
    assert rep_boot.to_launch == []
    assert rep_boot.infeasible_gang_resource_requests == []

    # 2) Idle rep-1 in pinned clique-0 stays protected while r0-0 is busy; rep-2 in clique-1 terminates.
    rep_idle = ResourceDemandScheduler().schedule(
        SchedulingRequest(
            disable_launch_config_check=True,
            node_type_configs=nt_cfgs,
            current_instances=[
                _run_inst("r0-0", "clique-0", "rep-0", busy=True),
                _run_inst("r0-1", "clique-0", "rep-0", idle_ms=60_000),
                _run_inst("r1-0", "clique-0", "rep-1", idle_ms=60_000),
                _run_inst("r1-1", "clique-0", "rep-1", idle_ms=60_000),
                _run_inst("r2-0", "clique-1", "rep-2", idle_ms=60_000),
                _run_inst("r2-1", "clique-1", "rep-2", idle_ms=60_000),
            ],
            gang_resource_requests=[_pinned_gang(4, "clique-0", "_pg_pin4")],
            idle_timeout_s=10.0,
        )
    )
    assert rep_idle.to_launch == []
    assert sorted(t.instance_id for t in rep_idle.to_terminate) == ["r2-0", "r2-1"]


def test_skipped_label_refs_warn_and_valid_label_refs_suppress_when_missing():
    """Skipped labelRefs entries log a warning while valid entries stay silent and
    populate runtime_label_keys so missing dynamic labels stop scaling on Tick 2."""
    import logging

    from ray.autoscaler._private.kuberay import autoscaling_config as ac_mod
    from ray.autoscaler.v2.instance_manager.config import AutoscalingConfig
    from ray.autoscaler.v2.scheduler import ResourceDemandScheduler, SchedulingRequest
    from ray.autoscaler.v2.schema import AutoscalerInstance
    from ray.core.generated.autoscaler_pb2 import (
        BundleSelector,
        GangResourceRequest,
        LocalityConstraint,
        LocalityRequirement,
        NodeState,
        NodeStatus,
        ResourceRequest,
    )
    from ray.core.generated.common_pb2 import PlacementStrategy
    from ray.core.generated.instance_manager_pb2 import Instance, NodeKind
    from ray.util.debug import reset_log_once

    records = []

    class _ListHandler(logging.Handler):
        def emit(self, record):
            records.append(record.getMessage())

    handler = _ListHandler(level=logging.WARNING)
    ac_mod.logger.addHandler(handler)
    try:
        reset_log_once()
        cr_valid = get_basic_ray_cr()
        cr_valid["metadata"]["namespace"] = "default"
        cr_valid["spec"]["workerGroupSpecs"][0]["labelRefs"] = [
            _make_label_ref("topology.kubernetes.io/zone"),
            _make_label_ref("nvidia.com/gpu.clique", name=""),
            _make_label_ref("cloud.google.com/rack", name="ray.io/rack"),
        ]
        records.clear()
        valid_cfg = _generate_available_node_types_from_ray_cr_spec(cr_valid["spec"])
        assert [m for m in records if "labelRefs" in m] == []
        assert valid_cfg["small-group"]["runtime_label_keys"] == [
            "topology.kubernetes.io/zone",
            "nvidia.com/gpu.clique",
            "ray.io/rack",
        ]

        reset_log_once()
        cr_mixed = get_basic_ray_cr()
        cr_mixed["metadata"]["namespace"] = "default"
        cr_mixed["spec"]["workerGroupSpecs"] = [cr_mixed["spec"]["workerGroupSpecs"][1]]
        gpu_wg = cr_mixed["spec"]["workerGroupSpecs"][0]
        gpu_wg["groupName"] = "gpu-wg"
        gpu_wg["numOfHosts"] = 2
        gpu_wg["minReplicas"] = 0
        gpu_wg["maxReplicas"] = 4
        gpu_wg["labelRefs"] = [
            {"valueFrom": {"nodeRef": {"fieldPath": "metadata.name"}}},
            _make_label_ref("nvidia.com/gpu.clique", name="bad name"),
            _make_label_ref("nvidia.com/gpu.clique"),
        ]
        records.clear()
        full_cfg = _derive_autoscaling_config_from_ray_cr(cr_mixed)
        warn_msgs = [m for m in records if "labelRefs" in m]
        assert len(warn_msgs) == 2
        assert all("gpu-wg" in msg for msg in warn_msgs)
        assert full_cfg["available_node_types"]["gpu-wg"]["runtime_label_keys"] == [
            "nvidia.com/gpu.clique"
        ]
    finally:
        ac_mod.logger.removeHandler(handler)

    # Unmocked producer -> consumer seam: valid labelRefs populates runtime_label_keys;
    # Tick 1 launches 1 replica (2 workers), Tick 2 suppresses once running replica lacks it.
    nt_cfgs = AutoscalingConfig(configs=full_cfg).get_node_type_configs()
    reqs = [ResourceRequest(resources_bundle={"CPU": 1.0}) for _ in range(2)]
    for r in reqs:
        pc = r.placement_constraints.add()
        pc.anti_affinity.label_name = "_pg_seam"
    gang = GangResourceRequest(
        bundle_selectors=[
            BundleSelector(
                resource_requests=reqs,
                locality_requirement=LocalityRequirement(
                    locality_constraint=LocalityConstraint(
                        label_name="nvidia.com/gpu.clique",
                        placement_strategy=PlacementStrategy.STRICT_PACK,
                    )
                ),
            )
        ]
    )
    rep_t1 = ResourceDemandScheduler().schedule(
        SchedulingRequest(
            disable_launch_config_check=True,
            node_type_configs=nt_cfgs,
            current_instances=[],
            gang_resource_requests=[gang],
        )
    )
    assert [(lr.instance_type, lr.count) for lr in rep_t1.to_launch] == [("gpu-wg", 2)]

    running_rep = [
        AutoscalerInstance(
            im_instance=Instance(
                instance_id=f"g{i}",
                instance_type="gpu-wg",
                status=Instance.RAY_RUNNING,
                node_id=(f"n{i}".encode().ljust(28, b"\0")).hex(),
                cloud_instance_id=f"c{i}",
                node_kind=NodeKind.WORKER,
            ),
            ray_node=NodeState(
                node_id=f"n{i}".encode().ljust(28, b"\0"),
                instance_id=f"c{i}",
                ray_node_type_name="gpu-wg",
                status=NodeStatus.RUNNING,
                total_resources=dict(nt_cfgs["gpu-wg"].resources),
                available_resources=dict(nt_cfgs["gpu-wg"].resources),
                labels={},
            ),
            cloud_instance_id=f"c{i}",
            cloud_instance_replica_id="rep-0",
        )
        for i in range(2)
    ]
    rep_t2 = ResourceDemandScheduler().schedule(
        SchedulingRequest(
            disable_launch_config_check=True,
            node_type_configs=nt_cfgs,
            current_instances=running_rep,
            gang_resource_requests=[gang],
        )
    )
    assert list(rep_t2.to_launch) == []
    assert len(rep_t2.infeasible_gang_resource_requests) == 1


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
