import copy
import platform
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Type
from unittest import mock

import pytest
import requests
import yaml

from ray.autoscaler._private.kuberay.autoscaling_config import (
    GKE_TPU_ACCELERATOR_LABEL,
    GKE_TPU_TOPOLOGY_LABEL,
    AutoscalingConfigProducer,
    _derive_autoscaling_config_from_ray_cr,
    _get_custom_resources,
    _get_num_tpus,
    _get_ray_resources_from_group_spec,
    _round_up_k8s_quantity,
)
from ray.autoscaler._private.kuberay.utils import tpu_node_selectors_to_type

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
                "labels": {},
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
            _get_autoscaling_config_with_labels(),
            None,
            None,
            None,
            id="groups-with-labels",
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


TPU_TYPES_ARGS = ",".join(
    [
        "accelerator",
        "topology",
        "expected_tpu_type",
    ]
)
TPU_TYPES_DATA = (
    []
    if platform.system() == "Windows"
    else [
        pytest.param(
            "tpu-v4-podslice",
            None,
            None,
            id="tpu-none-topology",
        ),
        pytest.param(
            None,
            "2x2x2",
            None,
            id="tpu-none-accelerator",
        ),
        pytest.param(
            "tpu-v4-podslice",
            "2x2x2",
            "v4-16",
            id="tpu-v4-test",
        ),
        pytest.param(
            "tpu-v5-lite-device",
            "2x2",
            "v5e-4",
            id="tpu-v5e-device-test",
        ),
        pytest.param(
            "tpu-v5-lite-podslice",
            "2x4",
            "v5e-8",
            id="tpu-v5e-podslice-test",
        ),
        pytest.param(
            "tpu-v5p-slice",
            "2x2x4",
            "v5p-32",
            id="tpu-v5p-test",
        ),
        pytest.param(
            "tpu-v6e-slice",
            "16x16",
            "v6e-256",
            id="tpu-v6e-test",
        ),
    ]
)


@pytest.mark.skipif(platform.system() == "Windows", reason="Not relevant.")
@pytest.mark.parametrize(TPU_TYPES_ARGS, TPU_TYPES_DATA)
def test_tpu_node_selectors_to_type(
    accelerator: str, topology: str, expected_tpu_type: str
):
    """Verify that tpu_node_selectors_to_type correctly returns TPU type from
    TPU nodeSelectors.
    """
    tpu_type = tpu_node_selectors_to_type(topology, accelerator)
    assert expected_tpu_type == tpu_type


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
    ]
)


@pytest.mark.skipif(platform.system() == "Windows", reason="Not relevant.")
@pytest.mark.parametrize(TPU_PARAM_ARGS, TPU_TEST_DATA)
def test_get_num_tpus(ray_cr_in: Dict[str, Any], expected_num_tpus: int):
    """Verify that _get_num_tpus correctly returns the number of requested TPUs."""
    for worker_group in ray_cr_in["spec"]["workerGroupSpecs"]:
        ray_start_params = worker_group["rayStartParams"]
        custom_resources = _get_custom_resources(
            ray_start_params, worker_group["groupName"]
        )
        k8s_resources = worker_group["template"]["spec"]["containers"][0]["resources"]

        num_tpus = _get_num_tpus(custom_resources, k8s_resources)

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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
