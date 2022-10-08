import copy
from pathlib import Path
import requests
from typing import Any, Dict, Optional
from unittest import mock

import platform
import pytest
import yaml

from ray.autoscaler._private.kuberay.autoscaling_config import (
    _derive_autoscaling_config_from_ray_cr,
    AutoscalingConfigProducer,
    _round_up_k8s_quantity,
)

AUTOSCALING_CONFIG_MODULE_PATH = "ray.autoscaler._private.kuberay.autoscaling_config"


def _get_basic_ray_cr() -> dict:
    """Returns the example Ray CR included in the Ray documentation,
    modified to include a GPU worker group.
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
    config["spec"]["workerGroupSpecs"].append(gpu_group)
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
            "worker_rpc_drain": True,
            "namespace": "default",
            "type": "kuberay",
        },
        "available_node_types": {
            "head-group": {
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
                "max_workers": 300,
                "min_workers": 1,
                "node_config": {},
                "resources": {
                    "CPU": 1,
                    "memory": 536870912,
                    "Custom2": 5,
                    "Custom3": 1,
                },
            },
            # Same as "small-group" with a GPU entry added.
            "gpu-group": {
                "max_workers": 300,
                "min_workers": 1,
                "node_config": {},
                "resources": {
                    "CPU": 1,
                    "memory": 536870912,
                    "Custom2": 5,
                    "Custom3": 1,
                    "GPU": 3,
                },
            },
        },
        "auth": {},
        "cluster_synced_files": [],
        "file_mounts": {},
        "file_mounts_sync_continuously": False,
        "head_node_type": "head-group",
        "head_setup_commands": [],
        "head_start_ray_commands": [],
        "idle_timeout_minutes": 1.0,
        "initialization_commands": [],
        "max_workers": 600,
        "setup_commands": [],
        "upscaling_speed": 1000,
        "worker_setup_commands": [],
        "worker_start_ray_commands": [],
    }


def _get_ray_cr_no_cpu_error() -> dict:
    """Incorrectly formatted Ray CR without num-cpus rayStartParam and without resource
    limits. Autoscaler should raise an error when reading this.
    """
    cr = _get_basic_ray_cr()
    # Verify that the num-cpus rayStartParam is not present for the worker type.
    assert "num-cpus" not in cr["spec"]["workerGroupSpecs"][0]["rayStartParams"]
    del cr["spec"]["workerGroupSpecs"][0]["template"]["spec"]["containers"][0][
        "resources"
    ]["limits"]["cpu"]
    return cr


def _get_no_cpu_error() -> str:
    return (
        "Autoscaler failed to detect `CPU` resources for group small-group."
        "\nSet the `--num-cpus` rayStartParam and/or "
        "the CPU resource limit for the Ray container."
    )


def _get_ray_cr_with_overrides() -> dict:
    """CR with memory, cpu, and gpu overrides from rayStartParams."""
    cr = _get_basic_ray_cr()
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


def _get_ray_cr_missing_gpu_arg() -> dict:
    """CR with gpu present in K8s limits but not in Ray start params.
    Should result in a warning that Ray doesn't see the GPUs.
    """
    cr = _get_basic_ray_cr()
    cr["spec"]["workerGroupSpecs"][0]["template"]["spec"]["containers"][0]["resources"][
        "limits"
    ]["nvidia.com/gpu"] = 1
    return cr


def _get_gpu_complaint() -> str:
    """The logger warning generated when processing the above CR."""
    return (
        "Detected GPUs in container resources for group small-group."
        "To ensure Ray and the autoscaler are aware of the GPUs,"
        " set the `--num-gpus` rayStartParam."
    )


def _get_ray_cr_with_autoscaler_options() -> dict:
    cr = _get_basic_ray_cr()
    cr["spec"]["autoscalerOptions"] = {
        "upscalingMode": "Conservative",
        "idleTimeoutSeconds": 300,
    }
    return cr


def _get_autoscaling_config_with_options() -> dict:
    config = _get_basic_autoscaling_config()
    config["upscaling_speed"] = 1
    config["idle_timeout_minutes"] = 5.0
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
            _get_basic_ray_cr(),
            _get_basic_autoscaling_config(),
            None,
            None,
            None,
            id="basic",
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
    ]
)


@pytest.mark.skipif(platform.system() == "Windows", reason="Not relevant.")
@pytest.mark.parametrize(PARAM_ARGS, TEST_DATA)
def test_autoscaling_config(
    ray_cr_in: Dict[str, Any],
    expected_config_out: Optional[Dict[str, Any]],
    expected_error: Optional[Exception],
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
    cr = _get_basic_ray_cr()

    group_specs = [cr["spec"]["headGroupSpec"]] + cr["spec"]["workerGroupSpecs"]
    # Head, CPU group, GPU group.
    assert len(group_specs) == 3

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

    class MockAutoscalingConfigProducer(AutoscalingConfigProducer):
        def __init__(self, *args, **kwargs):
            self.exception_counter = 0

        def _fetch_ray_cr_from_k8s(self) -> Dict[str, Any]:
            if self.exception_counter < num_exceptions:
                self.exception_counter += 1
                raise exception
            else:
                return {"ok-key": "ok-value"}

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


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
