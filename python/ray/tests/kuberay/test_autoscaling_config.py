from pathlib import Path
from typing import Any, Dict, Optional
from unittest import mock

import platform
import pytest
import yaml

from ray.autoscaler._private.kuberay.autoscaling_config import (
    _derive_autoscaling_config_from_ray_cr,
)

AUTOSCALING_CONFIG_MODULE_PATH = "ray.autoscaler._private.kuberay.autoscaling_config"


def _get_basic_ray_cr() -> dict:
    """Returns the example Ray CR included in the Ray documentation."""
    cr_path = str(
        Path(__file__).resolve().parents[2]
        / "autoscaler"
        / "kuberay"
        / "ray-cluster.complete.yaml"
    )
    return yaml.safe_load(open(cr_path).read())


def _get_basic_autoscaling_config() -> dict:
    """The expected autoscaling derived from the example Ray CR."""
    return {
        "cluster_name": "raycluster-complete",
        "provider": {
            "disable_launch_config_check": True,
            "disable_node_updaters": True,
            "foreground_node_launch": True,
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
                    "Custom2": 5,
                    "Custom3": 1,
                },
            },
        },
        "auth": {},
        "cluster_synced_files": [],
        "file_mounts": {},
        "file_mounts_sync_continuously": False,
        "head_node": {},
        "head_node_type": "head-group",
        "head_setup_commands": [],
        "head_start_ray_commands": [],
        "idle_timeout_minutes": 5,
        "initialization_commands": [],
        "max_workers": 300,
        "setup_commands": [],
        "upscaling_speed": 1,
        "worker_nodes": {},
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


def _get_ray_cr_memory_and_gpu() -> dict:
    """CR with memory and gpu rayStartParams."""
    cr = _get_basic_ray_cr()
    cr["spec"]["workerGroupSpecs"][0]["rayStartParams"]["memory"] = "300000000"
    cr["spec"]["workerGroupSpecs"][0]["rayStartParams"]["num-gpus"] = "1"
    return cr


def _get_autoscaling_config_memory_and_gpu() -> dict:
    """Autoscaling config with memory and gpu annotations."""
    config = _get_basic_autoscaling_config()
    config["available_node_types"]["small-group"]["resources"]["memory"] = 300000000
    config["available_node_types"]["small-group"]["resources"]["GPU"] = 1
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
            _get_ray_cr_memory_and_gpu(),
            _get_autoscaling_config_memory_and_gpu(),
            None,
            None,
            None,
            id="memory-and-gpu",
        ),
        pytest.param(
            _get_ray_cr_missing_gpu_arg(),
            _get_basic_autoscaling_config(),
            None,
            None,
            _get_gpu_complaint(),
            id="gpu-complaint",
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
    assert len(group_specs) == 2

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


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
