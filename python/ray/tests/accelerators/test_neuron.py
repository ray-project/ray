import subprocess
import sys
from unittest.mock import patch

import pytest

import ray
from ray._private.accelerators import NeuronAcceleratorManager


def test_user_configured_more_than_visible(monkeypatch, call_ray_stop_only):
    # Test more neuron_cores are configured than visible.
    monkeypatch.setenv("NEURON_RT_VISIBLE_CORES", "0,1,2")
    with pytest.raises(ValueError):
        ray.init(resources={"neuron_cores": 4})


@patch(
    "ray._private.accelerators.NeuronAcceleratorManager.get_current_node_num_accelerators",  # noqa: E501
    return_value=4,
)
def test_auto_detected_more_than_visible(
    mock_get_num_accelerators, monkeypatch, shutdown_only
):
    # Test more neuron_cores are detected than visible.
    monkeypatch.setenv("NEURON_RT_VISIBLE_CORES", "0,1,2")
    ray.init()
    _ = mock_get_num_accelerators.called
    assert ray.available_resources()["neuron_cores"] == 3


@patch(
    "ray._private.accelerators.NeuronAcceleratorManager.get_current_node_num_accelerators",  # noqa: E501
    return_value=2,
)
def test_auto_detect_resources(mock_get_num_accelerators, shutdown_only):
    # Test that ray node resources are filled with auto detected count.
    ray.init()
    _ = mock_get_num_accelerators.called
    assert ray.available_resources()["neuron_cores"] == 2


@patch(
    "subprocess.run",
    return_value=subprocess.CompletedProcess(
        args=[],
        returncode=0,
        stdout=(
            b'[{"neuron_device":0,"bdf":"00:1e.0",'
            b'"connected_to":null,"nc_count":2,'
            b'"memory_size":34359738368,"neuron_processes":[]}]'
        ),
    ),
)
@patch("os.path.isdir", return_value=True)
@patch("sys.platform", "linux")
def test_get_neuron_core_count_single_device(mock_isdir, mock_subprocess):
    assert NeuronAcceleratorManager.get_current_node_num_accelerators() == 2


@patch(
    "subprocess.run",
    return_value=subprocess.CompletedProcess(
        args=[],
        returncode=0,
        stdout=(
            b'[{"neuron_device":0,"bdf":"00:1e.0",'
            b'"connected_to":null,"nc_count":2,'
            b'"memory_size":34359738368,"neuron_processes":[]},'
            b'{"neuron_device":1,"bdf":"00:1f.0","connected_to":null,'
            b'"nc_count":2,"memory_size":34359738368,"neuron_processes":[]}]'
        ),
    ),
)
@patch("os.path.isdir", return_value=True)
@patch("sys.platform", "linux")
def test_get_neuron_core_count_multiple_devices(mock_isdir, mock_subprocess):
    assert NeuronAcceleratorManager.get_current_node_num_accelerators() == 4


@patch(
    "subprocess.run",
    return_value=subprocess.CompletedProcess(
        args=[], returncode=1, stdout=b"AccessDenied"
    ),
)
@patch("os.path.isdir", return_value=True)
@patch("sys.platform", "linux")
def test_get_neuron_core_count_failure_with_error(mock_isdir, mock_subprocess):
    assert NeuronAcceleratorManager.get_current_node_num_accelerators() == 0


@patch(
    "subprocess.run",
    return_value=subprocess.CompletedProcess(args=[], returncode=0, stdout=b"[{}]"),
)
@patch("os.path.isdir", return_value=True)
@patch("sys.platform", "linux")
def test_get_neuron_core_count_failure_with_empty_results(mock_isdir, mock_subprocess):
    assert NeuronAcceleratorManager.get_current_node_num_accelerators() == 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
