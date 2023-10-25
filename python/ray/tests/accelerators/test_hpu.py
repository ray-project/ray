import os
import sys
import subprocess
import pytest
from unittest.mock import patch

import ray
from ray._private.accelerators import HPUAcceleratorManager


def test_user_configured_more_than_visible(monkeypatch, shutdown_only):
    # Test more hpus are configured than visible.
    monkeypatch.setenv("HABANA_VISIBLE_MODULES", "0,1,2")
    with pytest.raises(ValueError):
        ray.init(resources={"HPU": 4})


@patch(
    "ray._private.accelerators.HPUAcceleratorManager.get_current_node_num_accelerators",  # noqa: E501
    return_value=4,
)
def test_auto_detected_more_than_visible(
    mock_get_num_accelerators, monkeypatch, shutdown_only
):
    # Test more hpus are detected than visible.
    monkeypatch.setenv("HABANA_VISIBLE_MODULES", "0,1,2")
    ray.init()
    mock_get_num_accelerators.called
    assert ray.available_resources()["HPU"] == 3


@patch(
    "ray._private.accelerators.HPUAcceleratorManager.get_current_node_num_accelerators",  # noqa: E501
    return_value=2,
)
def test_auto_detect_resources(mock_get_num_accelerators, shutdown_only):
    # Test that ray node resources are filled with auto detected count.
    ray.init()
    mock_get_num_accelerators.called
    assert ray.available_resources()["HPU"] == 2


def test_autodetect_num_hpus_accel():
    assert HPUAcceleratorManager.get_current_node_num_accelerators() == 8 #default num cards in a node


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
