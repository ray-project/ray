import os
import sys
import pytest
from unittest.mock import patch

import ray
from ray._private.accelerators import HPUAcceleratorManager
from ray._private.accelerators import hpu


def test_user_configured_more_than_visible(monkeypatch, call_ray_stop_only):
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


def test_get_current_process_visible_accelerator_ids():
    os.environ[hpu.HABANA_VISIBLE_DEVICES_ENV_VAR] = "0,1,2"
    assert HPUAcceleratorManager.get_current_process_visible_accelerator_ids() == [
        "0",
        "1",
        "2",
    ]  # noqa: E501

    del os.environ[hpu.HABANA_VISIBLE_DEVICES_ENV_VAR]
    assert HPUAcceleratorManager.get_current_process_visible_accelerator_ids() is None

    os.environ[hpu.HABANA_VISIBLE_DEVICES_ENV_VAR] = ""
    assert HPUAcceleratorManager.get_current_process_visible_accelerator_ids() == []

    del os.environ[hpu.HABANA_VISIBLE_DEVICES_ENV_VAR]


def test_set_current_process_visible_accelerator_ids():
    HPUAcceleratorManager.set_current_process_visible_accelerator_ids(["0"])
    assert os.environ[hpu.HABANA_VISIBLE_DEVICES_ENV_VAR] == "0"

    HPUAcceleratorManager.set_current_process_visible_accelerator_ids(["0", "1"])
    assert os.environ[hpu.HABANA_VISIBLE_DEVICES_ENV_VAR] == "0,1"

    HPUAcceleratorManager.set_current_process_visible_accelerator_ids(["0", "1", "2"])
    assert os.environ[hpu.HABANA_VISIBLE_DEVICES_ENV_VAR] == "0,1,2"

    del os.environ[hpu.HABANA_VISIBLE_DEVICES_ENV_VAR]


@pytest.mark.parametrize(
    "test_config",
    [
        (1, False),
        (0.5, True),
        (3, False),
    ],
)
def test_validate_resource_request_quantity(test_config):
    num_hpus, expect_error = test_config

    if expect_error:
        assert (
            HPUAcceleratorManager.validate_resource_request_quantity(num_hpus)[0]
            is False
        )
        assert (
            HPUAcceleratorManager.validate_resource_request_quantity(num_hpus)[1]
            is not None
        )
    else:
        assert (
            HPUAcceleratorManager.validate_resource_request_quantity(num_hpus)[0]
            is True
        )
        assert (
            HPUAcceleratorManager.validate_resource_request_quantity(num_hpus)[1]
            is None
        )


def test_check_accelerator_info():

    if HPUAcceleratorManager.is_initialized():
        assert (
            "Intel-GAUDI" in HPUAcceleratorManager.get_current_node_accelerator_type()
        )
    else:
        assert HPUAcceleratorManager.get_current_node_accelerator_type() is None

    assert HPUAcceleratorManager.get_resource_name() == "HPU"


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
