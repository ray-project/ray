import os
import sys
from unittest.mock import Mock, patch

import pytest

from ray._private.accelerators import MTGPUAcceleratorManager


def _mock_pymtml(device_count=8, device_name="MTT S5000"):
    pymtml = Mock()
    pymtml.nvmlDeviceGetCount.return_value = device_count
    pymtml.nvmlDeviceGetHandleByIndex.return_value = object()
    pymtml.nvmlDeviceGetName.return_value = device_name
    return pymtml


def test_get_resource_name_and_visible_devices_env_var():
    assert MTGPUAcceleratorManager.get_resource_name() == "GPU"
    assert (
        MTGPUAcceleratorManager.get_visible_accelerator_ids_env_var()
        == "MUSA_VISIBLE_DEVICES"
    )


@pytest.mark.parametrize(
    "env_value, expected",
    [
        (None, None),
        ("", []),
        ("NoDevFiles", []),
        ("0", ["0"]),
        ("0, 2", ["0", "2"]),
        ("0,,2,", ["0", "2"]),
    ],
)
def test_get_current_process_visible_accelerator_ids(monkeypatch, env_value, expected):
    if env_value is None:
        monkeypatch.delenv("MUSA_VISIBLE_DEVICES", raising=False)
    else:
        monkeypatch.setenv("MUSA_VISIBLE_DEVICES", env_value)

    assert (
        MTGPUAcceleratorManager.get_current_process_visible_accelerator_ids()
        == expected
    )


def test_set_current_process_visible_accelerator_ids(monkeypatch):
    monkeypatch.delenv("RAY_EXPERIMENTAL_NOSET_MUSA_VISIBLE_DEVICES", raising=False)
    MTGPUAcceleratorManager.set_current_process_visible_accelerator_ids(["1", "3"])
    assert os.environ["MUSA_VISIBLE_DEVICES"] == "1,3"


def test_set_current_process_visible_accelerator_ids_can_be_disabled(monkeypatch):
    monkeypatch.setenv("MUSA_VISIBLE_DEVICES", "7")
    monkeypatch.setenv("RAY_EXPERIMENTAL_NOSET_MUSA_VISIBLE_DEVICES", "1")

    MTGPUAcceleratorManager.set_current_process_visible_accelerator_ids(["0"])

    assert os.environ["MUSA_VISIBLE_DEVICES"] == "7"


@pytest.mark.parametrize(
    "name, expected",
    [
        ("MTT S5000", "S5000"),
        ("MTT S4000", "S4000"),
        (None, None),
        ("", None),
    ],
)
def test_gpu_name_to_accelerator_type(name, expected):
    assert MTGPUAcceleratorManager._gpu_name_to_accelerator_type(name) == expected


def test_get_current_node_num_accelerators_and_type(monkeypatch):
    pymtml = _mock_pymtml(device_count=8, device_name=b"MTT S5000")
    monkeypatch.setitem(sys.modules, "pymtml", pymtml)

    assert MTGPUAcceleratorManager.get_current_node_num_accelerators() == 8
    assert MTGPUAcceleratorManager.get_current_node_accelerator_type() == "S5000"
    assert pymtml.nvmlInit.call_count == 2
    assert pymtml.nvmlShutdown.call_count == 2


def test_device_count_is_preserved_when_name_query_fails(monkeypatch):
    pymtml = _mock_pymtml(device_count=8)
    pymtml.nvmlDeviceGetHandleByIndex.side_effect = RuntimeError(
        "device name unavailable"
    )
    monkeypatch.setitem(sys.modules, "pymtml", pymtml)

    assert MTGPUAcceleratorManager.get_current_node_num_accelerators() == 8
    assert MTGPUAcceleratorManager.get_current_node_accelerator_type() is None
    assert pymtml.nvmlShutdown.call_count == 2


def test_detection_failure_returns_zero_and_none(monkeypatch):
    pymtml = _mock_pymtml()
    pymtml.nvmlInit.side_effect = RuntimeError("MTML unavailable")
    monkeypatch.setitem(sys.modules, "pymtml", pymtml)

    assert MTGPUAcceleratorManager.get_current_node_num_accelerators() == 0
    assert MTGPUAcceleratorManager.get_current_node_accelerator_type() is None
    pymtml.nvmlShutdown.assert_not_called()


def test_validate_resource_request_quantity():
    assert MTGPUAcceleratorManager.validate_resource_request_quantity(0.5) == (
        True,
        None,
    )


if __name__ == "__main__":
    with patch.dict(os.environ, {}, clear=False):
        pytest.main(["-sv", __file__])
