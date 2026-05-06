import os
import sys
import types

import pytest

from ray._private.accelerators.mblt import (
    MBLT_RT_VISIBLE_DEVICES_ENV_VAR,
    NOSET_MBLT_RT_VISIBLE_DEVICES_ENV_VAR,
    MBLTAcceleratorManager,
)


@pytest.fixture(autouse=True)
def mock_qbruntime_module(monkeypatch):
    class MockAccelerator:
        def __init__(self, dev_no):
            if dev_no >= 4:
                raise RuntimeError("No such device")
            self._dev_no = dev_no

        def get_available_cores(self):
            return [0]

        def get_device_name(self):
            return "ARIES"

    mock_qbruntime = types.ModuleType("qbruntime")
    mock_qbruntime_accelerator = types.ModuleType("qbruntime.accelerator")
    mock_qbruntime_accelerator.Accelerator = MockAccelerator
    mock_qbruntime.accelerator = mock_qbruntime_accelerator

    monkeypatch.setitem(sys.modules, "qbruntime", mock_qbruntime)
    monkeypatch.setitem(
        sys.modules, "qbruntime.accelerator", mock_qbruntime_accelerator
    )
    # Prevent real system calls (glob /dev/aries*, lspci) from bounding
    # the probe range: return None so max_probe always falls back to 256.
    monkeypatch.setattr(
        "ray._private.accelerators.mblt._hint_num_from_os", lambda: None
    )


@pytest.fixture
def clear_mblt_environment(monkeypatch):
    monkeypatch.delenv(MBLT_RT_VISIBLE_DEVICES_ENV_VAR, raising=False)
    monkeypatch.delenv(NOSET_MBLT_RT_VISIBLE_DEVICES_ENV_VAR, raising=False)


@pytest.mark.usefixtures("clear_mblt_environment")
class TestMBLTAcceleratorManager:
    def test_get_resource_name(self):
        assert MBLTAcceleratorManager.get_resource_name() == "MBLT"

    def test_get_visible_accelerator_ids_env_var(self):
        assert (
            MBLTAcceleratorManager.get_visible_accelerator_ids_env_var()
            == MBLT_RT_VISIBLE_DEVICES_ENV_VAR
        )

    def test_get_current_process_visible_accelerator_ids(self):
        os.environ[MBLT_RT_VISIBLE_DEVICES_ENV_VAR] = "0,1,2,3"
        assert MBLTAcceleratorManager.get_current_process_visible_accelerator_ids() == [
            "0",
            "1",
            "2",
            "3",
        ]

        os.environ[MBLT_RT_VISIBLE_DEVICES_ENV_VAR] = ""
        assert (
            MBLTAcceleratorManager.get_current_process_visible_accelerator_ids() == []
        )

        os.environ.pop(MBLT_RT_VISIBLE_DEVICES_ENV_VAR, None)
        assert (
            MBLTAcceleratorManager.get_current_process_visible_accelerator_ids() is None
        )

    def test_get_current_node_num_accelerators(self):
        assert MBLTAcceleratorManager.get_current_node_num_accelerators() == 4

    def test_get_current_node_num_accelerators_does_not_underprobe(self, monkeypatch):
        monkeypatch.setattr(
            "ray._private.accelerators.mblt._hint_num_from_os", lambda: 1
        )

        assert MBLTAcceleratorManager.get_current_node_num_accelerators() == 4

    def test_get_current_node_accelerator_type(self):
        assert MBLTAcceleratorManager.get_current_node_accelerator_type() == "ARIES"

    def test_get_current_node_accelerator_type_from_property(self, monkeypatch):
        class MockAcceleratorWithProperty:
            def __init__(self, dev_no):
                self.device_name = "ARIES-PROP"

        mock_qbruntime = types.ModuleType("qbruntime")
        mock_qbruntime_accelerator = types.ModuleType("qbruntime.accelerator")
        mock_qbruntime_accelerator.Accelerator = MockAcceleratorWithProperty
        mock_qbruntime.accelerator = mock_qbruntime_accelerator

        monkeypatch.setitem(sys.modules, "qbruntime", mock_qbruntime)
        monkeypatch.setitem(
            sys.modules, "qbruntime.accelerator", mock_qbruntime_accelerator
        )

        assert (
            MBLTAcceleratorManager.get_current_node_accelerator_type() == "ARIES-PROP"
        )

    def test_validate_resource_request_quantity(self):
        valid, error = MBLTAcceleratorManager.validate_resource_request_quantity(1)
        assert valid is True
        assert error is None

        valid, error = MBLTAcceleratorManager.validate_resource_request_quantity(1.0)
        assert valid is True
        assert error is None

        valid, error = MBLTAcceleratorManager.validate_resource_request_quantity(1.5)
        assert valid is False
        assert "must be whole numbers" in error
        assert "1.5" in error

    def test_set_current_process_visible_accelerator_ids(self):
        MBLTAcceleratorManager.set_current_process_visible_accelerator_ids(["0", "1"])
        assert os.environ[MBLT_RT_VISIBLE_DEVICES_ENV_VAR] == "0,1"

    def test_set_current_process_visible_accelerator_ids_respects_noset(self):
        os.environ[MBLT_RT_VISIBLE_DEVICES_ENV_VAR] = "0,1"
        os.environ[NOSET_MBLT_RT_VISIBLE_DEVICES_ENV_VAR] = "1"

        MBLTAcceleratorManager.set_current_process_visible_accelerator_ids(["2", "3"])
        assert os.environ[MBLT_RT_VISIBLE_DEVICES_ENV_VAR] == "0,1"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
