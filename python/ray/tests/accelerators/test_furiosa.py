import os
import sys

import pytest

from ray._private.accelerators.furiosa import (
    FURIOSA_VISIBLE_DEVICES_ENV_VAR,
    NOSET_FURIOSA_VISIBLE_DEVICES_ENV_VAR,
    FuriosaAcceleratorManager,
)


@pytest.fixture(autouse=True)
def mock_furiosa_smi_py(monkeypatch):
    from ray.tests.accelerators import mock_furiosa_smi_py

    monkeypatch.setitem(sys.modules, "furiosa_smi_py", mock_furiosa_smi_py)


@pytest.fixture
def clear_furiosa_environment():
    original_env = os.environ.get(FURIOSA_VISIBLE_DEVICES_ENV_VAR)
    original_no_set_env = os.environ.get(NOSET_FURIOSA_VISIBLE_DEVICES_ENV_VAR)

    os.environ.pop(FURIOSA_VISIBLE_DEVICES_ENV_VAR, None)
    os.environ.pop(NOSET_FURIOSA_VISIBLE_DEVICES_ENV_VAR, None)

    yield

    if original_env is not None:
        os.environ[FURIOSA_VISIBLE_DEVICES_ENV_VAR] = original_env
    if original_no_set_env is not None:
        os.environ[NOSET_FURIOSA_VISIBLE_DEVICES_ENV_VAR] = original_no_set_env


@pytest.mark.usefixtures("clear_furiosa_environment")
class TestFuriosaAcceleratorManager:
    def test_get_resource_name(self):
        assert FuriosaAcceleratorManager.get_resource_name() == "FURIOSA"

    def test_get_visible_accelerator_ids_env_var(self):
        assert (
            FuriosaAcceleratorManager.get_visible_accelerator_ids_env_var()
            == FURIOSA_VISIBLE_DEVICES_ENV_VAR
        )

    def test_get_current_process_visible_accelerator_ids(self):
        os.environ[FURIOSA_VISIBLE_DEVICES_ENV_VAR] = "0,1,2,3"
        assert (
            FuriosaAcceleratorManager.get_current_process_visible_accelerator_ids()
            == ["0", "1", "2", "3"]
        )

        os.environ[FURIOSA_VISIBLE_DEVICES_ENV_VAR] = ""
        assert (
            FuriosaAcceleratorManager.get_current_process_visible_accelerator_ids()
            == []
        )

        os.environ.pop(FURIOSA_VISIBLE_DEVICES_ENV_VAR)
        assert (
            FuriosaAcceleratorManager.get_current_process_visible_accelerator_ids()
            is None
        )

    def test_get_current_node_num_accelerators(self):
        assert FuriosaAcceleratorManager.get_current_node_num_accelerators() == 8

    def test_get_current_node_accelerator_type(self):
        assert (
            FuriosaAcceleratorManager.get_current_node_accelerator_type()
            == "FURIOSA_RNGD"
        )

    @pytest.mark.parametrize(
        "arch_name,expected",
        [
            # Real Furiosa SMI arch enum values (from furiosa-smi-go types.go).
            ("Rngd", "FURIOSA_RNGD"),
            ("RngdMax", "FURIOSA_RNGDMAX"),
            ("RngdS", "FURIOSA_RNGDS"),
            ("RngdPlus", "FURIOSA_RNGDPLUS"),
            # Future-proofing: non-alphanumeric characters are stripped so
            # arch values like "rngd-max" or "rngd+" still produce valid
            # accelerator type labels.
            ("rngd-max", "FURIOSA_RNGDMAX"),
            ("rngd+", "FURIOSA_RNGD"),
        ],
    )
    def test_get_current_node_accelerator_type_dynamic(
        self, monkeypatch, arch_name, expected
    ):
        from ray.tests.accelerators import mock_furiosa_smi_py

        def mocked_list_devices():
            return [mock_furiosa_smi_py._MockDevice(0, arch_name=arch_name)]

        monkeypatch.setattr(mock_furiosa_smi_py, "list_devices", mocked_list_devices)
        assert FuriosaAcceleratorManager.get_current_node_accelerator_type() == expected

    def test_get_current_node_accelerator_type_no_devices(self, monkeypatch):
        from ray.tests.accelerators import mock_furiosa_smi_py

        monkeypatch.setattr(mock_furiosa_smi_py, "list_devices", lambda: [])
        assert FuriosaAcceleratorManager.get_current_node_accelerator_type() is None

    def test_get_current_node_accelerator_type_arch_is_none(self, monkeypatch):
        """Regression: arch() returning None must not produce 'FURIOSA_NONE'."""
        from ray.tests.accelerators import mock_furiosa_smi_py

        class _NullArchDeviceInfo:
            def arch(self):
                return None

        class _NullArchDevice:
            def device_info(self):
                return _NullArchDeviceInfo()

        monkeypatch.setattr(
            mock_furiosa_smi_py, "list_devices", lambda: [_NullArchDevice()]
        )
        assert FuriosaAcceleratorManager.get_current_node_accelerator_type() is None

    def test_set_current_process_visible_accelerator_ids(self):
        FuriosaAcceleratorManager.set_current_process_visible_accelerator_ids(
            ["0", "1"]
        )
        assert os.environ[FURIOSA_VISIBLE_DEVICES_ENV_VAR] == "0,1"

        os.environ[NOSET_FURIOSA_VISIBLE_DEVICES_ENV_VAR] = "1"
        FuriosaAcceleratorManager.set_current_process_visible_accelerator_ids(
            ["2", "3"]
        )
        assert os.environ[FURIOSA_VISIBLE_DEVICES_ENV_VAR] == "0,1"

    def test_validate_resource_request_quantity(self):
        valid, _ = FuriosaAcceleratorManager.validate_resource_request_quantity(1)
        assert valid

        valid, _ = FuriosaAcceleratorManager.validate_resource_request_quantity(2.0)
        assert valid

        valid, msg = FuriosaAcceleratorManager.validate_resource_request_quantity(0.5)
        assert not valid
        assert "whole number" in msg


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
