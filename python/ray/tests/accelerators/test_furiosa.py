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

    @pytest.mark.parametrize(
        "env_value,expected",
        [
            # furiosa-llm --devices form (preferred).
            ("npu:0,npu:1,npu:2,npu:3", ["0", "1", "2", "3"]),
            # Bare integer form is also accepted for convenience.
            ("0,1,2,3", ["0", "1", "2", "3"]),
            # Core range notation: only the device index is returned.
            ("npu:0:0-3,npu:1:0-3", ["0", "1"]),
            # Empty string yields an empty list.
            ("", []),
            # Sentinel ``None`` means the env var is unset.
            (None, None),
        ],
    )
    def test_get_current_process_visible_accelerator_ids(self, env_value, expected):
        if env_value is None:
            os.environ.pop(FURIOSA_VISIBLE_DEVICES_ENV_VAR, None)
        else:
            os.environ[FURIOSA_VISIBLE_DEVICES_ENV_VAR] = env_value
        assert (
            FuriosaAcceleratorManager.get_current_process_visible_accelerator_ids()
            == expected
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
            # PyO3 enum form (CamelCase).
            ("Rngd", "FURIOSA_RNGD"),
            ("RngdMax", "FURIOSA_RNGDMAX"),
            ("RngdS", "FURIOSA_RNGDS"),
            ("RngdPlus", "FURIOSA_RNGDPLUS"),
            # ``Arch::ToString`` form is also accepted, and both forms must
            # resolve to the same label.
            ("rngd-max", "FURIOSA_RNGDMAX"),
            # ``+`` must NOT be silently stripped, since that would collapse
            # ``rngd+`` into ``rngd`` and collide with the base RNGD SKU; it
            # is mapped to ``plus`` so the label matches ``RngdPlus``.
            ("rngd+", "FURIOSA_RNGDPLUS"),
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
        # Ray's scheduler hands us bare integer IDs; we serialize them in
        # the ``npu:<id>`` form expected by ``furiosa-llm --devices``.
        FuriosaAcceleratorManager.set_current_process_visible_accelerator_ids(
            ["0", "1"]
        )
        assert os.environ[FURIOSA_VISIBLE_DEVICES_ENV_VAR] == "npu:0,npu:1"

        os.environ[NOSET_FURIOSA_VISIBLE_DEVICES_ENV_VAR] = "1"
        FuriosaAcceleratorManager.set_current_process_visible_accelerator_ids(
            ["2", "3"]
        )
        assert os.environ[FURIOSA_VISIBLE_DEVICES_ENV_VAR] == "npu:0,npu:1"

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
