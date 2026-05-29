import os
import sys
import types

import pytest

from ray._private.accelerators.mblt import (
    MBLT_RT_VISIBLE_DEVICES_ENV_VAR,
    NOSET_MBLT_RT_VISIBLE_DEVICES_ENV_VAR,
    MBLTAcceleratorManager,
)


def _install_qbruntime_mock(monkeypatch, num_present: int = 4):
    """Install a fake ``qbruntime`` module exposing ``get_available_device_numbers``.

    The real ``qbruntime`` package (see qb Runtime v1.2.0) returns a list of
    integer device indices from this function; we mirror that contract.
    """
    mock_qbruntime = types.ModuleType("qbruntime")
    mock_qbruntime.get_available_device_numbers = lambda: list(range(num_present))
    monkeypatch.setitem(sys.modules, "qbruntime", mock_qbruntime)
    return mock_qbruntime


@pytest.fixture
def clear_mblt_environment(monkeypatch):
    monkeypatch.delenv(MBLT_RT_VISIBLE_DEVICES_ENV_VAR, raising=False)
    monkeypatch.delenv(NOSET_MBLT_RT_VISIBLE_DEVICES_ENV_VAR, raising=False)


@pytest.fixture(autouse=True)
def isolate_dev_and_lspci(monkeypatch):
    """Prevent the real /dev tree and lspci from influencing detection."""
    monkeypatch.setattr(
        "ray._private.accelerators.mblt._count_mblt_dev_nodes", lambda: 0
    )
    monkeypatch.setattr(
        "ray._private.accelerators.mblt._count_mblt_pci_entries", lambda: 0
    )
    # Default: no qbruntime installed. Individual tests can install the mock.
    monkeypatch.delitem(sys.modules, "qbruntime", raising=False)


@pytest.mark.usefixtures("clear_mblt_environment")
class TestMBLTAcceleratorManager:
    def test_get_resource_name(self):
        assert MBLTAcceleratorManager.get_resource_name() == "MBLT"

    def test_get_visible_accelerator_ids_env_var(self):
        assert (
            MBLTAcceleratorManager.get_visible_accelerator_ids_env_var()
            == MBLT_RT_VISIBLE_DEVICES_ENV_VAR
        )

    def test_get_current_process_visible_accelerator_ids_set(self, monkeypatch):
        monkeypatch.setenv(MBLT_RT_VISIBLE_DEVICES_ENV_VAR, "0,1,2,3")
        assert MBLTAcceleratorManager.get_current_process_visible_accelerator_ids() == [
            "0",
            "1",
            "2",
            "3",
        ]

    def test_get_current_process_visible_accelerator_ids_empty(self, monkeypatch):
        monkeypatch.setenv(MBLT_RT_VISIBLE_DEVICES_ENV_VAR, "")
        assert (
            MBLTAcceleratorManager.get_current_process_visible_accelerator_ids() == []
        )

    def test_get_current_process_visible_accelerator_ids_unset(self):
        assert (
            MBLTAcceleratorManager.get_current_process_visible_accelerator_ids() is None
        )

    @pytest.mark.parametrize("num_present", [0, 1, 4, 8])
    def test_get_current_node_num_accelerators_sdk(self, monkeypatch, num_present):
        _install_qbruntime_mock(monkeypatch, num_present=num_present)
        assert (
            MBLTAcceleratorManager.get_current_node_num_accelerators() == num_present
        )

    def test_get_current_node_num_accelerators_sdk_raises_falls_back_to_dev(
        self, monkeypatch
    ):
        mock_qbruntime = types.ModuleType("qbruntime")

        def _boom():
            raise RuntimeError("driver mismatch")

        mock_qbruntime.get_available_device_numbers = _boom
        monkeypatch.setitem(sys.modules, "qbruntime", mock_qbruntime)
        monkeypatch.setattr(
            "ray._private.accelerators.mblt._count_mblt_dev_nodes", lambda: 2
        )
        assert MBLTAcceleratorManager.get_current_node_num_accelerators() == 2

    def test_get_current_node_num_accelerators_no_sdk_uses_dev_fallback(
        self, monkeypatch
    ):
        monkeypatch.setattr(
            "ray._private.accelerators.mblt._count_mblt_dev_nodes", lambda: 2
        )
        assert MBLTAcceleratorManager.get_current_node_num_accelerators() == 2

    def test_get_current_node_num_accelerators_no_sdk_no_dev_uses_pci(
        self, monkeypatch
    ):
        monkeypatch.setattr(
            "ray._private.accelerators.mblt._count_mblt_pci_entries", lambda: 3
        )
        assert MBLTAcceleratorManager.get_current_node_num_accelerators() == 3

    def test_get_current_node_num_accelerators_none(self):
        assert MBLTAcceleratorManager.get_current_node_num_accelerators() == 0

    @pytest.mark.parametrize(
        "globbed,expected",
        [
            (["/dev/aries0", "/dev/aries1"], "MOBILINT_ARIES"),
            (["/dev/regulus0"], "MOBILINT_REGULUS"),
            ([], None),
        ],
    )
    def test_get_current_node_accelerator_type_from_dev(
        self, monkeypatch, globbed, expected
    ):
        def fake_glob(pattern):
            if expected == "MOBILINT_ARIES" and "aries" in pattern:
                return globbed
            if expected == "MOBILINT_REGULUS" and "regulus" in pattern:
                return globbed
            return []

        monkeypatch.setattr("ray._private.accelerators.mblt.glob.glob", fake_glob)
        monkeypatch.setattr(
            "ray._private.accelerators.mblt.subprocess.check_output",
            lambda *a, **k: "",
        )
        assert MBLTAcceleratorManager.get_current_node_accelerator_type() == expected

    def test_validate_resource_request_quantity_integer(self):
        valid, error = MBLTAcceleratorManager.validate_resource_request_quantity(1)
        assert valid is True
        assert error is None

    def test_validate_resource_request_quantity_whole_float(self):
        valid, error = MBLTAcceleratorManager.validate_resource_request_quantity(1.0)
        assert valid is True
        assert error is None

    def test_validate_resource_request_quantity_fractional(self):
        valid, error = MBLTAcceleratorManager.validate_resource_request_quantity(1.5)
        assert valid is False
        assert "whole number" in error
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
