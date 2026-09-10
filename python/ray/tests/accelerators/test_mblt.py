import os
import sys
import types
from decimal import Decimal
from fractions import Fraction

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
def isolate_dev_detection(monkeypatch):
    """Isolate detection from host state: no /dev nodes, no qbruntime SDK."""
    monkeypatch.setattr(
        "ray._private.accelerators.mblt._count_mblt_dev_nodes", lambda: 0
    )
    monkeypatch.setattr("ray._private.accelerators.mblt.glob.glob", lambda *a, **k: [])
    # A ``None`` entry in ``sys.modules`` forces ``from qbruntime import ...`` to
    # raise ``ImportError`` even when the real SDK is installed on disk, so the
    # /dev fallback path is exercised deterministically. Tests that need the SDK
    # re-inject a mock via ``setitem``, which overrides this sentinel.
    monkeypatch.setitem(sys.modules, "qbruntime", None)


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
        assert MBLTAcceleratorManager.get_current_node_num_accelerators() == num_present

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

    def test_get_current_node_num_accelerators_none(self):
        assert MBLTAcceleratorManager.get_current_node_num_accelerators() == 0

    @pytest.mark.parametrize(
        "globbed,expected",
        [
            (["/dev/aries0", "/dev/aries1"], "MOBILINT_ARIES"),
            (["/dev/regulus-npu"], "MOBILINT_REGULUS"),
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
        assert MBLTAcceleratorManager.get_current_node_accelerator_type() == expected

    def test_get_current_node_accelerator_type_returns_none_when_no_dev(
        self, monkeypatch
    ):
        # No /dev nodes and no SDK: type detection returns None rather than
        # guessing the family. ARIES vs REGULUS is only distinguishable from
        # the kernel driver's /dev node names.
        monkeypatch.setattr(
            "ray._private.accelerators.mblt.glob.glob", lambda *a, **k: []
        )
        assert MBLTAcceleratorManager.get_current_node_accelerator_type() is None

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

    @pytest.mark.parametrize("quantity", [Decimal("1.5"), Fraction(3, 2)])
    def test_validate_resource_request_quantity_fractional_non_float(self, quantity):
        # Non-``float`` numeric types (Decimal/Fraction) must also be rejected;
        # a plain ``isinstance(quantity, float)`` guard would let them through.
        valid, error = MBLTAcceleratorManager.validate_resource_request_quantity(
            quantity
        )
        assert valid is False
        assert "whole number" in error

    @pytest.mark.parametrize("quantity", [Decimal("2"), Fraction(4, 2)])
    def test_validate_resource_request_quantity_whole_non_float(self, quantity):
        valid, error = MBLTAcceleratorManager.validate_resource_request_quantity(
            quantity
        )
        assert valid is True
        assert error is None

    def test_set_current_process_visible_accelerator_ids(self, monkeypatch):
        MBLTAcceleratorManager.set_current_process_visible_accelerator_ids(["0", "1"])
        # qb Runtime reads QBRUNTIME_VISIBLE_DEVICES; Ray sets exactly this one
        # env var (no second mirrored name) so it is restorable on worker reuse.
        assert os.environ[MBLT_RT_VISIBLE_DEVICES_ENV_VAR] == "0,1"

    def test_set_current_process_visible_accelerator_ids_respects_noset(
        self, monkeypatch
    ):
        monkeypatch.setenv(MBLT_RT_VISIBLE_DEVICES_ENV_VAR, "0,1")
        monkeypatch.setenv(NOSET_MBLT_RT_VISIBLE_DEVICES_ENV_VAR, "1")

        MBLTAcceleratorManager.set_current_process_visible_accelerator_ids(["2", "3"])
        # The NOSET flag must leave the env var untouched.
        assert os.environ[MBLT_RT_VISIBLE_DEVICES_ENV_VAR] == "0,1"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
