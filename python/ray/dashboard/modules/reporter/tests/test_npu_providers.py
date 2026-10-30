"""Unit tests for the Ascend NPU metric provider."""

import ctypes
import sys
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from ray.dashboard.modules.reporter.npu_providers import (
    DCMI_HEALTH_MISSING,
    DCMI_LIBRARY_PATH_ENV,
    MB,
    NPU_HEALTH_MISSING,
    NpuMetricProvider,
    _DcmiLibrary,
)


def _make_dcmi():
    dcmi = Mock()
    dcmi.get_card_list.return_value = [2]
    dcmi.get_device_count.return_value = 2
    dcmi.get_logic_id.side_effect = [0, 1]
    dcmi.get_physical_id.side_effect = [4, 5]
    dcmi.get_device_name.return_value = "Ascend910"
    dcmi.get_aicore_utilization.side_effect = [97, 85]
    dcmi.get_hbm_info.side_effect = [
        SimpleNamespace(memory_usage=4270, memory_size=65536),
        SimpleNamespace(memory_usage=4036, memory_size=65536),
    ]
    dcmi.get_health.return_value = 0
    dcmi.get_power.return_value = 9407
    dcmi.get_temperature.side_effect = [71, 70]
    dcmi.get_processes.side_effect = [
        {1001: {"pid": 1001, "npu_memory_usage": 2048 * MB}},
        {1002: {"pid": 1002, "npu_memory_usage": 1024 * MB}},
    ]
    return dcmi


def test_collects_all_devices_and_normalizes_units():
    dcmi = _make_dcmi()
    provider = NpuMetricProvider(dcmi=dcmi)

    assert provider.initialize()
    usage = provider.get_npu_usage()

    dcmi.initialize.assert_called_once_with()
    dcmi.get_device_count.assert_called_once_with(2)
    assert len(usage) == 2
    assert usage[0] == {
        "index": 0,
        "card_id": 2,
        "device_id": 0,
        "physical_id": 4,
        "name": "Ascend910",
        "utilization_aicore": 97,
        "memory_used": 4270 * MB,
        "memory_total": 65536 * MB,
        "health_status": 0,
        "power_mw": 940700,
        "temperature_c": 71,
        "processes_pids": {1001: {"pid": 1001, "npu_memory_usage": 2048 * MB}},
    }
    assert usage[1]["index"] == 1
    assert usage[1]["physical_id"] == 5
    assert usage[1]["power_mw"] is None
    assert usage[1]["processes_pids"] == {
        1002: {"pid": 1002, "npu_memory_usage": 1024 * MB}
    }
    dcmi.get_power.assert_called_once_with(2, 0)


def test_device_field_failure_does_not_drop_other_metrics():
    dcmi = _make_dcmi()
    dcmi.get_temperature.side_effect = [RuntimeError("not supported"), 70]
    provider = NpuMetricProvider(dcmi=dcmi)

    usage = provider.get_npu_usage()

    assert len(usage) == 2
    assert usage[0]["temperature_c"] is None
    assert usage[0]["utilization_aicore"] == 97
    assert usage[1]["temperature_c"] == 70


def test_logic_id_failure_uses_synthetic_index_that_never_collides():
    # Device 0 fails to report its logical ID while device 1's real logical ID
    # is 1. Synthetic indices must stay negative so they cannot collide with
    # the non-negative logical IDs reported by other devices.
    dcmi = _make_dcmi()
    dcmi.get_logic_id.side_effect = [RuntimeError("not supported"), 1]
    dcmi.get_physical_id.side_effect = [RuntimeError("not supported"), 5]
    provider = NpuMetricProvider(dcmi=dcmi)

    usage = provider.get_npu_usage()

    assert len(usage) == 2
    assert usage[0]["index"] == -1
    assert usage[0]["physical_id"] is None
    assert usage[1]["index"] == 1


def test_process_query_failure_does_not_drop_device_metrics():
    dcmi = _make_dcmi()
    dcmi.get_processes.side_effect = [RuntimeError("not supported"), {}]
    provider = NpuMetricProvider(dcmi=dcmi)

    usage = provider.get_npu_usage()

    assert len(usage) == 2
    assert usage[0]["processes_pids"] is None
    assert usage[0]["memory_used"] == 4270 * MB
    assert usage[1]["processes_pids"] == {}


def test_card_failure_does_not_drop_other_cards():
    dcmi = _make_dcmi()
    dcmi.get_card_list.return_value = [1, 2]
    dcmi.get_device_count.side_effect = [RuntimeError("unavailable"), 2]
    provider = NpuMetricProvider(dcmi=dcmi)

    usage = provider.get_npu_usage()

    assert len(usage) == 2
    assert all(info["card_id"] == 2 for info in usage)


def test_missing_dcmi_library_disables_provider():
    with patch(
        "ray.dashboard.modules.reporter.npu_providers._DcmiLibrary.load",
        side_effect=OSError("not installed"),
    ) as load:
        provider = NpuMetricProvider()
        assert not provider.initialize()
        assert provider.get_npu_usage() == []
        load.assert_called_once_with()


def test_loads_dcmi_without_rtld_local(monkeypatch):
    library_path = "/fake/libdcmi.so"
    handle = SimpleNamespace()
    cdll = Mock(return_value=handle)
    monkeypatch.setenv(DCMI_LIBRARY_PATH_ENV, library_path)
    monkeypatch.delattr(ctypes, "RTLD_LOCAL", raising=False)
    monkeypatch.setattr(ctypes, "CDLL", cdll)

    dcmi = _DcmiLibrary.load()

    assert dcmi._handle is handle
    cdll.assert_called_once_with(library_path)


def test_device_name_falls_back_to_legacy_dcmi_api():
    dcmi = object.__new__(_DcmiLibrary)
    dcmi._handle = SimpleNamespace(dcmi_get_device_chip_info=object())

    def populate_chip_info(function_name, card_id, device_id, chip_info_pointer):
        assert function_name == "dcmi_get_device_chip_info"
        assert (card_id, device_id) == (2, 0)
        chip_info_pointer._obj.chip_name[:6] = b"910B2C"

    dcmi._call = Mock(side_effect=populate_chip_info)

    assert dcmi.get_device_name(2, 0) == "910B2C"


@pytest.mark.parametrize(
    ("dcmi_status", "expected_status"),
    [
        (0, 0),
        (1, 1),
        (2, 2),
        (3, 3),
        (DCMI_HEALTH_MISSING, NPU_HEALTH_MISSING),
    ],
)
def test_get_health_normalizes_missing_status(dcmi_status, expected_status):
    dcmi = object.__new__(_DcmiLibrary)

    def populate_health(function_name, card_id, device_id, health_pointer):
        assert function_name == "dcmi_get_device_health"
        assert (card_id, device_id) == (2, 0)
        health_pointer._obj.value = dcmi_status

    dcmi._call = Mock(side_effect=populate_health)

    assert dcmi.get_health(2, 0) == expected_status


def test_get_processes_returns_memory_bytes_by_pid():
    dcmi = object.__new__(_DcmiLibrary)

    def populate_processes(
        function_name, card_id, device_id, process_infos, process_count_pointer
    ):
        assert function_name == "dcmi_get_device_resource_info"
        assert (card_id, device_id) == (2, 0)
        process_infos[0].proc_id = 1234
        process_infos[0].proc_mem_usage = 4096 * MB
        process_infos[1].proc_id = 5678
        process_infos[1].proc_mem_usage = 1024 * MB
        process_count_pointer._obj.value = 2

    dcmi._call = Mock(side_effect=populate_processes)

    assert dcmi.get_processes(2, 0) == {
        1234: {"pid": 1234, "npu_memory_usage": 4096 * MB},
        5678: {"pid": 5678, "npu_memory_usage": 1024 * MB},
    }


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
