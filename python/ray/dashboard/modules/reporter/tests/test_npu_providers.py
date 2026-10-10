"""Unit tests for the Ascend NPU metric provider."""

from types import SimpleNamespace
from unittest.mock import Mock, patch

from ray.dashboard.modules.reporter.npu_providers import (
    MB,
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
    }
    assert usage[1]["index"] == 1
    assert usage[1]["physical_id"] == 5
    assert usage[1]["power_mw"] is None
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


def test_device_name_falls_back_to_legacy_dcmi_api():
    dcmi = object.__new__(_DcmiLibrary)
    dcmi._handle = SimpleNamespace(dcmi_get_device_chip_info=object())

    def populate_chip_info(function_name, card_id, device_id, chip_info_pointer):
        assert function_name == "dcmi_get_device_chip_info"
        assert (card_id, device_id) == (2, 0)
        chip_info_pointer._obj.chip_name[:6] = b"910B2C"

    dcmi._call = Mock(side_effect=populate_chip_info)

    assert dcmi.get_device_name(2, 0) == "910B2C"
