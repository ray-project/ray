"""Unit tests for the NVIDIA accelerator inventory (node-agent-visible
physical GPUs + CPU NUMA locality via Linux sysfs).

These tests mock the bundled pynvml and are NOT real GPU verification.
"""

import sys
import types
from unittest.mock import mock_open, patch

import pytest

import ray._private.thirdparty.pynvml as pynvml
from ray._private.accelerators import NvidiaGPUAcceleratorManager
from ray._private.accelerators.accelerator import AcceleratorInventoryStatus


class FakePciInfo:
    def __init__(self, bus_id):
        self.busId = bus_id


class FakeMemoryInfo:
    def __init__(self, total):
        self.total = total


def _device(
    name,
    uuid,
    bus_id="00000000:3B:00.0",
    memory_total=16 * 1024**3,
):
    return {
        "name": name,
        "uuid": uuid,
        "bus_id": bus_id,
        "memory_total": memory_total,
    }


class FakeNVML:
    """Configurable fake of the pynvml surface used by inventory discovery."""

    def __init__(self, devices):
        self.devices = devices
        self.init_error = None
        self.count_error = None
        self.init_called = False
        self.shutdown_called = False
        self.numa_calls = 0

    def nvmlInit(self):
        self.init_called = True
        if self.init_error is not None:
            raise self.init_error

    def nvmlShutdown(self):
        self.shutdown_called = True

    def nvmlDeviceGetCount(self):
        if self.count_error is not None:
            raise self.count_error
        return len(self.devices)

    def nvmlDeviceGetHandleByIndex(self, index):
        handle = self.devices[index]
        if handle.get("handle_error") is not None:
            raise handle["handle_error"]
        return handle

    def nvmlDeviceGetName(self, handle):
        if handle.get("name_error") is not None:
            raise handle["name_error"]
        return handle["name"]

    def nvmlDeviceGetUUID(self, handle):
        if handle.get("uuid_error") is not None:
            raise handle["uuid_error"]
        return handle["uuid"]

    def nvmlDeviceGetMemoryInfo(self, handle):
        if handle.get("memory_error") is not None:
            raise handle["memory_error"]
        return FakeMemoryInfo(handle["memory_total"])

    def nvmlDeviceGetPciInfo(self, handle):
        if handle.get("pci_error") is not None:
            raise handle["pci_error"]
        return FakePciInfo(handle["bus_id"])

    def nvmlDeviceGetNumaNodeId(self, handle):
        # NVML's own NUMA API: must NOT be used for CPU locality. This spy
        # returns a deliberately "wrong" value (8) so any use would be caught.
        self.numa_calls += 1
        return 8


_PATCHED_FUNCS = [
    "nvmlInit",
    "nvmlShutdown",
    "nvmlDeviceGetCount",
    "nvmlDeviceGetHandleByIndex",
    "nvmlDeviceGetName",
    "nvmlDeviceGetUUID",
    "nvmlDeviceGetMemoryInfo",
    "nvmlDeviceGetPciInfo",
    "nvmlDeviceGetNumaNodeId",
]


@pytest.fixture
def fake_nvml(monkeypatch):
    """Patch the pynvml module with a FakeNVML and return it."""
    fake = FakeNVML([])
    for name in _PATCHED_FUNCS:
        monkeypatch.setattr(pynvml, name, getattr(fake, name))
    return fake


def _set_devices(fake, devices):
    fake.devices = devices


def test_inventory_multi_device_and_uuid_mapping(fake_nvml):
    _set_devices(
        fake_nvml,
        [
            _device(
                "Tesla V100-SXM2-16GB",
                "GPU-00000000-0000-0000-0000-000000000001",
                bus_id="00000000:3B:00.0",
                memory_total=16945512448,
            ),
            _device(
                "NVIDIA A100-SXM4-40GB",
                "GPU-00000000-0000-0000-0000-000000000002",
                bus_id="00000000:61:00.0",
                memory_total=42949672960,
            ),
        ],
    )
    with patch(
        "ray._private.accelerators.nvidia_gpu.sys",
        types.SimpleNamespace(platform="linux"),
    ):
        inventory = NvidiaGPUAcceleratorManager.get_current_node_accelerator_inventory()

    assert inventory.status is AcceleratorInventoryStatus.OK
    assert len(inventory.devices) == 2
    first, second = inventory.devices
    assert first.uuid == "GPU-00000000-0000-0000-0000-000000000001"
    assert first.model == "Tesla V100-SXM2-16GB"
    assert first.memory_total_bytes == 16945512448
    assert first.pci_bus_id == "0000:3b:00.0"
    assert first.kind == "gpu"
    assert first.parent_uuid is None
    assert second.uuid == "GPU-00000000-0000-0000-0000-000000000002"
    assert second.pci_bus_id == "0000:61:00.0"
    # The GPU-own NUMA API must never have been consulted.
    assert fake_nvml.numa_calls == 0
    assert fake_nvml.init_called and fake_nvml.shutdown_called


def test_inventory_empty_when_zero_devices(fake_nvml):
    _set_devices(fake_nvml, [])
    inventory = NvidiaGPUAcceleratorManager.get_current_node_accelerator_inventory()
    assert inventory.status is AcceleratorInventoryStatus.OK
    assert inventory.devices == []


def test_inventory_unsupported_on_library_not_installed(fake_nvml):
    fake_nvml.init_error = pynvml.NVMLError_LibraryNotFound()
    inventory = NvidiaGPUAcceleratorManager.get_current_node_accelerator_inventory()
    assert inventory.status is AcceleratorInventoryStatus.UNSUPPORTED


def test_inventory_unavailable_on_init_error(fake_nvml):
    fake_nvml.init_error = pynvml.NVMLError_DriverNotLoaded()
    inventory = NvidiaGPUAcceleratorManager.get_current_node_accelerator_inventory()
    assert inventory.status is AcceleratorInventoryStatus.UNAVAILABLE
    assert "NVML failed to initialize" in inventory.status_message


def test_inventory_error_on_count_error(fake_nvml):
    fake_nvml.count_error = pynvml.NVMLError_Unknown()
    inventory = NvidiaGPUAcceleratorManager.get_current_node_accelerator_inventory()
    assert inventory.status is AcceleratorInventoryStatus.ERROR
    assert fake_nvml.shutdown_called


def test_inventory_error_on_empty_uuid(fake_nvml):
    _set_devices(fake_nvml, [_device("Tesla V100", "")])
    inventory = NvidiaGPUAcceleratorManager.get_current_node_accelerator_inventory()
    assert inventory.status is AcceleratorInventoryStatus.ERROR
    assert "empty device UUID" in inventory.status_message


def test_inventory_error_on_empty_name(fake_nvml):
    _set_devices(fake_nvml, [_device("", "GPU-abc")])
    inventory = NvidiaGPUAcceleratorManager.get_current_node_accelerator_inventory()
    assert inventory.status is AcceleratorInventoryStatus.ERROR
    assert "empty device name" in inventory.status_message


def test_inventory_error_on_memory_read_failure(fake_nvml):
    device = _device("Tesla V100", "GPU-abc")
    device["memory_error"] = pynvml.NVMLError_Unknown()
    _set_devices(fake_nvml, [device])
    inventory = NvidiaGPUAcceleratorManager.get_current_node_accelerator_inventory()
    assert inventory.status is AcceleratorInventoryStatus.ERROR
    # The unknown memory must not have been reported as zero.
    assert inventory.devices == []


def test_inventory_error_on_device_name_failure(fake_nvml):
    device = _device("Tesla V100", "GPU-abc")
    device["name_error"] = pynvml.NVMLError_Unknown()
    _set_devices(fake_nvml, [device])
    inventory = NvidiaGPUAcceleratorManager.get_current_node_accelerator_inventory()
    assert inventory.status is AcceleratorInventoryStatus.ERROR


def test_inventory_shutdown_on_device_error(fake_nvml):
    device = _device("Tesla V100", "")
    _set_devices(fake_nvml, [device])
    NvidiaGPUAcceleratorManager.get_current_node_accelerator_inventory()
    assert fake_nvml.shutdown_called


@pytest.mark.parametrize(
    "bus_id,expected",
    [
        ("00000000:3B:00.0", "0000:3b:00.0"),
        ("0000:3b:00.0", "0000:3b:00.0"),
        ("0000:00:1D.7", "0000:00:1d.7"),
        # Nonzero high domain bits are preserved, not truncated.
        ("00010000:00:00.0", "10000:00:00.0"),
        # Malformed / out-of-range are rejected.
        ("garbage", None),
        ("", None),
        ("0000:1ff:00.0", None),  # bus 0x1ff > 0xff
        ("0000:00:20.0", None),  # device 0x20 > 0x1f
        ("0000:00:00.8", None),  # function 0x8 > 0x7
    ],
)
def test_normalize_pci_bus_id(bus_id, expected):
    assert NvidiaGPUAcceleratorManager._normalize_pci_bus_id(bus_id) == expected


def test_cpu_numa_from_sysfs_linux():
    with (
        patch(
            "ray._private.accelerators.nvidia_gpu.sys",
            types.SimpleNamespace(platform="linux"),
        ),
        patch("builtins.open", mock_open(read_data="1")) as m,
    ):
        assert (
            NvidiaGPUAcceleratorManager._get_cpu_numa_node_from_pci("0000:3b:00.0") == 1
        )
        m.assert_called_once_with(
            "/sys/bus/pci/devices/0000:3b:00.0/numa_node", encoding="utf-8"
        )


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("-1", None),  # NVML/sysfs unknown sentinel -> None, not node 0
        ("", None),  # empty file
        ("garbage", None),  # malformed
    ],
)
def test_cpu_numa_from_sysfs_values(raw, expected):
    with (
        patch(
            "ray._private.accelerators.nvidia_gpu.sys",
            types.SimpleNamespace(platform="linux"),
        ),
        patch("builtins.open", mock_open(read_data=raw)),
    ):
        assert (
            NvidiaGPUAcceleratorManager._get_cpu_numa_node_from_pci("0000:3b:00.0")
            == expected
        )


def test_cpu_numa_from_sysfs_unreadable():
    with (
        patch(
            "ray._private.accelerators.nvidia_gpu.sys",
            types.SimpleNamespace(platform="linux"),
        ),
        patch("builtins.open", side_effect=OSError("permission denied")),
    ):
        assert (
            NvidiaGPUAcceleratorManager._get_cpu_numa_node_from_pci("0000:3b:00.0")
            is None
        )


def test_cpu_numa_non_linux_returns_none():
    with (
        patch(
            "ray._private.accelerators.nvidia_gpu.sys",
            types.SimpleNamespace(platform="darwin"),
        ),
        patch("builtins.open") as m,
    ):
        assert (
            NvidiaGPUAcceleratorManager._get_cpu_numa_node_from_pci("0000:3b:00.0")
            is None
        )
        m.assert_not_called()


def test_cpu_numa_none_pci_bus_id_returns_none():
    assert NvidiaGPUAcceleratorManager._get_cpu_numa_node_from_pci(None) is None


def test_full_path_uses_sysfs_not_nvml_numa(fake_nvml):
    """NVML's nvmlDeviceGetNumaNodeId returns the GPU's own NUMA node (8) on
    platforms where GPUs are NUMA nodes; CPU locality must come from sysfs."""
    _set_devices(
        fake_nvml, [_device("Tesla V100", "GPU-abc", bus_id="00000000:3B:00.0")]
    )
    with (
        patch(
            "ray._private.accelerators.nvidia_gpu.sys",
            types.SimpleNamespace(platform="linux"),
        ),
        patch("builtins.open", mock_open(read_data="1")),
    ):
        inventory = NvidiaGPUAcceleratorManager.get_current_node_accelerator_inventory()
    assert inventory.status is AcceleratorInventoryStatus.OK
    assert inventory.devices[0].numa_node == 1
    # The GPU-own NUMA API must not have been called.
    assert fake_nvml.numa_calls == 0


def test_pci_read_failure_degrades_to_none(fake_nvml):
    device = _device("Tesla V100", "GPU-abc")
    device["pci_error"] = pynvml.NVMLError_NotSupported()
    _set_devices(fake_nvml, [device])
    inventory = NvidiaGPUAcceleratorManager.get_current_node_accelerator_inventory()
    assert inventory.status is AcceleratorInventoryStatus.OK
    assert inventory.devices[0].pci_bus_id is None
    assert inventory.devices[0].numa_node is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
