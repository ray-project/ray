"""A small NVML-compatible fake for Iluvatar accelerator tests.

The real libixml.so is only available on Iluvatar hardware. Tests import this
module instead so accelerator detection and dashboard metrics can be exercised
on CPU-only CI machines.
"""

from types import SimpleNamespace

MB = 1024 * 1024


class NVMLError(Exception):
    """Base error raised by the fake NVML module."""


class NVMLLibraryMismatchError(Exception):
    """Mirrors pynvml: raised when a versioned symbol is missing.

    This is intentionally not a subclass of NVMLError, matching the real
    binding.
    """


_DEVICES = [
    SimpleNamespace(
        name="Iluvatar BI-V150",
        uuid="GPU-00000000-0000-0000-0000-000000000000",
        used=4 * 1024 * MB,
        total=16 * 1024 * MB,
        utilization=37,
    ),
]


def nvmlInit():
    return None


def nvmlShutdown():
    return None


def nvmlDeviceGetCount():
    return len(_DEVICES)


def nvmlDeviceGetHandleByIndex(index):
    return index


def nvmlDeviceGetName(handle):
    return _DEVICES[handle].name


def nvmlDeviceGetUUID(handle):
    return _DEVICES[handle].uuid


def nvmlDeviceGetMemoryInfo(handle):
    device = _DEVICES[handle]
    return SimpleNamespace(used=device.used, total=device.total)


def nvmlDeviceGetUtilizationRates(handle):
    return SimpleNamespace(gpu=_DEVICES[handle].utilization)


def nvmlDeviceGetProcessUtilization(handle, timestamp):
    return [
        SimpleNamespace(pid=1234, smUtil=42, memUtil=25),
    ]


def nvmlDeviceGetComputeRunningProcesses(handle):
    return [
        SimpleNamespace(pid=1234, usedGpuMemory=4 * 1024 * MB),
    ]


def nvmlDeviceGetGraphicsRunningProcesses(handle):
    return []
