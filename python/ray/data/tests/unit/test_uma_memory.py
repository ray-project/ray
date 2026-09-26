import os
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from ray.data._internal import uma_memory
from ray.data._internal.util import MemoryProfiler


@pytest.fixture
def device(monkeypatch):
    import ray._private.thirdparty.pynvml as nvml

    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "0")
    uma_memory._get_gpu_memory_reader.cache_clear()
    cuda = MagicMock()
    cuda.cuInit.return_value = 0

    def put(value):
        def call(pointer, *args):
            pointer._obj.value = value
            return 0

        return call

    cuda.cuDeviceGetCount.side_effect = put(1)
    cuda.cuDeviceGet.side_effect = put(3)
    cuda.cuDeviceGetAttribute.side_effect = put(1)

    def get_uuid(pointer, device):
        pointer._obj[:] = bytes(range(16))
        return 0

    cuda.cuDeviceGetUuid.side_effect = get_uuid
    load = MagicMock(return_value=cuda)
    monkeypatch.setattr(uma_memory.ctypes, "CDLL", load)
    init, shutdown = MagicMock(), MagicMock()
    lookup = MagicMock(return_value="matched-device")
    processes = MagicMock(
        return_value=[SimpleNamespace(pid=os.getpid(), usedGpuMemory=256)]
    )
    monkeypatch.setattr(nvml, "nvmlInit", init)
    monkeypatch.setattr(nvml, "nvmlShutdown", shutdown)
    monkeypatch.setattr(nvml, "nvmlDeviceGetHandleByUUID", lookup)
    monkeypatch.setattr(nvml, "nvmlDeviceGetComputeRunningProcesses", processes)
    monkeypatch.setattr(uma_memory.atexit, "register", MagicMock())
    yield SimpleNamespace(
        cuda=cuda,
        load=load,
        init=init,
        shutdown=shutdown,
        lookup=lookup,
        processes=processes,
        nvml=nvml,
        put=put,
    )
    uma_memory._get_gpu_memory_reader.cache_clear()


def test_integrated_gpu_identity_and_cache(device):
    reader = uma_memory.get_gpu_memory_reader(os.getpid())
    assert reader() == 256
    assert uma_memory.get_gpu_memory_reader(os.getpid()) is reader
    device.load.assert_called_once_with("libcuda.so.1")
    device.init.assert_called_once()
    _, attribute, cuda_device = device.cuda.cuDeviceGetAttribute.call_args.args
    assert attribute == 18  # CU_DEVICE_ATTRIBUTE_INTEGRATED
    assert cuda_device.value == 3
    device.lookup.assert_called_once_with("GPU-00010203-0405-0607-0809-0a0b0c0d0e0f")
    device.processes.assert_called_once_with("matched-device")
    # A different process cannot reuse an inherited reader.
    uma_memory.get_gpu_memory_reader(os.getpid() + 1)
    assert device.load.call_count == 2


@pytest.mark.parametrize("count,integrated", [(1, 0), (0, 0), (2, 1)])
def test_unsupported_devices_keep_legacy_path(device, count, integrated):
    device.cuda.cuDeviceGetCount.side_effect = device.put(count)
    device.cuda.cuDeviceGetAttribute.side_effect = device.put(integrated)
    assert uma_memory.get_gpu_memory_reader(os.getpid()) is None
    assert uma_memory.get_gpu_memory_reader(os.getpid()) is None
    device.load.assert_called_once()
    device.init.assert_not_called()


def test_no_visible_gpu_does_not_load_driver(device, monkeypatch):
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "")
    assert uma_memory.get_gpu_memory_reader(os.getpid()) is None
    device.load.assert_not_called()


def test_missing_driver(device):
    device.load.side_effect = OSError("no CUDA driver")
    assert uma_memory.get_gpu_memory_reader(os.getpid()) is None
    device.init.assert_not_called()


def test_cuda_query_error(device):
    device.cuda.cuInit.return_value = 100
    assert uma_memory.get_gpu_memory_reader(os.getpid()) is None
    device.init.assert_not_called()


def test_failed_uuid_match_releases_nvml(device):
    device.lookup.side_effect = device.nvml.NVMLError_NotFound()
    assert uma_memory.get_gpu_memory_reader(os.getpid()) is None
    device.shutdown.assert_called_once()


def test_nvml_init_failure_is_cached(device):
    device.init.side_effect = device.nvml.NVMLError_DriverNotLoaded()
    assert uma_memory.get_gpu_memory_reader(os.getpid()) is None
    assert uma_memory.get_gpu_memory_reader(os.getpid()) is None
    device.init.assert_called_once()
    device.shutdown.assert_not_called()


@pytest.mark.parametrize("value", [None, 2**64 - 1, -1])
def test_unknown_memory_is_not_zero(device, value, monkeypatch):
    warning = MagicMock()
    monkeypatch.setattr(uma_memory.logger, "warning", warning)
    device.processes.return_value[0].usedGpuMemory = value
    reader = uma_memory.get_gpu_memory_reader(os.getpid())
    assert reader() is None
    assert reader() is None
    warning.assert_called_once()


def test_only_current_process_is_counted(device):
    device.processes.return_value.insert(
        0, SimpleNamespace(pid=os.getpid() + 1, usedGpuMemory=4096)
    )
    reader = uma_memory.get_gpu_memory_reader(os.getpid())
    assert reader() == 256
    device.processes.return_value = []
    assert reader() == 0
    device.processes.side_effect = device.nvml.NVMLError_NotSupported()
    assert reader() is None


@pytest.fixture
def profiler(monkeypatch):
    reader = MagicMock(return_value=32)
    monkeypatch.setattr(uma_memory, "get_gpu_memory_reader", lambda pid: reader)
    monkeypatch.setattr(MemoryProfiler, "_can_estimate_uss", lambda self: True)
    profiler = MemoryProfiler(poll_interval_s=1)
    profiler._process = MagicMock()
    profiler._process.memory_full_info.return_value = SimpleNamespace(uss=64)
    return profiler, reader


def test_combined_sample_at_task_output(profiler, monkeypatch):
    p, reader = profiler
    monkeypatch.setattr(p, "_start_uss_poll_thread", lambda: (MagicMock(), MagicMock()))
    with p:
        assert p.estimate_max_uss() == 96
        reader.assert_called_once()
        p._process.memory_full_info.assert_called_once()
        p._process.memory_info.assert_not_called()
        reader.return_value = 128
        assert p.estimate_max_uss() == 192
        p.reset()
        reader.return_value = 0
        assert p.estimate_max_uss() == 64


def test_peak_of_sum_not_sum_of_peaks(profiler):
    p, reader = profiler
    p._process.memory_full_info.side_effect = [
        SimpleNamespace(uss=1000),
        SimpleNamespace(uss=0),
    ]
    reader.side_effect = [0, 1000]
    p._sample_memory()
    p._sample_memory()
    assert p._max_uss == 1000


def test_failed_sample_invalidates_task(profiler):
    p, reader = profiler
    p._sample_memory()
    reader.return_value = None
    assert p.estimate_max_uss() is None
    reader.return_value = 32
    assert p.estimate_max_uss() is None
    p.reset()
    assert p.estimate_max_uss() == 96


def test_full_uss_failure_does_not_escape(profiler):
    import psutil

    p, _ = profiler
    p._process.memory_full_info.side_effect = psutil.AccessDenied()
    assert p.estimate_max_uss() is None


def test_legacy_sampler_unchanged(monkeypatch):
    monkeypatch.setattr(uma_memory, "get_gpu_memory_reader", lambda pid: None)
    monkeypatch.setattr(MemoryProfiler, "_can_estimate_uss", lambda self: True)
    p = MemoryProfiler(poll_interval_s=None)
    p._process = MagicMock()
    p._process.memory_info.return_value = SimpleNamespace(rss=100, shared=40)
    assert p.estimate_max_uss() == 60
    p._process.memory_full_info.assert_not_called()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
