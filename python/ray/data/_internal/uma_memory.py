"""Process memory accounting for unified memory architectures (UMA)."""

import atexit
import ctypes
import functools
import logging
import os
import threading
import uuid
from typing import Callable, Optional

logger = logging.getLogger(__name__)
_reader_lock = threading.Lock()
_CU_DEVICE_ATTRIBUTE_INTEGRATED = 18


def get_gpu_memory_reader(pid: int) -> Optional[Callable[[], Optional[int]]]:
    with _reader_lock:
        return _get_gpu_memory_reader(pid)


@functools.lru_cache(maxsize=1)
def _get_gpu_memory_reader(pid: int) -> Optional[Callable[[], Optional[int]]]:
    """Detect and match one visible integrated GPU, once per worker process.

    The PID cache key prevents inheriting a parent's NVML reader after fork.
    Driver device queries do not require creating a CUDA context.
    """
    if os.environ.get("CUDA_VISIBLE_DEVICES") == "":
        return None

    try:
        cuda = ctypes.CDLL("libcuda.so.1")

        def check(status):
            if status != 0:
                raise RuntimeError(f"CUDA device query failed: {status}")

        check(cuda.cuInit(0))
        count = ctypes.c_int()
        check(cuda.cuDeviceGetCount(ctypes.byref(count)))
        if count.value != 1:
            return None
        device, integrated = ctypes.c_int(), ctypes.c_int()
        check(cuda.cuDeviceGet(ctypes.byref(device), 0))
        check(
            cuda.cuDeviceGetAttribute(
                ctypes.byref(integrated), _CU_DEVICE_ATTRIBUTE_INTEGRATED, device
            )
        )
        if not integrated.value:
            return None
        device_uuid = (ctypes.c_ubyte * 16)()
        check(cuda.cuDeviceGetUuid(ctypes.byref(device_uuid), device))
    except (OSError, AttributeError, RuntimeError):
        return None

    import ray._private.thirdparty.pynvml as nvml

    try:
        nvml.nvmlInit()
    except nvml.NVMLError:
        logger.warning("UMA memory statistics unavailable", exc_info=True)
        return None
    try:
        handle = nvml.nvmlDeviceGetHandleByUUID(
            f"GPU-{uuid.UUID(bytes=bytes(device_uuid))}"
        )
    except nvml.NVMLError:
        nvml.nvmlShutdown()
        logger.warning(
            "Could not match the CUDA device to NVML for UMA accounting", exc_info=True
        )
        return None
    atexit.register(nvml.nvmlShutdown)
    warned = False

    def read():
        nonlocal warned
        try:
            for process in nvml.nvmlDeviceGetComputeRunningProcesses(handle):
                if process.pid == pid:
                    value = process.usedGpuMemory
                    if value is None or value < 0 or value >= 2**63:
                        raise ValueError("NVML process memory is unavailable")
                    return value
            # No CUDA context (including after cudaDeviceReset).
            return 0
        except (nvml.NVMLError, ValueError):
            if not warned:
                logger.warning("UMA memory sample unavailable", exc_info=True)
                warned = True
            return None

    return read
