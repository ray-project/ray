import ctypes
import ctypes.util
import logging
import os
from typing import List, Optional, Tuple

from ray._private.accelerators.accelerator import AcceleratorManager
from ray._private.ray_constants import env_bool

logger = logging.getLogger(__name__)

ZE_AFFINITY_MASK_ENV_VAR = "ZE_AFFINITY_MASK"
NOSET_ZE_AFFINITY_MASK_ENV_VAR = "RAY_EXPERIMENTAL_NOSET_ZE_AFFINITY_MASK"

ONEAPI_DEVICE_SELECTOR_ENV_VAR = "ONEAPI_DEVICE_SELECTOR"
ONEAPI_DEVICE_BACKEND_TYPE = "level_zero"

# Constants from the Level Zero API (ze_api.h).
_ZE_STRUCTURE_TYPE_DEVICE_PROPERTIES = 0x3
_ZE_DEVICE_TYPE_GPU = 1


class _ZeDeviceUuid(ctypes.Structure):
    _fields_ = [("id", ctypes.c_uint8 * 16)]


class _ZeDeviceProperties(ctypes.Structure):
    """ctypes mirror of ``ze_device_properties_t``."""

    _fields_ = [
        ("stype", ctypes.c_int32),
        ("pNext", ctypes.c_void_p),
        ("type", ctypes.c_int32),
        ("vendorId", ctypes.c_uint32),
        ("deviceId", ctypes.c_uint32),
        ("flags", ctypes.c_uint32),
        ("subdeviceId", ctypes.c_uint32),
        ("coreClockRate", ctypes.c_uint32),
        ("maxMemAllocSize", ctypes.c_uint64),
        ("maxHardwareContexts", ctypes.c_uint32),
        ("maxCommandQueuePriority", ctypes.c_uint32),
        ("numThreadsPerEU", ctypes.c_uint32),
        ("physicalEUSimdWidth", ctypes.c_uint32),
        ("numEUsPerSubslice", ctypes.c_uint32),
        ("numSubslicesPerSlice", ctypes.c_uint32),
        ("numSlices", ctypes.c_uint32),
        ("timerResolution", ctypes.c_uint64),
        ("timestampValidBits", ctypes.c_uint32),
        ("kernelTimestampValidBits", ctypes.c_uint32),
        ("uuid", _ZeDeviceUuid),
        ("name", ctypes.c_char * 256),
    ]


def _load_level_zero_loader() -> Optional[ctypes.CDLL]:
    for name in ("libze_loader.so.1", ctypes.util.find_library("ze_loader")):
        if not name:
            continue
        try:
            return ctypes.CDLL(name)
        except OSError:
            continue
    return None


def _get_level_zero_gpu_names() -> List[str]:
    """Return the names of the Level Zero GPU devices visible to this process.

    Queries the Level Zero loader directly instead of going through dpctl, so
    Ray needs no Python dependency to detect Intel GPUs. Only devices are
    enumerated: no context or queue is created, so no device memory is held.
    ``ZE_AFFINITY_MASK`` is honored by the driver. Returns an empty list when
    the loader or a driver is unavailable.
    """
    lib = _load_level_zero_loader()
    if lib is None or lib.zeInit(0) != 0:
        return []

    num_drivers = ctypes.c_uint32(0)
    if lib.zeDriverGet(ctypes.byref(num_drivers), None) != 0 or not num_drivers.value:
        return []
    drivers = (ctypes.c_void_p * num_drivers.value)()
    if lib.zeDriverGet(ctypes.byref(num_drivers), drivers) != 0:
        return []

    names = []
    for driver in drivers[: num_drivers.value]:
        num_devices = ctypes.c_uint32(0)
        if (
            lib.zeDeviceGet(ctypes.c_void_p(driver), ctypes.byref(num_devices), None)
            != 0
            or not num_devices.value
        ):
            continue
        devices = (ctypes.c_void_p * num_devices.value)()
        if (
            lib.zeDeviceGet(ctypes.c_void_p(driver), ctypes.byref(num_devices), devices)
            != 0
        ):
            continue
        for device in devices[: num_devices.value]:
            props = _ZeDeviceProperties()
            props.stype = _ZE_STRUCTURE_TYPE_DEVICE_PROPERTIES
            if (
                lib.zeDeviceGetProperties(ctypes.c_void_p(device), ctypes.byref(props))
                == 0
                and props.type == _ZE_DEVICE_TYPE_GPU
            ):
                names.append(props.name.decode(errors="replace"))
    return names


class IntelGPUAcceleratorManager(AcceleratorManager):
    """Intel GPU accelerators."""

    @staticmethod
    def get_resource_name() -> str:
        return "GPU"

    @staticmethod
    def get_visible_accelerator_ids_env_var() -> str:
        return ZE_AFFINITY_MASK_ENV_VAR

    @staticmethod
    def get_current_process_visible_accelerator_ids() -> Optional[List[str]]:
        # Primary: ZE_AFFINITY_MASK uses bare IDs ("0,1,2"), like CUDA_VISIBLE_DEVICES.
        ze_mask = os.environ.get(ZE_AFFINITY_MASK_ENV_VAR, None)
        if ze_mask is not None:
            if ze_mask == "":
                return []
            return list(ze_mask.split(","))

        # Fallback: ONEAPI_DEVICE_SELECTOR for backward compatibility.
        oneapi_visible_devices = os.environ.get(ONEAPI_DEVICE_SELECTOR_ENV_VAR, None)
        if oneapi_visible_devices is None:
            return None
        if oneapi_visible_devices == "" or oneapi_visible_devices == "NoDevFiles":
            return []

        prefix = ONEAPI_DEVICE_BACKEND_TYPE + ":"
        if prefix in oneapi_visible_devices:
            return list(oneapi_visible_devices.split(prefix)[1].split(","))
        # bare IDs without prefix (e.g. "0,1") — accepted as-is
        return list(oneapi_visible_devices.split(","))

    @staticmethod
    def get_current_node_num_accelerators() -> int:
        try:
            return len(_get_level_zero_gpu_names())
        except Exception:
            return 0

    @staticmethod
    def get_current_node_accelerator_type() -> Optional[str]:
        """Get the name of first Intel GPU. (supposed only one GPU type on a node)
        Example:
            name: 'Intel(R) Data Center GPU Max 1550'
            return name: 'Intel-GPU-Max-1550'
        Returns:
            A string representing the name of Intel GPU type.
        """
        try:
            names = _get_level_zero_gpu_names()
        except Exception:
            return None
        if not names:
            return None
        return "Intel-GPU-" + "-".join(names[0].split(" ")[-2:])

    @staticmethod
    def validate_resource_request_quantity(
        quantity: float,
    ) -> Tuple[bool, Optional[str]]:
        return (True, None)

    @staticmethod
    def set_current_process_visible_accelerator_ids(
        visible_xpu_devices: List[str],
    ) -> None:
        if env_bool(NOSET_ZE_AFFINITY_MASK_ENV_VAR, False):
            return

        # ZE_AFFINITY_MASK masks devices at the Level Zero driver, below oneAPI/SYCL
        # and torch-xpu, so it fully restricts visibility on its own. It uses bare
        # IDs ("0,1,2") like CUDA_VISIBLE_DEVICES. Ray only sets this one env var:
        # writing ONEAPI_DEVICE_SELECTOR too would collide with frameworks (e.g.
        # SGLang) that manage it themselves, and would need a separate save/restore.
        os.environ[ZE_AFFINITY_MASK_ENV_VAR] = ",".join(
            [str(i) for i in visible_xpu_devices]
        )
