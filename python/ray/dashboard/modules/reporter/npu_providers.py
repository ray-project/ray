"""Ascend NPU provider for node-level monitoring in the Ray dashboard."""

import ctypes
import logging
import os
from typing import Dict, List, Optional, TypedDict

logger = logging.getLogger(__name__)

MB = 1024 * 1024
DCMI_LIBRARY_PATH_ENV = "RAY_NPU_DCMI_LIBRARY_PATH"
DCMI_UTILIZATION_RATE_AICORE = 2
# DCMI uses UINT32_MAX when health data is unavailable. Ray normalizes that
# sentinel to the next value after DCMI's defined 0-3 severity range.
DCMI_HEALTH_MISSING = 0xFFFFFFFF
NPU_HEALTH_MISSING = 4
_MAX_CARDS = 64
_MAX_PROCESSES = 128


class ProcessNpuInfo(TypedDict):
    """NPU memory information for one process."""

    pid: int
    npu_memory_usage: int  # bytes


class NpuUtilizationInfo(TypedDict):
    """Utilization information for one Ascend NPU device."""

    # Logical index used by ASCEND_RT_VISIBLE_DEVICES.
    index: int
    card_id: int
    device_id: int
    physical_id: Optional[int]
    name: str
    utilization_aicore: Optional[int]
    memory_used: Optional[int]
    memory_total: Optional[int]
    health_status: Optional[int]  # 0-3 DCMI severity, 4 means missing
    power_mw: Optional[int]
    temperature_c: Optional[int]
    processes_pids: Optional[Dict[int, ProcessNpuInfo]]


class DcmiError(RuntimeError):
    """Error returned by an Ascend DCMI function."""

    def __init__(self, function: str, return_code: Optional[int] = None):
        self.function = function
        self.return_code = return_code
        if return_code is None:
            message = f"DCMI function {function} is not available"
        else:
            message = f"DCMI function {function} failed with code {return_code}"
        super().__init__(message)


class _DcmiChipInfo(ctypes.Structure):
    _fields_ = [
        ("chip_type", ctypes.c_ubyte * 32),
        ("chip_name", ctypes.c_ubyte * 32),
        ("chip_ver", ctypes.c_ubyte * 32),
        ("aicore_count", ctypes.c_uint),
    ]


class _DcmiChipInfoV2(ctypes.Structure):
    _fields_ = [
        ("chip_type", ctypes.c_ubyte * 32),
        ("chip_name", ctypes.c_ubyte * 32),
        ("chip_ver", ctypes.c_ubyte * 32),
        ("aicore_count", ctypes.c_uint),
        ("npu_name", ctypes.c_ubyte * 32),
    ]


class _DcmiHbmInfo(ctypes.Structure):
    _fields_ = [
        ("memory_size", ctypes.c_ulonglong),
        ("frequency", ctypes.c_uint),
        ("memory_usage", ctypes.c_ulonglong),
        ("temperature", ctypes.c_int),
        ("bandwidth_utilization", ctypes.c_uint),
    ]


class _DcmiProcMemInfo(ctypes.Structure):
    _fields_ = [
        ("proc_id", ctypes.c_int),
        ("proc_mem_usage", ctypes.c_ulong),
    ]


class _DcmiLibrary:
    """Small ctypes wrapper around the read-only DCMI monitoring APIs."""

    _DEFAULT_LIBRARY_PATHS = (
        "/usr/local/Ascend/driver/lib64/driver/libdcmi.so",
        "/usr/local/Ascend/driver/lib64/common/libdcmi.so",
        "libdcmi.so",
    )

    def __init__(self, handle):
        self._handle = handle
        self._configure_signatures()

    @classmethod
    def load(cls):
        configured_path = os.environ.get(DCMI_LIBRARY_PATH_ENV)
        candidates = (
            (configured_path,) if configured_path else cls._DEFAULT_LIBRARY_PATHS
        )
        load_mode = getattr(ctypes, "RTLD_LOCAL", None)
        last_error = None
        for path in candidates:
            try:
                handle = (
                    ctypes.CDLL(path, mode=load_mode)
                    if load_mode is not None
                    else ctypes.CDLL(path)
                )
                return cls(handle)
            except OSError as error:
                last_error = error
        raise OSError("Unable to load the Ascend DCMI library") from last_error

    def _configure_signatures(self):
        signatures = {
            "dcmi_init": [],
            "dcmi_get_card_list": [
                ctypes.POINTER(ctypes.c_int),
                ctypes.POINTER(ctypes.c_int),
                ctypes.c_int,
            ],
            "dcmi_get_device_num_in_card": [
                ctypes.c_int,
                ctypes.POINTER(ctypes.c_int),
            ],
            "dcmi_get_device_logic_id": [
                ctypes.POINTER(ctypes.c_int),
                ctypes.c_int,
                ctypes.c_int,
            ],
            "dcmi_get_device_phyid_from_logicid": [
                ctypes.c_uint,
                ctypes.POINTER(ctypes.c_uint),
            ],
            "dcmi_get_device_chip_info_v2": [
                ctypes.c_int,
                ctypes.c_int,
                ctypes.POINTER(_DcmiChipInfoV2),
            ],
            "dcmi_get_device_chip_info": [
                ctypes.c_int,
                ctypes.c_int,
                ctypes.POINTER(_DcmiChipInfo),
            ],
            "dcmi_get_device_utilization_rate": [
                ctypes.c_int,
                ctypes.c_int,
                ctypes.c_int,
                ctypes.POINTER(ctypes.c_uint),
            ],
            "dcmi_get_device_hbm_info": [
                ctypes.c_int,
                ctypes.c_int,
                ctypes.POINTER(_DcmiHbmInfo),
            ],
            "dcmi_get_device_temperature": [
                ctypes.c_int,
                ctypes.c_int,
                ctypes.POINTER(ctypes.c_int),
            ],
            "dcmi_get_device_power_info": [
                ctypes.c_int,
                ctypes.c_int,
                ctypes.POINTER(ctypes.c_int),
            ],
            "dcmi_get_device_health": [
                ctypes.c_int,
                ctypes.c_int,
                ctypes.POINTER(ctypes.c_uint),
            ],
            "dcmi_get_device_resource_info": [
                ctypes.c_int,
                ctypes.c_int,
                ctypes.POINTER(_DcmiProcMemInfo),
                ctypes.POINTER(ctypes.c_int),
            ],
        }
        for name, argument_types in signatures.items():
            function = getattr(self._handle, name, None)
            if function is not None:
                function.argtypes = argument_types
                function.restype = ctypes.c_int

    def _call(self, function_name: str, *args):
        function = getattr(self._handle, function_name, None)
        if function is None:
            raise DcmiError(function_name)
        return_code = function(*args)
        if return_code != 0:
            raise DcmiError(function_name, return_code)

    def initialize(self):
        self._call("dcmi_init")

    def get_card_list(self) -> List[int]:
        card_count = ctypes.c_int()
        card_list = (ctypes.c_int * _MAX_CARDS)()
        self._call(
            "dcmi_get_card_list",
            ctypes.byref(card_count),
            card_list,
            len(card_list),
        )
        if not 0 <= card_count.value <= len(card_list):
            raise ValueError(f"Invalid DCMI card count: {card_count.value}")
        return list(card_list[: card_count.value])

    def get_device_count(self, card_id: int) -> int:
        device_count = ctypes.c_int()
        self._call("dcmi_get_device_num_in_card", card_id, ctypes.byref(device_count))
        if device_count.value < 0:
            raise ValueError(f"Invalid DCMI device count: {device_count.value}")
        return device_count.value

    def get_logic_id(self, card_id: int, device_id: int) -> int:
        logic_id = ctypes.c_int()
        self._call(
            "dcmi_get_device_logic_id",
            ctypes.byref(logic_id),
            card_id,
            device_id,
        )
        return logic_id.value

    def get_physical_id(self, logic_id: int) -> int:
        physical_id = ctypes.c_uint()
        self._call(
            "dcmi_get_device_phyid_from_logicid",
            logic_id,
            ctypes.byref(physical_id),
        )
        return physical_id.value

    def get_device_name(self, card_id: int, device_id: int) -> str:
        if getattr(self._handle, "dcmi_get_device_chip_info_v2", None) is not None:
            chip_info_v2 = _DcmiChipInfoV2()
            try:
                self._call(
                    "dcmi_get_device_chip_info_v2",
                    card_id,
                    device_id,
                    ctypes.byref(chip_info_v2),
                )
                chip_name = bytes(chip_info_v2.chip_name).split(b"\0", 1)[0]
                npu_name = bytes(chip_info_v2.npu_name).split(b"\0", 1)[0]
                return (chip_name or npu_name).decode("utf-8", errors="replace")
            except DcmiError:
                # Older DCMI versions may export the v2 symbol but not support it
                # for the installed device. Fall through to the v1 API.
                pass

        chip_info = _DcmiChipInfo()
        self._call(
            "dcmi_get_device_chip_info",
            card_id,
            device_id,
            ctypes.byref(chip_info),
        )
        chip_name = bytes(chip_info.chip_name).split(b"\0", 1)[0]
        chip_type = bytes(chip_info.chip_type).split(b"\0", 1)[0]
        return (chip_name or chip_type).decode("utf-8", errors="replace")

    def get_aicore_utilization(self, card_id: int, device_id: int) -> int:
        utilization = ctypes.c_uint()
        self._call(
            "dcmi_get_device_utilization_rate",
            card_id,
            device_id,
            DCMI_UTILIZATION_RATE_AICORE,
            ctypes.byref(utilization),
        )
        return utilization.value

    def get_hbm_info(self, card_id: int, device_id: int) -> _DcmiHbmInfo:
        hbm_info = _DcmiHbmInfo()
        self._call(
            "dcmi_get_device_hbm_info",
            card_id,
            device_id,
            ctypes.byref(hbm_info),
        )
        return hbm_info

    def get_temperature(self, card_id: int, device_id: int) -> int:
        temperature = ctypes.c_int()
        self._call(
            "dcmi_get_device_temperature",
            card_id,
            device_id,
            ctypes.byref(temperature),
        )
        return temperature.value

    def get_power(self, card_id: int, device_id: int) -> int:
        power = ctypes.c_int()
        self._call(
            "dcmi_get_device_power_info",
            card_id,
            device_id,
            ctypes.byref(power),
        )
        return power.value

    def get_health(self, card_id: int, device_id: int) -> int:
        health = ctypes.c_uint()
        self._call(
            "dcmi_get_device_health",
            card_id,
            device_id,
            ctypes.byref(health),
        )
        if health.value == DCMI_HEALTH_MISSING:
            return NPU_HEALTH_MISSING
        return health.value

    def get_processes(self, card_id: int, device_id: int) -> Dict[int, ProcessNpuInfo]:
        process_count = ctypes.c_int(_MAX_PROCESSES)
        process_infos = (_DcmiProcMemInfo * _MAX_PROCESSES)()
        self._call(
            "dcmi_get_device_resource_info",
            card_id,
            device_id,
            process_infos,
            ctypes.byref(process_count),
        )
        if not 0 <= process_count.value <= len(process_infos):
            raise ValueError(f"Invalid DCMI process count: {process_count.value}")

        return {
            int(process_info.proc_id): ProcessNpuInfo(
                pid=int(process_info.proc_id),
                npu_memory_usage=int(process_info.proc_mem_usage),
            )
            for process_info in process_infos[: process_count.value]
        }


class NpuMetricProvider:
    """Collect node-level Ascend NPU metrics through DCMI."""

    def __init__(self, dcmi=None):
        self._dcmi = dcmi
        self._initialized = False
        self._initialization_attempted = False

    def initialize(self) -> bool:
        if self._initialization_attempted:
            return self._initialized
        self._initialization_attempted = True
        try:
            if self._dcmi is None:
                self._dcmi = _DcmiLibrary.load()
            self._dcmi.initialize()
            self._initialized = True
            logger.info("Using Ascend DCMI for NPU metrics")
            return True
        except Exception as error:
            logger.debug("Ascend DCMI is not available: %s", error)
            self._dcmi = None
            return False

    def _query_optional(self, function_name: str, *args):
        try:
            function = getattr(self._dcmi, function_name)
            return function(*args)
        except Exception as error:
            logger.debug(
                "Failed to query NPU with %s%r: %s",
                function_name,
                args,
                error,
            )
            return None

    def get_npu_usage(self) -> List[NpuUtilizationInfo]:
        if not self._initialized and not self.initialize():
            return []

        try:
            card_ids = self._dcmi.get_card_list()
        except Exception as error:
            logger.debug("Failed to enumerate Ascend NPU cards: %s", error)
            return []

        npu_utilizations = []
        # `get_logic_id` returns non-negative logical IDs, so synthetic
        # fallback indices are negative to stay out of that value range.
        fallback_index = 0
        for card_id in card_ids:
            try:
                device_count = self._dcmi.get_device_count(card_id)
            except Exception as error:
                logger.debug("Failed to enumerate NPU card %s: %s", card_id, error)
                continue

            for device_id in range(device_count):
                logic_id = self._query_optional("get_logic_id", card_id, device_id)
                if logic_id is not None:
                    index = logic_id
                else:
                    fallback_index += 1
                    index = -fallback_index
                physical_id = (
                    self._query_optional("get_physical_id", logic_id)
                    if logic_id is not None
                    else None
                )
                name = self._query_optional("get_device_name", card_id, device_id)
                hbm_info = self._query_optional("get_hbm_info", card_id, device_id)
                # DCMI reports board power for server cards. npu-smi likewise shows
                # it only on the first device row, so emit it once per card to avoid
                # double counting when a card contains multiple devices.
                power = (
                    self._query_optional("get_power", card_id, device_id)
                    if device_id == 0
                    else None
                )

                info = NpuUtilizationInfo(
                    index=index,
                    card_id=card_id,
                    device_id=device_id,
                    physical_id=physical_id,
                    name=name or "Ascend NPU",
                    utilization_aicore=self._query_optional(
                        "get_aicore_utilization", card_id, device_id
                    ),
                    memory_used=(
                        int(hbm_info.memory_usage) * MB
                        if hbm_info is not None
                        else None
                    ),
                    memory_total=(
                        int(hbm_info.memory_size) * MB if hbm_info is not None else None
                    ),
                    health_status=self._query_optional(
                        "get_health", card_id, device_id
                    ),
                    power_mw=power * 100 if power is not None else None,
                    temperature_c=self._query_optional(
                        "get_temperature", card_id, device_id
                    ),
                    processes_pids=self._query_optional(
                        "get_processes", card_id, device_id
                    ),
                )
                npu_utilizations.append(info)

        return npu_utilizations
