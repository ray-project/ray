"""GPU providers for monitoring GPU usage in Ray dashboard.

This module provides an object-oriented interface for different GPU providers
(NVIDIA, AMD) to collect GPU utilization information.
"""

import abc
import dataclasses
import enum
import logging
import math
import os
import re
import subprocess
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple, TypedDict, Union

try:
    from typing import NotRequired
except ImportError:
    from typing_extensions import NotRequired

from ray._common.utils import env_bool
from ray.dashboard.modules.reporter.gpu_accounting import WindowAccountant
from ray.util.debug import log_once

logger = logging.getLogger(__name__)

# Constants
MB = 1024 * 1024
RAY_SKIP_PROCESS_UTIL_API = "RAY_SKIP_PROCESS_UTIL_API"
RAY_SKIP_PROCESS_UTIL_API_DEVICE_NAMES = "RAY_SKIP_PROCESS_UTIL_API_DEVICE_NAMES"
# PPU-compatible devices may append a model identifier directly to the prefix
# (for example, PPU-ZW810, PPU810, or PPUZW910). Keep the leading boundary so
# unrelated names such as XPPU are not classified as unsafe.
_UNSAFE_PROCESS_UTIL_NAME_PATTERNS = (re.compile(r"(?<!\w)PPU", re.IGNORECASE),)
_PROCESS_UTIL_DETECTION_LOG_INTERVAL_S = 60.0
# A cursor further ahead than this means the wall clock stepped back.
_PROCESS_UTIL_CURSOR_FUTURE_TOLERANCE_US = 1_000_000

# Types
Percentage = int
Megabytes = int
Bytes = int


class GpuProviderType(enum.Enum):
    """Enum for GPU provider types."""

    NVIDIA = "nvidia"
    AMD = "amd"


class ProcessGPUInfo(TypedDict):
    """Information about GPU usage for a single process."""

    pid: int
    gpu_memory_usage: Megabytes
    gpu_utilization: Optional[Percentage]


class GpuUtilizationInfo(TypedDict):
    """GPU utilization information for a single GPU device."""

    index: int
    name: str
    uuid: str
    utilization_gpu: Optional[Percentage]
    memory_used: Megabytes
    memory_total: Megabytes
    processes_pids: Optional[Dict[int, ProcessGPUInfo]]
    # Optional: power in milliwatts, temperature in Celsius (e.g. from NVIDIA/AMD)
    power_mw: NotRequired[Optional[int]]
    temperature_c: NotRequired[Optional[int]]
    # Optional (GPM, Hopper and newer): SM-active share of the poll window, and seconds.
    sm_active_percent: NotRequired[Optional[float]]
    sm_active_seconds: NotRequired[Optional[float]]
    # Optional (GPM): graphics/compute engine active share (DCGM GR_ENGINE_ACTIVE).
    graphics_engine_active_percent: NotRequired[Optional[float]]


# tpu utilization for google tpu
class TpuUtilizationInfo(TypedDict):
    index: int
    name: str
    tpu_type: str
    tpu_topology: str
    tensorcore_utilization: Percentage
    hbm_utilization: Percentage
    duty_cycle: Percentage
    memory_used: Bytes
    memory_total: Bytes


# NVML errors for an API the device or driver doesn't have; not counted as failures.
_UNSUPPORTED_ERRORS = frozenset(
    {"NVMLError_NotSupported", "NVMLError_FunctionNotFound"}
)


class GpuProvider(abc.ABC):
    """Abstract base class for GPU providers."""

    def __init__(self):
        self._initialized = False
        # (api, reason) -> failed reads since the last drain
        self._collection_errors: Dict[Tuple[str, str], int] = defaultdict(int)

    def _count_collection_error(self, api: str, error: Union[Exception, str]) -> None:
        """Count a failed read by source (`api`) and error class or reason."""
        reason = error if isinstance(error, str) else type(error).__name__
        if reason in _UNSUPPORTED_ERRORS:
            # The device or driver doesn't have this API; it never will.
            return
        self._collection_errors[(api, reason)] += 1

    def drain_collection_errors(self) -> Dict[Tuple[str, str], int]:
        """Return and reset the failed reads counted since the last call."""
        errors = dict(self._collection_errors)
        self._collection_errors.clear()
        return errors

    @abc.abstractmethod
    def get_provider_name(self) -> GpuProviderType:
        """Return the type of the GPU provider."""
        pass

    @abc.abstractmethod
    def is_available(self) -> bool:
        """Check if the GPU provider is available on this system."""
        pass

    @abc.abstractmethod
    def _initialize(self) -> bool:
        """Initialize the GPU provider. Returns True if successful."""
        pass

    @abc.abstractmethod
    def _shutdown(self):
        """Shutdown the GPU provider and clean up resources."""
        pass

    @abc.abstractmethod
    def get_gpu_utilization(self) -> List[GpuUtilizationInfo]:
        """Get GPU utilization information for all available GPUs."""
        pass

    @staticmethod
    def _decode(b: Union[str, bytes]) -> str:
        """Decode bytes to string for Python 3 compatibility."""
        if isinstance(b, bytes):
            return b.decode("utf-8")
        return b


# GPM metrics read each poll, by pynvml constant name.
_GPM_METRICS = ("NVML_GPM_METRIC_SM_UTIL", "NVML_GPM_METRIC_GRAPHICS_UTIL")
# NVML wants > 100 ms between GPM samples; a closer read is skipped.
_GPM_MIN_SPACING_S = 0.2
# Skip GPM for _GPM_BACKOFF_POLLS polls after this many failures in a row.
_GPM_FAILURES_BEFORE_BACKOFF = 3
_GPM_BACKOFF_POLLS = 10


@dataclasses.dataclass
class _GpmState:
    """Per-device GPM state, kept across polls and nvmlShutdown/nvmlInit.

    The sample buffers are reused after nvmlShutdown/nvmlInit. NVML doesn't
    document this, but a sample holds raw counters and nvmlGpmMetricsGet only
    differences two of them; on driver 580, pairs spanning a re-init read the
    same as pairs that don't (within 0.3 points). Out-of-range values are dropped.
    """

    supported: Optional[bool] = None
    samples: List = dataclasses.field(default_factory=list)
    # Index of the buffer holding the previous poll's sample.
    previous: Optional[int] = None
    accountant: WindowAccountant = dataclasses.field(default_factory=WindowAccountant)
    consecutive_failures: int = 0
    backoff_polls: int = 0


class NvidiaGpuProvider(GpuProvider):
    """NVIDIA GPU provider using pynvml."""

    def __init__(self, enable_gpm: bool = True):
        super().__init__()
        self._pynvml = None
        self._enable_gpm = enable_gpm
        self._gpm_state: Dict[str, _GpmState] = {}
        # Maintain per-GPU sampling timestamps when using process utilization API
        self._gpu_process_last_sample_ts: Dict[int, int] = {}
        self._force_skip_process_util_api = env_bool(RAY_SKIP_PROCESS_UTIL_API, False)
        self._skip_process_util_device_names = {
            name.strip().casefold()
            for name in os.environ.get(
                RAY_SKIP_PROCESS_UTIL_API_DEVICE_NAMES, ""
            ).split(",")
            if name.strip()
        }
        self._skip_process_util_gpu_indices: Set[int] = set()
        self._logged_skip_process_util_devices: Set[Tuple[int, str]] = set()
        self._process_util_override_logged = False
        self._ppu_detection_done = False
        self._detected_gpu_count: Optional[int] = None
        self._force_fallback_this_cycle = False
        self._process_util_detection_failure_count = 0
        self._last_process_util_detection_failure_log_ts = 0.0

    def get_provider_name(self) -> GpuProviderType:
        return GpuProviderType.NVIDIA

    def _is_nvml_error(self, error: Exception, class_name: str) -> bool:
        """Return whether `error` is an instance of pynvml's `class_name` error."""
        error_class = getattr(self._pynvml, class_name, None)
        return isinstance(error_class, type) and isinstance(error, error_class)

    def _gpm_supported(self, gpu_handle: Any, uuid: str, state: _GpmState) -> bool:
        """Check GPM support once per device and allocate its sample buffers."""
        if state.supported is None:
            try:
                support = self._pynvml.nvmlGpmQueryDeviceSupport(gpu_handle)
                if support.isSupportedDevice:
                    state.samples = [
                        self._pynvml.nvmlGpmSampleAlloc(),
                        self._pynvml.nvmlGpmSampleAlloc(),
                    ]
                state.supported = bool(support.isSupportedDevice)
            except Exception as e:
                if not (
                    isinstance(e, AttributeError)
                    or self._is_nvml_error(e, "NVMLError_NotSupported")
                    or self._is_nvml_error(e, "NVMLError_FunctionNotFound")
                ):
                    # Transient; check again next poll.
                    self._count_collection_error("gpm", e)
                    return False
                # Pre-Hopper GPUs or drivers without GPM.
                state.supported = False
            if state.supported is False:
                logger.info(
                    "GPU %s does not support GPM; SM activity metrics are not "
                    "reported for it",
                    uuid,
                )
        return bool(state.supported)

    def _read_gpm(self, gpu_handle: Any, uuid: str) -> Dict[str, Optional[float]]:
        """Return GPM metrics for the window since the previous poll."""
        out = {
            "sm_active_percent": None,
            "sm_active_seconds": None,
            "graphics_engine_active_percent": None,
        }
        if not self._enable_gpm:
            return out
        state = self._gpm_state.setdefault(uuid, _GpmState())
        if not self._gpm_supported(gpu_handle, uuid, state):
            return out
        if state.backoff_polls > 0:
            state.backoff_polls -= 1
            return out
        window_s = state.accountant.window_seconds()
        if window_s is not None and window_s < _GPM_MIN_SPACING_S:
            return out

        current = 0 if state.previous is None else 1 - state.previous
        try:
            self._pynvml.nvmlGpmSampleGet(gpu_handle, state.samples[current])
        except Exception as e:
            self._count_collection_error("gpm", e)
            state.accountant.fail()
            state.consecutive_failures += 1
            if state.consecutive_failures >= _GPM_FAILURES_BEFORE_BACKOFF:
                state.consecutive_failures = 0
                state.backoff_polls = _GPM_BACKOFF_POLLS
                if log_once(f"gpm_backoff_{uuid}"):
                    logger.warning(
                        "Reading GPM on GPU %s keeps failing; retrying in %d polls: %s",
                        uuid,
                        _GPM_BACKOFF_POLLS,
                        e,
                    )
            return out
        state.consecutive_failures = 0

        previous, state.previous = state.previous, current
        if previous is None:
            state.accountant.credit(None)
            return out
        try:
            request = self._pynvml.c_nvmlGpmMetricsGet_t()
            request.version = self._pynvml.NVML_GPM_METRICS_GET_VERSION
            request.numMetrics = len(_GPM_METRICS)
            request.sample1 = state.samples[previous]
            request.sample2 = state.samples[current]
            for i, name in enumerate(_GPM_METRICS):
                request.metrics[i].metricId = getattr(self._pynvml, name)
            self._pynvml.nvmlGpmMetricsGet(request)
            values = {}
            for i, name in enumerate(_GPM_METRICS):
                ret = int(request.metrics[i].nvmlReturn)
                value = float(request.metrics[i].value)
                if ret != 0 or math.isnan(value) or not 0.0 <= value <= 100.0:
                    self._count_collection_error(
                        "gpm", "out_of_range" if ret == 0 else f"nvml_return_{ret}"
                    )
                    continue
                values[name] = value
        except Exception as e:
            self._count_collection_error("gpm", e)
            state.accountant.credit(None)
            return out
        out["graphics_engine_active_percent"] = values.get(
            "NVML_GPM_METRIC_GRAPHICS_UTIL"
        )
        sm = values.get("NVML_GPM_METRIC_SM_UTIL")
        if sm is None:
            # No valid SM value; restart the window.
            state.accountant.credit(None)
            return out
        out["sm_active_percent"] = sm
        out["sm_active_seconds"] = state.accountant.credit(sm)
        return out

    def is_available(self) -> bool:
        """Check if NVIDIA GPUs are available."""
        try:
            import ray._private.thirdparty.pynvml as pynvml

            pynvml.nvmlInit()
            pynvml.nvmlShutdown()
            return True
        except Exception as e:
            logger.debug(f"NVIDIA GPU not available: {e}")
            return False

    def _initialize(self) -> bool:
        """Initialize the NVIDIA GPU provider."""
        if self._initialized:
            return True

        try:
            import ray._private.thirdparty.pynvml as pynvml

            self._pynvml = pynvml
            self._pynvml.nvmlInit()
            self._initialized = True
            return True
        except Exception as e:
            self._count_collection_error("init", e)
            logger.debug(f"Failed to initialize NVIDIA GPU provider: {e}")
            return False

    def _shutdown(self):
        """Shutdown the NVIDIA GPU provider."""
        if self._initialized and self._pynvml:
            try:
                self._pynvml.nvmlShutdown()
            except Exception as e:
                logger.debug(f"Error shutting down NVIDIA GPU provider: {e}")
            finally:
                self._initialized = False

    def get_gpu_utilization(self) -> List[GpuUtilizationInfo]:
        """Get GPU utilization information for all NVIDIA GPUs and MIG devices."""

        return self._get_pynvml_gpu_usage()

    def _detect_ppu_devices(self, num_gpus: int) -> None:
        """Detect devices that cannot safely use the process utilization API."""
        if self._force_skip_process_util_api:
            if not self._process_util_override_logged:
                logger.info(
                    "Skipping nvmlDeviceGetProcessesUtilizationInfo for all NVIDIA "
                    "devices because %s=true",
                    RAY_SKIP_PROCESS_UTIL_API,
                )
                self._process_util_override_logged = True
            return

        if self._ppu_detection_done and self._detected_gpu_count == num_gpus:
            return

        detected_indices: Set[int] = set()
        detected_names: Dict[int, str] = {}
        try:
            for gpu_index in range(num_gpus):
                gpu_handle = self._pynvml.nvmlDeviceGetHandleByIndex(gpu_index)
                gpu_name = self._decode(self._pynvml.nvmlDeviceGetName(gpu_handle))
                if gpu_name.casefold() in self._skip_process_util_device_names or any(
                    pattern.search(gpu_name)
                    for pattern in _UNSAFE_PROCESS_UTIL_NAME_PATTERNS
                ):
                    detected_indices.add(gpu_index)
                    detected_names[gpu_index] = gpu_name
        except Exception as e:
            # Fail closed: a partial classification could leave an unprobed,
            # unsafe device exposed to the process utilization API.
            self._ppu_detection_done = False
            self._detected_gpu_count = None
            self._skip_process_util_gpu_indices.clear()
            self._force_fallback_this_cycle = True
            self._process_util_detection_failure_count += 1
            now = time.monotonic()
            if (
                self._process_util_detection_failure_count == 1
                or now - self._last_process_util_detection_failure_log_ts
                >= _PROCESS_UTIL_DETECTION_LOG_INTERVAL_S
            ):
                logger.warning(
                    "Failed to detect devices that support GPU process SM "
                    "utilization; skipping the process utilization API for this "
                    "collection cycle and retrying: %s",
                    e,
                )
                self._last_process_util_detection_failure_log_ts = now
            return

        self._skip_process_util_gpu_indices = detected_indices
        self._ppu_detection_done = True
        self._detected_gpu_count = num_gpus
        self._process_util_detection_failure_count = 0
        self._last_process_util_detection_failure_log_ts = 0.0

        for gpu_index, gpu_name in detected_names.items():
            device_key = (gpu_index, gpu_name)
            if device_key not in self._logged_skip_process_util_devices:
                logger.info(
                    "Skipping nvmlDeviceGetProcessesUtilizationInfo for device %r "
                    "(index %d): per-process utilization API is known to be "
                    "unsafe on this device",
                    gpu_name,
                    gpu_index,
                )
                self._logged_skip_process_util_devices.add(device_key)

    def _get_pynvml_gpu_usage(self) -> List[GpuUtilizationInfo]:
        if not self._initialized:
            if not self._initialize():
                return []

        gpu_utilizations = []
        self._force_fallback_this_cycle = False

        try:
            try:
                num_gpus = self._pynvml.nvmlDeviceGetCount()
            except Exception:
                self._ppu_detection_done = False
                self._detected_gpu_count = None
                raise

            self._detect_ppu_devices(num_gpus)

            for i in range(num_gpus):
                gpu_handle = self._pynvml.nvmlDeviceGetHandleByIndex(i)

                # Check if MIG mode is enabled on this GPU
                try:
                    mig_mode = self._pynvml.nvmlDeviceGetMigMode(gpu_handle)
                    if mig_mode[0]:  # MIG mode is enabled
                        # Get MIG device instances
                        mig_devices = self._get_mig_devices(gpu_handle, i)
                        gpu_utilizations.extend(mig_devices)
                        continue
                except (self._pynvml.NVMLError, AttributeError):
                    # MIG not supported or not enabled, continue with regular GPU
                    pass

                # Process regular GPU (non-MIG)
                gpu_info = self._get_gpu_info(gpu_handle, i)
                if gpu_info:
                    gpu_utilizations.append(gpu_info)

        except Exception as e:
            self._count_collection_error("gpu_usage", e)
            logger.warning(f"Error getting NVIDIA GPU utilization: {e}")
        finally:
            self._shutdown()

        return gpu_utilizations

    def _get_mig_devices(self, gpu_handle, gpu_index: int) -> List[GpuUtilizationInfo]:
        """Get MIG device information for a GPU with MIG enabled."""
        mig_devices = []

        try:
            # Get all MIG device instances
            mig_count = self._pynvml.nvmlDeviceGetMaxMigDeviceCount(gpu_handle)

            for mig_idx in range(mig_count):
                try:
                    # Get MIG device handle
                    mig_handle = self._pynvml.nvmlDeviceGetMigDeviceHandleByIndex(
                        gpu_handle, mig_idx
                    )

                    # Get MIG device info
                    mig_info = self._get_mig_device_info(mig_handle, gpu_index, mig_idx)
                    if mig_info:
                        mig_devices.append(mig_info)

                except self._pynvml.NVMLError:
                    # MIG device not available at this index
                    continue

        except (self._pynvml.NVMLError, AttributeError) as e:
            self._count_collection_error("mig_devices", e)
            logger.debug(f"Error getting MIG devices: {e}")

        return mig_devices

    def _get_mig_device_info(
        self, mig_handle, gpu_index: int, mig_index: int
    ) -> Optional[GpuUtilizationInfo]:
        """Get utilization info for a single MIG device."""
        try:
            memory_info = self._pynvml.nvmlDeviceGetMemoryInfo(mig_handle)

            # Get MIG device utilization (NVML doesn't support it on MIG devices)
            utilization = None
            try:
                utilization_info = self._pynvml.nvmlDeviceGetUtilizationRates(
                    mig_handle
                )
                utilization = int(utilization_info.gpu)
            except self._pynvml.NVMLError as e:
                self._count_collection_error("utilization", e)
                logger.debug(f"Failed to retrieve MIG device utilization: {e}")

            # Get running processes on MIG device
            processes_pids = {}
            try:
                nv_comp_processes = self._pynvml.nvmlDeviceGetComputeRunningProcesses(
                    mig_handle
                )
                nv_graphics_processes = (
                    self._pynvml.nvmlDeviceGetGraphicsRunningProcesses(mig_handle)
                )

                for nv_process in nv_comp_processes + nv_graphics_processes:
                    processes_pids[int(nv_process.pid)] = ProcessGPUInfo(
                        pid=int(nv_process.pid),
                        gpu_memory_usage=(
                            int(nv_process.usedGpuMemory) // MB
                            if nv_process.usedGpuMemory
                            else 0
                        ),
                        # NOTE: According to nvml, this is not currently available in MIG mode
                        gpu_utilization=None,
                    )
            except self._pynvml.NVMLError as e:
                logger.debug(f"Failed to retrieve MIG device processes: {e}")

            # Get MIG device UUID and name
            try:
                mig_uuid = self._decode(self._pynvml.nvmlDeviceGetUUID(mig_handle))
                mig_name = self._decode(self._pynvml.nvmlDeviceGetName(mig_handle))
            except self._pynvml.NVMLError:
                # Fallback for older drivers
                try:
                    parent_name = self._decode(
                        self._pynvml.nvmlDeviceGetName(
                            self._pynvml.nvmlDeviceGetHandleByIndex(gpu_index)
                        )
                    )
                    mig_name = f"{parent_name} MIG {mig_index}"
                    mig_uuid = f"MIG-GPU-{gpu_index}-{mig_index}"
                except Exception:
                    mig_name = f"NVIDIA MIG Device {gpu_index}.{mig_index}"
                    mig_uuid = f"MIG-{gpu_index}-{mig_index}"

            return GpuUtilizationInfo(
                index=gpu_index * 1000 + mig_index,  # Unique index for MIG devices
                name=mig_name,
                uuid=mig_uuid,
                utilization_gpu=utilization,
                memory_used=int(memory_info.used) // MB,
                memory_total=int(memory_info.total) // MB,
                processes_pids=processes_pids,
                power_mw=None,  # MIG devices don't expose per-slice power in NVML
                temperature_c=None,
            )

        except Exception as e:
            self._count_collection_error("device_info", e)
            logger.debug(f"Error getting MIG device info: {e}")
            return None

    def _get_gpu_info(self, gpu_handle, gpu_index: int) -> Optional[GpuUtilizationInfo]:
        """Get utilization info for a regular (non-MIG) GPU."""
        try:
            uuid = self._decode(self._pynvml.nvmlDeviceGetUUID(gpu_handle))
            memory_info = self._pynvml.nvmlDeviceGetMemoryInfo(gpu_handle)

            # Get GPU utilization; None if the device doesn't report it
            utilization = None
            try:
                utilization_info = self._pynvml.nvmlDeviceGetUtilizationRates(
                    gpu_handle
                )
                utilization = int(utilization_info.gpu)
            except self._pynvml.NVMLError as e:
                self._count_collection_error("utilization", e)
                if log_once("gpu_utilization"):
                    logger.info(
                        f"Failed to retrieve GPU utilization via `nvmlDeviceGetUtilizationRates`: {e}"
                    )

            # Get running processes
            processes_pids = {}

            # Get per-process memory usage from the running-processes APIs.
            try:
                nv_comp_processes = self._pynvml.nvmlDeviceGetComputeRunningProcesses(
                    gpu_handle
                )
                nv_graphics_processes = (
                    self._pynvml.nvmlDeviceGetGraphicsRunningProcesses(gpu_handle)
                )
                for nv_process in nv_comp_processes + nv_graphics_processes:
                    pid = int(nv_process.pid)
                    processes_pids[pid] = ProcessGPUInfo(
                        pid=pid,
                        gpu_memory_usage=int(nv_process.usedGpuMemory) // MB
                        if nv_process.usedGpuMemory
                        else 0,
                        gpu_utilization=None,
                    )
            except self._pynvml.NVMLError as e:
                self._count_collection_error("process_memory", e)
                if log_once("gpu_per_process_memory"):
                    logger.info(
                        "Failed to retrieve per-process GPU memory via `nvmlDeviceGetComputeRunningProcesses` "
                        f"and `nvmlDeviceGetGraphicsRunningProcesses` APIs: {e}"
                    )

            skip_process_util = (
                self._force_skip_process_util_api
                or self._force_fallback_this_cycle
                or gpu_index in self._skip_process_util_gpu_indices
            )
            if not skip_process_util:
                # Use a newer API (driver 550+) to get per-process SM utilization,
                # but the user may not always have access to the newest API.
                try:
                    # NVML expects lastSeenTimeStamp in microseconds.
                    now_us = int(time.time() * 1_000_000)
                    last_seen_us = self._gpu_process_last_sample_ts.get(gpu_index, 0)
                    if last_seen_us > now_us + _PROCESS_UTIL_CURSOR_FUTURE_TOLERANCE_US:
                        # The clock stepped back; restart the window.
                        last_seen_us = now_us
                        self._gpu_process_last_sample_ts[gpu_index] = now_us
                    nv_processes = self._pynvml.nvmlDeviceGetProcessesUtilizationInfo(
                        gpu_handle, last_seen_us
                    )
                    # Continue from the newest sample; ignore timestamps in the future.
                    sample_ts = [
                        int(nv_process.timeStamp)
                        for nv_process in nv_processes
                        if isinstance(getattr(nv_process, "timeStamp", None), int)
                        and 0
                        < nv_process.timeStamp
                        <= now_us + _PROCESS_UTIL_CURSOR_FUTURE_TOLERANCE_US
                    ]
                    self._gpu_process_last_sample_ts[gpu_index] = (
                        max(sample_ts) if sample_ts else now_us
                    )

                    for nv_process in nv_processes:
                        pid = int(nv_process.pid)
                        if pid not in processes_pids:
                            # This API may include a process that the compute and
                            # graphics running-process APIs did not find.
                            processes_pids[pid] = ProcessGPUInfo(
                                pid=pid,
                                gpu_memory_usage=0,
                                gpu_utilization=int(nv_process.smUtil),
                            )
                        else:
                            processes_pids[pid]["gpu_utilization"] = int(
                                nv_process.smUtil
                            )
                except self._pynvml.NVMLError as e:
                    if self._is_nvml_error(e, "NVMLError_NotFound"):
                        # No samples since the cursor: the GPU was idle.
                        self._gpu_process_last_sample_ts[gpu_index] = now_us
                    else:
                        self._count_collection_error("process_utilization", e)
                        if log_once("gpu_process_sm_utilization"):
                            logger.info(
                                "Failed to retrieve GPU process SM utilization using "
                                "`nvmlDeviceGetProcessesUtilizationInfo`, "
                                f"error: {e}"
                            )

            # Optional: power (milliwatts) and temperature (Celsius)
            power_mw = None
            temperature_c = None
            try:
                power_mw = self._pynvml.nvmlDeviceGetPowerUsage(gpu_handle)
            except (self._pynvml.NVMLError, AttributeError) as e:
                self._count_collection_error("power", e)
                if log_once("gpu_power"):
                    logger.info(f"Failed to retrieve GPU power: {e}")
            try:
                # NVML_TEMPERATURE_GPU = 0
                temperature_c = self._pynvml.nvmlDeviceGetTemperature(
                    gpu_handle, self._pynvml.NVML_TEMPERATURE_GPU
                )
            except (self._pynvml.NVMLError, AttributeError) as e:
                self._count_collection_error("temperature", e)
                if log_once("gpu_temperature"):
                    logger.info(f"Failed to retrieve GPU temperature: {e}")

            gpm = self._read_gpm(gpu_handle, uuid)

            return GpuUtilizationInfo(
                index=gpu_index,
                name=self._decode(self._pynvml.nvmlDeviceGetName(gpu_handle)),
                uuid=uuid,
                utilization_gpu=utilization,
                memory_used=int(memory_info.used) // MB,
                memory_total=int(memory_info.total) // MB,
                processes_pids=processes_pids,
                power_mw=power_mw,
                temperature_c=temperature_c,
                sm_active_percent=gpm["sm_active_percent"],
                sm_active_seconds=gpm["sm_active_seconds"],
                graphics_engine_active_percent=gpm["graphics_engine_active_percent"],
            )

        except Exception as e:
            self._count_collection_error("device_info", e)
            logger.debug(f"Error getting GPU info: {e}")
            return None


class AmdGpuProvider(GpuProvider):
    """AMD GPU provider using pyamdsmi."""

    def __init__(self):
        super().__init__()
        self._pyamdsmi = None

    def get_provider_name(self) -> GpuProviderType:
        return GpuProviderType.AMD

    def is_available(self) -> bool:
        """Check if AMD GPUs are available."""
        try:
            import ray._private.thirdparty.pyamdsmi as pyamdsmi

            pyamdsmi.smi_initialize()
            pyamdsmi.smi_shutdown()
            return True
        except Exception as e:
            logger.debug(f"AMD GPU not available: {e}")
            return False

    def _initialize(self) -> bool:
        """Initialize the AMD GPU provider."""
        if self._initialized:
            return True

        try:
            import ray._private.thirdparty.pyamdsmi as pyamdsmi

            self._pyamdsmi = pyamdsmi
            self._pyamdsmi.smi_initialize()
            self._initialized = True
            return True
        except Exception as e:
            self._count_collection_error("init", e)
            logger.debug(f"Failed to initialize AMD GPU provider: {e}")
            return False

    def _shutdown(self):
        """Shutdown the AMD GPU provider."""
        if self._initialized and self._pyamdsmi:
            try:
                self._pyamdsmi.smi_shutdown()
            except Exception as e:
                logger.debug(f"Error shutting down AMD GPU provider: {e}")
            finally:
                self._initialized = False

    def get_gpu_utilization(self) -> List[GpuUtilizationInfo]:
        """Get GPU utilization information for all AMD GPUs."""
        if not self._initialized:
            if not self._initialize():
                return []

        gpu_utilizations = []

        try:
            num_gpus = self._pyamdsmi.smi_get_device_count()
            processes = self._pyamdsmi.smi_get_device_compute_process()

            for i in range(num_gpus):
                utilization = self._pyamdsmi.smi_get_device_utilization(i)
                if utilization == -1:
                    self._count_collection_error("utilization", "query_failed")
                    utilization = None

                # Get running processes
                processes_pids = {}
                for process in self._pyamdsmi.smi_get_compute_process_info_by_device(
                    i, processes
                ):
                    if process.vram_usage:
                        processes_pids[int(process.process_id)] = ProcessGPUInfo(
                            pid=int(process.process_id),
                            gpu_memory_usage=int(process.vram_usage) // MB,
                            gpu_utilization=None,
                        )

                # Optional: power in milliwatts (AMD returns watts)
                power_mw = None
                try:
                    power_watts = self._pyamdsmi.smi_get_device_average_power(i)
                    if power_watts >= 0:
                        power_mw = int(power_watts * 1000)
                except Exception as e:
                    self._count_collection_error("power", e)
                    logger.debug(f"Failed to retrieve AMD GPU power: {e}")

                info = GpuUtilizationInfo(
                    index=i,
                    name=self._decode(self._pyamdsmi.smi_get_device_name(i)),
                    uuid=self._pyamdsmi.smi_get_device_unique_id(i),
                    utilization_gpu=utilization,
                    memory_used=int(self._pyamdsmi.smi_get_device_memory_used(i)) // MB,
                    memory_total=int(self._pyamdsmi.smi_get_device_memory_total(i))
                    // MB,
                    processes_pids=processes_pids,
                    power_mw=power_mw,
                    temperature_c=None,  # not exposed in vendored pyamdsmi
                )
                gpu_utilizations.append(info)

        except Exception as e:
            self._count_collection_error("gpu_usage", e)
            logger.warning(f"Error getting AMD GPU utilization: {e}")
        finally:
            self._shutdown()

        return gpu_utilizations


class GpuMetricProvider:
    """Provider class for GPU metrics collection."""

    def __init__(
        self, enable_metric_report: bool = True, enable_gpm_metrics: bool = True
    ):
        self._provider: Optional[GpuProvider] = None
        self._enable_metric_report = enable_metric_report
        self._providers = (
            [NvidiaGpuProvider(enable_gpm=enable_gpm_metrics), AmdGpuProvider()]
            if enable_metric_report
            else []
        )
        self._initialized = False

    def initialize(self) -> bool:
        """Initialize the GPU metric provider by detecting available GPU providers."""
        if not self._enable_metric_report:
            self._initialized = True
            return False

        if self._initialized:
            return True

        self._provider = self._detect_gpu_provider()

        if self._provider is None:
            # Check if we should disable GPU check entirely
            try:
                # Try NVIDIA first to check for the specific error condition
                nvidia_provider = NvidiaGpuProvider()
                nvidia_provider._initialize()
            except Exception as e:
                if self._should_disable_gpu_check(e):
                    self._enable_metric_report = False
        else:
            logger.info(f"Using GPU Provider: {type(self._provider).__name__}")

        self._initialized = True
        return self._provider is not None

    def _detect_gpu_provider(self) -> Optional[GpuProvider]:
        """Detect and return the first available GPU provider."""
        for provider in self._providers:
            if provider.is_available():
                return provider
        return None

    def _should_disable_gpu_check(self, nvidia_error: Exception) -> bool:
        """
        Check if we should disable GPU usage check based on the error.

        On machines without GPUs, pynvml.nvmlInit() can run subprocesses that
        spew to stderr. Then with log_to_driver=True, we get log spew from every
        single raylet. To avoid this, disable the GPU usage check on certain errors.

        See: https://github.com/ray-project/ray/issues/14305
        """
        if type(nvidia_error).__name__ != "NVMLError_DriverNotLoaded":
            return False

        try:
            result = subprocess.check_output(
                "cat /sys/module/amdgpu/initstate |grep live",
                shell=True,
                stderr=subprocess.DEVNULL,
            )
            # If AMD GPU module is not live and NVIDIA driver not loaded,
            # disable GPU check
            return len(str(result)) == 0
        except Exception:
            return False

    def get_gpu_usage(self) -> List[GpuUtilizationInfo]:
        """Get GPU usage information from the available provider."""
        if not self._enable_metric_report:
            return []

        if not self._initialized:
            self.initialize()

        if self._provider is None:
            return []

        try:
            gpu_info_list = self._provider.get_gpu_utilization()
            return gpu_info_list  # Return TypedDict instances directly
        except Exception as e:
            logger.debug(
                f"Error getting GPU usage from {self._provider.get_provider_name().value}: {e}"
            )
            return []

    def get_provider_name(self) -> Optional[str]:
        """Get the name of the current GPU provider."""
        return self._provider.get_provider_name().value if self._provider else None

    def drain_collection_errors(self) -> Dict[Tuple[str, str], int]:
        """Return and reset the active provider's failed reads (empty if none)."""
        if self._provider is None:
            return {}
        return self._provider.drain_collection_errors()

    def is_metric_report_enabled(self) -> bool:
        """Check if GPU metric reporting is enabled."""
        return self._enable_metric_report
