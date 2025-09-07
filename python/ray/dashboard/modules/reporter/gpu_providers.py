"""GPU providers for monitoring GPU usage in Ray dashboard.

This module provides an object-oriented interface for different GPU providers
(NVIDIA, AMD) to collect GPU utilization information.
"""

import abc
import enum
import logging
import subprocess
from typing import Dict, List, Optional, Union, TypedDict
from collections import defaultdict

from ray._private.ray_constants import RAY_METRIC_ENABLE_GPU_NVSMI

logger = logging.getLogger(__name__)

# Constants
MB = 1024 * 1024

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


class GpuProvider(abc.ABC):
    """Abstract base class for GPU providers."""

    def __init__(self):
        self._initialized = False

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


class NvidiaGpuProvider(GpuProvider):
    """NVIDIA GPU provider using pynvml."""

    def __init__(self):
        super().__init__()
        self._pynvml = None
        self._using_nvidia_smi = RAY_METRIC_ENABLE_GPU_NVSMI

    def get_provider_name(self) -> GpuProviderType:
        return GpuProviderType.NVIDIA

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

        return (
            self._get_nvsmi_gpu_usage()
            if self._using_nvidia_smi
            else self._get_pynvml_gpu_usage()
        )

    def _get_nvsmi_gpu_usage(self) -> List[GpuUtilizationInfo]:
        try:
            gpu_info = subprocess.run(
                [
                    "nvidia-smi",
                    "--query-gpu=index,name,uuid,utilization.gpu,memory.used,memory.total",
                    "--format=csv,noheader,nounits",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            """Sample output:
            0, GPU-0, GPU-36e1567d-37ed-051e-f8ff-df807517b396, 0, 73348, 81559
            1, GPU-1, GPU-4a2c89ef-1b3d-492c-a8d5-e9c614f82d73, 0, 73444, 81559
            2, GPU-2, GPU-7f15d234-9c6a-4e8b-b3f2-c982a5d91b48, 0, 73444, 81559
            3, GPU-3, GPU-2b8d6f91-5e4c-47a3-96d7-8b31c4f9ae52, 0, 73332, 81559
            4, GPU-4, GPU-9d3a7c82-6b5f-4d1e-ae94-3f5c8d2e9b14, 0, 73344, 81559
            5, GPU-5, GPU-c4e6b853-2a9d-48f6-b1c7-d4f982e6a795, 0, 73440, 81559
            6, GPU-6, GPU-1f9b4c75-8e3a-4d2b-95c8-6a7d3b8f4e21, 0, 73440, 81559
            7, GPU-7, GPU-5d2e9f36-4c7b-483a-b9e1-2f8ac4d5b963, 0, 73328, 81559
            """
            gpus = []
            for line in sorted(gpu_info.stdout.strip().split("\n")):  # Sort by index
                index, name, uuid, util, mem_used, mem_total = line.split(", ")
                gpus.append(
                    GpuUtilizationInfo(
                        index=int(index),
                        name=name,
                        uuid=uuid,
                        utilization_gpu=int(util),
                        memory_used=int(mem_used),
                        memory_total=int(mem_total),
                        processes_pids={},
                    )
                )

            processes_info = subprocess.run(
                ["nvidia-smi", "pmon", "-c", "1"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
                text=True,
            )
            processes_info = self._parse_nvsmi_pmon_output(processes_info.stdout, gpus)
            for gpu in gpus:
                gpu_id = gpu["index"]
                if gpu_id in processes_info:
                    gpu["processes_pids"] = processes_info[gpu_id]
            return gpus
        except (subprocess.CalledProcessError, ValueError) as e:
            logger.warning(f"nvidia-smi failed to call: {e}. Falling back to pynvml.")
            self._using_nvidia_smi = False
            return self._get_pynvml_gpu_usage()

    @staticmethod
    def _parse_nvsmi_pmon_output(
        nvsmi_stdout: str,
        gpus: List[GpuUtilizationInfo],
    ) -> Dict[int, List[ProcessGPUInfo]]:
        """Parse the output of nvidia-smi pmon -c 1.

        Sample output of 'nvidia-smi pmon -c 1':
        # gpu         pid   type     sm    mem    enc    dec    jpg    ofa    command
        # Idx           #    C/G      %      %      %      %      %      %    name
            0       7175     C     84     26      -      -      -      -    ray::TorchGPUWo
            1       7175     C     86     26      -      -      -      -    ray::TorchGPUWo
            2          -     -      -      -      -      -      -      -    -

        Returns a dict mapping GPU index to dict of pid to ProcessGPUInfo.
        """
        process_utilizations = defaultdict(dict)
        lines = nvsmi_stdout.splitlines()
        # Get the first line that is started with #
        table_header = None
        for line in lines:
            if line.startswith("#"):
                table_header = line
                break
        if not table_header:
            raise ValueError(
                "nvidia-smi pmon output is not supported. Please upgrade to a newer version of nvidia-smi."
            )
        table_header = table_header.lower().split()[1:]
        # Base on different versions, the header may be different.
        # ValueError will be raised if the header is not found by the index function.
        gpu_id_index = table_header.index("gpu")
        pid_index = table_header.index("pid")
        sm_index = table_header.index("sm")
        mem_index = table_header.index("mem")

        for line in lines:
            if line.startswith("#") or not line.strip():
                continue

            columns = line.split()
            if len(columns) < max(gpu_id_index, pid_index, sm_index, mem_index) + 1:
                continue

            gpu_id, pid, sm, mem = (
                int(columns[gpu_id_index]),
                0 if columns[pid_index] == "-" else int(columns[pid_index]),
                0 if columns[sm_index] == "-" else int(columns[sm_index]),
                0 if columns[mem_index] == "-" else int(columns[mem_index]),
            )
            if pid == 0:  # no process on this GPU
                continue
            process_info = ProcessGPUInfo(
                pid=pid,
                gpu_memory_usage=int(
                    gpus[gpu_id]["memory_total"] * mem / 100
                ),  # Convert percentage to MB
                gpu_utilization=sm,
            )
            process_utilizations[gpu_id][pid] = process_info
        return process_utilizations

    def _get_pynvml_gpu_usage(self) -> List[GpuUtilizationInfo]:
        if not self._initialized:
            if not self._initialize():
                return []

        gpu_utilizations = []

        try:
            num_gpus = self._pynvml.nvmlDeviceGetCount()

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
            logger.debug(f"Error getting MIG devices: {e}")

        return mig_devices

    def _get_mig_device_info(
        self, mig_handle, gpu_index: int, mig_index: int
    ) -> Optional[GpuUtilizationInfo]:
        """Get utilization info for a single MIG device."""
        try:
            memory_info = self._pynvml.nvmlDeviceGetMemoryInfo(mig_handle)

            # Get MIG device utilization
            utilization = -1
            try:
                utilization_info = self._pynvml.nvmlDeviceGetUtilizationRates(
                    mig_handle
                )
                utilization = int(utilization_info.gpu)
            except self._pynvml.NVMLError as e:
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
                        gpu_utilization=None,  # Not available in pynvml
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
            )

        except Exception as e:
            logger.debug(f"Error getting MIG device info: {e}")
            return None

    def _get_gpu_info(self, gpu_handle, gpu_index: int) -> Optional[GpuUtilizationInfo]:
        """Get utilization info for a regular (non-MIG) GPU."""
        try:
            memory_info = self._pynvml.nvmlDeviceGetMemoryInfo(gpu_handle)

            # Get GPU utilization
            utilization = -1
            try:
                utilization_info = self._pynvml.nvmlDeviceGetUtilizationRates(
                    gpu_handle
                )
                utilization = int(utilization_info.gpu)
            except self._pynvml.NVMLError as e:
                logger.debug(f"Failed to retrieve GPU utilization: {e}")

            # Get running processes
            processes_pids = {}
            try:
                nv_comp_processes = self._pynvml.nvmlDeviceGetComputeRunningProcesses(
                    gpu_handle
                )
                nv_graphics_processes = (
                    self._pynvml.nvmlDeviceGetGraphicsRunningProcesses(gpu_handle)
                )

                for nv_process in nv_comp_processes + nv_graphics_processes:
                    processes_pids[int(nv_process.pid)] = ProcessGPUInfo(
                        pid=int(nv_process.pid),
                        gpu_memory_usage=(
                            int(nv_process.usedGpuMemory) // MB
                            if nv_process.usedGpuMemory
                            else 0
                        ),
                        gpu_utilization=None,  # Not available in pynvml
                    )
            except self._pynvml.NVMLError as e:
                logger.debug(f"Failed to retrieve GPU processes: {e}")

            return GpuUtilizationInfo(
                index=gpu_index,
                name=self._decode(self._pynvml.nvmlDeviceGetName(gpu_handle)),
                uuid=self._decode(self._pynvml.nvmlDeviceGetUUID(gpu_handle)),
                utilization_gpu=utilization,
                memory_used=int(memory_info.used) // MB,
                memory_total=int(memory_info.total) // MB,
                processes_pids=processes_pids,
            )

        except Exception as e:
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
                    utilization = -1

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

                info = GpuUtilizationInfo(
                    index=i,
                    name=self._decode(self._pyamdsmi.smi_get_device_name(i)),
                    uuid=hex(self._pyamdsmi.smi_get_device_unique_id(i)),
                    utilization_gpu=utilization,
                    memory_used=int(self._pyamdsmi.smi_get_device_memory_used(i)) // MB,
                    memory_total=int(self._pyamdsmi.smi_get_device_memory_total(i))
                    // MB,
                    processes_pids=processes_pids,
                )
                gpu_utilizations.append(info)

        except Exception as e:
            logger.warning(f"Error getting AMD GPU utilization: {e}")
        finally:
            self._shutdown()

        return gpu_utilizations


class GpuMetricProvider:
    """Provider class for GPU metrics collection."""

    def __init__(self):
        self._provider: Optional[GpuProvider] = None
        self._enable_metric_report = True
        self._providers = [NvidiaGpuProvider(), AmdGpuProvider()]
        self._initialized = False

    def initialize(self) -> bool:
        """Initialize the GPU metric provider by detecting available GPU providers."""
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

    def is_metric_report_enabled(self) -> bool:
        """Check if GPU metric reporting is enabled."""
        return self._enable_metric_report
