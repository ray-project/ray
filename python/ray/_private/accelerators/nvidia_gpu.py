import re
import os
import sys
import logging
import subprocess
import importlib
import warnings
from typing import Optional, List, Tuple

try:
    import GPUtil
except ImportError:
    pass

try:
    import gpustat.core as gpustat
except ModuleNotFoundError:
    gpustat = None
except ImportError:
    gpustat = None

from ray._private.accelerators.accelerator import AcceleratorManager
from ray._private.metrics_agent import Gauge, Record
from ray.util.debug import log_once

logger = logging.getLogger(__name__)

enable_gpu_usage_check = True

node_gpus_available_gauge = Gauge(
    "node_gpus_available",
    "Total GPUs available on a ray node",
    "percentage",
    ["ip", "SessionName", "GpuDeviceName", "GpuIndex"],
)
node_gpus_utilization_gauge = (
    Gauge(
        "node_gpus_utilization",
        "Total GPUs usage on a ray node",
        "percentage",
        ["ip", "SessionName", "GpuDeviceName", "GpuIndex"],
    ),
)
node_gram_used_gauge = (
    Gauge(
        "node_gram_used",
        "Total GPU RAM usage on a ray node",
        "bytes",
        ["ip", "SessionName", "GpuDeviceName", "GpuIndex"],
    ),
)
node_gram_available_guage = Gauge(
    "node_gram_available",
    "Total GPU RAM available on a ray node",
    "bytes",
    ["ip", "SessionName", "GpuDeviceName", "GpuIndex"],
)

CUDA_VISIBLE_DEVICES_ENV_VAR = "CUDA_VISIBLE_DEVICES"
NOSET_CUDA_VISIBLE_DEVICES_ENV_VAR = "RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES"

# TODO(Alex): This pattern may not work for non NVIDIA Tesla GPUs (which have
# the form "Tesla V100-SXM2-16GB" or "Tesla K80").
NVIDIA_GPU_NAME_PATTERN = re.compile(r"\w+\s+([A-Z0-9]+)")


class NvidiaGPUAcceleratorManager(AcceleratorManager):
    """Nvidia GPU accelerators."""

    @staticmethod
    def get_resource_name() -> str:
        return "GPU"

    @staticmethod
    def get_visible_accelerator_ids_env_var() -> str:
        return CUDA_VISIBLE_DEVICES_ENV_VAR

    @staticmethod
    def get_current_process_visible_accelerator_ids() -> Optional[List[str]]:
        cuda_visible_devices = os.environ.get(
            NvidiaGPUAcceleratorManager.get_visible_accelerator_ids_env_var(), None
        )

        if cuda_visible_devices is None:
            return None

        if cuda_visible_devices == "":
            return []

        if cuda_visible_devices == "NoDevFiles":
            return []

        return list(cuda_visible_devices.split(","))

    @staticmethod
    def get_current_node_num_accelerators() -> int:
        num_gpus = 0
        if importlib.util.find_spec("GPUtil"):
            gpu_list = GPUtil.getGPUs()
            num_gpus = len(gpu_list)
        elif sys.platform.startswith("linux"):
            proc_gpus_path = "/proc/driver/nvidia/gpus"
            if os.path.isdir(proc_gpus_path):
                num_gpus = len(os.listdir(proc_gpus_path))
        elif sys.platform == "win32":
            props = "AdapterCompatibility"
            cmdargs = ["WMIC", "PATH", "Win32_VideoController", "GET", props]
            lines = subprocess.check_output(cmdargs).splitlines()[1:]
            num_gpus = len([x.rstrip() for x in lines if x.startswith(b"NVIDIA")])
        return num_gpus

    @staticmethod
    def get_current_node_accelerator_type() -> Optional[str]:
        try:
            if importlib.util.find_spec("GPUtil"):
                gpu_list = GPUtil.getGPUs()
                if len(gpu_list) > 0:
                    gpu_list_names = [gpu.name for gpu in gpu_list]
                    return NvidiaGPUAcceleratorManager._gpu_name_to_accelerator_type(
                        gpu_list_names.pop()
                    )
            elif sys.platform.startswith("linux"):
                proc_gpus_path = "/proc/driver/nvidia/gpus"
                if not os.path.isdir(proc_gpus_path):
                    return None
                gpu_dirs = os.listdir(proc_gpus_path)
                if len(gpu_dirs) == 0:
                    return None
                gpu_info_path = f"{proc_gpus_path}/{gpu_dirs[0]}/information"
                info_str = open(gpu_info_path).read()
                if not info_str:
                    return None
                lines = info_str.split("\n")
                full_model_name = None
                for line in lines:
                    split = line.split(":")
                    if len(split) != 2:
                        continue
                    k, v = split
                    if k.strip() == "Model":
                        full_model_name = v.strip()
                        break
                return NvidiaGPUAcceleratorManager._gpu_name_to_accelerator_type(
                    full_model_name
                )
        except Exception:
            logger.exception("Could not parse gpu information.")
        return None

    @staticmethod
    def _gpu_name_to_accelerator_type(name):
        if name is None:
            return None
        match = NVIDIA_GPU_NAME_PATTERN.match(name)
        return match.group(1) if match else None

    @staticmethod
    def validate_resource_request_quantity(
        quantity: float,
    ) -> Tuple[bool, Optional[str]]:
        return (True, None)

    @staticmethod
    def set_current_process_visible_accelerator_ids(
        visible_cuda_devices: List[str],
    ) -> None:
        if os.environ.get(NOSET_CUDA_VISIBLE_DEVICES_ENV_VAR):
            return

        os.environ[
            NvidiaGPUAcceleratorManager.get_visible_accelerator_ids_env_var()
        ] = ",".join([str(i) for i in visible_cuda_devices])

    @staticmethod
    def get_ec2_instance_num_accelerators(
        instance_type: str, instances: dict
    ) -> Optional[int]:
        if instance_type not in instances:
            return None

        gpus = instances[instance_type].get("GpuInfo", {}).get("Gpus")
        if gpus is not None:
            # TODO(ameer): currently we support one gpu type per node.
            assert len(gpus) == 1
            return gpus[0]["Count"]
        return None

    @staticmethod
    def get_ec2_instance_accelerator_type(
        instance_type: str, instances: dict
    ) -> Optional[str]:
        if instance_type not in instances:
            return None

        gpus = instances[instance_type].get("GpuInfo", {}).get("Gpus")
        if gpus is not None:
            # TODO(ameer): currently we support one gpu type per node.
            assert len(gpus) == 1
            return gpus[0]["Name"]
        return None

    @staticmethod
    def get_current_node_accelerator_usage_metrics() -> List[Record]:
        if gpustat is None and log_once("gpustat_import_warning"):
            warnings.warn(
                "`gpustat` package is not installed or cannot be imported. "
                "GPU monitoring is not available. "
                "To have full functionality of the "
                "dashboard please install `pip install ray["
                "default]`."
            )
            return []

        global enable_gpu_usage_check
        if gpustat is None or not enable_gpu_usage_check:
            return []

        gpus = []
        try:
            gpus = gpustat.new_query().gpus
        except Exception as e:
            logger.debug(f"gpustat failed to retrieve GPU information: {e}")

            # gpustat calls pynvml.nvmlInit()
            # On machines without GPUs, this can run subprocesses that spew to
            # stderr. Then with log_to_driver=True, we get log spew from every
            # single raylet. To avoid this, disable the GPU usage check on
            # certain errors.
            # https://github.com/ray-project/ray/issues/14305
            # https://github.com/ray-project/ray/pull/21686
            if type(e).__name__ == "NVMLError_DriverNotLoaded":
                enable_gpu_usage_check = False

        gpu_stats = []
        for gpu in gpus:
            # Note the keys in this dict have periods which throws
            # off javascript so we change .s to _s
            gpu_data = {"_".join(key.split(".")): val for key, val in gpu.entry.items()}
            gpu_stats.append(gpu_data)

        # The output example of gpustats.
        """
        {'index': 0,
        'uuid': 'GPU-36e1567d-37ed-051e-f8ff-df807517b396',
        'name': 'NVIDIA A10G',
        'temperature_gpu': 20,
        'fan_speed': 0,
        'utilization_gpu': 1,
        'utilization_enc': 0,
        'utilization_dec': 0,
        'power_draw': 51,
        'enforced_power_limit': 300,
        'memory_used': 0,
        'memory_total': 22731,
        'processes': []}
        """
        records = []
        for gpu_stat in gpu_stats:
            gpu_index = gpu_stat.get("index")
            if gpu_index is None:
                continue

            gpu_utilization, gram_used, gram_total = 0, 0, 0
            # Consume GPU may not report its utilization.
            if gpu_stat["utilization_gpu"] is not None:
                gpu_utilization = gpu_stat["utilization_gpu"]
            gram_used = gpu_stat["memory_used"]
            gram_total = gpu_stat["memory_total"]
            gpu_name = gpu_stat.get("name")
            gram_available = gram_total - gram_used

            gpu_tags = {"GpuIndex": str(gpu_index)}
            if gpu_name:
                gpu_tags["GpuDeviceName"] = gpu_name

            # There's only 1 GPU per each index, so we record 1 here.
            records.append(
                Record(
                    gauge=node_gpus_available_gauge,
                    value=1,
                    tags=gpu_tags,
                )
            )
            records.append(
                Record(
                    gauge=node_gpus_utilization_gauge,
                    value=gpu_utilization,
                    tags=gpu_tags,
                )
            )
            records.append(
                Record(
                    gauge=node_gram_used_gauge,
                    value=gram_used,
                    tags=gpu_tags,
                )
            )
            records.append(
                Record(
                    gauge=node_gram_available_guage,
                    value=gram_available,
                    tags=gpu_tags,
                )
            )

        return records
