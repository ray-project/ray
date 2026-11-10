import ctypes
import importlib.util
import logging
import os
import sys
import threading
from typing import Dict, List, Optional, Tuple

from ray._private.accelerators.accelerator import AcceleratorManager
from ray._private.ray_constants import env_bool

logger = logging.getLogger(__name__)

CUDA_VISIBLE_DEVICES_ENV_VAR = "CUDA_VISIBLE_DEVICES"
NOSET_CUDA_VISIBLE_DEVICES_ENV_VAR = "RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES"

# The vendor NVML shim prefixes the device name with the vendor name, for
# example "Iluvatar BI-V150S". Strip only that known prefix and keep the model
# name, including any suffix, as the accelerator type.
ILUVATAR_GPU_NAME_PREFIX = "Iluvatar "


_nvml = None
_nvml_load_lock = threading.Lock()


def _load_nvml():
    """Load an isolated copy of the bundled NVML bindings for libixml.

    NVML bindings keep their library handle, function cache, and initialization
    count in module globals. Executing the same binding source in a separate
    module keeps Iluvatar state independent of NVIDIA without vendoring another
    copy of the source.
    """
    global _nvml

    with _nvml_load_lock:
        if _nvml is None:
            from ray._private.thirdparty.pynvml import pynvml as nvidia_nvml

            library = ctypes.CDLL("libixml.so")
            spec = importlib.util.spec_from_file_location(
                f"{__name__}._nvml", nvidia_nvml.__file__
            )
            nvml = importlib.util.module_from_spec(spec)
            # pynvml installs its generated error classes via sys.modules.
            previous_module = sys.modules.get(spec.name)
            sys.modules[spec.name] = nvml
            try:
                spec.loader.exec_module(nvml)
            except BaseException:
                if previous_module is None:
                    sys.modules.pop(spec.name, None)
                else:
                    sys.modules[spec.name] = previous_module
                raise
            nvml.nvmlLib = library
            _nvml = nvml

        return _nvml


class IluvatarGPUAcceleratorManager(AcceleratorManager):
    """Iluvatar CoreX GPU accelerators."""

    @staticmethod
    def get_resource_name() -> str:
        return "GPU"

    @staticmethod
    def get_visible_accelerator_ids_env_var() -> str:
        return CUDA_VISIBLE_DEVICES_ENV_VAR

    @staticmethod
    def get_current_process_visible_accelerator_ids() -> Optional[List[str]]:
        cuda_visible_devices = os.environ.get(
            IluvatarGPUAcceleratorManager.get_visible_accelerator_ids_env_var(), None
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
        try:
            pynvml = _load_nvml()
        except Exception as e:
            logger.debug("Could not load the Iluvatar NVML library: %s", e)
            return 0

        try:
            pynvml.nvmlInit()
        except Exception as e:
            logger.debug("Failed to initialize the Iluvatar NVML library: %s", e)
            return 0

        try:
            return pynvml.nvmlDeviceGetCount()
        except Exception as e:
            logger.debug("Failed to count Iluvatar GPUs: %s", e)
            return 0
        finally:
            try:
                pynvml.nvmlShutdown()
            except Exception:
                pass

    @staticmethod
    def get_current_node_accelerator_type() -> Optional[str]:
        try:
            pynvml = _load_nvml()
        except Exception as e:
            logger.debug("Could not load the Iluvatar NVML library: %s", e)
            return None

        try:
            pynvml.nvmlInit()
        except Exception as e:
            logger.debug("Failed to initialize the Iluvatar NVML library: %s", e)
            return None

        try:
            device_count = pynvml.nvmlDeviceGetCount()
            if device_count == 0:
                return None

            handle = pynvml.nvmlDeviceGetHandleByIndex(0)
            device_name = pynvml.nvmlDeviceGetName(handle)
            if isinstance(device_name, bytes):
                device_name = device_name.decode("utf-8")
            return IluvatarGPUAcceleratorManager._gpu_name_to_accelerator_type(
                device_name
            )
        except Exception as e:
            logger.debug("Failed to detect the Iluvatar GPU type: %s", e)
            return None
        finally:
            try:
                pynvml.nvmlShutdown()
            except Exception:
                pass

    @staticmethod
    def _gpu_name_to_accelerator_type(name: Optional[str]) -> Optional[str]:
        if not name:
            return None
        name = name.strip()
        if name.startswith(ILUVATAR_GPU_NAME_PREFIX):
            name = name[len(ILUVATAR_GPU_NAME_PREFIX) :].strip()
        return name or None

    @staticmethod
    def validate_resource_request_quantity(
        quantity: float,
    ) -> Tuple[bool, Optional[str]]:
        return (True, None)

    @staticmethod
    def set_current_process_visible_accelerator_ids(
        visible_cuda_devices: List[str],
    ) -> None:
        if env_bool(NOSET_CUDA_VISIBLE_DEVICES_ENV_VAR, False):
            return

        os.environ[
            IluvatarGPUAcceleratorManager.get_visible_accelerator_ids_env_var()
        ] = ",".join([str(i) for i in visible_cuda_devices])

    @staticmethod
    def get_current_node_additional_resources() -> Optional[Dict[str, float]]:
        return None
