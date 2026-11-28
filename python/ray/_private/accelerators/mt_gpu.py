import logging
import os
import re
from typing import List, Optional, Tuple

from ray._private.accelerators.accelerator import AcceleratorManager
from ray._private.ray_constants import env_bool

logger = logging.getLogger(__name__)

MUSA_VISIBLE_DEVICES_ENV_VAR = "MUSA_VISIBLE_DEVICES"
NOSET_MUSA_VISIBLE_DEVICES_ENV_VAR = "RAY_EXPERIMENTAL_NOSET_MUSA_VISIBLE_DEVICES"

# MTML reports names such as ``MTT S5000``. Keep the accelerator type stable by
# retaining the first digit-containing model token (``S5000``), so the value is
# stable and suitable for Ray's accelerator-type resource label.
MUSA_GPU_NAME_PATTERN = re.compile(r"\w+\s+((?:[A-Z]+\s+)*[A-Z0-9]*\d[A-Z0-9]*)")


class MTGPUAcceleratorManager(AcceleratorManager):
    """Moore Threads MUSA GPU accelerators."""

    @staticmethod
    def get_resource_name() -> str:
        # Use Ray's standard GPU resource so num_gpus, ray.get_gpu_ids(),
        # placement groups, and accelerator_type scheduling keep working.
        return "GPU"

    @staticmethod
    def get_visible_accelerator_ids_env_var() -> str:
        return MUSA_VISIBLE_DEVICES_ENV_VAR

    @staticmethod
    def get_current_process_visible_accelerator_ids() -> Optional[List[str]]:
        visible_devices = os.environ.get(
            MTGPUAcceleratorManager.get_visible_accelerator_ids_env_var(), None
        )
        if visible_devices is None:
            return None

        if visible_devices in ("", "NoDevFiles"):
            return []

        # Keep IDs as strings. Ray's worker resource IDs are converted to
        # strings before they are matched against this list.
        return [
            device_id.strip()
            for device_id in visible_devices.split(",")
            if device_id.strip()
        ]

    @staticmethod
    def _get_device_count_and_first_name() -> Tuple[int, Optional[str]]:
        """Query MTML once and return the device count and first device name.

        mthreads-ml-py exposes an NVML-compatible API through ``pymtml``.
        Import it lazily because Ray must remain importable on non-MUSA nodes,
        and loading the native MTML library is a runtime operation.
        """
        try:
            import pymtml
        except (ImportError, OSError) as exc:
            logger.debug("Could not import pymtml: %s", exc)
            return 0, None

        initialized = False
        try:
            pymtml.nvmlInit()
            initialized = True
            device_count = int(pymtml.nvmlDeviceGetCount())
            device_name = None
            if device_count > 0:
                try:
                    handle = pymtml.nvmlDeviceGetHandleByIndex(0)
                    device_name = pymtml.nvmlDeviceGetName(handle)
                    if isinstance(device_name, bytes):
                        device_name = device_name.decode("utf-8")
                except Exception as exc:
                    # The device count is still authoritative when querying
                    # the optional model name fails. This keeps resource
                    # discovery independent from accelerator-type labeling.
                    logger.debug("Could not query MUSA device name: %s", exc)
            return device_count, device_name
        except Exception as exc:
            # MTML can report driver/library failures as NVMLError, while a
            # missing or incompatible shared library can raise OSError or a
            # ctypes/runtime exception. Detection must fail closed so a
            # non-MUSA Ray node can still start.
            logger.debug("Could not detect MUSA devices through pymtml: %s", exc)
            return 0, None
        finally:
            if initialized:
                try:
                    pymtml.nvmlShutdown()
                except Exception:
                    logger.debug("Failed to shut down pymtml", exc_info=True)

    @staticmethod
    def get_current_node_num_accelerators() -> int:
        device_count, _ = MTGPUAcceleratorManager._get_device_count_and_first_name()
        return device_count

    @staticmethod
    def get_current_node_accelerator_type() -> Optional[str]:
        _, device_name = MTGPUAcceleratorManager._get_device_count_and_first_name()
        return MTGPUAcceleratorManager._gpu_name_to_accelerator_type(device_name)

    @staticmethod
    def _gpu_name_to_accelerator_type(name: Optional[str]) -> Optional[str]:
        if name is None:
            return None

        match = MUSA_GPU_NAME_PATTERN.match(name)
        result = match.group(1).replace(" ", "-") if match else None
        if result and len(result) > 1:
            return result

        cleaned = re.sub(r"^MTT\s+", "", name).strip()
        return cleaned.replace(" ", "-") if cleaned else None

    @staticmethod
    def get_current_node_additional_resources():
        return None

    @staticmethod
    def validate_resource_request_quantity(
        quantity: float,
    ) -> Tuple[bool, Optional[str]]:
        # Match Ray's other GPU managers. Whether fractional GPU sharing is
        # useful on a particular MUSA runtime is a deployment decision; Ray's
        # scheduler still performs the capacity check.
        return (True, None)

    @staticmethod
    def set_current_process_visible_accelerator_ids(
        visible_musa_devices: List[str],
    ) -> None:
        if env_bool(NOSET_MUSA_VISIBLE_DEVICES_ENV_VAR, False):
            return

        os.environ[
            MTGPUAcceleratorManager.get_visible_accelerator_ids_env_var()
        ] = ",".join(str(device_id) for device_id in visible_musa_devices)
