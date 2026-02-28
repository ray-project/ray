import glob
import logging
import os
import re
import subprocess
from typing import Dict, List, Optional, Tuple

from ray._private.accelerators.accelerator import AcceleratorManager
from ray._private.ray_constants import env_bool

logger = logging.getLogger(__name__)

MBLT_RT_VISIBLE_DEVICES_ENV_VAR = "MBLT_DEVICES"
NOSET_MBLT_RT_VISIBLE_DEVICES_ENV_VAR = "RAY_EXPERIMENTAL_NOSET_MBLT_RT_VISIBLE_DEVICES"

_MBLT_DEV_PATTERNS = ["/dev/aries*", "/dev/regulus*"]
_MBLT_PCI_FILTER = ["lspci", "-d", "209f:", "-nn"]


class MBLTAcceleratorManager(AcceleratorManager):
    """Mobilint MBLT accelerators (qb Runtime only - 1.0.0v)."""

    @staticmethod
    def get_resource_name() -> str:
        return "MBLT"

    @staticmethod
    def get_visible_accelerator_ids_env_var() -> str:
        return MBLT_RT_VISIBLE_DEVICES_ENV_VAR

    @staticmethod
    def get_current_process_visible_accelerator_ids() -> Optional[List[str]]:
        visible_devices = os.environ.get(
            MBLTAcceleratorManager.get_visible_accelerator_ids_env_var()
        )
        if visible_devices is None:
            return None
        if visible_devices == "":
            return []
        return visible_devices.split(",")

    @staticmethod
    def get_current_node_num_accelerators() -> int:
        try:
            from qbruntime.accelerator import Accelerator
            max_probe = _hint_num_from_os() or 256
            
            count = 0
            dev_no = 0
            for dev_no in range(max_probe):
                try:
                    acc = Accelerator(dev_no)
                    cores = acc.get_available_cores()

                    if not cores:
                        break
                    count += 1
                except Exception:
                    beak
            return count
        except Exception:
            return 0

    @staticmethod
    def get_current_node_accelerator_type() -> Optional[str]:
        """Gets the type of MBLT NPU on the current node."""
        try:
            from qbruntime.accelerator import Accelerator 

            acc = Accelerator(0)
            for attr in (
                "get_device_name",
                "get_name",
                "get_model",
                "get_chip_name",
                "device_name",
            ):
                if hasattr(acc, attr):
                    try:
                        val = getattr(acc, attr)()
                        if isinstance(val, str) and val.strip():
                            return val.strip()
                    except Exception:
                        pass
        except Exception as e:
            logger.debug("Failed to detect MBLT type via qbruntime: %s", e)

        try:
            out = subprocess.check_output(
                _MBLT_PCI_FILTER, text=True, stderr=subprocess.DEVNULL
            ).strip()
            if not out:
                return None
            first_line = out.splitlines()[0]
            m = re.search(r"]:\s*(.+?)\s*\[", first_line)
            return (m.group(1).strip() if m else first_line.strip()) or None
        except Exception as e:
            logger.debug("Failed to detect MBLT type via lspci: %s", e)
            return None

    @staticmethod
    def get_current_node_additional_resources() -> Optional[Dict[str, float]]:
        return None

    @staticmethod
    def validate_resource_request_quantity(
        quantity: float,
    ) -> Tuple[bool, Optional[str]]:
        if isinstance(quantity, float) and not quantity.is_integer():
            return (
                False,
                f"{MBLTAcceleratorManager.get_resource_name()} resource quantity"
                " must be whole numbers. "
                f"The specified quantity {quantity} is invalid.",
            )
        return (True, None)

    @staticmethod
    def set_current_process_visible_accelerator_ids(
        visible_mblt_devices: List[str],
    ) -> None:
        if env_bool(NOSET_MBLT_RT_VISIBLE_DEVICES_ENV_VAR, False):
            return

        os.environ[
            MBLTAcceleratorManager.get_visible_accelerator_ids_env_var()
        ] = ",".join(map(str, visible_mblt_devices))


def _hint_num_from_os() -> Optional[int]:
    """Best-effort hint for how many devices exist, used to bound probing."""
    try:
        count = 0
        for pattern in _MBLT_DEV_PATTERNS:
            count += len(glob.glob(pattern))
        if count > 0:
            return count
    except Exception:
        pass

    try:
        out = subprocess.check_output(
            _MBLT_PCI_FILTER, text=True, stderr=subprocess.DEVNULL
        ).strip()
        if out:
            return len(out.splitlines())
    except Exception:
        pass

    return None
