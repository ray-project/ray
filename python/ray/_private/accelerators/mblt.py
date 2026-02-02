# ray/_private/accelerators/mblt.py
import os
import re
import glob
import subprocess
import logging
from typing import Dict, List, Optional, Tuple

from ray._private.accelerators.accelerator import AcceleratorManager

logger = logging.getLogger(__name__)

MBLT_VISIBLE_DEVICES = "MBLT_VISIBLE_DEVICES"
NOSET_MBLT_VISIBLE_DEVICES = "RAY_EXPERIMENTAL_NOSET_MBLT_VISIBLE_DEVICES"

# OS 폴백 힌트
_MBLT_DEV_GLOB = "/dev/aries[0-9]*"       # manipulate /dev/mblt[0-9]* following environments
_MBLT_PCI_FILTER = ["lspci", "-d", "209f:", "-nn"]  # 0x209f = Mobilint

class MBLTAcceleratorManager(AcceleratorManager):
    """Mobilint MBLT NPU support. Policy: only MBLT=1 per task/actor."""

    @staticmethod
    def get_resource_name() -> str:
        return "MBLT"

    @staticmethod
    def get_visible_accelerator_ids_env_var() -> str:
        return MBLT_VISIBLE_DEVICES

    @staticmethod
    def get_current_node_num_accelerators() -> int:
        try:
            from maccel.accelerator import Accelerator
            max_try = _hint_num_from_os() or 9
            count = 0
            for dev_no in range(max_try):
                try:
                    acc = Accelerator(dev_no)
                    _ = acc.get_available_cores()
                    count += 1
                except Exception:
                    pass
            if count > 0:
                return count
        except Exception as e:
            logger.debug("maccel import failed in get_current_node_num_accelerators: %s", e)

        try:
            devs = glob.glob(_MBLT_DEV_GLOB)
            if devs:
                return len(devs)
        except Exception as e:
            logger.debug("dev glob fallback failed: %s", e)

        try:
            out = subprocess.check_output(_MBLT_PCI_FILTER, text=True).strip()
            if out:
                return len(out.splitlines())
        except Exception as e:
            logger.debug("lspci fallback failed: %s", e)

        return 0

    @staticmethod
    def get_current_node_accelerator_type() -> Optional[str]:
        try:
            from maccel.accelerator import Accelerator
            acc = Accelerator(0)
            for attr in ("get_device_name", "get_name", "get_model", "get_chip_name", "device_name"):
                if hasattr(acc, attr):
                    try:
                        val = getattr(acc, attr)()
                        if isinstance(val, str) and val.strip():
                            return val.strip()
                    except Exception:
                        pass
        except Exception:
            pass

        try:
            out = subprocess.check_output(_MBLT_PCI_FILTER, text=True, stderr=subprocess.DEVNULL).strip()
            line = out.splitlines()[0] if out else ""
            if not line:
                return None
            m = re.search(r"]:\s*(.+?)\s*\[", line)
            return (m.group(1).strip() if m else line) or None
        except Exception:
            return None

    @staticmethod
    def get_current_node_additional_resources() -> Optional[Dict[str, float]]:
        return None

    @staticmethod
    def validate_resource_request_quantity(
        quantity: float,
    ) -> Tuple[bool, Optional[str]]:
        if quantity == 1 or quantity == 1.0:
            return (True, None)
        return (False, "Only MBLT=1 is supported; fractional or >1 are not allowed.")

    @staticmethod
    def get_current_process_visible_accelerator_ids() -> Optional[List[str]]:
        s = os.environ.get(MBLT_VISIBLE_DEVICES, None)
        if s is None:
            return None
        if s in ("", "NoDevFiles"):
            return []
        return s.split(",")

    @staticmethod
    def set_current_process_visible_accelerator_ids(ids: List[str]) -> None:
        if os.environ.get(NOSET_MBLT_VISIBLE_DEVICES):
            return
        os.environ[MBLT_VISIBLE_DEVICES] = "" if not ids else str(ids[0])


def _hint_num_from_os() -> Optional[int]:
    try:
        devs = glob.glob(_MBLT_DEV_GLOB)
        if devs:
            return len(devs)
    except Exception:
        pass
    try:
        out = subprocess.check_output(_MBLT_PCI_FILTER, text=True, stderr=subprocess.DEVNULL).strip()
        if out:
            return len(out.splitlines())
    except Exception:
        pass
    return None
