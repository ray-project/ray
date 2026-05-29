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

# Character device files created by the Mobilint kernel driver, one per card.
# ARIES family enumerates as ``/dev/aries0``, ``/dev/aries1``, ...; REGULUS
# family enumerates as ``/dev/regulus0``, ``/dev/regulus1``, .... The driver
# caps each node at 8 devices.
_MBLT_ARIES_DEV_GLOB = "/dev/aries*"
_MBLT_REGULUS_DEV_GLOB = "/dev/regulus*"

# PCI vendor ID for Mobilint, used as a last-resort signal when neither the
# Python SDK nor the kernel driver is available.
_MBLT_PCI_FILTER = ("lspci", "-d", "209f:", "-nn")


class MBLTAcceleratorManager(AcceleratorManager):
    """Mobilint MBLT NPU accelerators (ARIES and REGULUS families)."""

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
        """Detects the number of Mobilint NPUs on the current node.

        Detection order:

        1. qb Runtime's Python binding
           (``qbruntime.get_available_device_numbers()``). This is the
           authoritative source on a node where qb Runtime is installed.
        2. If qb Runtime is unavailable (``ImportError`` or runtime error),
           count the ``/dev/aries*`` and ``/dev/regulus*`` character devices
           created by the Mobilint kernel driver.
        3. If the driver has not loaded yet, scan the PCI bus for Mobilint's
           vendor ID via ``lspci -d 209f:``.
        """
        try:
            from qbruntime import get_available_device_numbers
        except ImportError:
            logger.debug(
                "qbruntime is not installed; falling back to /dev and lspci "
                "for MBLT detection"
            )
            return _count_mblt_dev_nodes() or _count_mblt_pci_entries()

        try:
            return len(get_available_device_numbers())
        except Exception as e:
            logger.debug("qbruntime.get_available_device_numbers() failed: %s", e)
            return _count_mblt_dev_nodes() or _count_mblt_pci_entries()

    @staticmethod
    def get_current_node_accelerator_type() -> Optional[str]:
        """Gets the SKU family of Mobilint NPUs on the current node.

        Returns ``"MOBILINT_ARIES"``, ``"MOBILINT_REGULUS"``, or ``None`` if
        no Mobilint NPU is detected. Ray assumes a single accelerator type
        per node, so the first matching family is used.

        The family is determined by the kernel driver's ``/dev`` node name
        (``aries*`` vs ``regulus*``), with ``lspci`` as a fallback when the
        driver has not loaded. qb Runtime's Python binding does not expose
        the chip family directly, so it is not consulted here.
        """
        if glob.glob(_MBLT_ARIES_DEV_GLOB):
            return "MOBILINT_ARIES"
        if glob.glob(_MBLT_REGULUS_DEV_GLOB):
            return "MOBILINT_REGULUS"

        try:
            out = subprocess.check_output(
                _MBLT_PCI_FILTER,
                text=True,
                stderr=subprocess.DEVNULL,
                timeout=5,
            )
        except Exception as e:
            logger.debug("Failed to query lspci for Mobilint NPUs: %s", e)
            return None

        if re.search(r"\bregulus\b", out, re.IGNORECASE):
            return "MOBILINT_REGULUS"
        if re.search(r"\baries\b", out, re.IGNORECASE):
            return "MOBILINT_ARIES"
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
                " must be a whole number. Mobilint NPUs do not support"
                " fractional resource sharing."
                f" The specified quantity {quantity} is invalid.",
            )
        return True, None

    @staticmethod
    def set_current_process_visible_accelerator_ids(
        visible_mblt_devices: List[str],
    ) -> None:
        if env_bool(NOSET_MBLT_RT_VISIBLE_DEVICES_ENV_VAR, False):
            return

        os.environ[
            MBLTAcceleratorManager.get_visible_accelerator_ids_env_var()
        ] = ",".join(map(str, visible_mblt_devices))


def _count_mblt_dev_nodes() -> int:
    """Count Mobilint character device nodes created by the kernel driver."""
    count = 0
    for pattern in (_MBLT_ARIES_DEV_GLOB, _MBLT_REGULUS_DEV_GLOB):
        try:
            count += len(glob.glob(pattern))
        except Exception:
            continue
    return count


def _count_mblt_pci_entries() -> int:
    """Count Mobilint NPUs visible on the PCI bus via ``lspci -d 209f:``."""
    try:
        out = subprocess.check_output(
            _MBLT_PCI_FILTER, text=True, stderr=subprocess.DEVNULL, timeout=5
        ).strip()
    except Exception as e:
        logger.debug("Failed to query lspci for Mobilint NPUs: %s", e)
        return 0
    if not out:
        return 0
    return len(out.splitlines())
