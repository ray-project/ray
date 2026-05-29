import glob
import logging
import os
import subprocess
from typing import Dict, List, Optional, Tuple

from ray._private.accelerators.accelerator import AcceleratorManager
from ray._private.ray_constants import env_bool

logger = logging.getLogger(__name__)

# Ray-facing visibility env var. Ray's accelerator scheduler reads and writes
# this name; qb Runtime itself reads ``QBRUNTIME_VISIBLE_DEVICES`` (see
# ``set_current_process_visible_accelerator_ids`` below for the mirror).
MBLT_RT_VISIBLE_DEVICES_ENV_VAR = "MBLT_DEVICES"
NOSET_MBLT_RT_VISIBLE_DEVICES_ENV_VAR = "RAY_EXPERIMENTAL_NOSET_MBLT_RT_VISIBLE_DEVICES"

# The visibility env var the qb Runtime native library actually reads when
# resolving device numbers. Verified by inspecting the SDK shared object
# strings (``libqbruntime.so.1.2.0`` references
# ``QBRUNTIME_VISIBLE_DEVICES`` directly).
_QBRUNTIME_VISIBLE_DEVICES_ENV_VAR = "QBRUNTIME_VISIBLE_DEVICES"

# Character device files created by the Mobilint kernel driver.
# ARIES family enumerates as ``/dev/aries0``, ``/dev/aries1``, ... (one
# numeric-suffixed node per card; the kernel driver caps the node at 8 cards).
# REGULUS family exposes per-card NPU control through ``/dev/regulus-npu*``;
# the sibling ``/dev/regulus`` and ``/dev/regulus-usb`` paths are auxiliary
# nodes that must not be counted as additional cards.
_MBLT_ARIES_DEV_GLOB = "/dev/aries[0-9]*"
_MBLT_REGULUS_DEV_GLOB = "/dev/regulus-npu*"

# PCI vendor ID for Mobilint, used as a count fallback when neither the
# Python SDK nor the kernel driver is available. Mobilint's vendor ID is not
# in the standard ``pci.ids`` hwdata, so the lspci output for a Mobilint card
# is typically ``Device 209f:0000`` with no human-readable family name; this
# fallback is therefore only used to count cards, not to identify the SKU.
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
           count the ``/dev/aries[0-9]*`` and ``/dev/regulus-npu*`` character
           devices created by the Mobilint kernel driver. REGULUS exposes one
           NPU node per card alongside auxiliary ``/dev/regulus-usb`` paths
           that are intentionally excluded from the count.
        3. If the driver has not loaded yet, count rows of Mobilint's PCI
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

        Returns ``"MOBILINT_ARIES"`` or ``"MOBILINT_REGULUS"``, or ``None``
        if the family cannot be determined. Ray assumes a single accelerator
        type per node, so the first matching family is used. ARIES1 and
        ARIES2 hardware revisions both report as ``"MOBILINT_ARIES"``;
        finer-grained scheduling can read sysfs ``product_type`` or invoke
        ``ARIES_IOC_GET_ARIES_VERSION`` if needed in a future change.

        The family is determined by the kernel driver's ``/dev`` node name
        (``aries[0-9]*`` vs ``regulus-npu*``). lspci is intentionally not
        consulted for the family because Mobilint's vendor ID is not in the
        standard ``pci.ids`` hwdata; the lspci description has no stable
        human-readable substring to disambiguate ARIES from REGULUS.
        """
        if glob.glob(_MBLT_ARIES_DEV_GLOB):
            return "MOBILINT_ARIES"
        if glob.glob(_MBLT_REGULUS_DEV_GLOB):
            return "MOBILINT_REGULUS"
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

        ids_str = ",".join(map(str, visible_mblt_devices))
        # ``MBLT_DEVICES`` is the Ray-facing name used by user code; the qb
        # Runtime native library reads ``QBRUNTIME_VISIBLE_DEVICES``. Both
        # must be set for Ray's per-worker visibility to propagate into
        # ``qbruntime.Accelerator(...)`` and ``get_available_device_numbers()``.
        os.environ[MBLT_RT_VISIBLE_DEVICES_ENV_VAR] = ids_str
        os.environ[_QBRUNTIME_VISIBLE_DEVICES_ENV_VAR] = ids_str


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
