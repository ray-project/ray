import glob
import logging
import math
import os
from typing import Dict, List, Optional, Tuple

from ray._private.accelerators.accelerator import AcceleratorManager
from ray._private.ray_constants import env_bool

logger = logging.getLogger(__name__)

# Visibility env var for Mobilint NPUs. The qb Runtime native library reads
# ``QBRUNTIME_VISIBLE_DEVICES`` directly to decide which devices a process may
# open (verified by inspecting the SDK shared object strings;
# ``libqbruntime.so.1.2.0`` references ``QBRUNTIME_VISIBLE_DEVICES``). Ray uses
# this same single name as the MBLT visibility env var so the per-worker
# assignment propagates into ``qbruntime.Accelerator(...)`` /
# ``get_available_device_numbers()`` and so Ray's scheduler can save and
# restore exactly one env var when a worker is reused.
MBLT_RT_VISIBLE_DEVICES_ENV_VAR = "QBRUNTIME_VISIBLE_DEVICES"
NOSET_MBLT_RT_VISIBLE_DEVICES_ENV_VAR = "RAY_EXPERIMENTAL_NOSET_MBLT_RT_VISIBLE_DEVICES"

# Character device files created by the Mobilint kernel driver.
# ARIES family enumerates as ``/dev/aries0``, ``/dev/aries1``, ... (one
# numeric-suffixed node per card; the kernel driver caps the node at 8 cards).
# REGULUS family exposes per-card NPU control through ``/dev/regulus-npu*``;
# the sibling ``/dev/regulus`` and ``/dev/regulus-usb`` paths are auxiliary
# nodes that must not be counted as additional cards.
_MBLT_ARIES_DEV_GLOB = "/dev/aries[0-9]*"
_MBLT_REGULUS_DEV_GLOB = "/dev/regulus-npu*"


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
        2. If qb Runtime is unavailable -- not installed, unable to import or
           initialize (a broken SDK may raise ``OSError``/``RuntimeError`` and
           not just ``ImportError``), or failing at runtime -- count the
           ``/dev/aries[0-9]*`` and ``/dev/regulus-npu*`` character
           devices created by the Mobilint kernel driver. REGULUS exposes one
           NPU node per card alongside auxiliary ``/dev/regulus-usb`` paths
           that are intentionally excluded from the count.
        """
        try:
            from qbruntime import get_available_device_numbers
        except ImportError:
            logger.debug(
                "qbruntime is not installed; falling back to /dev " "for MBLT detection"
            )
            return _count_mblt_dev_nodes()
        except Exception as e:
            # A partially broken SDK install can fail to import ``qbruntime``
            # with more than ``ImportError`` -- e.g. a native library load
            # failure surfacing as ``OSError``/``RuntimeError`` during package
            # initialization. This runs on the Ray node startup path, so any
            # such failure must degrade to /dev detection rather than
            # propagate and abort node startup.
            logger.debug(
                "qbruntime import failed (%s); falling back to /dev "
                "for MBLT detection",
                e,
            )
            return _count_mblt_dev_nodes()

        try:
            return len(get_available_device_numbers())
        except Exception as e:
            logger.debug("qbruntime.get_available_device_numbers() failed: %s", e)
            return _count_mblt_dev_nodes()

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
        try:
            value = float(quantity)
        except (TypeError, ValueError):
            value = None
        if value is not None and math.isfinite(value) and not value.is_integer():
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

        # qb Runtime reads ``QBRUNTIME_VISIBLE_DEVICES`` to scope which Mobilint
        # NPUs this worker may open. Set exactly this one env var so that the
        # value stays consistent with what Ray's scheduler saves and restores
        # on worker reuse; mirroring into a second name would leave that name
        # stale after the task finishes.
        os.environ[MBLT_RT_VISIBLE_DEVICES_ENV_VAR] = ",".join(
            map(str, visible_mblt_devices)
        )


def _count_mblt_dev_nodes() -> int:
    """Count Mobilint character device nodes created by the kernel driver."""
    count = 0
    for pattern in (_MBLT_ARIES_DEV_GLOB, _MBLT_REGULUS_DEV_GLOB):
        try:
            count += len(glob.glob(pattern))
        except Exception:
            continue
    return count
