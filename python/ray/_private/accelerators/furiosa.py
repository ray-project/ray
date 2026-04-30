import logging
import os
from typing import List, Optional, Tuple

from ray._private.accelerators.accelerator import AcceleratorManager
from ray._private.ray_constants import env_bool

logger = logging.getLogger(__name__)

FURIOSA_VISIBLE_DEVICES_ENV_VAR = "FURIOSA_VISIBLE_DEVICES"
NOSET_FURIOSA_VISIBLE_DEVICES_ENV_VAR = "RAY_EXPERIMENTAL_NOSET_FURIOSA_VISIBLE_DEVICES"


class FuriosaAcceleratorManager(AcceleratorManager):
    """Furiosa AI NPU accelerators.

    Resource name is ``FURIOSA``. The accelerator type is reported as
    ``FURIOSA_<ARCH>`` where ``<ARCH>`` is the architecture identifier
    that the Furiosa SMI SDK exposes via its ``Arch`` enum. The current
    SDK variants are ``Rngd``, ``RngdS``, ``RngdMax`` and ``RngdPlus``,
    which surface here as ``FURIOSA_RNGD``, ``FURIOSA_RNGDS``,
    ``FURIOSA_RNGDMAX`` and ``FURIOSA_RNGDPLUS`` respectively.
    Supporting any architecture the SDK reports keeps this manager
    forward-compatible with new SKUs as Furiosa adds them to
    ``furiosa_smi_py``.
    """

    @staticmethod
    def get_resource_name() -> str:
        return "FURIOSA"

    @staticmethod
    def get_visible_accelerator_ids_env_var() -> str:
        return FURIOSA_VISIBLE_DEVICES_ENV_VAR

    @staticmethod
    def get_current_process_visible_accelerator_ids() -> Optional[List[str]]:
        visible_devices = os.environ.get(
            FuriosaAcceleratorManager.get_visible_accelerator_ids_env_var()
        )
        if visible_devices is None:
            return None
        if visible_devices == "":
            return []
        return visible_devices.split(",")

    @staticmethod
    def get_current_node_num_accelerators() -> int:
        """Detects the number of Furiosa NPU devices on the current machine."""
        try:
            from furiosa_smi_py import list_devices

            return len(list_devices())
        except Exception as e:
            logger.debug("Could not detect Furiosa NPU devices: %s", e)
            return 0

    @staticmethod
    def get_current_node_accelerator_type() -> Optional[str]:
        """Gets the architecture of the Furiosa NPU on the current node.

        Returns a string like ``FURIOSA_RNGD``, ``FURIOSA_RNGDMAX``,
        ``FURIOSA_RNGDS``, or ``FURIOSA_RNGDPLUS``. Ray assumes a single
        accelerator type per node, so the architecture of the first detected
        device is used.

        The architecture is read from
        ``device.device_info().arch()`` to mirror the upstream Furiosa SMI
        interface. Non-alphanumeric characters in the Arch enum string
        (e.g., the ``-`` in ``rngd-max`` or the ``+`` in ``rngd+``) are
        stripped so the resulting label is a valid Ray accelerator type.
        """
        try:
            from furiosa_smi_py import list_devices

            devices = list_devices()
            if not devices:
                return None

            arch_obj = devices[0].device_info().arch()
            # PyO3 enums typically stringify as "<EnumName.Variant>",
            # "EnumName.Variant", or just "Variant". Take the trailing
            # component and normalize.
            raw = str(arch_obj).split(".")[-1].strip()
            if not raw:
                return None
            normalized = "".join(ch for ch in raw if ch.isalnum()).upper()
            if not normalized:
                return None
            return f"FURIOSA_{normalized}"
        except Exception as e:
            logger.exception("Failed to detect Furiosa NPU type: %s", e)
            return None

    @staticmethod
    def validate_resource_request_quantity(
        quantity: float,
    ) -> Tuple[bool, Optional[str]]:
        if isinstance(quantity, float) and not quantity.is_integer():
            return (
                False,
                f"{FuriosaAcceleratorManager.get_resource_name()} resource quantity"
                " must be a whole number. Furiosa NPUs do not support"
                " fractional resource sharing."
                f" The specified quantity {quantity} is invalid.",
            )
        return (True, None)

    @staticmethod
    def set_current_process_visible_accelerator_ids(
        visible_furiosa_devices: List[str],
    ) -> None:
        if env_bool(NOSET_FURIOSA_VISIBLE_DEVICES_ENV_VAR, False):
            return

        os.environ[
            FuriosaAcceleratorManager.get_visible_accelerator_ids_env_var()
        ] = ",".join(map(str, visible_furiosa_devices))
