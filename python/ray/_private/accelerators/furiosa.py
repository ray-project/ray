import logging
import os
from functools import lru_cache
from typing import List, Optional, Tuple

from ray._private.accelerators.accelerator import AcceleratorManager
from ray._private.ray_constants import env_bool

logger = logging.getLogger(__name__)

# Ray uses ``FURIOSA_DEVICES`` to track which Furiosa NPUs are assigned to a
# worker/actor process. The value uses ``npu:<index>`` notation. Ray's
# scheduler operates at the device level, so the value Ray writes is always
# the device-level form (e.g. ``npu:0,npu:3``). Bare integer IDs are also
# accepted on read for convenience.
#
# Note that ``furiosa-llm``'s Python API does not honor ``FURIOSA_DEVICES``
# automatically; callers must pass ``devices=os.environ["FURIOSA_DEVICES"]``
# (or an equivalent list) explicitly to ``furiosa_llm.LLM(...)``. The
# ``furiosa-llm`` CLI does read the value but accepts a richer
# ``npu:X:Y`` (PE-level) form that Ray does not currently preserve through
# worker scheduling; see ``_strip_npu_prefix`` below.
FURIOSA_VISIBLE_DEVICES_ENV_VAR = "FURIOSA_DEVICES"
NOSET_FURIOSA_VISIBLE_DEVICES_ENV_VAR = "RAY_EXPERIMENTAL_NOSET_FURIOSA_DEVICES"

_FURIOSA_DEVICE_PREFIX = "npu:"


@lru_cache(maxsize=None)
def _ensure_furiosa_initialized() -> bool:
    """Run ``furiosa_smi_py.init()`` exactly once per process."""
    from furiosa_smi_py import init

    init()
    return True


def _get_furiosa_list_devices():
    """Lazy import + one-shot init of ``furiosa_smi_py``.

    Returns the current ``list_devices`` callable on success, or ``None`` if
    the SDK is unavailable or initialization fails. ``list_devices`` is
    re-imported on each call so test monkeypatches on the module attribute
    take effect.
    """
    try:
        _ensure_furiosa_initialized()
        from furiosa_smi_py import list_devices
    except Exception as e:
        logger.debug("furiosa_smi_py is unavailable: %s", e)
        return None
    return list_devices


def _strip_npu_prefix(token: str) -> str:
    """Return the numeric device index from an ``npu:<id>`` token.

    Accepts bare integers (``"3"``) as well as the prefixed form
    (``"npu:3"``) so that values written by other tooling round-trip
    cleanly.
    """
    token = token.strip()
    if token.startswith(_FURIOSA_DEVICE_PREFIX):
        token = token[len(_FURIOSA_DEVICE_PREFIX) :]
    # ``furiosa-llm`` accepts both ``npu:X`` (whole NPU) and ``npu:X:Y``
    # (PE-level, e.g. ``npu:0:0-3`` for fused PE 0-3 of NPU 0). Ray's
    # scheduler currently operates at the device level only, so we keep
    # the device index and drop any trailing PE selector. Round-tripping
    # PE-level partitioning through worker scheduling is tracked as a
    # follow-up enhancement.
    return token.split(":", 1)[0]


class FuriosaAcceleratorManager(AcceleratorManager):
    """FuriosaAI NPU accelerators.

    Resource name is ``FURIOSA``. The accelerator type is reported as
    ``FURIOSA_<ARCH>`` where ``<ARCH>`` is the architecture identifier
    that the Furiosa SMI SDK exposes via its ``Arch`` enum. The current
    SDK variants are ``Rngd``, ``RngdS``, ``RngdMax`` and ``RngdPlus``,
    which surface here as ``FURIOSA_RNGD``, ``FURIOSA_RNGDS``,
    ``FURIOSA_RNGDMAX`` and ``FURIOSA_RNGDPLUS`` respectively.
    Supporting any architecture the SDK reports keeps this manager
    forward-compatible with new SKUs as Furiosa adds them to
    ``furiosa_smi_py``.

    Device visibility is tracked through the ``FURIOSA_DEVICES``
    environment variable, formatted as ``npu:<id>`` tokens. The value
    can be passed to the ``furiosa-llm`` CLI (e.g.,
    ``furiosa-llm serve --devices "$FURIOSA_DEVICES" ...``). When
    invoking the ``furiosa_llm.LLM`` Python API directly, the assigned
    devices must be passed explicitly, e.g.
    ``LLM(model_path, devices=os.environ["FURIOSA_DEVICES"])``;
    ``LLM(devices=None)`` allocates all visible NPUs and would bypass
    Ray's per-worker isolation.
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
        return [
            _strip_npu_prefix(token)
            for token in visible_devices.split(",")
            if token.strip()
        ]

    @staticmethod
    def get_current_node_num_accelerators() -> int:
        """Detects the number of Furiosa NPU devices on the current machine."""
        list_devices = _get_furiosa_list_devices()
        if list_devices is None:
            return 0
        try:
            return len(list_devices())
        except Exception as e:
            logger.debug("Could not list Furiosa NPU devices: %s", e)
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
        interface. The Arch enum string is normalized into an
        accelerator-type label: ``+`` is mapped to ``plus`` so distinct
        SKUs do not collide (``rngd+`` becomes ``FURIOSA_RNGDPLUS``,
        matching the PyO3 enum form ``RngdPlus``), and any remaining
        non-alphanumeric characters are stripped.
        """
        list_devices = _get_furiosa_list_devices()
        if list_devices is None:
            return None
        try:
            devices = list_devices()
            if not devices:
                return None

            arch_obj = devices[0].device_info().arch()
            if arch_obj is None:
                return None

            # PyO3 enums typically stringify as "<EnumName.Variant>",
            # "EnumName.Variant", or just "Variant". Take the trailing
            # component.
            raw = str(arch_obj).split(".")[-1].strip()
            if not raw:
                return None
            # Map special suffixes to their alphabetic equivalents so that the
            # ``Arch::ToString`` form ("rngd+") and the PyO3 enum form
            # ("RngdPlus") produce the same Ray accelerator type label.
            # Without this, stripping "+" would collapse "rngd+" into
            # "rngd", colliding with the distinct ``Rngd`` SKU.
            raw = raw.replace("+", "plus")
            normalized = "".join(ch for ch in raw if ch.isalnum()).upper()
            if not normalized:
                return None
            return f"FURIOSA_{normalized}"
        except Exception as e:
            logger.debug("Failed to detect Furiosa NPU type: %s", e)
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

        formatted = ",".join(
            f"{_FURIOSA_DEVICE_PREFIX}{_strip_npu_prefix(str(d))}"
            for d in visible_furiosa_devices
        )
        os.environ[
            FuriosaAcceleratorManager.get_visible_accelerator_ids_env_var()
        ] = formatted
