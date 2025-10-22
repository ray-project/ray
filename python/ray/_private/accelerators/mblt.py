# ray/_private/accelerators/mblt.py
# Mobilint MBLT accelerator manager (card-based).
#
# 이 매니저는 "카드 수(정수)"를 리소스로 관리하고, 워커/Actor 프로세스의
# 가시 장치 목록을 환경변수로 격리합니다.
#
# 참고:
# - Ray는 작업/Actor에 할당된 가속기 ID를 런타임 컨텍스트로 노출합니다.
#   (ray.get_runtime_context().get_accelerator_ids())  # {'MBLT': ['0', ...]}
# - 대부분의 가속기는 Ray가 설정한 "*_VISIBLE_DEVICES"류의 환경변수를
#   프레임워크가 읽어 장치 격리를 수행하는 패턴을 사용합니다.
#   (예: CUDA_VISIBLE_DEVICES)  # Ray accelerators docs
#
# Docs:
#   https://docs.ray.io/en/latest/ray-core/scheduling/accelerators.html
#   https://docs.ray.io/en/latest/ray-core/api/doc/ray.runtime_context.RuntimeContext.get_accelerator_ids.html

from __future__ import annotations

import logging
import os
from typing import Dict, List, Optional, Tuple

from ray._private.accelerators.accelerator import AcceleratorManager

logger = logging.getLogger(__name__)

# Env var that constrains visible MBLT card IDs for the current process.
MBLT_VISIBLE_DEVICES_ENV_VAR = "MBLT_VISIBLE_DEVICES"

# If set (to any non-empty value), Ray won't set the MBLT visibility env var.
# (Mirrors the pattern used by other accelerators' "NOSET_*_VISIBLE_DEVICES".)
NOSET_MBLT_VISIBLE_DEVICES_ENV_VAR = "RAY_EXPERIMENTAL_NOSET_MBLT_VISIBLE_DEVICES"

# Compatibility env from Ray core behavior notes:
# When the requested accelerator quantity is 0, Ray may override visibility
# envs to empty. This flag mirrors core behavior toggles.
RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO_ENV_VAR = "RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO"


class MBLTAcceleratorManager(AcceleratorManager):
    """Mobilint MBLT accelerators (card-based scheduler integration).

    Design:
      - Resource name: "MBLT"
      - Unit: integer "cards" (no fractional sharing).
      - Visibility: the worker/actor process reads MBLT_VISIBLE_DEVICES to see
        assigned card IDs (comma-separated string, e.g., "0,1").

    Detection:
      - Uses optional helper package `maccel_etri_detect` if available:
          * get_card_count() -> int
          * get_npu_name()   -> str
      - Fallback: environment overrides for demos:
          * MBLT_NUM_CARDS (int), default 0
          * MBLT_NPU_NAME (str),  default "MBLT-Unknown"
    """

    # ---- Required abstract methods -------------------------------------------------

    @staticmethod
    def get_resource_name() -> str:
        """Return the Ray resource name used for scheduling."""
        return "MBLT"

    @staticmethod
    def get_visible_accelerator_ids_env_var() -> str:
        """Return the env var used to constrain visible MBLT card IDs."""
        return MBLT_VISIBLE_DEVICES_ENV_VAR

    @staticmethod
    def get_current_node_num_accelerators() -> int:
        """Detect total number of MBLT cards on this node."""
        # 1) Try vendor/helper package
        try:
            from maccel_etri_detect import get_card_count  # type: ignore

            num = int(get_card_count())
            if num < 0:
                logger.warning("Detected negative MBLT card count, clamping to 0.")
                return 0
            logger.info("Detected %s MBLT card(s) via maccel_etri_detect", num)
            return num
        except Exception as e:
            # 2) Fallback to env for demo/dev
            env_val = os.environ.get("MBLT_NUM_CARDS")
            if env_val is not None:
                try:
                    num = int(env_val)
                    logger.info(
                        "Using MBLT_NUM_CARDS=%s (fallback detection).", num
                    )
                    return max(0, num)
                except Exception:
                    logger.warning(
                        "Invalid MBLT_NUM_CARDS=%r; treating as 0.", env_val
                    )
                    return 0
            logger.debug(
                "MBLT detection failed or helper missing (%s); assuming 0 cards.",
                e,
            )
            return 0

    @staticmethod
    def get_current_node_accelerator_type() -> Optional[str]:
        """Return the MBLT accelerator type/model string, or None if unknown."""
        # Only attempt when at least one card exists.
        if MBLTAcceleratorManager.get_current_node_num_accelerators() <= 0:
            return None

        try:
            from maccel_etri_detect import get_npu_name  # type: ignore

            n = str(get_npu_name())
            return n if n else "MBLT-Unknown"
        except Exception as e:
            # Fallback env
            n = os.environ.get("MBLT_NPU_NAME")
            if n:
                return n
            logger.debug("Failed to query MBLT NPU name: %s", e)
            return None

    @staticmethod
    def get_current_node_additional_resources() -> Optional[Dict[str, float]]:
        """Return any additional logical resources for this accelerator family.

        Not required for MBLT at this time.
        """
        return None

    @staticmethod
    def validate_resource_request_quantity(
        quantity: float,
    ) -> Tuple[bool, Optional[str]]:
        """Validate that requested MBLT resource quantity is a whole card count."""
        # Disallow negative
        if quantity < 0:
            return False, f"Resource quantity cannot be negative: {quantity}"

        # Cards must be whole numbers (no fractional sharing).
        # Accept ints (e.g., 1.0 is okay if it represents an int).
        if isinstance(quantity, float) and not float(quantity).is_integer():
            return (
                False,
                "MBLT resource quantity must be an integer number of cards "
                f"(got {quantity}).",
            )

        return True, None

    @staticmethod
    def get_current_process_visible_accelerator_ids() -> Optional[List[str]]:
        """Return list of visible MBLT card IDs for the current process.

        Returns:
          - None  : all cards visible (no env constraint set).
          - []    : no cards visible (explicitly constrained to empty).
          - ["0"] : only card 0 visible, etc.
        """
        val = os.environ.get(MBLT_VISIBLE_DEVICES_ENV_VAR)
        if val is None:
            return None
        if val == "":
            return []
        # Normalize/strip and filter out empties
        ids = [tok.strip() for tok in val.split(",") if tok.strip() != ""]
        return ids

    @staticmethod
    def set_current_process_visible_accelerator_ids(ids: List[str]) -> None:
        """Set visible MBLT card IDs for this process via env var.

        Ray core will invoke this during worker/actor startup to enforce
        device isolation. If NOSET_* override is present, we skip.
        """
        # Respect experimental override to not set visibility env.
        if os.getenv(NOSET_MBLT_VISIBLE_DEVICES_ENV_VAR):
            logger.debug(
                "Skipping setting %s due to %s.",
                MBLT_VISIBLE_DEVICES_ENV_VAR,
                NOSET_MBLT_VISIBLE_DEVICES_ENV_VAR,
            )
            return

        # Convert to comma-separated string; ensure string type for each id.
        id_str = ",".join(str(x).strip() for x in ids)
        os.environ[MBLT_VISIBLE_DEVICES_ENV_VAR] = id_str
        logger.debug("Set %s=%r", MBLT_VISIBLE_DEVICES_ENV_VAR, id_str)

    # ---- Optional cloud helpers (return None by default) --------------------------

    @staticmethod
    def get_ec2_instance_num_accelerators(
        instance_type: str, instances: dict
    ) -> Optional[int]:
        """Return number of MBLT cards for the EC2 instance type, if known."""
        return None

    @staticmethod
    def get_ec2_instance_accelerator_type(
        instance_type: str, instances: dict
    ) -> Optional[str]:
        """Return MBLT accelerator type for the EC2 instance type, if known."""
        return None

    @staticmethod
    def get_current_node_accelerator_labels() -> Optional[Dict[str, str]]:
        """Return Ray node labels related to MBLT accelerators, if any."""
        return None
