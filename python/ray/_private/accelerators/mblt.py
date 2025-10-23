# mblt_npu.py
import os
import logging
from typing import List, Optional, Tuple
from ray._private.accelerators.accelerator import AcceleratorManager

logger = logging.getLogger(__name__)

MBLT_VISIBLE_DEVICES = "MBLT_VISIBLE_DEVICES"
NOSET_MBLT_VISIBLE_DEVICES = "RAY_EXPERIMENTAL_NOSET_MBLT_VISIBLE_DEVICES"

class MBLTAcceleratorManager(AcceleratorManager):
    """Mobilint MBLT NPU accelerators. Policy: only MBLT=1 per task/actor."""

    # ── 리소스/ENV 키 ──────────────────────────────────────────────────────────
    @staticmethod
    def get_resource_name() -> str:
        return "MBLT"

    @staticmethod
    def get_visible_accelerator_ids_env_var() -> str:
        return MBLT_VISIBLE_DEVICES

    # ── 프로세스 가시 디바이스 목록 ────────────────────────────────────────────
    @staticmethod
    def get_current_process_visible_accelerator_ids() -> Optional[List[str]]:
        s = os.environ.get(MBLT_VISIBLE_DEVICES, None)
        if s is None:
            return None
        if s in ("", "NoDevFiles"):
            return []
        return s.split(",")

    # ── 노드 장치 개수 탐지(클린, SDK만 사용) ───────────────────────────────────
    @staticmethod
    def get_current_node_num_accelerators() -> int:
        """
        maccel.accelerator.Accelerator(dev_no) 생성 + get_available_cores() 호출이
        성공하면 dev_no가 존재하는 것으로 간주. 0..63 스캔(충분히 보수적).
        """
        try:
            from maccel.accelerator import Accelerator
        except Exception as e:
            logger.debug("maccel import failed: %s", e)
            return 0

        count = 0
        # 상한은 보수적으로 64. 필요시 늘리거나 /dev 스캔으로 대체 가능.
        for dev_no in range(64):
            try:
                acc = Accelerator(dev_no)
                # 실제 존재 확인: 코어 목록 조회가 예외 없이 리턴되면 valid
                _ = acc.get_available_cores()  # List[CoreId]
                count += 1
            except Exception:
                # 존재하지 않는 dev_no에선 보통 예외 발생 → 스킵
                continue
        return count

    # ── 장치 타입(옵션: 없으면 None) ───────────────────────────────────────────
    @staticmethod
    def get_current_node_accelerator_type() -> Optional[str]:
        """
        공개된 타입 API가 없으므로 None 반환.
        SDK에 장치명 API가 추가되면 여기서 반환.
        """
        return None

    # ── 분수/다중 거부: MBLT=1만 허용 ──────────────────────────────────────────
    @staticmethod
    def validate_resource_request_quantity(quantity: float) -> Tuple[bool, Optional[str]]:
        if quantity == 1 or quantity == 1.0:
            return (True, None)
        return (False, "Only MBLT=1 is supported; fractional or >1 are not allowed.")

    # ── 프로세스 가시 디바이스 설정(ENV) ───────────────────────────────────────
    @staticmethod
    def set_current_process_visible_accelerator_ids(visible_ids: List[str]) -> None:
        if os.environ.get(NOSET_MBLT_VISIBLE_DEVICES):
            return
        os.environ[MBLT_VISIBLE_DEVICES] = "" if not visible_ids else str(visible_ids[0])
