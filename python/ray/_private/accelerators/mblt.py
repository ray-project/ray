# ray/_private/accelerators/mblt.py
import os
import logging
from typing import Dict, List, Optional, Tuple

from ray._private.accelerators.accelerator import AcceleratorManager

logger = logging.getLogger(__name__)

MBLT_VISIBLE_DEVICES = "MBLT_VISIBLE_DEVICES"
NOSET_MBLT_VISIBLE_DEVICES = "RAY_EXPERIMENTAL_NOSET_MBLT_VISIBLE_DEVICES"

class MBLTAcceleratorManager(AcceleratorManager):
    """Mobilint MBLT NPU support. Policy: only MBLT=1 per task/actor."""

    # 1) 리소스 이름
    @staticmethod
    def get_resource_name() -> str:
        return "MBLT"

    # 2) 가시 디바이스 ENV 이름
    @staticmethod
    def get_visible_accelerator_ids_env_var() -> str:
        return MBLT_VISIBLE_DEVICES

    # 3) 현재 노드의 MBLT 장치 개수
    @staticmethod
    def get_current_node_num_accelerators() -> int:
        try:
            # SDK: maccel.accelerator.Accelerator(dev_no)
            from maccel.accelerator import Accelerator
        except Exception as e:
            logger.debug("maccel import failed: %s", e)
            return 0

        count = 0
        # 보수적 스캔: 0..63
        for dev_no in range(64):
            try:
                acc = Accelerator(dev_no)
                # 존재 확인: 코어 조회가 성공하면 존재로 간주
                _ = acc.get_available_cores()
                count += 1
            except Exception:
                continue
        return count

    # 4) 현재 노드의 가속기 타입(선택적: API 없으면 None)
    @staticmethod
    def get_current_node_accelerator_type() -> Optional[str]:
        # 공개 타입 API가 확인되지 않았으므로 None 반환
        return None

    # 5) 추가 리소스(없음)
    @staticmethod
    def get_current_node_additional_resources() -> Optional[Dict[str, float]]:
        return None

    # 6) 수량 검증: 분수/2개 이상 거부, 오직 1만 허용
    @staticmethod
    def validate_resource_request_quantity(
        quantity: float,
    ) -> Tuple[bool, Optional[str]]:
        if quantity == 1 or quantity == 1.0:
            return (True, None)
        return (
            False,
            "Only MBLT=1 is supported; fractional or >1 are not allowed.",
        )

    # 7) 현재 프로세스에서 보이는 디바이스 ID들
    @staticmethod
    def get_current_process_visible_accelerator_ids() -> Optional[List[str]]:
        s = os.environ.get(MBLT_VISIBLE_DEVICES, None)
        if s is None:
            return None
        if s in ("", "NoDevFiles"):
            return []
        return s.split(",")

    # 8) 현재 프로세스 가시 디바이스 설정
    @staticmethod
    def set_current_process_visible_accelerator_ids(ids: List[str]) -> None:
        if os.environ.get(NOSET_MBLT_VISIBLE_DEVICES):
            return
        os.environ[MBLT_VISIBLE_DEVICES] = "" if not ids else str(ids[0])
