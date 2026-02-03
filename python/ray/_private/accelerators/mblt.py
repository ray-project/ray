import os
import re
import glob
import subprocess
import logging
import tempfile
from typing import Dict, List, Optional, Tuple

from ray._private.accelerators.accelerator import AcceleratorManager

logger = logging.getLogger(__name__)

# 환경 변수 정의
MBLT_VISIBLE_DEVICES = "MBLT_VISIBLE_DEVICES"
NOSET_MBLT_VISIBLE_DEVICES = "RAY_EXPERIMENTAL_NOSET_MBLT_VISIBLE_DEVICES"

# 매뉴얼 v1.0.0 기반 지원 디바이스 노드 및 PCI 필터 [3-5]
# Mobilint NPU는 Aries, Regulus, Aries2 시리즈를 지원합니다.
_MBLT_DEV_PATTERNS = ["/dev/aries*", "/dev/regulus*"]
_MBLT_PCI_FILTER = ["lspci", "-d", "209f:", "-nn"]  # 0x209f = Mobilint Vendor ID

class MBLTAcceleratorManager(AcceleratorManager):
    """Mobilint MBLT NPU support. 
    
    Ray 스케줄러를 통한 논리적 자원 분할(Fractional Resource)을 지원하도록 구성됨.
    참조 문서: MCS002-KR_SDK_qb_Compiler_v1.0.0
    """

    @staticmethod
    def get_resource_name() -> str:
        return "MBLT"

    @staticmethod
    def get_visible_accelerator_ids_env_var() -> str:
        return MBLT_VISIBLE_DEVICES

    @staticmethod
    def get_current_node_num_accelerators() -> int:
        """현재 노드에 장착된 Mobilint NPU 인스턴스 개수 탐색"""
        # 1. maccel 라이브러리 시도 (드라이버 레벨)
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
            logger.debug("maccel import failed: %s", e)

        # 2. 디바이스 노드 기반 탐색 (Aries/Regulus) [3, 4]
        try:
            devs = []
            for pattern in _MBLT_DEV_PATTERNS:
                devs.extend(glob.glob(pattern))
            if devs:
                return len(devs)
        except Exception as e:
            logger.debug("dev glob fallback failed: %s", e)

        # 3. PCI 버스 기반 탐색
        try:
            out = subprocess.check_output(_MBLT_PCI_FILTER, text=True).strip()
            if out:
                return len(out.splitlines())
        except Exception as e:
            logger.debug("lspci fallback failed: %s", e)

        return 0

    @staticmethod
    def get_current_node_accelerator_type() -> Optional[str]:
        """장치 모델명 탐색 (Aries, Regulus 등) [4, 5]"""
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
            line = out.splitlines() if out else ""
            if not line:
                return None
            # PCI 정보에서 모델명 추출 시도
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
        """
        Fractional Resource 지원을 위해 소수점 요청을 허용함.
        LLM 모델의 경우 공유 자원 환경에서 1.0 이하의 값을 가질 수 있음.
        """
        if 0 < quantity <= 1.0:
            return (True, None)
        if quantity > 1.0 and quantity.is_integer():
            return (True, None)
        
        return (False, "MBLT resource quantity must be <= 1.0 (fractional) or an integer > 1.")

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
        # Ray Actor/Task에 할당된 ID 설정
        os.environ[MBLT_VISIBLE_DEVICES] = ",".join(map(str, ids)) if ids else ""


def _hint_num_from_os() -> Optional[int]:
    """OS 환경에서 감지되는 장치 개수 힌트 반환"""
    try:
        count = 0
        for pattern in _MBLT_DEV_PATTERNS:
            count += len(glob.glob(pattern))
        if count > 0:
            return count
    except Exception:
        pass
    try:
        out = subprocess.check_output(_MBLT_PCI_FILTER, text=True, stderr=subprocess.DEVNULL).strip()
        if out:
            return len(out.splitlines())
    except Exception:
        pass
    return None
