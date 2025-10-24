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
_MBLT_DEV_GLOB = "/dev/aries[0-9]*"       # 환경에 따라 /dev/mblt[0-9]* 등으로 조정
_MBLT_PCI_FILTER = ["lspci", "-d", "209f:", "-nn"]  # 0x209f = Mobilint

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

    # 3) 현재 노드의 MBLT 장치 개수 (SDK 우선, 실패 시 OS 폴백)
    @staticmethod
    def get_current_node_num_accelerators() -> int:
        # (a) SDK 기반(정확)
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

        # (b) OS 폴백(근사)
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

    # 4) 현재 노드의 가속기 타입 (SDK 우선, 실패 시 lspci 파싱)
    @staticmethod
    def get_current_node_accelerator_type() -> Optional[str]:
        # (a) SDK에 이름/모델 API가 있으면 사용
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

        # (b) lspci 문자열 파싱(예: "Mobilint, Inc. Aries")
        try:
            out = subprocess.check_output(_MBLT_PCI_FILTER, text=True, stderr=subprocess.DEVNULL).strip()
            line = out.splitlines()[0] if out else ""
            if not line:
                return None
            m = re.search(r"]:\s*(.+?)\s*\[", line)
            return (m.group(1).strip() if m else line) or None
        except Exception:
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
        return (False, "Only MBLT=1 is supported; fractional or >1 are not allowed.")

    # 7) 현재 프로세스에서 보이는 디바이스 ID들
    @staticmethod
    def get_current_process_visible_accelerator_ids() -> Optional[List[str]]:
        s = os.environ.get(MBLT_VISIBLE_DEVICES, None)
        if s is None:
            return None
        if s in ("", "NoDevFiles"):
            return []
        return s.split(",")

    # 8) 현재 프로세스 가시 디바이스 설정(단일 ID 정책)
    @staticmethod
    def set_current_process_visible_accelerator_ids(ids: List[str]) -> None:
        if os.environ.get(NOSET_MBLT_VISIBLE_DEVICES):
            return
        os.environ[MBLT_VISIBLE_DEVICES] = "" if not ids else str(ids[0])


# ---- 내부 헬퍼: OS 힌트로 스캔 범위 축소 ----
def _hint_num_from_os() -> Optional[int]:
    # /dev 노드 수
    try:
        devs = glob.glob(_MBLT_DEV_GLOB)
        if devs:
            return len(devs)
    except Exception:
        pass
    # lspci 라인 수
    try:
        out = subprocess.check_output(_MBLT_PCI_FILTER, text=True, stderr=subprocess.DEVNULL).strip()
        if out:
            return len(out.splitlines())
    except Exception:
        pass
    return None
