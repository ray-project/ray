import os
import sys
from unittest.mock import patch

import pytest
import ray
from ray._private.accelerators import MBLTAcceleratorManager as Accelerator

# --- 1) 자동 탐지: maccel 가짜 모듈로 64개 디바이스 흉내 ---
def _install_fake_maccel(num_devices: int, fail=False):
    """
    sys.modules에 'maccel' 패키지와 'maccel.accelerator' 서브모듈을 주입.
    Accelerator(dev_no) 생성 시:
      - 0 <= dev_no < num_devices 이면 get_available_cores() 정상 동작
      - 그 외/또는 fail=True 이면 예외 발생
    """
    import types

    maccel_pkg = types.ModuleType("maccel")
    accel_mod = types.ModuleType("maccel.accelerator")

    class AcceleratorFake:
        def __init__(self, dev_no=0):
            if fail or dev_no < 0 or dev_no >= num_devices:
                raise RuntimeError("No such device")
            self.dev_no = dev_no

        def get_available_cores(self):
            # 단순히 존재 확인 용도
            return [("dummy_core", 0)]

    accel_mod.Accelerator = AcceleratorFake
    maccel_pkg.accelerator = accel_mod

    sys.modules["maccel"] = maccel_pkg
    sys.modules["maccel.accelerator"] = accel_mod


def test_autodetect_num_mblt_devices():
    with patch.dict(sys.modules, clear=False):
        _install_fake_maccel(num_devices=64, fail=False)
        assert Accelerator.get_current_node_num_accelerators() == 64


def test_autodetect_num_mblt_devices_without_any():
    with patch.dict(sys.modules, clear=False):
        _install_fake_maccel(num_devices=0, fail=True)
        assert Accelerator.get_current_node_num_accelerators() == 0


# --- 2) 매니저 API 표면 계약 ---
def test_mblt_accelerator_manager_api():
    assert Accelerator.get_resource_name() == "MBLT"
    assert Accelerator.get_visible_accelerator_ids_env_var() == "MBLT_VISIBLE_DEVICES"
    # 정책: MBLT=1만 허용
    assert Accelerator.validate_resource_request_quantity(1) == (True, None)
    assert Accelerator.validate_resource_request_quantity(0.5)[0] is False
    assert Accelerator.validate_resource_request_quantity(2)[0] is False


def test_visible_mblt_type(monkeypatch, shutdown_only):
    # 타입 API는 None일 수 있으므로 patch로 임의 타입을 반환하게 확인
    with patch.object(
        Accelerator, "get_current_node_num_accelerators", return_value=4
    ), patch.object(
        Accelerator, "get_current_node_accelerator_type", return_value="ARIES"
    ):
        monkeypatch.setenv("MBLT_VISIBLE_DEVICES", "0,1,2")
        manager = ray._private.accelerators.get_accelerator_manager_for_resource("MBLT")
        assert manager.get_current_node_accelerator_type() == "ARIES"


@pytest.mark.skipif(sys.platform == "win32", reason="Not supported mock on Windows")
@pytest.mark.skipif(
    sys.version_info >= (3, 12),
    reason="Not passing on Python 3.12. Being followed up by external contributors.",
)
def test_visible_mblt_ids(monkeypatch, shutdown_only):
    with patch.dict(sys.modules, clear=False):
        _install_fake_maccel(num_devices=4, fail=False)

        monkeypatch.setenv("MBLT_VISIBLE_DEVICES", "0,1,2")
        with patch.object(
            Accelerator, "get_current_node_num_accelerators", return_value=4
        ):
            ray.init()
            manager = ray._private.accelerators.get_accelerator_manager_for_resource(
                "MBLT"
            )
            assert manager.get_current_node_num_accelerators() == 4
            assert manager.__name__ == "MBLTAcceleratorManager"
            # visible ids = 3 → 리소스 3
            assert ray.available_resources()["MBLT"] == 3


def test_get_current_process_visible_accelerator_ids(monkeypatch, shutdown_only):
    monkeypatch.setenv("MBLT_VISIBLE_DEVICES", "0,1,2")
    assert Accelerator.get_current_process_visible_accelerator_ids() == ["0", "1", "2"]

    monkeypatch.delenv("MBLT_VISIBLE_DEVICES", raising=False)
    assert Accelerator.get_current_process_visible_accelerator_ids() is None

    monkeypatch.setenv("MBLT_VISIBLE_DEVICES", "")
    assert Accelerator.get_current_process_visible_accelerator_ids() == []

    monkeypatch.setenv("MBLT_VISIBLE_DEVICES", "NoDevFiles")
    assert Accelerator.get_current_process_visible_accelerator_ids() == []


def test_set_current_process_visible_accelerator_ids(shutdown_only):
    # 구현이 "첫번째만 반영" 정책이면 아래처럼 검증
    Accelerator.set_current_process_visible_accelerator_ids(["0"])
    assert os.environ["MBLT_VISIBLE_DEVICES"] == "0"

    Accelerator.set_current_process_visible_accelerator_ids(["0", "1"])
    # 첫번째만 반영 → "0"
    assert os.environ["MBLT_VISIBLE_DEVICES"] == "0"

    Accelerator.set_current_process_visible_accelerator_ids(["0", "1", "2"])
    assert os.environ["MBLT_VISIBLE_DEVICES"] == "0"


@pytest.mark.skipif(sys.platform == "win32", reason="Not supported mock on Windows")
@pytest.mark.skipif(
    sys.version_info >= (3, 12),
    reason="Not passing on Python 3.12. Being followed up by external contributors.",
)
def test_auto_detected_more_than_visible(monkeypatch, shutdown_only):
    with patch.dict(sys.modules, clear=False):
        _install_fake_maccel(num_devices=4, fail=False)

        with patch.object(
            Accelerator, "get_current_node_num_accelerators", return_value=4
        ):
            # 자동 탐지(4) > visible(3) → 리소스 3
            monkeypatch.setenv("MBLT_VISIBLE_DEVICES", "0,1,2")
            ray.init()
            assert ray.available_resources()["MBLT"] == 3


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
