# tests/test_mblt.py
import os
import sys
from unittest.mock import patch

import pytest
import ray
from ray._private.accelerators import MBLTAcceleratorManager as Accelerator

# --- helper: mock_mblt를 maccel 네임스페이스에 주입 ---
def _inject_mock_mblt(num_devices: int, *, install=True):
    import importlib
    import types
    mock = importlib.import_module("tests.mock_mblt") if "tests.mock_mblt" in sys.modules else importlib.import_module("mock_mblt")
    # 동적으로 디바이스 개수 변경
    setattr(mock, "NUM_DEVICES", num_devices)

    # maccel 패키지/서브모듈로 주입
    maccel_pkg = types.ModuleType("maccel")
    accel_mod = types.ModuleType("maccel.accelerator")
    accel_mod.Accelerator = mock.accelerator.Accelerator

    if install:
        sys.modules["maccel"] = maccel_pkg
        sys.modules["maccel.accelerator"] = accel_mod
    return mock


def test_autodetect_num_mblt_devices():
    with patch.dict(sys.modules, clear=False):
        _inject_mock_mblt(num_devices=64)
        assert Accelerator.get_current_node_num_accelerators() == 64


def test_autodetect_num_mblt_devices_without_any():
    with patch.dict(sys.modules, clear=False):
        # 존재하지 않게 만들기: 존재 확인 시 항상 예외
        import types
        maccel_pkg = types.ModuleType("maccel")
        accel_mod = types.ModuleType("maccel.accelerator")

        class _AccelFail:
            def __init__(self, dev_no=0):
                raise RuntimeError("No devices")

        accel_mod.Accelerator = _AccelFail
        sys.modules["maccel"] = maccel_pkg
        sys.modules["maccel.accelerator"] = accel_mod

        assert Accelerator.get_current_node_num_accelerators() == 0


def test_mblt_accelerator_manager_api():
    assert Accelerator.get_resource_name() == "MBLT"
    assert Accelerator.get_visible_accelerator_ids_env_var() == "MBLT_VISIBLE_DEVICES"
    # 정책: 오직 1만 허용
    assert Accelerator.validate_resource_request_quantity(1) == (True, None)
    assert Accelerator.validate_resource_request_quantity(0.5)[0] is False
    assert Accelerator.validate_resource_request_quantity(2)[0] is False


def test_visible_mblt_type(monkeypatch, shutdown_only):
    with patch.object(Accelerator, "get_current_node_num_accelerators", return_value=4), \
         patch.object(Accelerator, "get_current_node_accelerator_type", return_value="ARIES"):
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
        _inject_mock_mblt(num_devices=4)
        monkeypatch.setenv("MBLT_VISIBLE_DEVICES", "0,1,2")

        with patch.object(Accelerator, "get_current_node_num_accelerators", return_value=4):
            ray.init()
            manager = ray._private.accelerators.get_accelerator_manager_for_resource("MBLT")
            assert manager.get_current_node_num_accelerators() == 4
            assert manager.__name__ == "MBLTAcceleratorManager"
            assert ray.available_resources()["MBLT"] == 3  # visible 3개


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
    # 구현이 "첫 번째만 반영"이라면 아래 assert가 맞음.
    Accelerator.set_current_process_visible_accelerator_ids(["0"])
    assert os.environ["MBLT_VISIBLE_DEVICES"] == "0"

    Accelerator.set_current_process_visible_accelerator_ids(["0", "1"])
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
        _inject_mock_mblt(num_devices=4)
        with patch.object(Accelerator, "get_current_node_num_accelerators", return_value=4):
            monkeypatch.setenv("MBLT_VISIBLE_DEVICES", "0,1,2")
            ray.init()
            assert ray.available_resources()["MBLT"] == 3


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
