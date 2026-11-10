import os
import sys
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

import ray
from ray._private.accelerators import IluvatarGPUAcceleratorManager as Accelerator
from ray._private.test_utils import mock_accelerator_detection
from ray.tests.accelerators import mock_iluvatar_nvml


@pytest.fixture
def patch_iluvatar_nvml(monkeypatch):
    monkeypatch.setattr(
        "ray._private.accelerators.iluvatar_gpu._load_nvml",
        lambda: mock_iluvatar_nvml,
    )


@pytest.fixture
def clear_iluvatar_environment(monkeypatch):
    monkeypatch.delenv("CUDA_VISIBLE_DEVICES", raising=False)
    monkeypatch.delenv("RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES", raising=False)


@patch(
    "ray._private.accelerators.iluvatar_gpu._load_nvml",
    side_effect=OSError("libixml.so not found"),
)
def test_missing_library(mock_load_nvml):
    assert Accelerator.get_current_node_num_accelerators() == 0
    assert Accelerator.get_current_node_accelerator_type() is None


@pytest.mark.usefixtures("patch_iluvatar_nvml")
def test_num_accelerators():
    assert Accelerator.get_current_node_num_accelerators() == 1


@pytest.mark.usefixtures("patch_iluvatar_nvml")
def test_accelerator_type():
    assert Accelerator.get_current_node_accelerator_type() == "BI-V150"


def test_load_nvml_library(monkeypatch):
    from ray._private.accelerators import iluvatar_gpu
    from ray._private.thirdparty.pynvml import pynvml as nvidia_nvml

    monkeypatch.setattr(iluvatar_gpu, "_nvml", None)
    nvidia_library = nvidia_nvml.nvmlLib
    nvidia_cache = dict(nvidia_nvml._nvmlGetFunctionPointer_cache)
    nvidia_refcount = nvidia_nvml._nvmlLib_refcount

    def get_count(count):
        count._obj.value = 2
        return 0

    fake_library = SimpleNamespace(
        nvmlInitWithFlags=Mock(return_value=0),
        nvmlDeviceGetCount_v2=Mock(side_effect=get_count),
        nvmlShutdown=Mock(return_value=0),
    )
    with patch.object(iluvatar_gpu.ctypes, "CDLL", return_value=fake_library) as cdll:
        nvml = iluvatar_gpu._load_nvml()
        nvml.nvmlInit()
        assert nvml.nvmlDeviceGetCount() == 2
        assert nvml._nvmlLib_refcount == 1
        nvml.nvmlShutdown()
        assert nvml._nvmlLib_refcount == 0
        assert iluvatar_gpu._load_nvml() is nvml

    cdll.assert_called_once_with("libixml.so")
    fake_library.nvmlInitWithFlags.assert_called_once_with(0)
    fake_library.nvmlDeviceGetCount_v2.assert_called_once()
    fake_library.nvmlShutdown.assert_called_once()
    assert nvml.nvmlInit.__globals__["nvmlLib"] is fake_library
    assert nvidia_nvml.nvmlLib is nvidia_library
    assert nvidia_nvml._nvmlGetFunctionPointer_cache == nvidia_cache
    assert nvidia_nvml._nvmlLib_refcount == nvidia_refcount


@pytest.mark.parametrize("nvidia_first", [True, False])
def test_nvml_provider_order(monkeypatch, nvidia_first):
    from ray._private.accelerators import iluvatar_gpu
    from ray._private.thirdparty.pynvml import pynvml as nvidia_nvml

    monkeypatch.setattr(iluvatar_gpu, "_nvml", None)
    monkeypatch.setattr(nvidia_nvml, "_nvmlGetFunctionPointer_cache", {})
    monkeypatch.setattr(nvidia_nvml, "_nvmlLib_refcount", 0)
    nvidia_library = SimpleNamespace(
        nvmlInitWithFlags=Mock(return_value=0),
        nvmlShutdown=Mock(return_value=0),
    )
    iluvatar_library = SimpleNamespace(
        nvmlInitWithFlags=Mock(return_value=0),
        nvmlShutdown=Mock(return_value=0),
    )
    monkeypatch.setattr(nvidia_nvml, "nvmlLib", nvidia_library)

    with patch.object(iluvatar_gpu.ctypes, "CDLL", return_value=iluvatar_library):
        iluvatar_nvml = iluvatar_gpu._load_nvml()
    first, second = (
        (nvidia_nvml, iluvatar_nvml) if nvidia_first else (iluvatar_nvml, nvidia_nvml)
    )
    first.nvmlInit()
    assert first._nvmlLib_refcount == 1
    assert second._nvmlLib_refcount == 0
    assert second._nvmlGetFunctionPointer_cache == {}
    second.nvmlInit()
    first.nvmlShutdown()
    assert second._nvmlLib_refcount == 1
    second.nvmlShutdown()

    for library in (nvidia_library, iluvatar_library):
        library.nvmlInitWithFlags.assert_called_once_with(0)
        library.nvmlShutdown.assert_called_once()
    assert nvidia_nvml.nvmlLib is nvidia_library
    assert iluvatar_nvml.nvmlLib is iluvatar_library
    assert (
        nvidia_nvml._nvmlGetFunctionPointer_cache
        is not iluvatar_nvml._nvmlGetFunctionPointer_cache
    )


def test_load_nvml_retry(monkeypatch):
    from ray._private.accelerators import iluvatar_gpu

    monkeypatch.setattr(iluvatar_gpu, "_nvml", None)
    fake_library = object()
    with patch.object(
        iluvatar_gpu.ctypes,
        "CDLL",
        side_effect=[OSError("libixml.so not found"), fake_library],
    ) as cdll:
        with pytest.raises(OSError):
            iluvatar_gpu._load_nvml()
        assert iluvatar_gpu._nvml is None
        assert iluvatar_gpu._load_nvml().nvmlLib is fake_library
        assert cdll.call_count == 2


@pytest.mark.parametrize(
    "name,expected",
    [
        ("Iluvatar BI-V150", "BI-V150"),
        # Only the known vendor prefix is stripped, so future names that add
        # another token are not truncated to a single character.
        ("Iluvatar CoreX BI-V150S", "CoreX BI-V150S"),
        ("BI-V150S", "BI-V150S"),
        (None, None),
        ("", None),
    ],
)
def test_gpu_name_to_accelerator_type(name, expected):
    assert Accelerator._gpu_name_to_accelerator_type(name) == expected


@pytest.mark.usefixtures("clear_iluvatar_environment")
def test_get_current_process_visible_accelerator_ids(monkeypatch):
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "0,1,2")
    assert Accelerator.get_current_process_visible_accelerator_ids() == [
        "0",
        "1",
        "2",
    ]

    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "")
    assert Accelerator.get_current_process_visible_accelerator_ids() == []

    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "NoDevFiles")
    assert Accelerator.get_current_process_visible_accelerator_ids() == []

    monkeypatch.delenv("CUDA_VISIBLE_DEVICES")
    assert Accelerator.get_current_process_visible_accelerator_ids() is None


@pytest.mark.usefixtures("clear_iluvatar_environment")
def test_set_current_process_visible_accelerator_ids():
    Accelerator.set_current_process_visible_accelerator_ids(["0", "1"])
    assert os.environ["CUDA_VISIBLE_DEVICES"] == "0,1"

    os.environ["RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES"] = "1"
    Accelerator.set_current_process_visible_accelerator_ids(["2", "3"])
    assert os.environ["CUDA_VISIBLE_DEVICES"] == "0,1"


@pytest.mark.skipif(sys.platform == "win32", reason="Not supported on Windows")
def test_visible_iluvatar_gpu_ids(monkeypatch, shutdown_only):
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "0,1,2")
    with mock_accelerator_detection(Accelerator, num_accelerators=4), patch.object(
        Accelerator,
        "get_current_node_accelerator_type",
        return_value="BI-V150",
    ):
        ray.init()
        assert ray.available_resources()["GPU"] == 3


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
