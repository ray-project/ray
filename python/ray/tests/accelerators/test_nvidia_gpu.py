import os
import sys
from unittest.mock import patch

import pytest

from ray._private.accelerators import NvidiaGPUAcceleratorManager
from ray._private.accelerators.nvidia_gpu import (
    _count_cuda_visible_devices,
    _detect_nvidia_device_files,
)
from ray.tests.accelerators.mock_pynvml import (
    DeviceHandleMock,
    PyNVMLMock,
    patch_mock_pynvml,
)

GPU_MOCK_DATA = [
    DeviceHandleMock(
        "Ampere A100-SXM4-40GB",
        "GPU-8eaaebb8-bb64-8489-fda2-62256e821983",
        mig_devices=[
            DeviceHandleMock(
                "Ampere A100-SXM4-40GB MIG 1g.5gb",
                "MIG-c6d4f1ef-42e4-5de3-91c7-45d71c87eb3f",
                gi_id=0,
                ci_instance=0,
            ),
            DeviceHandleMock(
                "Ampere A100-SXM4-40GB MIG 1g.5gb",
                "MIG-0c757cd7-e942-5726-a0b8-0e8fb7067135",
                gi_id=1,
                ci_instance=0,
            ),
        ],
    ),
    DeviceHandleMock(
        "Ampere A100-SXM4-40GB",
        "GPU-8eaaebb8-bb64-8489-fda2-62256e821983",
        mig_devices=[
            DeviceHandleMock(
                "Ampere A100-SXM4-40GB MIG 1g.5gb",
                "MIG-a28ad590-3fda-56dd-84fc-0a0b96edc58d",
                gi_id=0,
                ci_instance=0,
            )
        ],
    ),
    DeviceHandleMock(
        "Tesla V100-SXM2-16GB", "GPU-8eaaebb8-bb64-8489-fda2-62256e821983"
    ),
]

mock_nvml = PyNVMLMock(GPU_MOCK_DATA)

patch_mock_pynvml = patch_mock_pynvml  # avoid format error


@pytest.mark.parametrize("mock_nvml", [mock_nvml])
def test_num_gpus_parsing(patch_mock_pynvml):
    # without mig instance
    assert NvidiaGPUAcceleratorManager.get_current_node_num_accelerators() == len(
        GPU_MOCK_DATA
    )


@pytest.mark.parametrize("mock_nvml", [mock_nvml])
def test_gpu_info_parsing(patch_mock_pynvml):
    assert NvidiaGPUAcceleratorManager.get_current_node_accelerator_type() == "A100"


def test_count_cuda_visible_devices():
    """Test counting GPUs from CUDA_VISIBLE_DEVICES environment variable."""
    # Test with no env var set
    with patch.dict(os.environ, {}, clear=True):
        assert _count_cuda_visible_devices() == 0

    # Test with empty env var
    with patch.dict(os.environ, {"CUDA_VISIBLE_DEVICES": ""}):
        assert _count_cuda_visible_devices() == 0

    # Test with NoDevFiles marker
    with patch.dict(os.environ, {"CUDA_VISIBLE_DEVICES": "NoDevFiles"}):
        assert _count_cuda_visible_devices() == 0

    # Test with single device
    with patch.dict(os.environ, {"CUDA_VISIBLE_DEVICES": "0"}):
        assert _count_cuda_visible_devices() == 1

    # Test with multiple devices
    with patch.dict(os.environ, {"CUDA_VISIBLE_DEVICES": "0,1,2,3"}):
        assert _count_cuda_visible_devices() == 4

    # Test with UUIDs
    with patch.dict(
        os.environ,
        {"CUDA_VISIBLE_DEVICES": "GPU-abc123,GPU-def456"},
    ):
        assert _count_cuda_visible_devices() == 2


def test_detect_nvidia_device_files():
    """Test detecting NVIDIA GPUs from /dev/nvidia* device files."""
    # Test with no device files (mock glob to return empty list)
    with patch("glob.glob", return_value=[]):
        assert _detect_nvidia_device_files() == 0

    # Test with device files present
    with patch(
        "glob.glob", return_value=["/dev/nvidia0", "/dev/nvidia1", "/dev/nvidia2"]
    ):
        assert _detect_nvidia_device_files() == 3

    # Test with glob raising an exception
    with patch("glob.glob", side_effect=OSError("Permission denied")):
        assert _detect_nvidia_device_files() == 0


def test_nvml_failure_with_device_files_warning(caplog):
    """Test that a warning is logged when NVML fails but device files exist."""
    import ray._private.thirdparty.pynvml as pynvml

    def mock_nvml_init_fail():
        raise pynvml.NVMLError(pynvml.NVML_ERROR_DRIVER_NOT_LOADED)

    with patch(
        "ray._private.thirdparty.pynvml.nvmlInit", side_effect=mock_nvml_init_fail
    ), patch(
        "ray._private.accelerators.nvidia_gpu._detect_nvidia_device_files",
        return_value=4,
    ), patch(
        "ray._private.accelerators.nvidia_gpu._count_cuda_visible_devices",
        return_value=4,
    ):
        import logging

        caplog.set_level(logging.WARNING)
        result = NvidiaGPUAcceleratorManager.get_current_node_num_accelerators()

        # Should return 0 since NVML failed
        assert result == 0
        # Should have logged a warning about detected GPUs
        assert "NVML failed to detect GPUs" in caplog.text
        assert "4 device file(s)" in caplog.text
        assert "--num-gpus=4" in caplog.text


def test_nvml_failure_no_evidence_no_warning(caplog):
    """Test that only debug log when NVML fails and no GPU evidence exists."""
    import ray._private.thirdparty.pynvml as pynvml

    def mock_nvml_init_fail():
        raise pynvml.NVMLError(pynvml.NVML_ERROR_LIBRARY_NOT_FOUND)

    with patch(
        "ray._private.thirdparty.pynvml.nvmlInit", side_effect=mock_nvml_init_fail
    ), patch(
        "ray._private.accelerators.nvidia_gpu._detect_nvidia_device_files",
        return_value=0,
    ), patch(
        "ray._private.accelerators.nvidia_gpu._count_cuda_visible_devices",
        return_value=0,
    ):
        import logging

        caplog.set_level(logging.DEBUG)
        result = NvidiaGPUAcceleratorManager.get_current_node_num_accelerators()

        # Should return 0
        assert result == 0
        # Should not have warning level log
        assert "NVML failed to detect GPUs" not in caplog.text


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
