import os
import sys
import pytest
from unittest.mock import patch

from ray._private.accelerators import NvidiaGPUAcceleratorManager
import ray._private.thirdparty.pynvml as pynvml
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
                ci_id=0,
            ),
            DeviceHandleMock(
                "Ampere A100-SXM4-40GB MIG 1g.5gb",
                "MIG-0c757cd7-e942-5726-a0b8-0e8fb7067135",
                gi_id=1,
                ci_id=0,
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
                ci_id=0,
            )
        ],
    ),
    DeviceHandleMock(
        "Tesla V100-SXM2-16GB", "GPU-8eaaebb8-bb64-8489-fda2-62256e821983"
    ),
]

OLD_DRIVER_VERSION = "470.41.01"

mock_nvml = PyNVMLMock(GPU_MOCK_DATA)
mock_nvml_old_driver = PyNVMLMock(GPU_MOCK_DATA, OLD_DRIVER_VERSION)
patch_mock_pynvml = patch_mock_pynvml  # avoid format error


@pytest.mark.parametrize("mock_nvml", [mock_nvml])
def test_num_gpus_parsing(patch_mock_pynvml):
    # without mig instance
    with patch("ray._private.ray_constants.RAY_ENABLE_MIG_DETECTION", False):
        assert NvidiaGPUAcceleratorManager.get_current_node_num_accelerators() == len(
            GPU_MOCK_DATA
        )

    # with mig instance
    gpu_instances = []
    for gpu in GPU_MOCK_DATA:
        if "mig_devices" in gpu:
            for mig in gpu["mig_devices"]:
                gpu_instances.append(mig)
        else:
            gpu_instances.append(gpu)
    assert NvidiaGPUAcceleratorManager.get_current_node_num_accelerators() == len(
        gpu_instances
    )


@pytest.mark.parametrize("mock_nvml", [mock_nvml])
def test_gpu_info_parsing(patch_mock_pynvml):
    with patch("ray._private.ray_constants.RAY_ENABLE_MIG_DETECTION", False):
        assert NvidiaGPUAcceleratorManager.get_current_node_accelerator_type() == "A100"

    # mig instance should map to it's accelerator type
    assert NvidiaGPUAcceleratorManager.get_current_node_accelerator_type() == "A100"


@pytest.mark.parametrize("mock_nvml", [mock_nvml, mock_nvml_old_driver])
def test_auto_detect_visible_devices(patch_mock_pynvml):
    # test auto detect updates the CUDA_VISIBLE_DEVICES with detected gpus

    # without mig instance
    with patch("ray._private.ray_constants.RAY_ENABLE_MIG_DETECTION", False):
        NvidiaGPUAcceleratorManager.get_current_node_num_accelerators()
        detected_gpus = (
            NvidiaGPUAcceleratorManager.get_current_process_visible_accelerator_ids()
        )
        for i, gpu in enumerate(detected_gpus):
            assert gpu == str(i)

    # with mig instance
    NvidiaGPUAcceleratorManager.get_current_node_num_accelerators()
    detected_gpus = (
        NvidiaGPUAcceleratorManager.get_current_process_visible_accelerator_ids()
    )
    index = 0
    for i, gpu in enumerate(GPU_MOCK_DATA):
        if "mig_devices" in gpu:
            for mig in gpu["mig_devices"]:
                if pynvml.nvmlSystemGetDriverVersion() == OLD_DRIVER_VERSION:
                    assert (
                        detected_gpus[index]
                        == f"MIG-{gpu['uuid']}/{mig['gi_id']}/{mig['ci_id']}"
                    )
                else:
                    assert detected_gpus[index] == mig["uuid"]
                index += 1
        else:
            assert detected_gpus[index] == str(i)
            index += 1


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
