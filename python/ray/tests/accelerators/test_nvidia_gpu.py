import os
import sys
import pytest
from unittest.mock import patch
from typing import List

from ray._private.accelerators import NvidiaGPUAcceleratorManager
import ray._private.ray_constants as ray_constants
import ray._private.thirdparty.pynvml as pynvml


class DeviceHandleMock(dict):
    def __init__(
        self,
        name: str,
        uuid: str,
        mig_devices: List["DeviceHandleMock"] = None,
        **kwargs
    ):
        super().__init__()
        self["name"] = name
        self["uuid"] = uuid
        if mig_devices is not None:
            self["mig_devices"] = mig_devices
        self.update(kwargs)


GPU_MOCK_DATA = [
    DeviceHandleMock(
        "Tesla V100-SXM2-16GB", "GPU-8eaaebb8-bb64-8489-fda2-62256e821983"
    ),
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
]


# pnvml mock for gpu resources
class PyNVMLMock:
    def __init__(self, mock_data=GPU_MOCK_DATA, driver_version="535.104.12"):
        self._mock_data = mock_data
        self.driver_version = driver_version

    def nvmlInit(self):
        return

    def nvmlShutdown(self):
        return

    def nvmlSystemGetDriverVersion(self):
        return self.driver_version

    def nvmlDeviceGetCount(self):
        return len(self._mock_data)

    def nvmlDeviceGetHandleByIndex(self, index):
        return self._mock_data[index]

    def nvmlDeviceGetName(self, handle):
        return handle.get("name", "")

    def nvmlDeviceGetMaxMigDeviceCount(self, handle):
        if "mig_devices" in handle:
            return len(handle["mig_devices"])
        else:
            raise pynvml.NVMLError_NotSupported

    def nvmlDeviceGetMigDeviceHandleByIndex(self, handle, mig_index):
        return handle["mig_devices"][mig_index]

    def nvmlDeviceGetUUID(self, handle):
        return handle.get("uuid", "")

    def nvmlDeviceGetComputeInstanceId(self, mig_handle):
        return mig_handle["ci_id"]

    def nvmlDeviceGetGpuInstanceId(self, mig_handle):
        return mig_handle["gi_id"]


mock_nvml = PyNVMLMock()


@pytest.fixture
def patch_mock_pynvml():
    with patch("ray._private.thirdparty.pynvml.nvmlInit", mock_nvml.nvmlInit), patch(
        "ray._private.thirdparty.pynvml.nvmlShutdown", mock_nvml.nvmlShutdown
    ), patch(
        "ray._private.thirdparty.pynvml.nvmlSystemGetDriverVersion",
        mock_nvml.nvmlSystemGetDriverVersion,
    ), patch(
        "ray._private.thirdparty.pynvml.nvmlDeviceGetCount",
        mock_nvml.nvmlDeviceGetCount,
    ), patch(
        "ray._private.thirdparty.pynvml.nvmlDeviceGetHandleByIndex",
        mock_nvml.nvmlDeviceGetHandleByIndex,
    ), patch(
        "ray._private.thirdparty.pynvml.nvmlDeviceGetName", mock_nvml.nvmlDeviceGetName
    ), patch(
        "ray._private.thirdparty.pynvml.nvmlDeviceGetMaxMigDeviceCount",
        mock_nvml.nvmlDeviceGetMaxMigDeviceCount,
    ), patch(
        "ray._private.thirdparty.pynvml.nvmlDeviceGetMigDeviceHandleByIndex",
        mock_nvml.nvmlDeviceGetMigDeviceHandleByIndex,
    ), patch(
        "ray._private.thirdparty.pynvml.nvmlDeviceGetUUID", mock_nvml.nvmlDeviceGetUUID
    ), patch(
        "ray._private.thirdparty.pynvml.nvmlDeviceGetComputeInstanceId",
        mock_nvml.nvmlDeviceGetComputeInstanceId,
    ), patch(
        "ray._private.thirdparty.pynvml.nvmlDeviceGetGpuInstanceId",
        mock_nvml.nvmlDeviceGetGpuInstanceId,
    ):
        yield


@pytest.mark.usefixtures("patch_mock_pynvml")
def test_num_gpus_parsing():
    # without mig instance
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
    with patch.dict(
        os.environ, {ray_constants.RAY_ENABLE_MIG_DETECTION_ENV_VAR: "true"}
    ):
        assert NvidiaGPUAcceleratorManager.get_current_node_num_accelerators() == len(
            gpu_instances
        )


@pytest.mark.usefixtures("patch_mock_pynvml")
def test_gpu_info_parsing():
    assert NvidiaGPUAcceleratorManager.get_current_node_accelerator_type() == "A100"

    # mig instance should map to it's accelerator type
    with patch.dict(
        os.environ, {ray_constants.RAY_ENABLE_MIG_DETECTION_ENV_VAR: "true"}
    ):
        assert NvidiaGPUAcceleratorManager.get_current_node_accelerator_type() == "A100"


@pytest.mark.usefixtures("patch_mock_pynvml")
def test_auto_detect_visible_devices():
    # test auto detect updates the CUDA_VISIBLE_DEVICES with detected gpus

    # without mig instance
    NvidiaGPUAcceleratorManager.get_current_node_num_accelerators()
    detected_gpus = (
        NvidiaGPUAcceleratorManager.get_current_process_visible_accelerator_ids()
    )
    for i, gpu in enumerate(detected_gpus):
        assert gpu == str(i)

    # with mig instance
    with patch.dict(
        os.environ, {ray_constants.RAY_ENABLE_MIG_DETECTION_ENV_VAR: "true"}
    ):
        NvidiaGPUAcceleratorManager.get_current_node_num_accelerators()
        detected_gpus = (
            NvidiaGPUAcceleratorManager.get_current_process_visible_accelerator_ids()
        )
        index = 0
        for i, gpu in enumerate(GPU_MOCK_DATA):
            if "mig_devices" in gpu:
                for mig in gpu["mig_devices"]:
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
