import pytest
from typing import List
from unittest.mock import patch

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


# pnvml mock for gpu resources
class PyNVMLMock:
    def __init__(self, mock_data, driver_version="535.104.12"):
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
            return max(7, len(handle["mig_devices"]))
        else:
            raise pynvml.NVMLError_NotSupported

    def nvmlDeviceGetMigDeviceHandleByIndex(self, handle, mig_index):
        try:
            return handle["mig_devices"][mig_index]
        except IndexError:
            raise pynvml.NVMLError_NotFound

    def nvmlDeviceGetUUID(self, handle):
        return handle.get("uuid", "")

    def nvmlDeviceGetComputeInstanceId(self, mig_handle):
        return mig_handle["ci_id"]

    def nvmlDeviceGetGpuInstanceId(self, mig_handle):
        return mig_handle["gi_id"]


@pytest.fixture
def patch_mock_pynvml(mock_nvml):
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
