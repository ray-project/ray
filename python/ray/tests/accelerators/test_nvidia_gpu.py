import os
import sys
import pytest

from ray._private.accelerators import NvidiaACCAcceleratorManager
from ray.tests.accelerators.mock_pynvml import (
    DeviceHandleMock,
    PyNVMLMock,
    patch_mock_pynvml,
)

ACC_MOCK_DATA = [
    DeviceHandleMock(
        "Ampere A100-SXM4-40GB",
        "ACC-8eaaebb8-bb64-8489-fda2-62256e821983",
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
        "ACC-8eaaebb8-bb64-8489-fda2-62256e821983",
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
        "Tesla V100-SXM2-16GB", "ACC-8eaaebb8-bb64-8489-fda2-62256e821983"
    ),
]

mock_nvml = PyNVMLMock(ACC_MOCK_DATA)

patch_mock_pynvml = patch_mock_pynvml  # avoid format error


@pytest.mark.parametrize("mock_nvml", [mock_nvml])
def test_num_accs_parsing(patch_mock_pynvml):
    # without mig instance
    assert NvidiaACCAcceleratorManager.get_current_node_num_accelerators() == len(
        ACC_MOCK_DATA
    )


@pytest.mark.parametrize("mock_nvml", [mock_nvml])
def test_acc_info_parsing(patch_mock_pynvml):
    assert NvidiaACCAcceleratorManager.get_current_node_accelerator_type() == "A100"


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
