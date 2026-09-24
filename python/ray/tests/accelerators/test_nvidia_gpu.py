import os
import sys
from unittest.mock import MagicMock

import pytest

import ray
from ray._private.accelerators import NvidiaGPUAcceleratorManager
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


@pytest.mark.parametrize("mock_nvml", [mock_nvml])
def test_gpu_memory_label_unsupported(patch_mock_pynvml):
    assert NvidiaGPUAcceleratorManager.get_current_node_accelerator_labels() is None


@pytest.mark.parametrize(
    "mock_nvml",
    [
        PyNVMLMock(
            [
                DeviceHandleMock("NVIDIA L4", "GPU-0", memory_total=24 * 2**30),
                DeviceHandleMock("NVIDIA L4", "GPU-1", memory_total=23 * 2**30),
            ]
        )
    ],
)
def test_gpu_memory_label(patch_mock_pynvml):
    assert NvidiaGPUAcceleratorManager.get_current_node_accelerator_labels() == {
        ray._raylet.RAY_NODE_GPU_MEMORY_PER_DEVICE_KEY: str(23 * 2**30)
    }


@pytest.mark.parametrize(
    "noset,resource_ids,expected",
    [
        (False, [(3, 0.25)], "0=20480MB"),
        (True, [(3, 0.25)], "3=20480MB"),
        (False, [(0, 0.5), (1, 0.5)], None),
    ],
)
def test_mps_gpu_memory_limit(monkeypatch, noset, resource_ids, expected):
    monkeypatch.delenv("CUDA_MPS_PINNED_DEVICE_MEM_LIMIT", raising=False)
    monkeypatch.setenv("RAY_ENABLE_MPS_GPU_MEMORY_LIMIT", "1")
    if noset:
        monkeypatch.setenv("RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES", "1")
    core_worker = MagicMock()
    core_worker.resource_ids.return_value = {"GPU": resource_ids}
    monkeypatch.setattr(
        ray._private.worker.global_worker, "core_worker", core_worker, raising=False
    )

    original = ray._private.utils._set_gpu_memory_limit(20 * 2**30)
    assert os.environ.get("CUDA_MPS_PINNED_DEVICE_MEM_LIMIT") == expected
    if expected:
        assert original == {"CUDA_MPS_PINNED_DEVICE_MEM_LIMIT": None}


def test_mps_gpu_memory_limit_disabled(monkeypatch):
    monkeypatch.delenv("CUDA_MPS_PINNED_DEVICE_MEM_LIMIT", raising=False)
    monkeypatch.delenv("RAY_ENABLE_MPS_GPU_MEMORY_LIMIT", raising=False)
    assert ray._private.utils._set_gpu_memory_limit(20 * 2**30) == {}
    assert "CUDA_MPS_PINNED_DEVICE_MEM_LIMIT" not in os.environ


@pytest.mark.parametrize(
    "name,expected",
    [
        # Legacy datacenter GPU names: keep labels produced by the previous
        # parser stable.
        ("Tesla V100-SXM2-16GB", "V100"),
        ("Tesla P100-PCIE-16GB", "P100"),
        ("Tesla T4", "T4"),
        ("Tesla P4", "P4"),
        ("Tesla K80", "K80"),
        ("NVIDIA A10G", "A10G"),
        ("NVIDIA L4", "L4"),
        ("NVIDIA L40S", "L40S"),
        ("NVIDIA A100-SXM4-40GB", "A100"),
        ("NVIDIA H100 80GB HBM3", "H100"),
        ("NVIDIA H200", "H200"),
        ("NVIDIA H20", "H20"),
        ("NVIDIA B200", "B200"),
        ("NVIDIA B300", "B300"),
        ("NVIDIA GB200", "GB200"),
        ("NVIDIA GB300", "GB300"),
        # Consumer GPUs: the regex does not match the mixed-case product line,
        # so we fall back to a hyphen-joined product name.
        ("NVIDIA GeForce RTX 5090", "GeForce-RTX-5090"),
        ("NVIDIA GeForce RTX 4090", "GeForce-RTX-4090"),
        # RTX PRO cards: "RTX" alone is just a brand prefix, so the model is
        # captured through the first digit-containing token instead of
        # collapsing to the ambiguous "RTX".
        ("NVIDIA RTX PRO 6000 Blackwell Server Edition", "RTX-PRO-6000"),
        # Edge cases.
        (None, None),
        ("", None),
    ],
)
def test_gpu_name_to_accelerator_type(name, expected):
    assert NvidiaGPUAcceleratorManager._gpu_name_to_accelerator_type(name) == expected


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
