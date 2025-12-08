import collections
import os
import sys

import pytest
import torch

import ray
from ray._common.test_utils import assert_tensors_equivalent

USE_GPU = os.environ.get("RAY_PYTEST_USE_GPU") == "1"


@pytest.mark.skipif(not USE_GPU, reason="Skipping GPU Test")
def test_gpu_tensor_serialization(ray_start_cluster_with_zero_copy_tensors):
    if not torch.cuda.is_available():
        pytest.skip("CUDA not available")

    # === GPU tensor core cases ===
    PRIMITIVE_GPU_OBJECTS = [
        torch.tensor(42, device="cuda"),
        torch.tensor(3.14159, device="cuda"),
        torch.tensor(True, device="cuda"),
        torch.tensor([1, 2, 3], device="cuda"),
        torch.randn(3, 4, device="cuda"),
        torch.randint(0, 10, (2, 3), device="cuda"),
        torch.zeros(5, dtype=torch.int64, device="cuda"),
        torch.ones(2, 2, dtype=torch.float32, device="cuda"),
        torch.randn(1, 1, 1, 1, device="cuda"),
        torch.randn(2, 3, 4, 5, 6, device="cuda"),
        torch.arange(8, device="cuda").reshape(2, 2, 2),
        torch.tensor(99, device="cuda").expand(1, 3, 1),
        torch.zeros(2, 0, 4, device="cuda"),
        # Empty and strided on GPU
        torch.tensor([], device="cuda"),
        torch.zeros(0, 5, device="cuda"),
        torch.arange(12, device="cuda").reshape(3, 4).t(),
        torch.randn(4, 4, device="cuda")[:, :2],
    ]

    REPRESENTATIVE_COMPLEX_GPU_OBJECTS = [
        # List of GPU tensors
        [torch.tensor([1, 2], device="cuda"), torch.randn(3, device="cuda")],
        # Dict with GPU tensors
        {
            "features": torch.randn(100, device="cuda"),
            "labels": torch.randint(0, 10, (100,), device="cuda"),
        },
        # NamedTuple on GPU
        collections.namedtuple("GPULayer", ["weight", "bias"])(
            weight=torch.randn(20, 10, device="cuda"),
            bias=torch.zeros(10, device="cuda"),
        ),
        # Deep nesting with GPU tensor
        {"encoder": {"blocks": [{"attn": torch.tensor(1.0, device="cuda")}]}},
    ]

    TEST_GPU_OBJECTS = PRIMITIVE_GPU_OBJECTS + REPRESENTATIVE_COMPLEX_GPU_OBJECTS

    @ray.remote(num_gpus=1)
    def echo(x):
        return x

    for obj in TEST_GPU_OBJECTS:
        restored1 = ray.get(ray.put(obj))
        restored2 = ray.get(echo.remote(obj))
        assert_tensors_equivalent(obj, restored1)
        assert_tensors_equivalent(obj, restored2)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
