import collections
import os
import sys
from dataclasses import make_dataclass

import pytest
import torch

import ray
from ray._common.test_utils import is_named_tuple

USE_GPU = bool(os.environ.get("RAY_PYTEST_USE_GPU", 0))


@pytest.fixture
def ray_start_cluster_with_zero_copy_tensors(monkeypatch):
    """Start a Ray cluster with zero-copy PyTorch tensors enabled."""
    with monkeypatch.context() as m:
        # Enable zero-copy sharing of PyTorch tensors in Ray
        m.setenv("RAY_ENABLE_ZERO_COPY_TORCH_TENSORS", "1")

        # Initialize Ray with the required environment variable.
        ray.init(runtime_env={"env_vars": {"RAY_ENABLE_ZERO_COPY_TORCH_TENSORS": "1"}})

        # Yield control to the test session
        yield

        # Shutdown Ray after tests complete
        ray.shutdown()


def assert_tensors_equivalent(obj1, obj2):
    """
    Recursively compare objects with special handling for torch.Tensor.

    Tensors are considered equivalent if:
      - Same dtype and shape
      - Same device type (e.g., both 'cpu' or both 'cuda'), index ignored
      - Values are equal (or close for floats)
    """
    if isinstance(obj1, torch.Tensor) and isinstance(obj2, torch.Tensor):
        # 1. dtype
        assert obj1.dtype == obj2.dtype, f"dtype mismatch: {obj1.dtype} vs {obj2.dtype}"
        # 2. shape
        assert obj1.shape == obj2.shape, f"shape mismatch: {obj1.shape} vs {obj2.shape}"
        # 3. device type must match (cpu/cpu or cuda/cuda), ignore index
        assert (
            obj1.device.type == obj2.device.type
        ), f"Device type mismatch: {obj1.device} vs {obj2.device}"

        # 4. Compare values safely on CPU
        t1_cpu = obj1.cpu()
        t2_cpu = obj2.cpu()
        if obj1.dtype.is_floating_point or obj1.dtype.is_complex:
            assert torch.allclose(
                t1_cpu, t2_cpu, atol=1e-6, rtol=1e-5
            ), "Floating-point tensors not close"
        else:
            assert torch.equal(t1_cpu, t2_cpu), "Integer/bool tensors not equal"
        return

    # Type must match
    if type(obj1) is not type(obj2):
        raise AssertionError(f"Type mismatch: {type(obj1)} vs {type(obj2)}")

    # Handle namedtuples
    if is_named_tuple(type(obj1)):
        assert len(obj1) == len(obj2)
        for a, b in zip(obj1, obj2):
            assert_tensors_equivalent(a, b)
    elif isinstance(obj1, dict):
        assert obj1.keys() == obj2.keys()
        for k in obj1:
            assert_tensors_equivalent(obj1[k], obj2[k])
    elif isinstance(obj1, (list, tuple)):
        assert len(obj1) == len(obj2)
        for a, b in zip(obj1, obj2):
            assert_tensors_equivalent(a, b)
    elif hasattr(obj1, "__dict__") and hasattr(obj2, "__dict__"):
        # Compare user-defined objects by their public attributes
        keys1 = {
            k
            for k in obj1.__dict__.keys()
            if not k.startswith("_ray_") and k != "_pytype_"
        }
        keys2 = {
            k
            for k in obj2.__dict__.keys()
            if not k.startswith("_ray_") and k != "_pytype_"
        }
        assert keys1 == keys2, f"Object attribute keys differ: {keys1} vs {keys2}"
        for k in keys1:
            assert_tensors_equivalent(obj1.__dict__[k], obj2.__dict__[k])
    else:
        # Fallback for primitives: int, float, str, bool, etc.
        assert obj1 == obj2, f"Non-tensor values differ: {obj1} vs {obj2}"


def test_cpu_tensor_serialization(ray_start_cluster_with_zero_copy_tensors):
    PRIMITIVE_OBJECTS = [
        # Scalars and basic types
        torch.tensor(42),
        torch.tensor(3.14159),
        torch.tensor(True),
        torch.tensor(1 + 2j, dtype=torch.complex64),
        torch.tensor(1 + 2j, dtype=torch.complex128),
        # Lower dimensions
        torch.tensor([1, 2, 3]),
        torch.tensor([1.0, 2.0, 3.0]),
        torch.tensor([True, False]),
        torch.randn(3, 4),
        torch.randint(0, 10, (2, 3)),
        torch.zeros(5, dtype=torch.int64),
        torch.ones(2, 2, dtype=torch.float32),
        # Empty and edge cases
        torch.tensor([]),
        torch.tensor((), dtype=torch.float32),
        torch.zeros(0, 5),
        torch.zeros(3, 0),
        torch.zeros(2, 0, 4),
        torch.empty(0, 10, 0),
        # Higher dimensions
        torch.randn(1, 1, 1, 1),
        torch.randn(2, 3, 4, 5, 6),
        # Strided / non-contiguous
        torch.arange(12).reshape(3, 4).t(),
        torch.arange(10)[::2],
        torch.randn(4, 4)[:, :2],
        # Scalar vs 0-dim
        torch.tensor(99),
        torch.tensor([99]).squeeze(),
    ]

    REPRESENTATIVE_COMPLEX_OBJECTS = [
        # List of tensors (explicitly requested)
        [torch.tensor([1, 2]), torch.tensor(3.5), torch.randn(2, 2)],
        # Dict with tensor values
        {
            "weights": torch.randn(10),
            "bias": torch.zeros(10),
            "flag": torch.tensor(True),
        },
        # NamedTuple with multiple tensors
        collections.namedtuple("Layer", ["w", "b", "meta"])(
            w=torch.randn(5, 5), b=torch.zeros(5), meta="test"
        ),
        # Dataclass
        make_dataclass("ModelParams", [("kernel", torch.Tensor), ("stride", int)])(
            kernel=torch.randn(3, 3), stride=1
        ),
        # One deep nesting example (covers recursive logic without explosion)
        {"model": {"layers": [{"w": torch.tensor([1.0])}], "output": torch.tensor(42)}},
        # Mixed types in container (tensor + primitive)
        [torch.tensor(100), "metadata", 42, {"aux": torch.tensor([1, 2])}],
    ]

    TEST_OBJECTS = PRIMITIVE_OBJECTS + REPRESENTATIVE_COMPLEX_OBJECTS

    @ray.remote
    def echo(x):
        return x

    for obj in TEST_OBJECTS:
        restored1 = ray.get(ray.put(obj))
        restored2 = ray.get(echo.remote(obj))
        assert_tensors_equivalent(obj, restored1)
        assert_tensors_equivalent(obj, restored2)


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

    @ray.remote
    def echo(x):
        return x

    for obj in TEST_GPU_OBJECTS:
        restored1 = ray.get(ray.put(obj))
        restored2 = ray.get(echo.remote(obj))
        assert_tensors_equivalent(obj, restored1)
        assert_tensors_equivalent(obj, restored2)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
