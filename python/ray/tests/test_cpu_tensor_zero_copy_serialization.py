import collections
import sys
from dataclasses import make_dataclass

import pytest
import torch

import ray
from ray._common.test_utils import assert_tensors_equivalent


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


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
