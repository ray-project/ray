import math

import pytest

from ray.data._internal.cluster_autoscaler.throughput_solver import (
    allocate_resources,
    compute_optimal_throughput,
)
from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.util import get_max_task_capacity


class TestComputeOptimalThroughput:
    def test_one_op_cpu_bound(self):
        result = compute_optimal_throughput(
            rates={"A": 1.0},
            resource_requirements={"A": ExecutionResources(cpu=1)},
            resource_limits=ExecutionResources.for_limits(cpu=2),
            concurrency_limits={"A": float("inf")},
        )
        assert result == pytest.approx(2.0)

    def test_one_op_gpu_bound(self):
        result = compute_optimal_throughput(
            rates={"A": 1.0},
            resource_requirements={"A": ExecutionResources(gpu=1)},
            resource_limits=ExecutionResources.for_limits(gpu=2),
            concurrency_limits={"A": float("inf")},
        )
        assert result == pytest.approx(2.0)

    def test_one_op_memory_bound(self):
        result = compute_optimal_throughput(
            rates={"A": 1.0},
            resource_requirements={"A": ExecutionResources(memory=1e6)},
            resource_limits=ExecutionResources.for_limits(memory=2e6),
            concurrency_limits={"A": float("inf")},
        )
        assert result == pytest.approx(2.0)

    def test_one_op_concurrency_bound(self):
        result = compute_optimal_throughput(
            rates={"A": 1.0},
            resource_requirements={"A": ExecutionResources(cpu=1)},
            resource_limits=ExecutionResources.for_limits(),
            concurrency_limits={"A": 2},
        )
        assert result == pytest.approx(2.0)

    def test_two_ops_equal_rates(self):
        result = compute_optimal_throughput(
            rates={"A": 1.0, "B": 1.0},
            resource_requirements={
                "A": ExecutionResources(cpu=1),
                "B": ExecutionResources(cpu=1),
            },
            resource_limits=ExecutionResources.for_limits(cpu=2),
            concurrency_limits={"A": float("inf"), "B": float("inf")},
        )
        assert result == pytest.approx(1.0)

    def test_two_ops_different_rates(self):
        result = compute_optimal_throughput(
            rates={"A": 1.0, "B": 2.0},
            resource_requirements={
                "A": ExecutionResources(cpu=1),
                "B": ExecutionResources(cpu=1),
            },
            resource_limits=ExecutionResources.for_limits(cpu=3),
            concurrency_limits={"A": float("inf"), "B": float("inf")},
        )
        assert result == pytest.approx(2.0)

    def test_two_ops_different_resource_requirements(self):
        result = compute_optimal_throughput(
            rates={"A": 1.0, "B": 1.0},
            resource_requirements={
                "A": ExecutionResources(cpu=1),
                "B": ExecutionResources(cpu=2),
            },
            resource_limits=ExecutionResources.for_limits(cpu=3),
            concurrency_limits={"A": float("inf"), "B": float("inf")},
        )
        assert result == pytest.approx(1.0)

    def test_zero_resource_requirement(self):
        result = compute_optimal_throughput(
            rates={"A": 1.0},
            resource_requirements={"A": ExecutionResources.zero()},
            resource_limits=ExecutionResources.for_limits(cpu=1),
            concurrency_limits={"A": float("inf")},
        )
        assert result == float("inf")


class TestAllocateResources:
    def test_empty_rates(self):
        result = allocate_resources(
            0.0,
            rates={},
            resource_requirements={},
        )
        assert result == {}

    def test_zero_throughput(self):
        result = allocate_resources(
            0.0,
            rates={"A": 1.0},
            resource_requirements={
                "A": ExecutionResources(cpu=1),
            },
        )
        assert result == {
            "A": ExecutionResources.zero(),
        }

    def test_one_op(self):
        result = allocate_resources(
            1.0,
            rates={"A": 1.0},
            resource_requirements={"A": ExecutionResources(cpu=1)},
        )
        assert result["A"] == ExecutionResources(cpu=1)

    def test_two_ops_different_rates(self):
        result = allocate_resources(
            2.0,
            rates={"A": 1.0, "B": 2.0},
            resource_requirements={
                "A": ExecutionResources(cpu=1),
                "B": ExecutionResources(cpu=1),
            },
        )
        assert result["A"] == ExecutionResources(cpu=2)
        assert result["B"] == ExecutionResources(cpu=1)

    def test_two_ops_different_resource_requirements(self):
        result = allocate_resources(
            1.0,
            rates={"A": 1.0, "B": 1.0},
            resource_requirements={
                "A": ExecutionResources(cpu=1),
                "B": ExecutionResources(cpu=2),
            },
        )
        assert result["A"] == ExecutionResources(cpu=1)
        assert result["B"] == ExecutionResources(cpu=2)

    def test_infinite_throughput_does_not_produce_nan(self):
        """Unbounded throughput scales a 0 requirement by an infinite task count."""
        # A GPU-only operator requires 0 CPUs, so scaling leaves CPU as `0 * inf`.
        allocation = allocate_resources(
            float("inf"),
            rates={"A": 1.0},
            resource_requirements={"A": ExecutionResources(gpu=1)},
        )["A"]

        assert allocation.cpu == 0
        assert math.isinf(allocation.gpu)

        # Dividing by a non-zero requirement is where the NaN surfaced, as
        # `ValueError: cannot convert float NaN to integer`.
        assert get_max_task_capacity(allocation, ExecutionResources(cpu=1)) == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
