from typing import Dict, TypeVar

from ray.data._internal.execution.interfaces import ExecutionResources

# The math functions defined in this module use a generic type rather than
# `PhysicalOperator` so it's easier to test. We already pass in all of the necessary
# inputs, so the actual type doesn't matter.
T = TypeVar("T")


_SCHEDULABLE_RESOURCE_NAMES = ("cpu", "gpu", "memory")


def allocate_resources(
    throughput: float,
    *,
    rates: Dict[T, float],
    resource_requirements: Dict[T, ExecutionResources],
) -> Dict[T, ExecutionResources]:
    """Allocate resources for a pipeline to sustain the given throughput.

    Key insight: in a pipeline, all operators must sustain the same throughput T.
    Operator i with per-task rate r_i needs T/r_i tasks to sustain T. So maximizing
    throughput is equivalent to finding the largest feasible T, then deriving task
    counts from it.

    Args:
        throughput: The throughput for the pipeline in the same units as the rates.
        rates: The rate at which a task or actor produces outputs for each operator.
        resource_requirements: The logical resources required to schedule a task or
            actor for each operator.

    Returns:
        A dictionary mapping operators to the allocated resources.
    """
    assert throughput >= 0, "Throughput must be non-negative"
    assert all(rate > 0 for rate in rates.values()), "Rates must be positive"

    if not rates:
        return {}

    if throughput == 0:
        return {op: ExecutionResources.zero() for op in rates}

    # NOTE: This implementation computes fractional task counts. In practice, you
    # can't schedule a fractional task or actor, so the allocations might be infeasible.
    task_counts = {op: throughput / rate for op, rate in rates.items()}
    return {op: resource_requirements[op].scale(task_counts[op]) for op in rates}


def compute_optimal_throughput(
    *,
    rates: Dict[T, float],
    resource_requirements: Dict[T, ExecutionResources],
    resource_limits: ExecutionResources,
    concurrency_limits: Dict[T, int],
) -> float:
    """Compute the optimal throughput for a pipeline.

    The optimal throughput is bounded by two constraints (we take the tightest):
      1. Resource limits — total resource usage across all operators must fit the
         budget.
      2. Concurrency limits — each operator's task count cannot exceed its limit.

    Args:
        rates: The rate at which a task or actor produces outputs for each operator.
        resource_requirements: The logical resources required to schedule a task or
            actor for each operator.
        resource_limits: The resource limits for the cluster.
        concurrency_limits: The maximum number of tasks or actors that can be scheduled
            concurrently for each operator.

    Returns:
        The optimal throughput for the pipeline in the same units as the rates.
    """
    assert rates, "Rates must be non-empty"
    return min(
        _max_throughput_from_resources(rates, resource_requirements, resource_limits),
        _max_throughput_from_concurrency(rates, concurrency_limits),
    )


def _max_throughput_from_resources(
    rates: Dict[T, float],
    resource_requirements: Dict[T, ExecutionResources],
    resource_limits: ExecutionResources,
) -> float:
    """For each resource type, compute the max throughput the resource budget allows."""
    assert rates, "Rates must be non-empty"
    assert all(rate > 0 for rate in rates.values()), "Rates must be positive"
    assert (
        rates.keys() == resource_requirements.keys()
    ), "Rates and resource requirements must have the same keys"

    max_throughput = float("inf")

    for resource_name in _SCHEDULABLE_RESOURCE_NAMES:
        resource_limit = getattr(resource_limits, resource_name)
        resource_cost_per_unit_throughput = sum(
            getattr(resource_requirements[op], resource_name) / rates[op]
            for op in rates
        )
        if resource_cost_per_unit_throughput > 0:
            max_throughput = min(
                max_throughput, resource_limit / resource_cost_per_unit_throughput
            )

    assert max_throughput >= 0, "Max throughput must be non-negative"
    return max_throughput


def _max_throughput_from_concurrency(
    rates: Dict[T, float],
    concurrency_limits: Dict[T, int],
) -> float:
    """Each operator's throughput is capped at rate * concurrency_limit."""
    assert rates, "Rates must be non-empty"
    assert (
        rates.keys() == concurrency_limits.keys()
    ), "Rates and concurrency limits must have the same keys"

    return min(rates[op] * concurrency_limits[op] for op in rates)
