from unittest.mock import MagicMock

import pytest

from ray.data import ExecutionResources
from ray.data._internal.actor_autoscaler.resource_based_actor_autoscaler import (
    ResourceBasedActorAutoscaler,
)
from ray.data._internal.execution.operators.actor_pool_map_operator import _ActorPool
from ray.data.context import AutoscalingConfig


def test_calculate_min_pool_size():
    """Test calculate min pool size from min resources (rounding up)"""
    autoscaler = ResourceBasedActorAutoscaler(
        topology={},
        resource_manager=MagicMock(),
        config=AutoscalingConfig(),
    )

    per_actor_resources = ExecutionResources(cpu=2, gpu=1, memory=2e9)

    # CPU is the bottleneck: ceil(5/2) = 3
    min_resources = ExecutionResources(cpu=5, gpu=0, memory=0)
    assert autoscaler._calculate_min_pool_size(min_resources, per_actor_resources) == 3

    # GPU is the bottleneck: ceil(3/1) = 3
    min_resources = ExecutionResources(cpu=0, gpu=3, memory=0)
    assert autoscaler._calculate_min_pool_size(min_resources, per_actor_resources) == 3

    # Memory is the bottleneck: ceil(5e9/2e9) = 3
    min_resources = ExecutionResources(cpu=0, gpu=0, memory=5e9)
    assert autoscaler._calculate_min_pool_size(min_resources, per_actor_resources) == 3

    # Multiple resources, take maximum: max(ceil(10/2), ceil(2/1), ceil(6e9/2e9)) = max(5, 2, 3) = 5
    min_resources = ExecutionResources(cpu=10, gpu=2, memory=6e9)
    assert autoscaler._calculate_min_pool_size(min_resources, per_actor_resources) == 5


def test_calculate_max_pool_size():
    """Test calculate max pool size from max resources (rounding down)"""
    autoscaler = ResourceBasedActorAutoscaler(
        topology={},
        resource_manager=MagicMock(),
        config=AutoscalingConfig(),
    )

    per_actor_resources = ExecutionResources(cpu=2, gpu=1, memory=2e9)

    # CPU is the bottleneck: floor(5/2) = 2
    max_resources = ExecutionResources(cpu=5, gpu=10, memory=10e9)
    assert autoscaler._calculate_max_pool_size(max_resources, per_actor_resources) == 2

    # GPU is the bottleneck: floor(3/1) = 3
    max_resources = ExecutionResources(cpu=10, gpu=3, memory=10e9)
    assert autoscaler._calculate_max_pool_size(max_resources, per_actor_resources) == 3

    # Memory is the bottleneck: floor(5e9/2e9) = 2
    max_resources = ExecutionResources(cpu=10, gpu=10, memory=5e9)
    assert autoscaler._calculate_max_pool_size(max_resources, per_actor_resources) == 2


def test_distribute_resources_by_weight():
    """Test distribute resources by weight among multiple actor pools"""
    # mock operator 和 actor pools
    op1 = MagicMock()
    pool1 = MagicMock(spec=_ActorPool)
    pool1.per_actor_resource_usage.return_value = ExecutionResources(
        cpu=1, gpu=0,memory=1e9
    )
    pool1.get_pool_util.return_value = 1.5
    pool1.max_tasks_in_flight_per_actor.return_value = 4
    pool1.max_actor_concurrency.return_value = 1
    op1.get_autoscaling_actor_pools.return_value = [pool1]

    op2 = MagicMock()
    pool2 = MagicMock(spec=_ActorPool)
    pool2.per_actor_resource_usage.return_value = ExecutionResources(
        cpu=2, gpu=1, memory=2e9
    )
    pool2.get_pool_util.return_value = 0.8
    pool2.max_tasks_in_flight_per_actor.return_value = 4
    pool2.max_actor_concurrency.return_value = 1
    op2.get_autoscaling_actor_pools.return_value = [pool2]

    topology = {op1: MagicMock(), op2: MagicMock()}

    autoscaler = ResourceBasedActorAutoscaler(
        topology=topology,
        resource_manager=MagicMock(),
        config=AutoscalingConfig(),
    )

    # setting job-level resource limits
    autoscaler.update_job_resource_limits(
        min_resources=ExecutionResources(cpu=10, gpu=2, memory=10e9),
        max_resources=ExecutionResources(cpu=20, gpu=4, memory=20e9),
    )

    # Verify that the pool with higher utilization, pool1, should receive more resources
    # pool1 weight = 1.5, pool2 weight = 0.8, total weight = 2.3
    # pool1 weight ratio = 1.5/2.3 ≈ 0.652, pool2 weight ratio = 0.8/2.3 ≈ 0.348

    # Calculate resource allocation for pool1
    #   CPU: 20 * 0.652 ≈ 13.04,
    #   Memory: 20 * 0.652 ≈ 13.04GB
    # pool1 every actor needs 1 CPU + 1GB Memory
    # expected_pool1_max = min(13.04 // 1, 13.04 // 1) = 13

    # Calculate resource allocation for pool2
    #  CPU: 20 * 0.348 ≈ 6.96,
    #  GPU: 4 (give all resource to pool2),
    #  Memory: 20 * 0.348 ≈ 6.96GB
    # pool2 every actor needs 2 CPU + 1 GPU + 2GB Memory
    # expected_pool2_max = min(6.96 // 2, 4 // 1, 6.96 // 2) = 3

    # Verify that the pool with lower utilization, pool2, receives fewer resources
    # pool2's max_size should be based on the minimum value of all resource limits:
    # CPU: 20 * 0.348 ≈ 6.96 → floor(6.96/2) = 3
    # GPU: 4 * 1.0 = 4 → floor(4/1) = 4
    # Memory: 20 * 0.348 ≈ 6.96 → floor(6.96/2) = 3
    # The final should be: min(3, 4, 3) = 3
    assert pool1._max_size == 13
    assert pool2._max_size == 3

    # calculate pool1's min_size:
    # CPU: 10 * (15/23) ≈ 6.521 → ceil(6.521/1) = 7
    # Memory: 10 * (15/23) ≈ 6.521 → ceil(6.521/1) = 7
    assert pool1._min_size == 7

    # pool2's min_size calculation:
    # CPU: 10 * (8/23) ≈ 3.478 → ceil(3.478/2) = 2
    # GPU: 2 * 1.0 = 2 → ceil(2/1) = 2
    # Memory: 10 * (8/23) ≈ 3.478 → ceil(3.478/2) = 2
    # Use the max value：max(2, 2, 2) = 2
    assert pool2._min_size == 2


def test_gpu_only_allocated_to_gpu_pools():
    """Test GPU resources are allocated only to pools that need GPUs"""
    # CPU-only pool
    op1 = MagicMock()
    pool1 = MagicMock(spec=_ActorPool)
    pool1.per_actor_resource_usage.return_value = ExecutionResources(
        cpu=1, gpu=0, memory=1e9
    )
    pool1.get_pool_util.return_value = 1.0
    pool1.max_tasks_in_flight_per_actor.return_value = 4
    pool1.max_actor_concurrency.return_value = 1
    op1.get_autoscaling_actor_pools.return_value = [pool1]

    # GPU pool
    op2 = MagicMock()
    pool2 = MagicMock(spec=_ActorPool)
    pool2.per_actor_resource_usage.return_value = ExecutionResources(
        cpu=2, gpu=1, memory=2e9
    )
    pool2.get_pool_util.return_value = 1.0
    pool2.max_tasks_in_flight_per_actor.return_value = 4
    pool2.max_actor_concurrency.return_value = 1
    op2.get_autoscaling_actor_pools.return_value = [pool2]

    topology = {op1: MagicMock(), op2: MagicMock()}

    autoscaler = ResourceBasedActorAutoscaler(
        topology=topology,
        resource_manager=MagicMock(),
        config=AutoscalingConfig(),
    )

    # Set resource limits that include GPU
    autoscaler.update_job_resource_limits(
        min_resources=ExecutionResources(cpu=10, gpu=4, memory=10e9),
        max_resources=ExecutionResources(cpu=20, gpu=8, memory=20e9),
    )

    # Verify resource allocation - should set min_size and max_size for each pool
    assert pool1._min_size is not None
    assert pool1._max_size is not None
    assert pool2._min_size is not None
    assert pool2._max_size is not None

    # Calculate accurate resource allocation
    # Weights: pool1=1.0, pool2=1.0, total weight=2.0
    # pool1 weight ratio = 0.5, pool2 weight ratio = 0.5

    # Calculate pool1's(CPU-only) max_size:
    # CPU: 20 * 0.5 = 10 → floor(10/1) = 10
    # Memory: 20 * 0.5 = 10GB → floor(10/1) = 10
    # The final is: min(10, 10) = 10
    assert pool1._max_size == 10

    #  Calculate pool2's(GPU) max_size:
    # CPU: 20 * 0.5 = 10 → floor(10/2) = 5
    # GPU: 8 * 1.0 = 8 → floor(8/1) = 8  (pool2 is GPU only pool)
    # Memory: 20 * 0.5 = 10GB → floor(10/2) = 5
    # The final is: ：min(5, 8, 5) = 5
    assert pool2._max_size == 5

    # pool1's min_size calculation:
    # CPU: 10 * 0.5 = 5 → ceil(5/1) = 5
    # Memory: 10 * 0.5 = 5GB → ceil(5/1) = 5
    # The final is: max(5, 5) = 5
    assert pool1._min_size == 5

    # pool2's min_size calculation:
    # CPU: 10 * 0.5 = 5 → ceil(5/2) = 3
    # GPU: 4 * 1.0 = 4 → ceil(4/1) = 4
    # Memory: 10 * 0.5 = 5GB → ceil(5/2) = 3
    # The final is: max(3, 4, 3) = 4
    assert pool2._min_size == 4

    # Verify that GPU resources are allocated only to pools that need GPUs
    # pool1 does not need GPU, so all GPU resources are allocated to pool2


def test_update_job_resource_limits_validation():
    """Test update job resource limits"""
    autoscaler = ResourceBasedActorAutoscaler(
        topology={},
        resource_manager=MagicMock(),
        config=AutoscalingConfig(),
    )

    autoscaler.update_job_resource_limits(
        min_resources=ExecutionResources(cpu=5, gpu=1, memory=5e9),
        max_resources=ExecutionResources(cpu=10, gpu=2, memory=10e9),
    )

    with pytest.raises(AssertionError):
        autoscaler.update_job_resource_limits(
            min_resources=ExecutionResources(cpu=10, gpu=2, memory=10e9),
            max_resources=ExecutionResources(cpu=5, gpu=1, memory=5e9),
        )


def test_get_current_job_resource_limits():
    """Test get current job resource limits"""
    autoscaler = ResourceBasedActorAutoscaler(
        topology={},
        resource_manager=MagicMock(),
        config=AutoscalingConfig(),
    )

    # Initial state
    min_res, max_res = autoscaler.get_current_job_resource_limits()
    assert min_res is None
    assert max_res is None

    # After setting
    min_resources = ExecutionResources(cpu=5, gpu=1, memory=5e9)
    max_resources = ExecutionResources(cpu=10, gpu=2, memory=10e9)
    autoscaler.update_job_resource_limits(
        min_resources=min_resources,
        max_resources=max_resources,
    )

    min_res, max_res = autoscaler.get_current_job_resource_limits()
    assert min_res == min_resources
    assert max_res == max_resources


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
