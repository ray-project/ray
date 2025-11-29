import pytest
from unittest.mock import MagicMock
from ray.data import ExecutionResources
from ray.data._internal.actor_autoscaler.resource_based_actor_autoscaler import (
    ResourceBasedActorAutoscaler
)
from ray.data._internal.execution.operators.actor_pool_map_operator import \
    _ActorPool
from ray.data.context import AutoscalingConfig


def test_calculate_min_pool_size():
    """测试从 min resources 计算 min pool size (向上取整)"""
    autoscaler = ResourceBasedActorAutoscaler(
        topology={},
        resource_manager=MagicMock(),
        config=AutoscalingConfig(),
    )

    per_actor_resources = ExecutionResources(cpu=2, gpu=1, memory=2e9)

    # CPU 是瓶颈: ceil(5/2) = 3
    min_resources = ExecutionResources(cpu=5, gpu=0, memory=0)
    assert autoscaler._calculate_min_pool_size(min_resources,
                                               per_actor_resources) == 3

    # GPU 是瓶颈: ceil(3/1) = 3
    min_resources = ExecutionResources(cpu=0, gpu=3, memory=0)
    assert autoscaler._calculate_min_pool_size(min_resources,
                                               per_actor_resources) == 3

    # Memory 是瓶颈: ceil(5e9/2e9) = 3
    min_resources = ExecutionResources(cpu=0, gpu=0, memory=5e9)
    assert autoscaler._calculate_min_pool_size(min_resources,
                                               per_actor_resources) == 3

    # 多个资源,取最大值: max(ceil(10/2), ceil(2/1), ceil(6e9/2e9)) = max(5, 2, 3) = 5
    min_resources = ExecutionResources(cpu=10, gpu=2, memory=6e9)
    assert autoscaler._calculate_min_pool_size(min_resources,
                                               per_actor_resources) == 5


def test_calculate_max_pool_size():
    """测试从 max resources 计算 max pool size (向下取整)"""
    autoscaler = ResourceBasedActorAutoscaler(
        topology={},
        resource_manager=MagicMock(),
        config=AutoscalingConfig(),
    )

    per_actor_resources = ExecutionResources(cpu=2, gpu=1, memory=2e9)

    # CPU 是瓶颈: floor(5/2) = 2
    max_resources = ExecutionResources(cpu=5, gpu=10, memory=10e9)
    assert autoscaler._calculate_max_pool_size(max_resources,
                                               per_actor_resources) == 2

    # GPU 是瓶颈: floor(3/1) = 3
    max_resources = ExecutionResources(cpu=10, gpu=3, memory=10e9)
    assert autoscaler._calculate_max_pool_size(max_resources,
                                               per_actor_resources) == 3

    # Memory 是瓶颈: floor(5e9/2e9) = 2
    max_resources = ExecutionResources(cpu=10, gpu=10, memory=5e9)
    assert autoscaler._calculate_max_pool_size(max_resources,
                                               per_actor_resources) == 2


def test_distribute_resources_by_weight():
    """测试按权重分配资源给多个 actor pools"""
    # 创建 mock operator 和 actor pools
    op1 = MagicMock()
    pool1 = MagicMock(spec=_ActorPool)
    pool1.per_actor_resource_usage.return_value = ExecutionResources(cpu=1,
                                                                     gpu=0,
                                                                     memory=1e9)
    pool1.get_pool_util.return_value = 1.5  # 高利用率
    pool1.max_tasks_in_flight_per_actor.return_value = 4
    pool1.max_actor_concurrency.return_value = 1
    op1.get_autoscaling_actor_pools.return_value = [pool1]

    op2 = MagicMock()
    pool2 = MagicMock(spec=_ActorPool)
    pool2.per_actor_resource_usage.return_value = ExecutionResources(cpu=2,
                                                                     gpu=1,
                                                                     memory=2e9)
    pool2.get_pool_util.return_value = 0.8  # 低利用率
    pool2.max_tasks_in_flight_per_actor.return_value = 4
    pool2.max_actor_concurrency.return_value = 1
    op2.get_autoscaling_actor_pools.return_value = [pool2]

    topology = {op1: MagicMock(), op2: MagicMock()}

    autoscaler = ResourceBasedActorAutoscaler(
        topology=topology,
        resource_manager=MagicMock(),
        config=AutoscalingConfig(),
    )

    # 设置 job 级别的资源限制
    autoscaler.update_job_resource_limits(
        min_resources=ExecutionResources(cpu=10, gpu=2, memory=10e9),
        max_resources=ExecutionResources(cpu=20, gpu=4, memory=20e9)
    )
    
    # 验证高利用率的 pool1 应该获得更多资源
    # pool1 权重 = 1.5, pool2 权重 = 0.8, 总权重 = 2.3
    # pool1 权重比例 = 1.5/2.3 ≈ 0.652, pool2 权重比例 = 0.8/2.3 ≈ 0.348
    
    # 计算 pool1 的资源分配
    # CPU: 20 * 0.652 ≈ 13.04, Memory: 20 * 0.652 ≈ 13.04GB
    # pool1 每个 actor 需要 1 CPU + 1GB Memory
    # expected_pool1_max = min(13.04 // 1, 13.04 // 1) = 13
    
    # 计算 pool2 的资源分配
    # CPU: 20 * 0.348 ≈ 6.96, GPU: 4 (全部给 pool2), Memory: 20 * 0.348 ≈ 6.96GB
    # pool2 每个 actor 需要 2 CPU + 1 GPU + 2GB Memory
    # expected_pool2_max = min(6.96 // 2, 4 // 1, 6.96 // 2) = 3
    
    # 验证实际分配结果（基于正确的资源限制计算）
    # pool2的max_size应该基于所有资源限制取最小值：
    # CPU: 20 * 0.348 ≈ 6.96 → floor(6.96/2) = 3
    # GPU: 4 * 1.0 = 4 → floor(4/1) = 4  
    # Memory: 20 * 0.348 ≈ 6.96 → floor(6.96/2) = 3
    # 最终应该是 min(3, 4, 3) = 3
    assert pool1._max_size == 13
    assert pool2._max_size == 3
    
    # pool1 的 min_size 计算：
    # CPU: 10 * (15/23) ≈ 6.521 → ceil(6.521/1) = 7
    # Memory: 10 * (15/23) ≈ 6.521 → ceil(6.521/1) = 7
    assert pool1._min_size == 7
    
    # pool2 的 min_size 计算：
    # CPU: 10 * (8/23) ≈ 3.478 → ceil(3.478/2) = 2
    # GPU: 2 * 1.0 = 2 → ceil(2/1) = 2
    # Memory: 10 * (8/23) ≈ 3.478 → ceil(3.478/2) = 2
    # 取最大值：max(2, 2, 2) = 2
    assert pool2._min_size == 2


def test_gpu_only_allocated_to_gpu_pools():
    """测试 GPU 资源只分配给需要 GPU 的 pools"""
    # CPU-only pool
    op1 = MagicMock()
    pool1 = MagicMock(spec=_ActorPool)
    pool1.per_actor_resource_usage.return_value = ExecutionResources(cpu=1,
                                                                     gpu=0,
                                                                     memory=1e9)
    pool1.get_pool_util.return_value = 1.0
    pool1.max_tasks_in_flight_per_actor.return_value = 4
    pool1.max_actor_concurrency.return_value = 1
    op1.get_autoscaling_actor_pools.return_value = [pool1]

    # GPU pool
    op2 = MagicMock()
    pool2 = MagicMock(spec=_ActorPool)
    pool2.per_actor_resource_usage.return_value = ExecutionResources(cpu=2,
                                                                     gpu=1,
                                                                     memory=2e9)
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

    # 设置包含 GPU 的资源限制
    autoscaler.update_job_resource_limits(
        min_resources=ExecutionResources(cpu=10, gpu=4, memory=10e9),
        max_resources=ExecutionResources(cpu=20, gpu=8, memory=20e9)
    )

    # 验证资源分配 - 应该为每个 pool 设置 min_size 和 max_size
    assert pool1._min_size is not None
    assert pool1._max_size is not None
    assert pool2._min_size is not None
    assert pool2._max_size is not None
    
    # 计算准确的资源分配
    # 权重：pool1=1.0, pool2=1.0, 总权重=2.0
    # pool1权重比例=0.5, pool2权重比例=0.5
    
    # pool1 (CPU-only) 的 max_size 计算：
    # CPU: 20 * 0.5 = 10 → floor(10/1) = 10
    # Memory: 20 * 0.5 = 10GB → floor(10/1) = 10
    # 最终：min(10, 10) = 10
    assert pool1._max_size == 10
    
    # pool2 (GPU) 的 max_size 计算：
    # CPU: 20 * 0.5 = 10 → floor(10/2) = 5
    # GPU: 8 * 1.0 = 8 → floor(8/1) = 8  (pool2是唯一GPU pool)
    # Memory: 20 * 0.5 = 10GB → floor(10/2) = 5
    # 最终：min(5, 8, 5) = 5
    assert pool2._max_size == 5
    
    # pool1 的 min_size 计算：
    # CPU: 10 * 0.5 = 5 → ceil(5/1) = 5
    # Memory: 10 * 0.5 = 5GB → ceil(5/1) = 5
    # 最终：max(5, 5) = 5
    assert pool1._min_size == 5
    
    # pool2 的 min_size 计算：
    # CPU: 10 * 0.5 = 5 → ceil(5/2) = 3
    # GPU: 4 * 1.0 = 4 → ceil(4/1) = 4
    # Memory: 10 * 0.5 = 5GB → ceil(5/2) = 3
    # 最终：max(3, 4, 3) = 4
    assert pool2._min_size == 4
    
    # 验证 GPU 只分配给需要 GPU 的 pools
    # pool1 不需要 GPU，所以 GPU 资源全部分配给 pool2


def test_update_job_resource_limits_validation():
    """测试资源限制更新时的验证"""
    autoscaler = ResourceBasedActorAutoscaler(
        topology={},
        resource_manager=MagicMock(),
        config=AutoscalingConfig(),
    )

    # 有效的更新
    autoscaler.update_job_resource_limits(
        min_resources=ExecutionResources(cpu=5, gpu=1, memory=5e9),
        max_resources=ExecutionResources(cpu=10, gpu=2, memory=10e9)
    )

    # 无效的更新: max < min
    with pytest.raises(AssertionError):
        autoscaler.update_job_resource_limits(
            min_resources=ExecutionResources(cpu=10, gpu=2, memory=10e9),
            max_resources=ExecutionResources(cpu=5, gpu=1, memory=5e9)
        )


def test_get_current_job_resource_limits():
    """测试获取当前的 job 资源限制"""
    autoscaler = ResourceBasedActorAutoscaler(
        topology={},
        resource_manager=MagicMock(),
        config=AutoscalingConfig(),
    )

    # 初始状态
    min_res, max_res = autoscaler.get_current_job_resource_limits()
    assert min_res is None
    assert max_res is None

    # 设置后
    min_resources = ExecutionResources(cpu=5, gpu=1, memory=5e9)
    max_resources = ExecutionResources(cpu=10, gpu=2, memory=10e9)
    autoscaler.update_job_resource_limits(
        min_resources=min_resources,
        max_resources=max_resources
    )

    min_res, max_res = autoscaler.get_current_job_resource_limits()
    assert min_res == min_resources
    assert max_res == max_resources
