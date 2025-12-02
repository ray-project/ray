import logging
import math
from typing import TYPE_CHECKING, Optional, Dict
from .default_actor_autoscaler import DefaultActorAutoscaler
from ray.data._internal.execution.interfaces.execution_options import \
    ExecutionResources
from ray.data.context import AutoscalingConfig

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import PhysicalOperator
    from ray.data._internal.execution.streaming_executor_state import Topology
    from ray.data._internal.execution.resource_manager import ResourceManager
    from ray.data._internal.actor_autoscaler.autoscaling_actor_pool import \
        AutoscalingActorPool

logger = logging.getLogger(__name__)


class ResourceBasedActorAutoscaler(DefaultActorAutoscaler):
    """基于 job 级别资源配置自动计算所有 actor pools size 的 autoscaler"""

    def __init__(
        self,
        topology: "Topology",
        resource_manager: "ResourceManager",
        *,
        config: AutoscalingConfig,
    ):
        super().__init__(topology, resource_manager, config=config)
        # 存储 job 级别的资源限制
        self._job_min_resources: Optional[ExecutionResources] = None
        self._job_max_resources: Optional[ExecutionResources] = None

    def update_job_resource_limits(
        self,
        min_resources: Optional[ExecutionResources] = None,
        max_resources: Optional[ExecutionResources] = None
    ) -> None:
        """设置整个 Ray Data job 的资源限制,自动计算所有 actor pools 的 size

        Args:
            min_resources: Job 级别的最小资源(可选)
            max_resources: Job 级别的最大资源(可选)
        """
        # 验证 min_resources <= max_resources
        if min_resources is not None and max_resources is not None:
            # 检查 CPU
            if min_resources.cpu > max_resources.cpu:
                raise AssertionError(
                    f"min_resources.cpu ({min_resources.cpu}) cannot be greater than "
                    f"max_resources.cpu ({max_resources.cpu})"
                )
            # 检查 GPU
            if min_resources.gpu > max_resources.gpu:
                raise AssertionError(
                    f"min_resources.gpu ({min_resources.gpu}) cannot be greater than "
                    f"max_resources.gpu ({max_resources.gpu})"
                )
            # 检查 Memory
            if min_resources.memory > max_resources.memory:
                raise AssertionError(
                    f"min_resources.memory ({min_resources.memory}) cannot be greater than "
                    f"max_resources.memory ({max_resources.memory})"
                )

        if min_resources is not None:
            self._job_min_resources = min_resources
            logger.info(f"Updated job min_resources to {min_resources}")

        if max_resources is not None:
            self._job_max_resources = max_resources
            logger.info(f"Updated job max_resources to {max_resources}")

            # 自动为所有 actor pools 分配资源并计算 size
        self._distribute_resources_to_pools_by_weight()

        # 立即触发扩缩容
        self.try_trigger_scaling()

    def _distribute_resources_to_pools_by_weight(self) -> None:
        """按权重将 job 级别的资源分配给所有 actor pools,考虑实际资源需求"""
        # 收集所有 actor pools
        all_pools = []
        for op in self._topology.keys():
            all_pools.extend(op.get_autoscaling_actor_pools())

        if not all_pools:
            logger.warning("No actor pools found in topology")
            return

        # 计算每个 pool 的权重和资源需求
        pool_weights = {}
        pool_resource_needs = {}
        total_weight = 0

        for actor_pool in all_pools:
            per_actor_resources = actor_pool.per_actor_resource_usage()
            pool_resource_needs[actor_pool] = per_actor_resources

            # 计算权重(基于利用率)
            weight = self._calculate_pool_weight(actor_pool)
            pool_weights[actor_pool] = weight
            total_weight += weight

        if total_weight == 0:
            logger.warning("Total weight is 0, using equal distribution")
            total_weight = len(all_pools)
            for actor_pool in all_pools:
                pool_weights[actor_pool] = 1.0

        # 分别处理 CPU、GPU、Memory 资源
        for actor_pool in all_pools:
            per_actor_resources = pool_resource_needs[actor_pool]
            weight_ratio = pool_weights[actor_pool] / total_weight

            # 计算 min_size
            if self._job_min_resources is not None:
                pool_min_resources = self._calculate_pool_min_resources(
                    actor_pool, per_actor_resources, weight_ratio, all_pools,
                    pool_weights, pool_resource_needs
                )
                new_min_size = self._calculate_min_pool_size(
                    pool_min_resources, per_actor_resources
                )
                actor_pool._min_size = max(1, new_min_size)
                logger.info(
                    f"Updated actor pool min_size to {actor_pool._min_size} "
                    f"based on min_resources={pool_min_resources}"
                )

            # 计算 max_size
            if self._job_max_resources is not None:
                pool_max_resources = self._calculate_pool_max_resources(
                    actor_pool, per_actor_resources, weight_ratio, all_pools,
                    pool_weights, pool_resource_needs
                )
                new_max_size = self._calculate_max_pool_size(
                    pool_max_resources, per_actor_resources
                )
                actor_pool._max_size = max(actor_pool._min_size, new_max_size)
                logger.info(
                    f"Updated actor pool max_size to {actor_pool._max_size} "
                    f"based on max_resources={pool_max_resources}"
                )

    def _calculate_pool_min_resources(
        self,
        actor_pool: "AutoscalingActorPool",
        per_actor_resources: ExecutionResources,
        weight_ratio: float,
        all_pools: list,
        pool_weights: Dict,
        pool_resource_needs: Dict
    ) -> ExecutionResources:
        """计算单个 pool 的 min resources"""
        # CPU 分配
        pool_min_cpu = (
            self._job_min_resources.cpu * weight_ratio
            if per_actor_resources.cpu > 0 else 0
        )

        # GPU 分配:只分配给需要 GPU 的 pools
        if per_actor_resources.gpu > 0:
            gpu_pools = [p for p in all_pools if
                         pool_resource_needs[p].gpu > 0]
            if gpu_pools:
                gpu_total_weight = sum(pool_weights[p] for p in gpu_pools)
                gpu_weight_ratio = (
                    pool_weights[actor_pool] / gpu_total_weight
                    if gpu_total_weight > 0 else 1.0 / len(gpu_pools)
                )
                pool_min_gpu = self._job_min_resources.gpu * gpu_weight_ratio
            else:
                pool_min_gpu = 0
        else:
            pool_min_gpu = 0

        # Memory 分配
        pool_min_memory = (
            self._job_min_resources.memory * weight_ratio
            if per_actor_resources.memory > 0 else 0
        )

        return ExecutionResources(
            cpu=pool_min_cpu,
            gpu=pool_min_gpu,
            memory=pool_min_memory,
        )

    def _calculate_pool_max_resources(
        self,
        actor_pool: "AutoscalingActorPool",
        per_actor_resources: ExecutionResources,
        weight_ratio: float,
        all_pools: list,
        pool_weights: Dict,
        pool_resource_needs: Dict
    ) -> ExecutionResources:
        """计算单个 pool 的 max resources"""
        # CPU 分配
        pool_max_cpu = (
            self._job_max_resources.cpu * weight_ratio
            if per_actor_resources.cpu > 0 else 0
        )

        # GPU 分配:只分配给需要 GPU 的 pools
        if per_actor_resources.gpu > 0:
            gpu_pools = [p for p in all_pools if
                         pool_resource_needs[p].gpu > 0]
            if gpu_pools:
                gpu_total_weight = sum(pool_weights[p] for p in gpu_pools)
                gpu_weight_ratio = (
                    pool_weights[actor_pool] / gpu_total_weight
                    if gpu_total_weight > 0 else 1.0 / len(gpu_pools)
                )
                pool_max_gpu = self._job_max_resources.gpu * gpu_weight_ratio
            else:
                pool_max_gpu = 0
        else:
            pool_max_gpu = 0

        # Memory 分配
        pool_max_memory = (
            self._job_max_resources.memory * weight_ratio
            if per_actor_resources.memory > 0 else 0
        )

        return ExecutionResources(
            cpu=pool_max_cpu,
            gpu=pool_max_gpu,
            memory=pool_max_memory,
        )

    def _calculate_pool_weight(
        self,
        actor_pool: "AutoscalingActorPool"
    ) -> float:
        """计算 actor pool 的权重,用于资源分配

        基于当前利用率:利用率越高,权重越大
        """
        util = actor_pool.get_pool_util()
        # 避免利用率为0，所以最小权重为0.1
        return max(0.1, util)

    def _calculate_min_pool_size(
        self,
        min_resources: ExecutionResources,
        per_actor_resources: ExecutionResources
    ) -> int:
        """根据 min resources 计算 min pool size (向上取整)

        确保总资源 >= min_resources
        """
        min_size_by_cpu = (
            math.ceil(min_resources.cpu / per_actor_resources.cpu)
            if per_actor_resources.cpu > 0 else 0
        )

        min_size_by_gpu = (
            math.ceil(min_resources.gpu / per_actor_resources.gpu)
            if per_actor_resources.gpu > 0 else 0
        )

        min_size_by_memory = (
            math.ceil(min_resources.memory / per_actor_resources.memory)
            if per_actor_resources.memory > 0 else 0
        )

        # 取最大值,确保所有资源类型都满足
        return max(min_size_by_cpu, min_size_by_gpu, min_size_by_memory, 1)

    def _calculate_max_pool_size(
        self,
        max_resources: ExecutionResources,
        per_actor_resources: ExecutionResources
    ) -> int:
        """根据 max resources 计算 max pool size (向下取整)

        确保总资源 <= max_resources
        """
        max_size_by_cpu = (
            math.floor(max_resources.cpu / per_actor_resources.cpu)
            if per_actor_resources.cpu > 0 else float('inf')
        )

        max_size_by_gpu = (
            math.floor(max_resources.gpu / per_actor_resources.gpu)
            if per_actor_resources.gpu > 0 else float('inf')
        )

        max_size_by_memory = (
            math.floor(max_resources.memory / per_actor_resources.memory)
            if per_actor_resources.memory > 0 else float('inf')
        )

        # 取最小值,确保不超过任何资源类型的限制
        max_size = min(max_size_by_cpu, max_size_by_gpu, max_size_by_memory)

        # 如果是无限大,返回一个合理的默认值
        if math.isinf(max_size):
            return 100  # 默认最大值

        return int(max_size)

    def get_current_job_resource_limits(
        self
    ) -> tuple[Optional[ExecutionResources], Optional[ExecutionResources]]:
        """获取当前的 job 级别资源限制

        Returns:
            (min_resources, max_resources) 元组
        """
        return (self._job_min_resources, self._job_max_resources)
