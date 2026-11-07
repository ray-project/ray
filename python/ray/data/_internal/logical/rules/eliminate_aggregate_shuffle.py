from typing import List, Optional, Type

from ray.data._internal.compute import TaskPoolStrategy
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.hash_aggregate_v2 import (
    _make_complete_aggregating_block_fn,
)
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
)
from ray.data._internal.execution.operators.shuffle_operators.disk_shuffle_map_operator import (  # noqa: E501
    DiskHashShuffleMapOp,
)
from ray.data._internal.execution.operators.shuffle_operators.disk_shuffle_reduce_operator import (  # noqa: E501
    DiskHashShuffleReduceOp,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_map_operator import (  # noqa: E501
    ShuffleMapOp,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_reduce_operator import (  # noqa: E501
    ShuffleReduceOp,
)
from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    PhysicalPlan,
    Rule,
)
from ray.data._internal.logical.operators import Aggregate
from ray.data._internal.logical.rules.configure_map_task_memory import (
    ConfigureMapTaskMemoryUsingOutputSize,
)
from ray.data._internal.logical.rules.inherit_target_max_block_size import (
    InheritTargetMaxBlockSizeRule,
)
from ray.data._internal.logical.rules.operator_fusion import FuseOperators
from ray.data._internal.output_buffer import OutputBlockSizeOption
from ray.data._internal.planner.plan_all_to_all_op import (
    _plan_hash_shuffle_aggregate_v2,
)

__all__ = [
    "EliminateAggregateShuffle",
    "EnsureLocalAggregateReadsWholePartitions",
]

_SHUFFLE_MAP_OPS = (ShuffleMapOp, DiskHashShuffleMapOp)
_SHUFFLE_REDUCE_OPS = (ShuffleReduceOp, DiskHashShuffleReduceOp)


def _is_local_aggregate(
    op: PhysicalOperator, logical_op: Optional[LogicalOperator]
) -> bool:
    """Whether ``op`` is a one-phase aggregation EliminateAggregateShuffle made:
    no other map op is planned from an Aggregate."""
    return isinstance(op, MapOperator) and isinstance(logical_op, Aggregate)


class EliminateAggregateShuffle(Rule):
    """Replaces a group-by's shuffle with a one-phase aggregation when its input
    is already hash-partitioned on a subset of the group-by keys, e.g. by a join
    on those keys::

        JoinShuffleReduce -> HashAggregateMap -> HashAggregateReduce   # before
        JoinShuffleReduce -> HashAggregate(mode=complete)              # after

    Runs before FuseOperators, while each map between the two shuffles still
    declares the partitioning it passes on. The one-phase aggregation is only
    correct if each of its tasks gets whole partitions, which
    EnsureLocalAggregateReadsWholePartitions arranges after fusion, or else
    restores the shuffle.
    """

    @classmethod
    def dependencies(cls) -> List[Type[Rule]]:
        # So the aggregation keeps the reduce's inherited block size.
        return [InheritTargetMaxBlockSizeRule]

    @classmethod
    def dependents(cls) -> List[Type[Rule]]:
        return [FuseOperators]

    def apply(self, plan: PhysicalPlan) -> PhysicalPlan:
        self._op_map = plan.op_map.copy()
        dag = self._eliminate_in_dag(plan.dag)
        return PhysicalPlan(dag, self._op_map, plan.context)

    def _eliminate_in_dag(self, op: PhysicalOperator) -> PhysicalOperator:
        # Upstream first. The one-phase aggregation is a map, so a group-by
        # right after it keeps its shuffle.
        op._input_dependencies = [
            self._eliminate_in_dag(input_op) for input_op in op.input_dependencies
        ]
        local_op = self._try_replace_shuffle(op)
        return op if local_op is None else local_op

    def _try_replace_shuffle(
        self, reduce_op: PhysicalOperator
    ) -> Optional[MapOperator]:
        """Replace ``reduce_op`` and its map op, the two halves of a group-by's
        shuffle, with a one-phase aggregation if the shuffle is redundant."""
        aggregate = self._op_map.get(reduce_op)
        if not (
            isinstance(reduce_op, _SHUFFLE_REDUCE_OPS)
            and isinstance(aggregate, Aggregate)
        ):
            return None
        # A map fused into the reduce would be dropped. Only a shuffle that
        # EnsureLocalAggregateReadsWholePartitions restored on an earlier
        # optimizer pass can have one here; it stays a shuffle.
        if reduce_op._fused_output_map_transformer is not None:
            return None
        (map_op,) = reduce_op.input_dependencies
        if not isinstance(map_op, _SHUFFLE_MAP_OPS):
            return None
        (source,) = map_op.input_dependencies

        distribution = aggregate.required_input_distribution
        partitioning = source.output_partitioning
        if distribution is None or not distribution.is_satisfied_by(partitioning):
            return None
        # An explicit groupby(num_partitions=...) asks for that parallelism.
        if (
            aggregate.num_partitions is not None
            and aggregate.num_partitions != partitioning.num_partitions
        ):
            return None
        key_columns = distribution.columns
        aggregate_fn = _make_complete_aggregating_block_fn(
            key_columns, tuple(aggregate.aggs)
        )
        if aggregate_fn is None:
            return None

        transformer = MapTransformer(
            [
                BlockMapTransformFn(
                    aggregate_fn,
                    output_block_size_option=OutputBlockSizeOption.of(
                        target_max_block_size=reduce_op.data_context.target_max_block_size
                    ),
                )
            ]
        )
        local_op = MapOperator.create(
            transformer,
            source,
            reduce_op.data_context,
            target_max_block_size_override=reduce_op.target_max_block_size_override,
            name=f"HashAggregate(key_columns={key_columns}, mode=complete)",
            compute_strategy=TaskPoolStrategy(),
            # Runs in its own tasks. Fused into the upstream reduce, it would
            # add its memory to that of the reduce's tasks.
            supports_fusion=False,
        )
        local_op.set_logical_operators(aggregate)

        # local_op already reads from source; unlink the shuffle's ops.
        source._output_dependencies.remove(map_op)
        local_op._output_dependencies = reduce_op.output_dependencies
        self._op_map.pop(map_op)
        self._op_map.pop(reduce_op)
        self._op_map[local_op] = aggregate
        return local_op


class EnsureLocalAggregateReadsWholePartitions(Rule):
    """Makes each one-phase aggregation from EliminateAggregateShuffle read
    whole partitions, or plans the shuffle it replaced again.

    After fusion, an aggregation that reads a shuffle reduce directly gets whole
    partitions: the reduce then emits each partition as one bundle, and the
    aggregation's bundler merges bundles but never splits them. Any other input,
    e.g. a map that FuseOperators didn't fuse into the reduce, hands on each
    partition over several bundles, so the shuffle is restored.
    """

    @classmethod
    def dependencies(cls) -> List[Type[Rule]]:
        return [FuseOperators]

    @classmethod
    def dependents(cls) -> List[Type[Rule]]:
        # So the aggregation's tasks get a memory request too.
        return [ConfigureMapTaskMemoryUsingOutputSize]

    def apply(self, plan: PhysicalPlan) -> PhysicalPlan:
        self._op_map = plan.op_map.copy()
        self._restored_shuffle = False
        dag = self._ensure_in_dag(plan.dag)
        plan = PhysicalPlan(dag, self._op_map, plan.context)
        if self._restored_shuffle:
            # Fuse the maps after a restored reduce into it, as without the
            # one-phase aggregation.
            plan = FuseOperators().apply(plan)
        return plan

    def _ensure_in_dag(self, op: PhysicalOperator) -> PhysicalOperator:
        op._input_dependencies = [
            self._ensure_in_dag(input_op) for input_op in op.input_dependencies
        ]
        aggregate = self._op_map.get(op)
        if not _is_local_aggregate(op, aggregate):
            return op
        (source,) = op.input_dependencies
        if isinstance(source, _SHUFFLE_REDUCE_OPS):
            source.set_emit_whole_partitions()
            return op
        return self._restore_shuffle(op, aggregate, source)

    def _restore_shuffle(
        self,
        local_op: MapOperator,
        aggregate: Aggregate,
        source: PhysicalOperator,
    ) -> PhysicalOperator:
        reduce_op = _plan_hash_shuffle_aggregate_v2(
            local_op.data_context, aggregate, source
        )
        (map_op,) = reduce_op.input_dependencies
        for op in (map_op, reduce_op):
            op.set_logical_operators(aggregate)
            if local_op.target_max_block_size_override is not None:
                op.override_target_max_block_size(
                    local_op.target_max_block_size_override
                )
            self._op_map[op] = aggregate

        # map_op already reads from source; unlink the aggregation.
        source._output_dependencies.remove(local_op)
        reduce_op._output_dependencies = local_op.output_dependencies
        self._op_map.pop(local_op)
        self._restored_shuffle = True
        return reduce_op
