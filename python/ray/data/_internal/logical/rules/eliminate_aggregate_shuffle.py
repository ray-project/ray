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
from ray.data._internal.execution.operators.shuffle_operators.disk_shuffle_reduce_operator import (  # noqa: E501
    DiskHashShuffleReduceOp,
    DiskHashShuffleRegroupOp,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_map_operator import (  # noqa: E501
    ShuffleMapOp,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_reduce_operator import (  # noqa: E501
    ShuffleReduceOp,
)
from ray.data._internal.logical.interfaces import PhysicalPlan, Plan, Rule
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

_SHUFFLE_REDUCE_OPS = (ShuffleReduceOp, DiskHashShuffleReduceOp)
# Set on the map ops EliminateAggregateShuffle creates.
_LOCAL_AGGREGATE_ATTR = "_is_local_aggregate"


def _shuffle_ops_before(
    reduce_op: PhysicalOperator,
) -> Optional[List[PhysicalOperator]]:
    """Return the ops between a single-input shuffle reduce and its source,
    nearest first: ``[ShuffleMapOp]``, or ``[regroup, writer]`` for the disk
    shuffle. None if the reduce isn't fed that way."""
    (upstream,) = reduce_op.input_dependencies
    if isinstance(upstream, ShuffleMapOp):
        return [upstream]
    if isinstance(upstream, DiskHashShuffleRegroupOp):
        (writer,) = upstream.input_dependencies
        return [upstream, writer]
    return None


class EliminateAggregateShuffle(Rule):
    """Skips a group-by's shuffle when its input is already hash-partitioned on
    a subset of the group-by keys, e.g. by a join on those keys::

        JoinShuffleReduce -> HashAggregateMap -> HashAggregateReduce   # before
        JoinShuffleReduce -> HashAggregate(mode=complete)              # after

    Runs before FuseOperators, which drops the partitioning. Each task of the
    new aggregation needs whole partitions; EnsureLocalAggregateReadsWholePartitions
    checks that after fusion, or puts the shuffle back.
    """

    @classmethod
    def dependencies(cls) -> List[Type[Rule]]:
        # So the aggregation copies the reduce's inherited block size.
        return [InheritTargetMaxBlockSizeRule]

    @classmethod
    def dependents(cls) -> List[Type[Rule]]:
        return [FuseOperators]

    def apply(self, plan: Plan) -> Plan:
        assert isinstance(plan, PhysicalPlan)
        if not plan.context.enable_aggregate_shuffle_elimination:
            return plan
        self._op_map = plan.op_map.copy()
        dag = self._eliminate_in_dag(plan.dag)
        return PhysicalPlan(dag, self._op_map, plan.context)

    def _eliminate_in_dag(self, op: PhysicalOperator) -> PhysicalOperator:
        # Upstream first. The new aggregation declares no partitioning, so a
        # group-by right after it keeps its shuffle.
        op._input_dependencies = [
            self._eliminate_in_dag(input_op) for input_op in op.input_dependencies
        ]
        local_op = self._try_replace_shuffle(op)
        return op if local_op is None else local_op

    def _try_replace_shuffle(
        self, reduce_op: PhysicalOperator
    ) -> Optional[MapOperator]:
        """Replace a group-by's redundant shuffle with a one-phase aggregation,
        or return None to keep it."""
        aggregate = self._op_map.get(reduce_op)
        if not (
            isinstance(reduce_op, _SHUFFLE_REDUCE_OPS)
            and isinstance(aggregate, Aggregate)
        ):
            return None
        # Replacing it would drop the fused map. Only a shuffle restored in an
        # earlier optimizer pass has one.
        if reduce_op._fused_output_map_transformer is not None:
            return None
        shuffle_ops = _shuffle_ops_before(reduce_op)
        if shuffle_ops is None:
            return None
        map_op = shuffle_ops[-1]
        (source,) = map_op.input_dependencies

        distribution = aggregate.required_input_distribution
        partitioning = source.output_partitioning
        if distribution is None or partitioning is None:
            return None
        if not distribution.is_satisfied_by(partitioning):
            return None
        # Keep the shuffle if groupby(num_partitions=...) asks for another count.
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
            # Fused into the join's reduce tasks, it would add to their memory.
            supports_fusion=False,
        )
        local_op.set_logical_operators(aggregate)
        setattr(local_op, _LOCAL_AGGREGATE_ATTR, True)

        # create() already linked source to local_op; drop the shuffle's ops.
        source._output_dependencies.remove(map_op)
        local_op._output_dependencies = reduce_op.output_dependencies
        for op in (*shuffle_ops, reduce_op):
            self._op_map.pop(op)
        self._op_map[local_op] = aggregate
        return local_op


class EnsureLocalAggregateReadsWholePartitions(Rule):
    """After fusion, makes each one-phase aggregation read whole partitions, or
    puts the shuffle back.

    Reading straight from a shuffle reduce works: the reduce emits each
    partition as one bundle, and the aggregation's input queue never splits
    bundles. Any op in between, e.g. an unfused map, can split a partition.
    """

    @classmethod
    def dependencies(cls) -> List[Type[Rule]]:
        return [FuseOperators]

    @classmethod
    def dependents(cls) -> List[Type[Rule]]:
        # Size task memory on the final DAG, after any shuffle is restored.
        return [ConfigureMapTaskMemoryUsingOutputSize]

    def apply(self, plan: Plan) -> Plan:
        assert isinstance(plan, PhysicalPlan)
        self._op_map = plan.op_map.copy()
        self._restored_shuffle = False
        dag = self._ensure_in_dag(plan.dag)
        plan = PhysicalPlan(dag, self._op_map, plan.context)
        if self._restored_shuffle:
            # Fuse later maps into the restored reduce, as without this rule.
            plan = FuseOperators().apply(plan)
        return plan

    def _ensure_in_dag(self, op: PhysicalOperator) -> PhysicalOperator:
        op._input_dependencies = [
            self._ensure_in_dag(input_op) for input_op in op.input_dependencies
        ]
        aggregate = self._op_map.get(op)
        # Other map ops can map to an Aggregate too, e.g. the disk shuffle's
        # writer.
        if not (
            isinstance(op, MapOperator)
            and isinstance(aggregate, Aggregate)
            and getattr(op, _LOCAL_AGGREGATE_ATTR, False)
        ):
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
        shuffle_ops = _shuffle_ops_before(reduce_op)
        assert shuffle_ops is not None
        for op in (*shuffle_ops, reduce_op):
            op.set_logical_operators(aggregate)
            if local_op.target_max_block_size_override is not None:
                op.override_target_max_block_size(
                    local_op.target_max_block_size_override
                )
            self._op_map[op] = aggregate

        # The new map op already reads source; drop the aggregation.
        source._output_dependencies.remove(local_op)
        reduce_op._output_dependencies = local_op.output_dependencies
        self._op_map.pop(local_op)
        self._restored_shuffle = True
        return reduce_op
