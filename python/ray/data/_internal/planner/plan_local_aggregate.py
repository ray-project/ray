import logging
from typing import Any, Dict, List, Optional, Tuple, Union

from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.hash_aggregate_v2 import (
    _make_complete_aggregating_block_fn,
)
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
)
from ray.data._internal.execution.operators.shuffle_operators.disk_shuffle_reduce_operator import (  # noqa: E501
    DiskHashShuffleReduceOp,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_reduce_operator import (  # noqa: E501
    ShuffleReduceOp,
)
from ray.data._internal.execution.operators.task_pool_map_operator import (
    TaskPoolMapOperator,
)
from ray.data._internal.logical.interfaces import HashPartitioning, LogicalOperator
from ray.data._internal.logical.operators import Aggregate
from ray.data._internal.logical.rules.operator_fusion import (
    are_op_remote_args_compatible,
    fuse_map_into_shuffle_reduce_op,
)
from ray.data._internal.output_buffer import OutputBlockSizeOption
from ray.data.context import DataContext

__all__ = [
    "try_plan_local_aggregate",
]

logger = logging.getLogger(__name__)

_ShuffleReduce = Union[ShuffleReduceOp, DiskHashShuffleReduceOp]


def try_plan_local_aggregate(
    logical_op: Aggregate,
    input_physical_op: PhysicalOperator,
    data_context: DataContext,
) -> Optional[_ShuffleReduce]:
    """Fold ``logical_op`` into the upstream shuffle's reduce tasks, the only
    place a whole partition is seen, if its input is already partitioned as
    required; else return ``None``. Maps in between are fused in too::

        JoinShuffleReduce -> HashAggregateMap -> HashAggregateReduce  # before
        JoinShuffleReduce->HashAggregate(mode=complete)               # after
    """
    distribution = logical_op.required_input_distribution
    partitioning = input_physical_op.output_partitioning
    if distribution is None or not distribution.is_satisfied_by(partitioning):
        return None
    # A differing explicit groupby(num_partitions=...) needs a shuffle.
    if (
        logical_op.num_partitions is not None
        and logical_op.num_partitions != partitioning.num_partitions
    ):
        return None

    # Walk back through the (partition-preserving) maps to the source reduce.
    maps: List[TaskPoolMapOperator] = []
    src = input_physical_op
    while isinstance(src, TaskPoolMapOperator):
        maps.append(src)
        src = src.input_dependencies[0]
    if not isinstance(src, (ShuffleReduceOp, DiskHashShuffleReduceOp)):
        # E.g. an actor-pool filter, which can't run in the reduce's tasks.
        return None
    src_logical_op = src._logical_operators[0]
    if not all(_can_run_in_reduce_tasks(m, src_logical_op) for m in maps):
        return None

    local_aggregate = _fold_aggregate_into_reduce(
        src, maps, logical_op, distribution.columns, partitioning, data_context
    )
    logger.debug(
        "Planned %s without a shuffle: its input is already partitioned by %s",
        logical_op.name,
        partitioning.columns,
    )
    return local_aggregate


def _can_run_in_reduce_tasks(
    map_op: TaskPoolMapOperator, reduce_logical_op: LogicalOperator
) -> bool:
    """Same policy as ``FuseOperators._can_fuse_map_into_shuffle_reduce``."""
    if not map_op.supports_fusion():
        return False
    # The reduce has no concurrency cap; a fused concurrency= would be lost.
    if map_op.get_max_concurrency_limit() is not None:
        return False
    return are_op_remote_args_compatible(
        reduce_logical_op, map_op._logical_operators[0]
    )


def _fold_aggregate_into_reduce(
    src: _ShuffleReduce,
    maps: List[TaskPoolMapOperator],
    logical_op: Aggregate,
    key_columns: Tuple[str, ...],
    input_partitioning: HashPartitioning,
    data_context: DataContext,
) -> _ShuffleReduce:
    """Rebuild ``src`` to also run ``maps`` (downstream-first), then the aggregation."""
    aggregate_transformer = MapTransformer(
        [
            BlockMapTransformFn(
                _make_complete_aggregating_block_fn(
                    key_columns, tuple(logical_op.aggs)
                ),
                output_block_size_option=OutputBlockSizeOption.of(
                    target_max_block_size=data_context.target_max_block_size
                ),
            )
        ]
    )

    transformers: List[MapTransformer] = []
    names: List[str] = [src.name]
    task_kwargs: Dict[str, Any] = {}
    if src._fused_output_map_transformer is not None:
        transformers.append(src._fused_output_map_transformer)
        task_kwargs.update(src._fused_output_map_task_kwargs)
    for map_op in reversed(maps):
        transformers.append(map_op.get_map_transformer())
        task_kwargs.update(map_op.get_map_task_kwargs())
        names.append(map_op.name)
    transformers.append(aggregate_transformer)
    names.append(f"HashAggregate(key_columns={key_columns}, mode=complete)")

    fused_transformer = transformers[0]
    for transformer in transformers[1:]:
        fused_transformer = fused_transformer.fuse(transformer)

    fused_op = fuse_map_into_shuffle_reduce_op(
        src,
        name="->".join(names),
        map_transformer=fused_transformer,
        map_task_kwargs=task_kwargs,
        target_max_block_size_override=None,
        # Groups stay within their partition, so the input's layout is kept.
        output_partitioning=input_partitioning,
    )
    # The shuffle map ops still list src as an output; replace it with
    # fused_op so src and maps drop out of the DAG.
    fused_op._rewire_output_deps(src, fused_op.input_dependencies)
    fused_op.set_logical_operators(
        *src._logical_operators,
        *(lo for map_op in reversed(maps) for lo in map_op._logical_operators),
        logical_op,
    )
    return fused_op
