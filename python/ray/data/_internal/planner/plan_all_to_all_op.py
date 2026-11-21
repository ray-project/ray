import functools
import logging
import math
import secrets
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import pyarrow as pa

from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.base_physical_operator import (
    AllToAllOperator,
)
from ray.data._internal.execution.operators.hash_aggregate_v2 import (
    PartialAggMode,
    PartialAggPolicy,
    _make_aggregating_reduce_fn,
    _make_aggregating_transformer,
)
from ray.data._internal.execution.operators.hash_shuffle_v2 import (
    _SHUFFLE_MAP_RUNTIME_ENV,
    _concat_reduce,
    _make_hash_partition_fn,
    _make_slice_partition_fn,
    _sort_reduce,
)
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
)
from ray.data._internal.execution.operators.random_shuffle import (
    _RANDOM_SHUFFLE_REDUCE_PEAK_MEMORY_MULTIPLIER,
    RandomShufflePartitioner,
    make_random_shuffle_reduce_fn,
)
from ray.data._internal.execution.operators.shuffle_operators.disk_shuffle_reduce_operator import (  # noqa: E501
    DiskHashShuffleReduceOp,
    DiskHashShuffleRegroupOp,
    DiskRangeShuffleRegroupOp,
)
from ray.data._internal.execution.operators.shuffle_operators.disk_shuffle_runtime import (  # noqa: E501
    disk_shuffle_dirs,
)
from ray.data._internal.execution.operators.shuffle_operators.disk_shuffle_tasks import (  # noqa: E501
    BlockTransformer,
    DiskShuffleWriteFn,
    DiskSortRunWriteFn,
    PartitionFn,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_map_operator import (  # noqa: E501
    ShuffleMapOp,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_reduce_operator import (  # noqa: E501
    ShuffleReduceOp,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_tasks import (
    SHUFFLE_PEAK_MEMORY_MULTIPLIER,
)
from ray.data._internal.execution.operators.shuffle_operators.sort_sampling_operator import (  # noqa: E501
    SORT_SAMPLE_ROWS_PER_BLOCK,
    SortSamplingOp,
)
from ray.data._internal.execution.operators.shuffle_operators.sort_shuffle_map_operator import (  # noqa: E501
    SortShuffleMapOp,
)
from ray.data._internal.execution.operators.sort_shuffle import (
    make_sort_reduce_fn,
    user_boundaries,
)
from ray.data._internal.execution.util import merge_runtime_envs
from ray.data._internal.logical.interfaces import HashPartitioning
from ray.data._internal.logical.operators import (
    AbstractAllToAll,
    Aggregate,
    RandomizeBlocks,
    RandomShuffle,
    Repartition,
    Sort,
    TopN,
)
from ray.data._internal.planner.aggregate import generate_aggregate_fn
from ray.data._internal.planner.exchange.sort_task_spec import SortKey, SortTaskSpec
from ray.data._internal.planner.random_shuffle import generate_random_shuffle_fn
from ray.data._internal.planner.randomize_blocks import generate_randomize_blocks_fn
from ray.data._internal.planner.repartition import generate_repartition_fn
from ray.data._internal.planner.sort import generate_sort_fn
from ray.data._internal.planner.top_n import plan_top_n_op
from ray.data._internal.random_config import get_single_integer_random_seed
from ray.data.context import DataContext, ShuffleStrategy

logger = logging.getLogger(__name__)


# A sorting reduce (`_sort_reduce`, used by repartition(sort=True) and thus
# map_groups) peaks at ~3x its input: on top of holding the decoded input
# shards and the concatenated table, sort_by materializes a sorted copy.
# The generic 2x shuffle estimate under-requests it, overpacking reducers
# per node and risking OOM kills.
_SORT_REDUCE_PEAK_MEMORY_MULTIPLIER = 3


def plan_disk_hash_shuffle_map(
    input_op: PhysicalOperator,
    data_context: DataContext,
    *,
    num_partitions: int,
    partition_fn: PartitionFn,
    block_transformer: Optional[BlockTransformer] = None,
    map_cpus: float = ShuffleMapOp._DEFAULT_SHUFFLE_MAP_TASK_NUM_CPUS,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    name: str = "DiskHashShuffleMap",
    on_start: Optional[Callable[[Optional[pa.Schema]], None]] = None,
) -> MapOperator:
    """Plan the disk-shuffle map stage: a task-pool ``MapOperator`` whose
    transform partitions each input bundle into one local file and yields a
    one-row handle block for ``DiskHashShuffleRegroupOp``.

    Args:
        input_op: Upstream physical operator.
        data_context: Runtime configuration.
        num_partitions: Total number of output partitions.
        partition_fn: Function mapping a pa.Table to Dict[int, pa.Table].
        block_transformer: Optional transform applied to the concatenation of
            each task's input blocks before partitioning (e.g. map-side
            partial aggregation). May change the block schema.
        map_cpus: CPU request per map task.
        ray_remote_args: User ``ray.remote`` options for the map tasks; they
            override the defaults, and ``runtime_env`` merges with the
            shuffle's own. ``None`` values mean "unset", as in ``ray.remote``.
        name: Display name of the map operator.
        on_start: Called with the first input's schema before any map task is
            submitted, i.e. before ``partition_fn`` is shipped to the tasks.

    Returns:
        The map operator writing this shuffle's mapper files.
    """
    return _plan_disk_shuffle_writer(
        input_op,
        data_context,
        make_write_fn=lambda map_dir, shuffle_id: DiskShuffleWriteFn(
            partition_fn=partition_fn,
            num_partitions=num_partitions,
            out_dir=map_dir,
            shuffle_id=shuffle_id,
            compression=data_context.shuffle_compression,
            block_transformer=block_transformer,
        ),
        map_cpus=map_cpus,
        ray_remote_args=ray_remote_args,
        name=name,
        on_start=on_start,
    )


def plan_disk_sort_shuffle_map(
    input_op: PhysicalOperator,
    data_context: DataContext,
    *,
    num_partitions: int,
    sort_key: SortKey,
    estimated_num_input_blocks: Optional[int] = None,
    map_cpus: float = ShuffleMapOp._DEFAULT_SHUFFLE_MAP_TASK_NUM_CPUS,
    name: str = "DiskSortShuffleMap",
) -> MapOperator:
    """Plan the disk sort shuffle's map stage: a task-pool ``MapOperator``
    whose transform sorts each input bundle into one local file, written
    whole in chunks (``DiskSortRunWriteFn``), and yields a one-row handle
    block for ``DiskRangeShuffleRegroupOp``. Nothing waits for range
    boundaries: the regroup computes them from the handles' key samples.

    Args:
        input_op: Upstream physical operator.
        data_context: Runtime configuration.
        num_partitions: Total number of output partitions.
        sort_key: Sort columns and directions; validated against the first
            input's schema before any task runs.
        estimated_num_input_blocks: Sizes each task's key sample so the
            dataset contributes about ``SORT_SAMPLE_POINTS_PER_PARTITION``
            samples per partition, as the object-store sort samples.
        map_cpus: CPU request per map task.
        name: Display name of the map operator.

    Returns:
        The map operator writing this sort's run files.
    """
    if estimated_num_input_blocks:
        sample_rows = math.ceil(
            SortTaskSpec.SORT_SAMPLE_POINTS_PER_PARTITION
            * num_partitions
            / estimated_num_input_blocks
        )
    else:
        sample_rows = SORT_SAMPLE_ROWS_PER_BLOCK
    return _plan_disk_shuffle_writer(
        input_op,
        data_context,
        make_write_fn=lambda map_dir, shuffle_id: DiskSortRunWriteFn(
            sort_key=sort_key,
            num_partitions=num_partitions,
            sample_rows=max(1, sample_rows),
            max_chunk_bytes=data_context.target_max_block_size,
            out_dir=map_dir,
            shuffle_id=shuffle_id,
            compression=data_context.shuffle_compression,
        ),
        map_cpus=map_cpus,
        name=name,
        on_start=sort_key.validate_schema,
    )


def _plan_disk_shuffle_writer(
    input_op: PhysicalOperator,
    data_context: DataContext,
    *,
    make_write_fn: Callable[[str, str], Callable],
    map_cpus: float,
    name: str,
    on_start: Optional[Callable[[Optional[pa.Schema]], None]],
    ray_remote_args: Optional[Dict[str, Any]] = None,
) -> MapOperator:
    """A task-pool ``MapOperator`` running the block transform
    ``make_write_fn(map_dir, shuffle_id)`` builds, for a fresh shuffle id."""
    shuffle_id = secrets.token_hex(8)
    map_dir, _ = disk_shuffle_dirs(shuffle_id)
    write_fn = make_write_fn(map_dir, shuffle_id)
    user_args = {k: v for k, v in (ray_remote_args or {}).items() if v is not None}
    user_runtime_env = user_args.pop("runtime_env", None)
    remote_args: Dict[str, Any] = {
        "num_cpus": map_cpus,
        "runtime_env": merge_runtime_envs(
            _SHUFFLE_MAP_RUNTIME_ENV, user_runtime_env or {}
        ),
        **user_args,
    }
    return MapOperator.create(
        MapTransformer([BlockMapTransformFn(write_fn, disable_block_shaping=True)]),
        input_op,
        data_context,
        name=name,
        ray_remote_args=remote_args,
        supports_fusion=False,
        on_start=on_start,
    )


def plan_disk_hash_shuffle_reduce(
    input_op: Union[PhysicalOperator, List[PhysicalOperator]],
    data_context: DataContext,
    *,
    num_partitions: int,
    **reduce_kwargs: Any,
) -> DiskHashShuffleReduceOp:
    """Plan the disk-shuffle reduce stage over the writer(s) from
    ``plan_disk_hash_shuffle_map`` with a ``DiskHashShuffleRegroupOp`` that turns
    their handles into one bundle per partition, then the reduce.

    Args:
        input_op: The writer, or one writer per input (e.g. join).
        data_context: Runtime configuration.
        num_partitions: Total number of output partitions.
        **reduce_kwargs: Forwarded to ``DiskHashShuffleReduceOp``.

    Returns:
        The reduce operator.
    """
    input_ops = [input_op] if isinstance(input_op, PhysicalOperator) else input_op
    regroup_op = DiskHashShuffleRegroupOp(
        input_ops, data_context, num_partitions=num_partitions
    )
    return DiskHashShuffleReduceOp(
        regroup_op, data_context, num_partitions=num_partitions, **reduce_kwargs
    )


def plan_disk_sort_shuffle_reduce(
    input_op: PhysicalOperator,
    data_context: DataContext,
    *,
    num_partitions: int,
    sort_key: SortKey,
    **reduce_kwargs: Any,
) -> DiskHashShuffleReduceOp:
    """Plan the disk sort shuffle's reduce stage over the writer from
    ``plan_disk_sort_shuffle_map``: a ``DiskRangeShuffleRegroupOp`` that
    computes the range boundaries and turns the handles into one bundle per
    partition, then the reduce.

    Args:
        input_op: The writer.
        data_context: Runtime configuration.
        num_partitions: Total number of output partitions.
        sort_key: Sort columns, directions and any user boundaries.
        **reduce_kwargs: Forwarded to ``DiskHashShuffleReduceOp``.

    Returns:
        The reduce operator.
    """
    regroup_op = DiskRangeShuffleRegroupOp(
        input_op,
        data_context,
        num_partitions=num_partitions,
        sort_key=sort_key,
        boundaries=user_boundaries(sort_key),
        name=f"DiskRangeShuffleRegroup(partitions={num_partitions})",
    )
    return DiskHashShuffleReduceOp(
        regroup_op, data_context, num_partitions=num_partitions, **reduce_kwargs
    )


def _select_shuffle_v2_op_classes(
    data_context: DataContext,
) -> Tuple[Callable[..., PhysicalOperator], Callable[..., PhysicalOperator], str]:
    """Pick the SHUFFLE_V2 map/reduce op family: disk-based (file-transport) or
    object-store, per ``data_context.use_disk_based_hash_shuffle``.

    Returns ``(build_map, reduce_cls, name_prefix)``. ``build_map`` takes
    ``(input_op, data_context, *, num_partitions, partition_fn,
    block_transformer, name)`` and returns the operator the reduce consumes.
    ``name_prefix`` is ``"Disk"`` or ``""``, to be prepended to the op display
    names.
    """
    if data_context.use_disk_based_hash_shuffle:
        return plan_disk_hash_shuffle_map, plan_disk_hash_shuffle_reduce, "Disk"
    return (
        functools.partial(ShuffleMapOp, map_runtime_env=_SHUFFLE_MAP_RUNTIME_ENV),
        ShuffleReduceOp,
        "",
    )


def _plan_gpu_shuffle_repartition(
    data_context: DataContext,
    logical_op: Repartition,
    input_physical_op: PhysicalOperator,
) -> PhysicalOperator:
    from ray.data._internal.gpu_shuffle.hash_shuffle import GPUShuffleOperator
    from ray.data._internal.planner.exchange.sort_task_spec import SortKey

    normalized_key_columns = SortKey(logical_op.keys).get_columns()

    schema = logical_op.infer_schema()
    columns = list(schema.names) if schema is not None else None

    return GPUShuffleOperator(
        input_physical_op,
        data_context,
        key_columns=tuple(normalized_key_columns),
        columns=columns,
        num_partitions=logical_op.num_outputs,
        should_sort=logical_op.sort,
    )


def _plan_hash_shuffle_repartition_v2(
    data_context: DataContext,
    logical_op: Repartition,
    input_physical_op: PhysicalOperator,
) -> PhysicalOperator:
    """Build the two-op Map → Reduce DAG for SHUFFLE_V2 hash-shuffle repartition.

    Picks the disk-based (file-transport) or object-store op pair based on
    ``data_context.use_disk_based_hash_shuffle``. Returns the reduce op; the
    executor crawls upstream via its input_dependencies to find the map op.
    """
    from ray.data._internal.planner.exchange.sort_task_spec import SortKey

    normalized_key_columns = SortKey(logical_op.keys).get_columns()
    key_list = list(normalized_key_columns)

    target_num_partitions = (
        logical_op.num_outputs or data_context.default_hash_shuffle_parallelism
    )

    partition_fn = _make_hash_partition_fn(key_list, target_num_partitions)
    reduce_fn = _sort_reduce(key_list) if logical_op.sort else _concat_reduce
    peak_memory_multiplier = (
        _SORT_REDUCE_PEAK_MEMORY_MULTIPLIER
        if logical_op.sort
        else SHUFFLE_PEAK_MEMORY_MULTIPLIER
    )

    map_cls, reduce_cls, prefix = _select_shuffle_v2_op_classes(data_context)

    map_op = map_cls(
        input_physical_op,
        data_context,
        num_partitions=target_num_partitions,
        partition_fn=partition_fn,
        name=(
            f"{prefix}HashShuffleMap(keys={tuple(key_list)}, "
            f"partitions={target_num_partitions})"
        ),
    )
    reduce_op = reduce_cls(
        map_op,
        data_context,
        num_partitions=target_num_partitions,
        reduce_fn=reduce_fn,
        disallow_block_splitting=True,
        peak_memory_multiplier=peak_memory_multiplier,
        name=(
            f"{prefix}HashShuffleReduce(keys={tuple(key_list)}, "
            f"partitions={target_num_partitions})"
        ),
    )
    return reduce_op


def _plan_shuffle_repartition_v2(
    data_context: DataContext,
    logical_op: Repartition,
    input_physical_op: PhysicalOperator,
) -> PhysicalOperator:
    """Build the Map → Reduce DAG for SHUFFLE_V2 slice-based repartition."""
    num_partitions = (
        logical_op.num_outputs or data_context.default_hash_shuffle_parallelism
    )
    map_cls, reduce_cls, prefix = _select_shuffle_v2_op_classes(data_context)

    map_op = map_cls(
        input_physical_op,
        data_context,
        num_partitions=num_partitions,
        partition_fn=_make_slice_partition_fn(num_partitions),
        name=f"{prefix}SliceShuffleMap(partitions={num_partitions})",
    )
    return reduce_cls(
        map_op,
        data_context,
        num_partitions=num_partitions,
        reduce_fn=_concat_reduce,
        disallow_block_splitting=True,
        peak_memory_multiplier=SHUFFLE_PEAK_MEMORY_MULTIPLIER,
        name=f"{prefix}SliceShuffleReduce(partitions={num_partitions})",
    )


def _plan_hash_shuffle_repartition(
    data_context: DataContext,
    logical_op: Repartition,
    input_physical_op: PhysicalOperator,
) -> PhysicalOperator:
    from ray.data._internal.execution.operators.hash_shuffle import (
        HashShuffleOperator,
    )
    from ray.data._internal.planner.exchange.sort_task_spec import SortKey

    normalized_key_columns = SortKey(logical_op.keys).get_columns()

    return HashShuffleOperator(
        input_physical_op,
        data_context,
        key_columns=tuple(normalized_key_columns),
        num_partitions=logical_op.num_outputs,
        should_sort=logical_op.sort,
    )


def _plan_sort_v2(
    data_context: DataContext,
    logical_op: Sort,
    input_physical_op: PhysicalOperator,
) -> PhysicalOperator:
    sort_key = logical_op.sort_key
    estimated_num_input_blocks = logical_op.input_dependencies[
        0
    ].estimated_num_outputs()
    if sort_key.boundaries:
        num_partitions = len(sort_key.boundaries) + 1
    else:
        num_partitions = (
            estimated_num_input_blocks or data_context.default_hash_shuffle_parallelism
        )

    reduce_kwargs = dict(
        num_partitions=num_partitions,
        reduce_fn=make_sort_reduce_fn(sort_key, data_context),
        # Reduce output is split to ``target_max_block_size``; the splits of a
        # partition are emitted in order under the same partition key.
        preserve_partition_order=True,
        peak_memory_multiplier=_SORT_REDUCE_PEAK_MEMORY_MULTIPLIER,
        reduce_ray_remote_args=logical_op.ray_remote_args,
    )
    if data_context.use_disk_based_hash_shuffle:
        # The writers sort and write their input whole; the regroup computes
        # the boundaries afterwards, so no op holds the input back meanwhile.
        map_op = plan_disk_sort_shuffle_map(
            input_physical_op,
            data_context,
            num_partitions=num_partitions,
            sort_key=sort_key,
            estimated_num_input_blocks=estimated_num_input_blocks,
            name=f"DiskSortShuffleMap(partitions={num_partitions})",
        )
        return plan_disk_sort_shuffle_reduce(
            map_op,
            data_context,
            sort_key=sort_key,
            name=f"DiskSortShuffleReduce(partitions={num_partitions})",
            **reduce_kwargs,
        )

    # The object-store map partitions its input, so it needs the boundaries
    # first: the sampling op holds every input until they are computed.
    map_input_op = input_physical_op
    if not sort_key.boundaries and num_partitions > 1:
        map_input_op = SortSamplingOp(
            input_physical_op,
            data_context,
            num_partitions=num_partitions,
            sort_key=sort_key,
            estimated_num_input_blocks=estimated_num_input_blocks,
            name=f"SortSample(partitions={num_partitions})",
        )
    map_op = SortShuffleMapOp(
        map_input_op,
        data_context,
        num_partitions=num_partitions,
        sort_key=sort_key,
        map_runtime_env=_SHUFFLE_MAP_RUNTIME_ENV,
        name=f"SortShuffleMap(partitions={num_partitions})",
    )
    return ShuffleReduceOp(
        map_op,
        data_context,
        name=f"SortShuffleReduce(partitions={num_partitions})",
        **reduce_kwargs,
    )


def _plan_random_shuffle_v2(
    data_context: DataContext,
    logical_op: RandomShuffle,
    input_physical_op: PhysicalOperator,
) -> PhysicalOperator:
    """Build the disk Map -> Reduce DAG for a SHUFFLE_V2 random shuffle."""
    # Pinned at plan time so retried tasks reproduce their output.
    seed = get_single_integer_random_seed(logical_op.seed_config, data_context)
    num_partitions = (
        logical_op.input_dependencies[0].estimated_num_outputs()
        or data_context.default_hash_shuffle_parallelism
    )

    map_op = plan_disk_hash_shuffle_map(
        input_physical_op,
        data_context,
        num_partitions=num_partitions,
        partition_fn=RandomShufflePartitioner(num_partitions, seed),
        ray_remote_args=logical_op.ray_remote_args,
        name=f"DiskRandomShuffleMap(partitions={num_partitions})",
    )
    return plan_disk_hash_shuffle_reduce(
        map_op,
        data_context,
        num_partitions=num_partitions,
        reduce_fn=make_random_shuffle_reduce_fn(seed),
        # A user seed promises the same output across runs, so partitions must
        # come out in order. ``seed`` is never None: unseeded uses a timestamp.
        preserve_partition_order=(
            logical_op.seed_config.seed is not None
            or data_context.execution_options.preserve_order
        ),
        peak_memory_multiplier=_RANDOM_SHUFFLE_REDUCE_PEAK_MEMORY_MULTIPLIER,
        reduce_ray_remote_args=logical_op.ray_remote_args,
        name=f"DiskRandomShuffleReduce(partitions={num_partitions})",
    )


def _choose_partial_agg_policy(data_context: DataContext) -> PartialAggPolicy:
    """Picks how the map side of a SHUFFLE_V2 aggregation pre-aggregates."""
    try:
        mode = PartialAggMode(data_context.partial_aggregation_mode)
    except ValueError:
        raise ValueError(
            "DataContext.partial_aggregation_mode must be one of "
            f"{[m.value for m in PartialAggMode]}, got "
            f"{data_context.partial_aggregation_mode!r}"
        ) from None
    if mode != PartialAggMode.ESTIMATE:
        return PartialAggPolicy(mode=mode)
    sample_rows = data_context.adaptive_partial_aggregation_sample_rows
    if (
        isinstance(sample_rows, bool)
        or not isinstance(sample_rows, int)
        or (sample_rows <= 0)
    ):
        raise ValueError(
            "DataContext.adaptive_partial_aggregation_sample_rows must be a "
            f"positive integer, got {sample_rows!r}"
        )
    threshold = data_context.adaptive_partial_aggregation_unique_ratio_threshold
    if (
        isinstance(threshold, bool)
        or not isinstance(threshold, (int, float))
        or not 0 <= threshold <= 1
    ):
        raise ValueError(
            "DataContext.adaptive_partial_aggregation_unique_ratio_threshold "
            f"must be a number between 0 and 1, got {threshold!r}"
        )
    return PartialAggPolicy(
        mode=PartialAggMode.ESTIMATE,
        sample_rows=sample_rows,
        unique_ratio_threshold=float(threshold),
    )


def _plan_hash_shuffle_aggregate_v2(
    data_context: DataContext,
    logical_op: Aggregate,
    input_physical_op: PhysicalOperator,
) -> PhysicalOperator:
    from ray.data._internal.planner.exchange.sort_task_spec import SortKey

    normalized_key_columns = SortKey(logical_op.key).get_columns()
    key_columns = tuple(normalized_key_columns)
    key_list = list(key_columns)
    aggs = tuple(logical_op.aggs)

    if key_list:
        num_partitions = (
            logical_op.num_partitions or data_context.default_hash_shuffle_parallelism
        )
    else:
        # Global aggregation reduces the whole dataset to a single row.
        num_partitions = 1

    partition_fn = _make_hash_partition_fn(key_list, num_partitions)
    block_transformer = _make_aggregating_transformer(
        key_columns, aggs, policy=_choose_partial_agg_policy(data_context)
    )
    reduce_fn = _make_aggregating_reduce_fn(key_columns, aggs)

    map_cls, reduce_cls, prefix = _select_shuffle_v2_op_classes(data_context)

    # Partitioned by the group-by keys; a global aggregation has none. An
    # aggregation named after a key overwrites that column (see
    # _finalize_aggregates), so the output isn't partitioned by it either.
    output_partitioning = None
    agg_names = {agg.name for agg in aggs}
    if key_columns and not any(column in agg_names for column in key_columns):
        output_partitioning = HashPartitioning(
            columns=key_columns, num_partitions=num_partitions
        )

    map_op = map_cls(
        input_physical_op,
        data_context,
        num_partitions=num_partitions,
        partition_fn=partition_fn,
        block_transformer=block_transformer,
        name=(
            f"{prefix}HashAggregateMap(key_columns={key_columns}, "
            f"num_partitions={num_partitions})"
        ),
    )
    reduce_op = reduce_cls(
        map_op,
        data_context,
        num_partitions=num_partitions,
        reduce_fn=reduce_fn,
        # Empty partitions (groups absent from a partition) produce no output
        # block; a placeholder would carry the map's pre-finalize schema and
        # conflict with finalized non-empty partitions.
        should_emit_empty_partitions=False,
        # Aggregation collapses each key group to one row.
        preserves_row_count=False,
        name=(
            f"{prefix}HashAggregateReduce(key_columns={key_columns}, "
            f"num_partitions={num_partitions})"
        ),
        output_partitioning=output_partitioning,
    )
    return reduce_op


def _plan_hash_shuffle_aggregate(
    data_context: DataContext,
    logical_op: Aggregate,
    input_physical_op: PhysicalOperator,
) -> PhysicalOperator:
    from ray.data._internal.execution.operators.hash_aggregate import (
        HashAggregateOperator,
    )
    from ray.data._internal.planner.exchange.sort_task_spec import SortKey

    normalized_key_columns = SortKey(logical_op.key).get_columns()

    return HashAggregateOperator(
        data_context,
        input_physical_op,
        key_columns=tuple(normalized_key_columns),  # noqa: type
        aggregation_fns=tuple(logical_op.aggs),  # noqa: type
        # NOTE: In case number of partitions is not specified, we fall back to
        #       default min parallelism configured
        num_partitions=logical_op.num_partitions,
        # TODO wire in aggregator args overrides
    )


def _plan_gpu_shuffle_aggregate(
    data_context: DataContext,
    logical_op: Aggregate,
    input_physical_op: PhysicalOperator,
) -> PhysicalOperator:
    from ray.data._internal.gpu_shuffle.hash_aggregate import (
        GPUAggregateFn,
        GPUHashAggregateOperator,
        build_gpu_aggregation_plan,
    )
    from ray.data._internal.planner.exchange.sort_task_spec import SortKey

    normalized_key_columns = SortKey(logical_op.key).get_columns()
    key_columns = tuple(normalized_key_columns)
    aggregation_fns = tuple(logical_op.aggs)
    input_schema = logical_op.input_dependencies[0].infer_schema()

    aggregation_plan = build_gpu_aggregation_plan(
        key_columns, aggregation_fns, input_schema=input_schema
    )
    if isinstance(aggregation_plan, str):
        # Fall back to CPU hash aggregate if GPU aggregation plan is not supported.
        fallback_reason = aggregation_plan
        if any(isinstance(agg, GPUAggregateFn) for agg in aggregation_fns):
            raise ValueError(
                "GPU aggregation plan is not supported for a GPUAggregateFn "
                f"aggregate list with key={logical_op.key}, aggs={logical_op.aggs}: "
                f"{fallback_reason}."
            )
        logger.warning(
            "GPU aggregation plan is not supported for key=%s, aggs=%s: %s; "
            "falling back to CPU hash aggregate.",
            logical_op.key,
            logical_op.aggs,
            fallback_reason,
        )

        return _plan_hash_shuffle_aggregate(data_context, logical_op, input_physical_op)

    return GPUHashAggregateOperator(
        data_context,
        input_physical_op,
        key_columns=key_columns,  # noqa: type
        aggregation_plan=aggregation_plan,
        num_partitions=logical_op.num_partitions,
    )


def plan_all_to_all_op(
    op: AbstractAllToAll,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> PhysicalOperator:
    """Get the corresponding physical operators DAG for AbstractAllToAll operators.

    Note this method only converts the given `op`, but not its input dependencies.
    See Planner.plan() for more details.
    """
    assert len(physical_children) == 1
    input_physical_dag = physical_children[0]

    if isinstance(op, RandomizeBlocks):
        fn = generate_randomize_blocks_fn(op, data_context)
        # Randomize block order does not actually compute anything, so we
        # want to inherit the upstream op's target max block size.

    elif isinstance(op, RandomShuffle):
        # Only the disk transport gets a v2 random shuffle; the object-store
        # transport is being retired, so it keeps the legacy all-to-all op.
        if (
            data_context.shuffle_strategy == ShuffleStrategy.SHUFFLE_V2
            and data_context.use_disk_based_hash_shuffle
        ):
            return _plan_random_shuffle_v2(data_context, op, input_physical_dag)
        debug_limit_shuffle_execution_to_num_blocks = data_context.get_config(
            "debug_limit_shuffle_execution_to_num_blocks", None
        )
        fn = generate_random_shuffle_fn(
            data_context,
            op.seed_config,
            op.num_outputs,
            op.ray_remote_args,
            debug_limit_shuffle_execution_to_num_blocks,
        )

    elif isinstance(op, Repartition):
        if op.keys:
            if data_context.shuffle_strategy == ShuffleStrategy.GPU_SHUFFLE:
                return _plan_gpu_shuffle_repartition(
                    data_context, op, input_physical_dag
                )
            elif data_context.shuffle_strategy == ShuffleStrategy.SHUFFLE_V2:
                return _plan_hash_shuffle_repartition_v2(
                    data_context, op, input_physical_dag
                )
            elif data_context.shuffle_strategy == ShuffleStrategy.HASH_SHUFFLE:
                return _plan_hash_shuffle_repartition(
                    data_context, op, input_physical_dag
                )
            else:
                raise ValueError(
                    "Key-based repartitioning only supported for "
                    f"`DataContext.shuffle_strategy=HASH_SHUFFLE`, "
                    f"`DataContext.shuffle_strategy=SHUFFLE_V2` or "
                    f"`DataContext.shuffle_strategy=GPU_SHUFFLE` "
                    f"(got {data_context.shuffle_strategy})"
                )

        elif op.shuffle and data_context.shuffle_strategy == ShuffleStrategy.SHUFFLE_V2:
            return _plan_shuffle_repartition_v2(data_context, op, input_physical_dag)
        elif op.shuffle:
            debug_limit_shuffle_execution_to_num_blocks = data_context.get_config(
                "debug_limit_shuffle_execution_to_num_blocks", None
            )
        else:
            debug_limit_shuffle_execution_to_num_blocks = None

        fn = generate_repartition_fn(
            op.num_outputs,
            op.shuffle,
            data_context,
            debug_limit_shuffle_execution_to_num_blocks,
        )

    elif isinstance(op, TopN):
        if data_context.shuffle_strategy != ShuffleStrategy.SHUFFLE_V2:
            raise ValueError(
                "TopN is only supported with "
                "`DataContext.shuffle_strategy=SHUFFLE_V2` "
                f"(got {data_context.shuffle_strategy})."
            )
        return plan_top_n_op(op, data_context, input_physical_dag)

    elif isinstance(op, Sort):
        if data_context.shuffle_strategy == ShuffleStrategy.SHUFFLE_V2:
            return _plan_sort_v2(data_context, op, input_physical_dag)
        debug_limit_shuffle_execution_to_num_blocks = data_context.get_config(
            "debug_limit_shuffle_execution_to_num_blocks", None
        )
        fn = generate_sort_fn(
            op.sort_key,
            data_context,
            debug_limit_shuffle_execution_to_num_blocks,
        )

    elif isinstance(op, Aggregate):
        if data_context.shuffle_strategy == ShuffleStrategy.GPU_SHUFFLE:
            return _plan_gpu_shuffle_aggregate(data_context, op, input_physical_dag)
        elif data_context.shuffle_strategy == ShuffleStrategy.SHUFFLE_V2:
            return _plan_hash_shuffle_aggregate_v2(data_context, op, input_physical_dag)
        elif data_context.shuffle_strategy == ShuffleStrategy.HASH_SHUFFLE:
            return _plan_hash_shuffle_aggregate(data_context, op, input_physical_dag)

        debug_limit_shuffle_execution_to_num_blocks = data_context.get_config(
            "debug_limit_shuffle_execution_to_num_blocks", None
        )
        fn = generate_aggregate_fn(
            op.key,
            op.aggs,
            data_context,
            debug_limit_shuffle_execution_to_num_blocks,
        )
    else:
        raise ValueError(f"Found unknown logical operator during planning: {op}")

    return AllToAllOperator(
        fn,
        input_physical_dag,
        data_context,
        num_outputs=op.num_outputs,
        sub_progress_bar_names=op.sub_progress_bar_names,
        name=op.name,
    )
