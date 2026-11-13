import random
from unittest import mock

import pyarrow as pa
import pytest

import ray
from ray.data._internal.execution.interfaces import (
    BlockEntry,
    ExecutionOptions,
    RefBundle,
)
from ray.data._internal.execution.operators.base_physical_operator import (
    AllToAllOperator,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.shuffle_operators.disk_shuffle_reduce_operator import (  # noqa: E501
    DiskHashShuffleReduceOp,
    DiskHashShuffleRegroupOp,
)
from ray.data._internal.execution.operators.shuffle_operators.disk_shuffle_runtime import (  # noqa: E501
    handle_from_block,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_map_operator import (  # noqa: E501
    ShuffleMapOp,
    make_partition_sentinel,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_reduce_operator import (  # noqa: E501
    ShuffleReduceOp,
)
from ray.data._internal.execution.operators.shuffle_operators.sort_sampling_operator import (  # noqa: E501
    SORT_SAMPLE_ROWS_PER_BLOCK,
    SortSamplingOp,
)
from ray.data._internal.execution.operators.shuffle_operators.sort_shuffle_map_operator import (  # noqa: E501
    SortRangePartitioner,
    SortShuffleMapOp,
)
from ray.data._internal.execution.operators.sort_shuffle import make_sort_reduce_fn
from ray.data._internal.execution.util import make_ref_bundles
from ray.data._internal.logical.optimizers import get_execution_plan
from ray.data._internal.planner.exchange.sort_task_spec import SortKey, SortTaskSpec
from ray.data._internal.planner.plan_all_to_all_op import (
    plan_disk_hash_shuffle_map,
    plan_disk_hash_shuffle_reduce,
)
from ray.data.block import BlockAccessor, BlockMetadata
from ray.data.context import DataContext, ShuffleStrategy
from ray.data.tests.conftest import *  # noqa: F401, F403
from ray.data.tests.conftest import noop_counter
from ray.data.tests.util import run_op_tasks_sync
from ray.tests.conftest import *  # noqa: F401, F403


def test_get_boundaries_from_samples():
    samples = [
        pa.table({"id": [0, 2, 4, 6, 8]}),
        pa.table({"id": [1, 3, 5, 7, 9]}),
    ]

    boundaries = SortTaskSpec.get_boundaries_from_samples(
        samples, SortKey("id"), num_reducers=2
    )

    assert boundaries == [(4,)]


def test_get_boundaries_from_empty_samples():
    boundaries = SortTaskSpec.get_boundaries_from_samples(
        [pa.table({"id": pa.array([], type=pa.int64())})],
        SortKey("id"),
        num_reducers=3,
    )

    assert boundaries == [None, None]


def test_sort_sampling_starts_with_upstream_and_forwards_original_inputs(
    ray_start_regular_shared_2_cpus,
):
    ctx = DataContext.get_current()
    bundles = make_ref_bundles([[9, 8], [1, 2], [6, 5]])
    op = SortSamplingOp(
        InputDataBuffer(ctx, []),
        ctx,
        num_partitions=2,
        sort_key=SortKey("id"),
    )
    op.start(ExecutionOptions(), noop_counter())

    assert not op.supports_fusion()
    # Opted out of resource allocation while sampling (see the method's comment).
    assert op.throttling_disabled()
    op.add_input(bundles[0], 0)
    assert len(op.get_active_tasks()) == 1
    assert op.boundaries is None
    assert not op.has_next()

    run_op_tasks_sync(op, only_existing=True)
    assert not op.get_active_tasks()
    assert op.boundaries is None

    op.add_input(bundles[1], 0)
    op.add_input(bundles[2], 0)
    assert len(op.get_active_tasks()) == 2
    assert op.internal_input_queue_num_blocks() == 3
    assert op.internal_input_queue_num_bytes() == sum(
        bundle.size_bytes() for bundle in bundles
    )
    assert op.metrics.obj_store_mem_internal_inqueue == sum(
        bundle.size_bytes() for bundle in bundles
    )
    op.all_inputs_done()
    assert not op.has_next()
    run_op_tasks_sync(op)

    assert op.boundaries is not None
    assert not op.throttling_disabled()
    assert len(op.boundaries) == 1
    assert op.internal_input_queue_num_blocks() == 0
    assert op.internal_input_queue_num_bytes() == 0
    assert op.metrics.obj_store_mem_internal_inqueue == 0
    assert op.internal_output_queue_num_blocks() == 3

    output_bundles = []
    while op.has_next():
        output_bundles.append(op.get_next())

    assert [bundle.block_refs for bundle in output_bundles] == [
        bundle.block_refs for bundle in bundles
    ]
    assert op.has_completed()
    for bundle in output_bundles:
        bundle.destroy_if_owned()


@pytest.mark.parametrize(
    "num_partitions, estimated_num_input_blocks, expected_rows",
    [
        (2, None, SORT_SAMPLE_ROWS_PER_BLOCK),
        (5, 1, 5 * SortTaskSpec.SORT_SAMPLE_POINTS_PER_PARTITION),
        (4, 8, 4 * SortTaskSpec.SORT_SAMPLE_POINTS_PER_PARTITION // 8),
    ],
)
def test_sort_sampling_rows_per_block(
    ray_start_regular_shared_2_cpus,
    num_partitions,
    estimated_num_input_blocks,
    expected_rows,
):
    ctx = DataContext.get_current()
    bundle = make_ref_bundles([list(range(100))])[0]
    op = SortSamplingOp(
        InputDataBuffer(ctx, []),
        ctx,
        num_partitions=num_partitions,
        sort_key=SortKey("id"),
        estimated_num_input_blocks=estimated_num_input_blocks,
    )
    op.start(ExecutionOptions(), noop_counter())

    op.add_input(bundle, 0)
    run_op_tasks_sync(op, only_existing=True)
    assert len(op._sample_results) == 1
    assert BlockAccessor.for_block(op._sample_results[0]).num_rows() == expected_rows

    op.all_inputs_done()
    run_op_tasks_sync(op)
    assert op.boundaries is not None
    while op.has_next():
        op.get_next().destroy_if_owned()
    assert op.has_completed()


def test_sort_sampling_samples_more_than_twenty_blocks(
    ray_start_regular_shared_2_cpus,
):
    ctx = DataContext.get_current()
    bundles = make_ref_bundles([[i] for i in range(21)])
    op = SortSamplingOp(
        InputDataBuffer(ctx, []),
        ctx,
        num_partitions=2,
        sort_key=SortKey("id"),
    )
    op.start(ExecutionOptions(), noop_counter())

    for bundle in bundles:
        op.add_input(bundle, 0)

    # Submission is capped at the available parallelism; the rest wait in-op.
    assert op._max_sample_tasks_in_flight is not None
    assert len(op.get_active_tasks()) == op._max_sample_tasks_in_flight
    assert op._next_sample_task_idx == op._max_sample_tasks_in_flight
    assert op.progress_str() == f"sample: 0/{len(bundles)}"
    op.all_inputs_done()
    run_op_tasks_sync(op)

    assert op._next_sample_task_idx == len(bundles)
    assert op.progress_str() == f"sample: {len(bundles)}/{len(bundles)}"
    while op.has_next():
        op.get_next().destroy_if_owned()
    assert op.has_completed()


def test_sort_sampling_samples_nonempty_blocks_after_empty_blocks(
    ray_start_regular_shared_2_cpus,
):
    ctx = DataContext.get_current()
    bundles = make_ref_bundles([[], [], [3, 1, 2]])
    op = SortSamplingOp(
        InputDataBuffer(ctx, []),
        ctx,
        num_partitions=2,
        sort_key=SortKey("id"),
    )
    op.start(ExecutionOptions(), noop_counter())

    for bundle in bundles:
        op.add_input(bundle, 0)
    op.all_inputs_done()
    run_op_tasks_sync(op)

    assert op.boundaries == [(2,)]
    while op.has_next():
        op.get_next().destroy_if_owned()
    assert op.has_completed()


def test_sort_sampling_handles_all_empty_blocks(
    ray_start_regular_shared_2_cpus,
):
    ctx = DataContext.get_current()
    bundles = make_ref_bundles([[], []])
    op = SortSamplingOp(
        InputDataBuffer(ctx, []),
        ctx,
        num_partitions=2,
        sort_key=SortKey("id"),
    )
    op.start(ExecutionOptions(), noop_counter())

    for bundle in bundles:
        op.add_input(bundle, 0)
    op.all_inputs_done()
    run_op_tasks_sync(op)

    assert op.boundaries == [(None,)]
    while op.has_next():
        op.get_next().destroy_if_owned()
    assert op.has_completed()


def _disk_sort_writer(input_op, ctx, *, num_partitions, sort_key):
    """The disk sort writer as ``_plan_sort_v2`` builds it."""
    partitioner = SortRangePartitioner(sort_key, num_partitions, input_op, ctx)

    def resolve_boundaries(schema):
        partitioner.partition_fn_for(schema, input_op)

    return plan_disk_hash_shuffle_map(
        input_op,
        ctx,
        num_partitions=num_partitions,
        partition_fn=partitioner,
        on_start=resolve_boundaries,
    )


@pytest.mark.parametrize("build_map_op", [SortShuffleMapOp, _disk_sort_writer])
def test_sort_shuffle_map_uses_user_boundaries_without_sampling(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    build_map_op,
):
    ctx = DataContext.get_current()
    # One map task per input bundle so the task is visible right after add_input.
    ctx.shuffle_input_batch_bytes = 0
    bundle = make_ref_bundles([[3, 1, 2]])[0]
    op = build_map_op(
        InputDataBuffer(ctx, []),
        ctx,
        num_partitions=2,
        sort_key=SortKey("id", boundaries=[2]),
    )
    op.start(ExecutionOptions(), noop_counter())

    op.add_input(bundle, 0)

    if isinstance(op, SortShuffleMapOp):
        assert op.boundaries == [(2,)]
    assert len(op.get_active_tasks()) == 1

    op.all_inputs_done()
    run_op_tasks_sync(op)
    outputs = []
    while op.has_next():
        outputs.append(op.get_next())
    assert op.has_completed()
    if not isinstance(op, SortShuffleMapOp):
        # The disk writer's handle reports the rows per range: [1] and [2, 3].
        (handle_bundle,) = outputs
        handle = handle_from_block(ray.get(handle_bundle.block_refs[0]))
        assert list(handle["num_rows"]) == [1, 2]
    for bundle in outputs:
        bundle.destroy_if_owned()


@pytest.mark.parametrize(
    "map_cls, reduce_cls",
    [
        (ShuffleMapOp, ShuffleReduceOp),
        (plan_disk_hash_shuffle_map, plan_disk_hash_shuffle_reduce),
    ],
)
def test_shuffle_reduce_preserves_partition_order(
    ray_start_regular_shared_2_cpus, map_cls, reduce_cls
):
    ctx = DataContext.get_current()
    map_op = map_cls(
        InputDataBuffer(ctx, []),
        ctx,
        num_partitions=2,
        partition_fn=lambda table: {},
    )
    reduce_op = reduce_cls(
        map_op,
        ctx,
        num_partitions=2,
        reduce_fn=lambda partition_id, tables: [],
        preserve_partition_order=True,
    )
    partition_zero, partition_one = make_ref_bundles([[0], [1]])

    reduce_op._record_partition_output(1, partition_one, partition_complete=True)
    assert not reduce_op.has_next()

    reduce_op._record_partition_output(0, partition_zero, partition_complete=True)
    outputs = [reduce_op.get_next(), reduce_op.get_next()]
    first_ids = []
    for bundle in outputs:
        block = ray.get(bundle.block_refs[0])  # pyrefly: ignore[no-matching-overload]
        first_ids.append(block["id"].iloc[0])  # pyrefly: ignore[bad-index]
    assert first_ids == [0, 1]
    for bundle in outputs:
        bundle.destroy_if_owned()


def _empty_partition_bundle(partition_id: int) -> RefBundle:
    """A zero-row map-output wrapper stamped with ``partition_id``."""
    empty = pa.table({"id": pa.array([], pa.int64())})
    metadata = BlockMetadata(
        num_rows=0,
        size_bytes=0,
        input_files=make_partition_sentinel(partition_id),
        exec_stats=None,
    )
    return RefBundle(
        (BlockEntry(ray.put(empty), metadata),),  # pyrefly: ignore[bad-argument-type]
        owns_blocks=True,
        schema=empty.schema,
    )


@pytest.mark.parametrize(
    "map_cls, reduce_cls",
    [
        (ShuffleMapOp, ShuffleReduceOp),
        (plan_disk_hash_shuffle_map, plan_disk_hash_shuffle_reduce),
    ],
)
def test_shuffle_reduce_ordered_skips_discarded_empty_partition(
    ray_start_regular_shared_2_cpus, map_cls, reduce_cls
):
    """A discarded empty partition must not block later partitions' output."""
    ctx = DataContext.get_current()
    map_op = map_cls(
        InputDataBuffer(ctx, []),
        ctx,
        num_partitions=2,
        partition_fn=lambda table: {},
    )
    reduce_op = reduce_cls(
        map_op,
        ctx,
        num_partitions=2,
        reduce_fn=lambda partition_id, tables: [],
        preserve_partition_order=True,
        should_emit_empty_partitions=False,
    )
    (partition_one,) = make_ref_bundles([[1]])

    reduce_op._add_input_inner(_empty_partition_bundle(partition_id=0), 0)
    reduce_op._record_partition_output(1, partition_one, partition_complete=True)

    assert reduce_op.has_next()
    reduce_op.get_next().destroy_if_owned()
    assert not reduce_op.has_next()


_USE_DISK = [
    pytest.param(False, id="object_store"),
    pytest.param(True, id="disk"),
]

_SORT_TRANSPORTS = [
    pytest.param(False, SortShuffleMapOp, ShuffleReduceOp, "", id="object_store"),
    # The disk writer is a plain map op; a regroup sits between it and the reduce.
    pytest.param(True, MapOperator, DiskHashShuffleReduceOp, "Disk", id="disk"),
]


def _sort_map_op(reduce_op, use_disk):
    map_op = reduce_op.input_dependencies[0]
    if use_disk:
        assert isinstance(map_op, DiskHashShuffleRegroupOp)
        map_op = map_op.input_dependencies[0]
    return map_op


@pytest.mark.parametrize("use_disk, map_cls, reduce_cls, prefix", _SORT_TRANSPORTS)
def test_sort_planner_routes_to_shuffle_v2(
    restore_data_context, use_disk, map_cls, reduce_cls, prefix
):
    ctx = DataContext.get_current()
    # The dataset captures the context, so configure it first.
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ctx.use_disk_based_hash_shuffle = use_disk
    ctx.default_hash_shuffle_parallelism = 4
    ds = ray.data.range(10, override_num_blocks=2).sort("id")

    dag = get_execution_plan(ds._logical_plan)[0].dag
    assert isinstance(dag, reduce_cls)
    assert dag._preserve_partition_order
    assert dag.name.startswith(f"{prefix}SortShuffleReduce")
    map_op = _sort_map_op(dag, use_disk)
    assert isinstance(map_op, map_cls)
    assert map_op.name.startswith(f"{prefix}SortShuffleMap")
    sampling_op = map_op.input_dependencies[0]
    assert isinstance(sampling_op, SortSamplingOp)
    assert not sampling_op.supports_fusion()
    assert (
        sampling_op._sample_rows_per_block
        == SortTaskSpec.SORT_SAMPLE_POINTS_PER_PARTITION
    )
    assert dag._num_partitions == 2

    read_op_cls = type(ds._logical_plan.dag.input_dependencies[0])
    with mock.patch.object(read_op_cls, "estimated_num_outputs", return_value=None):
        dag = get_execution_plan(ds._logical_plan)[0].dag
    assert isinstance(dag, reduce_cls)
    assert isinstance(_sort_map_op(dag, use_disk), map_cls)
    assert dag._num_partitions == ctx.default_hash_shuffle_parallelism

    # A single partition needs no boundaries, so the sampling op is skipped.
    ds_single = ray.data.range(10, override_num_blocks=1).sort("id")
    dag = get_execution_plan(ds_single._logical_plan)[0].dag
    assert isinstance(dag, reduce_cls)
    single_map_op = _sort_map_op(dag, use_disk)
    assert isinstance(single_map_op, map_cls)
    assert dag._num_partitions == 1
    if not use_disk:
        assert single_map_op.boundaries == []
    assert not isinstance(single_map_op.input_dependencies[0], SortSamplingOp)

    ds_with_boundaries = ray.data.range(10, override_num_blocks=2).sort(
        "id", boundaries=[5]
    )
    dag = get_execution_plan(ds_with_boundaries._logical_plan)[0].dag
    map_op = _sort_map_op(dag, use_disk)
    assert isinstance(map_op, map_cls)
    assert not isinstance(map_op.input_dependencies[0], SortSamplingOp)


def test_sort_planner_legacy_strategy(restore_data_context):
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SORT_SHUFFLE_PULL_BASED
    ds_legacy = ray.data.range(10, override_num_blocks=2).sort("id")
    dag = get_execution_plan(ds_legacy._logical_plan)[0].dag
    assert isinstance(dag, AllToAllOperator)


@pytest.mark.parametrize("use_disk, map_cls, reduce_cls, prefix", _SORT_TRANSPORTS)
def test_sort_fused_map_keeps_partition_order(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    use_disk,
    map_cls,
    reduce_cls,
    prefix,
):
    """Fusing a map into the sort reduce must keep ``preserve_partition_order``."""
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ctx.use_disk_based_hash_shuffle = use_disk

    ds = (
        ray.data.range(50, override_num_blocks=5)
        .sort("id", descending=True)
        .map_batches(lambda batch: batch)
    )
    dag = get_execution_plan(ds._logical_plan)[0].dag
    assert isinstance(dag, reduce_cls)
    assert "->" in dag.name
    assert dag._preserve_partition_order

    ids = [row["id"] for row in ds.take_all()]
    assert ids == list(range(49, -1, -1))


@pytest.mark.parametrize("use_disk", _USE_DISK)
def test_sort_shuffle_v2_end_to_end(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    use_disk,
):
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ctx.use_disk_based_hash_shuffle = use_disk

    rows = [{"a": i % 4, "b": i} for i in range(40)]
    random.Random(0).shuffle(rows)
    result = (
        ray.data.from_items(rows, override_num_blocks=4)
        .sort(["a", "b"], descending=[False, True])
        .take_all()
    )

    assert result == sorted(rows, key=lambda row: (row["a"], -row["b"]))


def test_sort_reduce_fn_keeps_schema_for_empty_partition():
    schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
    reduce_fn = make_sort_reduce_fn(SortKey("a"), DataContext.get_current())

    empty_shards = [schema.empty_table(), schema.empty_table()]
    outputs = list(reduce_fn(0, [empty_shards]))

    assert len(outputs) == 1
    assert outputs[0].num_rows == 0
    assert outputs[0].schema == schema


@pytest.mark.parametrize("use_disk", _USE_DISK)
def test_sort_shuffle_v2_validates_sort_key(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    use_disk,
):
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ctx.use_disk_based_hash_shuffle = use_disk

    with pytest.raises(
        ValueError,
        match="You specified the column 'missing'.*dataset has columns: \\['id'\\]",
    ):
        ray.data.range(10, override_num_blocks=2).sort("missing").take_all()


@pytest.mark.parametrize("use_disk", _USE_DISK)
def test_sort_shuffle_v2_promotes_compatible_block_schemas(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    use_disk,
):
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ctx.use_disk_based_hash_shuffle = use_disk

    # Aggregations can produce a null-typed block for an all-null group and an
    # int64-typed block for non-null groups. Sort shuffle may merge these blocks
    # into one map task, so compatible schemas must be promoted before concat.
    null_block = pa.table(
        {
            "A": pa.array([0], type=pa.int64()),
            "sum(B)": pa.array([None], type=pa.null()),
        }
    )
    int_block = pa.table(
        {
            "A": pa.array([2, 1], type=pa.int64()),
            "sum(B)": pa.array([20, 10], type=pa.int64()),
        }
    )

    # Keep both blocks in one range partition so the shuffle map task must
    # concatenate their null and int64 column types.
    result = (
        ray.data.from_arrow([null_block, int_block])
        .sort("A", boundaries=[100])
        .take_all()
    )

    assert result == [
        {"A": 0, "sum(B)": None},
        {"A": 1, "sum(B)": 10},
        {"A": 2, "sum(B)": 20},
    ]


@pytest.mark.parametrize("use_disk", _USE_DISK)
def test_sort_shuffle_v2_samples_all_blocks_to_avoid_skew(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    use_disk,
):
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ctx.use_disk_based_hash_shuffle = use_disk

    num_input_blocks = 32
    num_output_partitions = 32
    num_rows = 320
    ctx.default_hash_shuffle_parallelism = num_output_partitions
    # Keep the input in descending order so early blocks cover a different key
    # range from later blocks. Sampling every block should still produce balanced
    # range partitions.
    rows = [{"id": i} for i in reversed(range(num_rows))]
    result = (
        ray.data.from_items(rows, override_num_blocks=num_input_blocks)
        .sort("id")
        .materialize()
    )

    assert [row["id"] for row in result.iter_rows()] == list(range(num_rows))
    assert result.num_blocks() == num_output_partitions
    block_num_rows = result._block_num_rows()
    # Every row is sampled, so boundaries are exact deciles; a boundary value
    # itself may land on either side, hence one row of slack per side.
    assert max(block_num_rows) - min(block_num_rows) <= 2


@pytest.mark.parametrize("use_disk", _USE_DISK)
def test_sort_shuffle_v2_splits_output_to_target_block_size(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    use_disk,
):
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ctx.use_disk_based_hash_shuffle = use_disk
    # ~80 KB of int64 rows over 2 partitions; force ~8 KB output blocks.
    ctx.target_max_block_size = 8 * 1024

    result = ray.data.range(10_000, override_num_blocks=2).sort("id").materialize()

    assert [row["id"] for row in result.iter_rows()] == list(range(10_000))
    assert result.num_blocks() > 2
    assert max(result._block_num_rows()) <= 2 * ctx.target_max_block_size // 8


@pytest.mark.parametrize("use_disk", _USE_DISK)
def test_sort_shuffle_v2_with_user_boundaries(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    use_disk,
):
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ctx.use_disk_based_hash_shuffle = use_disk

    result = (
        ray.data.range(20, override_num_blocks=4)
        .sort("id", descending=True, boundaries=[5, 10, 15])
        .materialize()
    )

    assert [row["id"] for row in result.iter_rows()] == list(reversed(range(20)))
    assert result.num_blocks() == 4


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
