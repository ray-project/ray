import random

import pyarrow as pa
import pytest

import ray
from ray.data._internal.execution.interfaces import ExecutionOptions
from ray.data._internal.execution.operators.base_physical_operator import (
    AllToAllOperator,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.shuffle_operators.shuffle_map_operator import (  # noqa: E501
    ShuffleMapOp,
    extract_partition_id,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_reduce_operator import (  # noqa: E501
    ShuffleReduceOp,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_tasks import (
    _read_partition_ipc,
)
from ray.data._internal.execution.operators.shuffle_operators.sort_shuffle_map_operator import (  # noqa: E501
    SortShuffleMapOp,
)
from ray.data._internal.execution.util import make_ref_bundles
from ray.data._internal.logical.optimizers import get_execution_plan
from ray.data._internal.planner.exchange.sort_task_spec import SortKey, SortTaskSpec
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


def test_sort_shuffle_map_samples_all_inputs_then_replays_buffered_inputs(
    ray_start_regular_shared_2_cpus,
    monkeypatch,
):
    ctx = DataContext.get_current()
    bundles = make_ref_bundles([[9, 8], [1, 2], [6, 5]])
    op = SortShuffleMapOp(
        InputDataBuffer(ctx, []),
        ctx,
        num_partitions=2,
        sort_key=SortKey("id"),
    )
    monkeypatch.setattr(op, "_get_max_num_sampling_tasks_in_flight", lambda: 3)
    op.start(ExecutionOptions(), noop_counter())

    assert op.throttling_disabled()
    op.add_input(bundles[0], 0)
    assert op.get_active_tasks() == []
    assert op.boundaries is None

    # Sampling waits for every input so the boundaries represent the entire
    # dataset, rather than only the earliest blocks.
    op.add_input(bundles[1], 0)
    assert op.get_active_tasks() == []
    op.add_input(bundles[2], 0)
    assert op.get_active_tasks() == []
    assert op.internal_input_queue_num_blocks() == 3
    assert op.internal_input_queue_num_bytes() == sum(
        bundle.size_bytes() for bundle in bundles
    )
    assert op.metrics.obj_store_mem_internal_inqueue == sum(
        bundle.size_bytes() for bundle in bundles
    )
    op.all_inputs_done()
    assert len(op.get_active_tasks()) == 3
    run_op_tasks_sync(op)

    assert op.boundaries is not None
    assert not op.throttling_disabled()
    assert len(op.boundaries) == 1
    assert op.internal_input_queue_num_blocks() == 0
    assert op.internal_input_queue_num_bytes() == 0
    assert op.metrics.obj_store_mem_internal_inqueue == 0

    partition_rows = []
    partition_ids = []
    while op.has_next():
        bundle = op.get_next()
        partition_ids.append(extract_partition_id(bundle))
        rows = []
        for block_ref in bundle.block_refs:
            table = _read_partition_ipc(ray.get(block_ref))
            if table is not None:
                rows.extend(table["id"].to_pylist())
        partition_rows.append(rows)
        bundle.destroy_if_owned()

    assert partition_ids == [0, 1]
    assert sorted(row for rows in partition_rows for row in rows) == [1, 2, 5, 6, 8, 9]
    assert max(partition_rows[0]) <= min(partition_rows[1])
    assert op.has_completed()


def test_sort_shuffle_map_bounds_sampling_tasks(
    ray_start_regular_shared_2_cpus,
    monkeypatch,
):
    ctx = DataContext.get_current()
    bundles = make_ref_bundles([[i] for i in range(5)])
    op = SortShuffleMapOp(
        InputDataBuffer(ctx, []),
        ctx,
        num_partitions=2,
        sort_key=SortKey("id"),
    )
    monkeypatch.setattr(op, "_get_max_num_sampling_tasks_in_flight", lambda: 2)
    op.start(ExecutionOptions(), noop_counter())

    for bundle in bundles:
        op.add_input(bundle, 0)
    op.all_inputs_done()

    assert len(op.get_active_tasks()) == 2
    assert len(op._pending_sample_block_refs) == 3
    assert op.progress_str().startswith("sample: 0/5")

    # Completing the first window submits only enough tasks to refill it.
    run_op_tasks_sync(op, only_existing=True)
    assert len(op.get_active_tasks()) == 2
    assert len(op._pending_sample_block_refs) == 1
    assert op.progress_str().startswith("sample: 2/5")

    run_op_tasks_sync(op)
    assert not op._pending_sample_block_refs
    assert op.boundaries is not None
    assert op.progress_str().startswith("sample: 5/5")

    while op.has_next():
        op.get_next().destroy_if_owned()
    assert op.has_completed()


def test_sort_shuffle_map_samples_nonempty_blocks_after_empty_blocks(
    ray_start_regular_shared_2_cpus,
):
    ctx = DataContext.get_current()
    bundles = make_ref_bundles([[], [], [3, 1, 2]])
    op = SortShuffleMapOp(
        InputDataBuffer(ctx, []),
        ctx,
        num_partitions=2,
        sort_key=SortKey("id"),
    )
    op.start(ExecutionOptions(), noop_counter())

    # Sampling must include the later non-empty block instead of deriving empty
    # boundaries from only the first two blocks.
    for bundle in bundles:
        op.add_input(bundle, 0)
    op.all_inputs_done()
    run_op_tasks_sync(op)

    assert op.boundaries == [(2,)]
    rows = []
    while op.has_next():
        bundle = op.get_next()
        for block_ref in bundle.block_refs:
            table = _read_partition_ipc(ray.get(block_ref))
            if table is not None:
                rows.extend(table["id"].to_pylist())
        bundle.destroy_if_owned()

    assert rows == [1, 2, 3]
    assert op.has_completed()


def test_sort_shuffle_map_handles_all_empty_sample_blocks(
    ray_start_regular_shared_2_cpus,
):
    ctx = DataContext.get_current()
    bundles = make_ref_bundles([[], []])
    op = SortShuffleMapOp(
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


def test_sort_shuffle_map_user_boundaries_skip_sampling(
    ray_start_regular_shared_2_cpus,
):
    ctx = DataContext.get_current()
    bundle = make_ref_bundles([[3, 1, 2]])[0]
    op = SortShuffleMapOp(
        InputDataBuffer(ctx, []),
        ctx,
        num_partitions=2,
        sort_key=SortKey("id", boundaries=[2]),
    )
    op.start(ExecutionOptions(), noop_counter())

    op.add_input(bundle, 0)

    assert op.boundaries == [(2,)]
    assert not op.throttling_disabled()
    assert len(op.get_active_tasks()) == 1

    op.all_inputs_done()
    run_op_tasks_sync(op)
    while op.has_next():
        op.get_next().destroy_if_owned()
    assert op.has_completed()


def test_shuffle_reduce_preserves_partition_order(
    ray_start_regular_shared_2_cpus,
):
    ctx = DataContext.get_current()
    map_op = ShuffleMapOp(
        InputDataBuffer(ctx, []),
        ctx,
        num_partitions=2,
        partition_fn=lambda table: {},
    )
    reduce_op = ShuffleReduceOp(
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
    assert [ray.get(bundle.block_refs[0])["id"].iloc[0] for bundle in outputs] == [
        0,
        1,
    ]
    for bundle in outputs:
        bundle.destroy_if_owned()


def test_sort_planner_routes_to_shuffle_v2(restore_data_context):
    ctx = DataContext.get_current()
    ds = ray.data.range(10, override_num_blocks=2).sort("id")

    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE_V2
    ctx.default_hash_shuffle_parallelism = 4
    dag = get_execution_plan(ds._logical_plan)[0].dag
    assert isinstance(dag, ShuffleReduceOp)
    assert dag._preserve_partition_order
    map_op = dag.input_dependencies[0]
    assert isinstance(map_op, SortShuffleMapOp)
    # Follow the other hash-shuffle-v2 planners: without explicit boundaries,
    # the configured default determines partition count rather than the
    # estimated number of upstream blocks.
    assert map_op._num_partitions == ctx.default_hash_shuffle_parallelism

    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE
    dag = get_execution_plan(ds._logical_plan)[0].dag
    assert isinstance(dag, AllToAllOperator)


def test_sort_shuffle_v2_end_to_end(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
):
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE_V2

    rows = [{"a": i % 4, "b": i} for i in range(40)]
    random.Random(0).shuffle(rows)
    result = (
        ray.data.from_items(rows, override_num_blocks=4)
        .sort(["a", "b"], descending=[False, True])
        .take_all()
    )

    assert result == sorted(rows, key=lambda row: (row["a"], -row["b"]))


def test_sort_shuffle_v2_validates_sort_key(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
):
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE_V2

    with pytest.raises(
        ValueError,
        match="You specified the column 'missing'.*dataset has columns: \\['id'\\]",
    ):
        ray.data.range(10, override_num_blocks=2).sort("missing").take_all()


def test_sort_shuffle_v2_promotes_compatible_block_schemas(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
):
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE_V2

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
        ray.data.from_arrow_refs([ray.put(null_block), ray.put(int_block)])
        .sort("A", boundaries=[100])
        .take_all()
    )

    assert result == [
        {"A": 0, "sum(B)": None},
        {"A": 1, "sum(B)": 10},
        {"A": 2, "sum(B)": 20},
    ]


def test_sort_shuffle_v2_samples_all_blocks_to_avoid_skew(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
):
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE_V2

    num_blocks = 32
    num_rows = 320
    # Keep the input in descending order so early blocks cover a different key
    # range from later blocks. Sampling every block should still produce balanced
    # range partitions.
    rows = [{"id": i} for i in reversed(range(num_rows))]
    result = (
        ray.data.from_items(rows, override_num_blocks=num_blocks)
        .sort("id")
        .materialize()
    )

    assert [row["id"] for row in result.iter_rows()] == list(range(num_rows))
    assert result.num_blocks() == num_blocks
    block_num_rows = result._block_num_rows()
    assert max(block_num_rows) - min(block_num_rows) <= 1


def test_sort_shuffle_v2_with_user_boundaries(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
):
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE_V2

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
