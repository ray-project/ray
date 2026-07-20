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
from ray.data.context import DataContext
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


def test_sort_shuffle_map_samples_then_replays_buffered_inputs(
    ray_start_regular_shared_2_cpus,
):
    ctx = DataContext.get_current()
    bundles = make_ref_bundles([[9, 8], [1, 2], [6, 5]])
    op = SortShuffleMapOp(
        InputDataBuffer(ctx, []),
        ctx,
        num_partitions=2,
        sort_key=SortKey("id"),
        sample_num_blocks=2,
        pre_map_merge_threshold=0,
    )
    op.start(ExecutionOptions(), noop_counter())

    op.add_input(bundles[0], 0)
    assert op.get_active_tasks() == []
    assert op.boundaries is None

    # Reaching the warm-up target starts two parallel sample tasks. Inputs that
    # arrive while they run stay buffered and are replayed after sampling.
    op.add_input(bundles[1], 0)
    assert len(op.get_active_tasks()) == 2
    op.add_input(bundles[2], 0)
    op.all_inputs_done()
    run_op_tasks_sync(op)

    assert op.boundaries is not None
    assert len(op.boundaries) == 1

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
        sample_num_blocks=2,
        pre_map_merge_threshold=0,
    )
    op.start(ExecutionOptions(), noop_counter())

    op.add_input(bundle, 0)

    assert op.boundaries == [(2,)]
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

    ctx.use_hash_shuffle_v2 = True
    dag = get_execution_plan(ds._logical_plan)[0].dag
    assert isinstance(dag, ShuffleReduceOp)
    assert dag._preserve_partition_order
    assert isinstance(dag.input_dependencies[0], SortShuffleMapOp)

    ctx.use_hash_shuffle_v2 = False
    dag = get_execution_plan(ds._logical_plan)[0].dag
    assert isinstance(dag, AllToAllOperator)


def test_sort_shuffle_v2_end_to_end(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
):
    ctx = DataContext.get_current()
    ctx.use_hash_shuffle_v2 = True

    rows = [{"a": i % 4, "b": i} for i in range(40)]
    random.Random(0).shuffle(rows)
    result = (
        ray.data.from_items(rows, override_num_blocks=4)
        .sort(["a", "b"], descending=[False, True])
        .take_all()
    )

    assert result == sorted(rows, key=lambda row: (row["a"], -row["b"]))


def test_sort_shuffle_v2_more_blocks_than_sampling_window(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
):
    ctx = DataContext.get_current()
    ctx.use_hash_shuffle_v2 = True

    num_blocks = 32
    num_rows = 320
    # Keep the input in descending order so the first 20 sampled blocks cover a
    # different key range from the remaining blocks. This exercises the default
    # bounded sampling window and verifies that all later inputs are still mapped.
    rows = [{"id": i} for i in reversed(range(num_rows))]
    result = (
        ray.data.from_items(rows, override_num_blocks=num_blocks)
        .sort("id")
        .materialize()
    )

    assert [row["id"] for row in result.iter_rows()] == list(range(num_rows))
    assert result.num_blocks() == num_blocks


def test_sort_shuffle_v2_with_user_boundaries(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
):
    ctx = DataContext.get_current()
    ctx.use_hash_shuffle_v2 = True

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
