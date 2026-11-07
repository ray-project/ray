"""Broadcast join tests.

Task level: ``_broadcast_join_task`` called directly, bypassing the operators.
End to end: ``Dataset.join`` under the disk-based ``SHUFFLE_V2`` strategy, with
the input arrival order controlled, checking both the result and which path
(broadcast or shuffle fallback) the reduce operator took.
"""

import pickle
import time
from unittest import mock

import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.join import (
    BuildSide,
    _make_join_reduce_fn,
)
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
)
from ray.data._internal.execution.operators.shuffle_operators.disk_shuffle_reduce_operator import (  # noqa: E501
    DiskHashShuffleReduceOp,
)
from ray.data._internal.execution.operators.shuffle_operators.disk_shuffle_tasks import (  # noqa: E501
    _broadcast_join_task,
)
from ray.data._internal.logical.operators import JoinType
from ray.data.block import BlockMetadataWithSchema
from ray.data.context import DataContext, ShuffleStrategy

LEFT, RIGHT = 0, 1


def _run_task(stream_blocks, build_blocks, build_side, join_type, **kwargs):
    """Submit the task and return (output table, metadata list)."""
    join_fn = _make_join_reduce_fn(
        join_type=join_type,
        left_key_col_names=("id",),
        right_key_col_names=("id",),
    )
    gen = _broadcast_join_task.remote(
        *stream_blocks,
        *build_blocks,
        num_stream_blocks=len(stream_blocks),
        build_side=build_side,
        join_fn=join_fn,
        task_idx=7,
        **{
            "target_max_block_size": None,
            "map_transformer": None,
            "map_task_context": None,
            "data_context": None,
            **kwargs,
        },
    )
    objs = [ray.get(ref) for ref in gen]
    blocks, metas = objs[0::2], objs[1::2]
    # Every metadata entry must deserialize to what the executor expects.
    for meta in metas:
        assert isinstance(pickle.loads(meta), BlockMetadataWithSchema)
    table = pa.concat_tables(blocks) if blocks else None
    return table, metas


def _expected(left, right, how):
    """Expected result: pandas merge in logical (left, right) order."""
    return pd.merge(left.to_pandas(), right.to_pandas(), on="id", how=how)


def _assert_same_rows(table, expected_pd):
    got = table.to_pandas().sort_values("id").reset_index(drop=True)
    exp = expected_pd.sort_values("id").reset_index(drop=True)
    pd.testing.assert_frame_equal(got[exp.columns], exp, check_dtype=False)


@pytest.fixture
def tables():
    stream = pa.table({"id": [0, 1, 2, 3, 5], "s": ["a", "b", "c", "d", "f"]})
    build = pa.table({"id": [1, 2, 9], "b": [10, 20, 90]})
    return stream, build


@pytest.mark.parametrize("build_side", [LEFT, RIGHT])
def test_inner_join_either_build_side(
    ray_start_regular_shared_2_cpus, tables, build_side
):
    """Ensure blocks are split into stream and build side, and the build side
    lands on the logical side (left/right) ``build_side`` says."""
    stream, build = tables
    # Two blocks per side so the split point is not trivially 0 or len-1.
    stream_blocks = [stream.slice(0, 3), stream.slice(3)]
    build_blocks = [build.slice(0, 1), build.slice(1)]
    table, _ = _run_task(stream_blocks, build_blocks, build_side, JoinType.INNER)
    left, right = (build, stream) if build_side == LEFT else (stream, build)
    _assert_same_rows(table, _expected(left, right, "inner"))


def test_output_is_split_to_target_block_size(ray_start_regular_shared_2_cpus):
    """Ensure the task chunks its output to ``target_max_block_size``."""
    # The join of two 2000-row tables is one ~48 KiB block; with a 4 KiB
    # target the task must re-chunk it into several blocks.
    n = 2_000
    stream = pa.table({"id": list(range(n)), "s": list(range(n))})
    build = pa.table({"id": list(range(n)), "b": list(range(n))})
    table, metas = _run_task(
        [stream], [build], RIGHT, JoinType.INNER, target_max_block_size=4 * 1024
    )
    assert table.num_rows == n
    assert len(metas) > 1


def test_fused_map_is_applied(ray_start_regular_shared_2_cpus, tables):
    """Ensure the fused downstream map is applied to the join output."""
    stream, build = tables

    def _mark(blocks, ctx):
        for block in blocks:
            yield block.append_column(
                "fused", pa.array([True] * block.num_rows, type=pa.bool_())
            )

    ctx = TaskContext(task_idx=7, op_name="test", target_max_block_size_override=None)
    table, _ = _run_task(
        [stream],
        [build],
        RIGHT,
        JoinType.INNER,
        # Adds a "fused" column to every block.
        map_transformer=MapTransformer([BlockMapTransformFn(_mark)]),
        map_task_context=ctx,
        data_context=DataContext.get_current(),
    )
    assert table.num_rows == 2
    assert table["fused"].to_pylist() == [True, True]


# --- end to end ---------------------------------------------------------------


@pytest.fixture
def disk_shuffle_v2(restore_data_context):
    """Use the disk-based SHUFFLE_V2 strategy, one map task per input bundle."""
    ctx = restore_data_context
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ctx.use_disk_based_hash_shuffle = True
    # One map task per input bundle, so each side arrives as several bundles.
    ctx.shuffle_input_batch_bytes = 0
    return ctx


@pytest.fixture
def patch_submit_tasks():
    """Count reduce-operator task submissions per path, driver side.

    Each spy calls through to the real method, so execution is unchanged.
    """
    with mock.patch.object(
        DiskHashShuffleReduceOp,
        "_submit_broadcast_join_task",
        autospec=True,
        side_effect=DiskHashShuffleReduceOp._submit_broadcast_join_task,
    ) as broadcast, mock.patch.object(
        DiskHashShuffleReduceOp,
        "_submit_reduce_task",
        autospec=True,
        side_effect=DiskHashShuffleReduceOp._submit_reduce_task,
    ) as shuffle:
        yield broadcast, shuffle


@pytest.fixture
def make_left_right():
    """Factory for fresh (left, right) datasets, built on call so they pick up
    the DataContext the test configured."""

    def make():
        # Both sides have unmatched ids. Each side is 4 blocks (left ~3.2 KB
        # total, 800 B per block; right ~800 B total, ~200 B per block), so it
        # arrives as several bundles. A wrongly broadcast outer side then shows
        # up as duplicated rows, the threshold is crossed only after
        # accumulating bundles, and the slow side's per-block delay runs as two
        # rounds on the 2-CPU cluster, which keeps the arrival order apart.
        left = ray.data.from_items(
            [{"id": i, "l": i * 2} for i in range(200)], override_num_blocks=4
        )
        right = ray.data.from_items(
            [{"id": i * 10, "r": i} for i in range(50)], override_num_blocks=4
        )
        return left, right

    return make


def _slow(ds, seconds=1.0):
    """Delay every bundle so the other side finishes first."""

    def _sleep(batch):
        time.sleep(seconds)
        return batch

    return ds.map_batches(_sleep, batch_size=None)


def _assert_join_result(joined, left, right, join_type):
    """Compare the join result against a pandas merge of the same inputs."""
    how = {"inner": "inner", "left_outer": "left"}[join_type]
    expected = pd.merge(left.to_pandas(), right.to_pandas(), on="id", how=how)
    _assert_same_rows(pa.Table.from_pandas(joined.to_pandas()), expected)


@pytest.mark.parametrize(
    "join_type,first_to_finish,expected_build_side",
    [
        ("inner", "left", BuildSide.BUILD_LEFT),
        ("inner", "right", BuildSide.BUILD_RIGHT),
        # left_outer may only build on the right. With left finishing first the
        # decision has to wait for the right side instead of taking left.
        ("left_outer", "left", BuildSide.BUILD_RIGHT),
        ("left_outer", "right", BuildSide.BUILD_RIGHT),
    ],
    ids=[
        "inner-left_first",
        "inner-right_first",
        "left_outer-left_first",
        "left_outer-right_first",
    ],
)
def test_arrival_order(
    ray_start_regular_shared_2_cpus,
    disk_shuffle_v2,
    patch_submit_tasks,
    make_left_right,
    join_type,
    first_to_finish,
    expected_build_side,
):
    """The first side to finish under the threshold becomes the build side,
    unless the join type disallows it."""
    left, right = make_left_right()
    if first_to_finish == "left":
        joined = left.join(_slow(right), join_type, on=("id",), num_partitions=4)
    else:
        joined = _slow(left).join(right, join_type, on=("id",), num_partitions=4)
    _assert_join_result(joined, left, right, join_type)

    broadcast, shuffle = patch_submit_tasks
    # The join took the broadcast path.
    assert broadcast.call_count > 0 and shuffle.call_count == 0
    # autospec records each call as (self, partition_id, stream_bundle).
    reduce_op, _, _ = broadcast.call_args.args
    assert reduce_op._decision.build_side is expected_build_side


def test_fallback_when_both_sides_exceed_threshold(
    ray_start_regular_shared_2_cpus,
    disk_shuffle_v2,
    patch_submit_tasks,
    make_left_right,
):
    # Make broadcast_join_threshold_bytes small so it's easy to exceed
    disk_shuffle_v2.broadcast_join_threshold_bytes = 1

    left, right = make_left_right()
    joined = left.join(right, "inner", on=("id",), num_partitions=4)
    _assert_join_result(joined, left, right, "inner")

    broadcast, shuffle = patch_submit_tasks
    # The join fell back to the shuffle path.
    assert shuffle.call_count > 0 and broadcast.call_count == 0


def test_timeout_demotes_to_shuffle(
    ray_start_regular_shared_2_cpus,
    disk_shuffle_v2,
    patch_submit_tasks,
    make_left_right,
):
    # Left (~3 KiB) exceeds the threshold, so the decision has to wait for the
    # slow right side. Set a short timeout that fires before right finishes.
    disk_shuffle_v2.broadcast_join_threshold_bytes = 1024
    disk_shuffle_v2.broadcast_join_decision_timeout_s = 0.1

    left, right = make_left_right()
    joined = left.join(_slow(right), "inner", on=("id",), num_partitions=4)
    _assert_join_result(joined, left, right, "inner")

    broadcast, shuffle = patch_submit_tasks
    # The join fell back to the shuffle path.
    assert shuffle.call_count > 0 and broadcast.call_count == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
