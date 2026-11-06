"""Tests for ``_broadcast_join_task`` called directly, bypassing the operators.

Checks the task's own contract: it splits ``*blocks`` into stream and build
side, places the build side on the requested input index, runs the shared
join reduce fn, and emits ``(block, pickled metadata)`` pairs with the fused
downstream map applied.
"""

import pickle

import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.join import _make_join_reduce_fn
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
)
from ray.data._internal.execution.operators.shuffle_operators.disk_shuffle_tasks import (  # noqa: E501
    _broadcast_join_task,
)
from ray.data._internal.logical.operators import JoinType
from ray.data.block import BlockMetadataWithSchema
from ray.data.context import DataContext

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
def test_inner_join_either_build_side(ray_start_regular_shared, tables, build_side):
    """Ensure blocks are split into stream and build side, and the build side
    lands on the logical side (left/right) ``build_side`` says."""
    stream, build = tables
    # Two blocks per side so the split point is not trivially 0 or len-1.
    stream_blocks = [stream.slice(0, 3), stream.slice(3)]
    build_blocks = [build.slice(0, 1), build.slice(1)]
    table, _ = _run_task(stream_blocks, build_blocks, build_side, JoinType.INNER)
    left, right = (build, stream) if build_side == LEFT else (stream, build)
    _assert_same_rows(table, _expected(left, right, "inner"))


def test_output_is_split_to_target_block_size(ray_start_regular_shared):
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


def test_fused_map_is_applied(ray_start_regular_shared, tables):
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
