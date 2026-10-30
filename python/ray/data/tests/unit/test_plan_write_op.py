"""Unit tests for the write-op transforms in ``plan_write_op.py``.

These drive the transform fns directly instead of through Ray tasks: what
matters here is what each fn does to the blocks it is handed and to
``TaskContext.kwargs``.
"""

import gc
import weakref
from types import SimpleNamespace
from typing import List, Optional

import pandas as pd
import pytest

from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
    TransformClock,
)
from ray.data._internal.planner.checkpoint.plan_write_op import (
    _generate_capture_id_columns_transform,
    _generate_non_atomic_write_checkpoint_transform,
)
from ray.data._internal.planner.plan_write_op import (
    generate_collect_write_stats_fn,
    generate_write_fn,
)
from ray.data.block import BlockAccessor
from ray.data.datasource.datasink import Datasink


def _block(num_rows: int) -> pd.DataFrame:
    return pd.DataFrame({"x": list(range(num_rows))})


class _RecordingSink(Datasink):
    """Datasink that records what it saw, optionally stopping early."""

    def __init__(self, max_blocks: Optional[int] = None, write_return=None):
        self.num_blocks = 0
        self.num_rows = 0
        self.max_blocks = max_blocks
        self.write_return = write_return

    def write(self, blocks, ctx):
        for block in blocks:
            self.num_blocks += 1
            self.num_rows += len(block)
            if self.max_blocks is not None and self.num_blocks >= self.max_blocks:
                break
        return self.write_return


class _RecordingLegacyDatasource:
    """Legacy write target: anything that is not a Datasink."""

    def __init__(self):
        self.num_rows = 0
        self.write_args = None

    def write(self, blocks, ctx, **write_args):
        for block in blocks:
            self.num_rows += len(block)
        self.write_args = write_args


class _RecordingCheckpointWriter:
    """Stand-in for CheckpointWriter that records what would be checkpointed."""

    def __init__(self):
        self.checkpointed_ids: Optional[List] = None

    def write_block_checkpoint(self, block: BlockAccessor):
        self.checkpointed_ids = block.to_arrow().column(0).to_pylist()


def _stats_row(ctx: TaskContext, blocks=()) -> dict:
    """Run the stats transform the way the operator chains it: on empty input."""
    stats = next(generate_collect_write_stats_fn()(iter(blocks), ctx))
    return dict(stats.iloc[0])


def test_write_fn_collects_stats_and_emits_no_blocks():
    ctx = TaskContext(task_idx=0, op_name="test")
    sink = _RecordingSink(write_return="done")
    blocks = [_block(2), _block(3)]

    out = generate_write_fn(sink)(iter(blocks), ctx)

    # The written blocks are not re-emitted: holding them for the stats stage
    # is what used to keep the whole input in memory.
    assert list(out) == []
    assert sink.num_blocks == 2
    assert sink.num_rows == 5

    stats = _stats_row(ctx)
    assert stats["num_rows"] == 5
    assert stats["size_bytes"] == sum(
        BlockAccessor.for_block(b).size_bytes() for b in blocks
    )
    assert stats["write_return"] == "done"


def test_write_stats_cover_blocks_the_sink_did_not_read():
    # A sink may stop early. The stats still describe the whole task input,
    # as they did when the tee-based implementation drained its second copy.
    ctx = TaskContext(task_idx=0, op_name="test")
    sink = _RecordingSink(max_blocks=1)
    blocks = [_block(2), _block(3), _block(4)]

    generate_write_fn(sink)(iter(blocks), ctx)

    assert sink.num_rows == 2
    assert _stats_row(ctx)["num_rows"] == 9


def test_write_fn_holds_no_blocks_for_the_stats():
    # Regression test for the `itertools.tee` implementation, which kept every
    # written block buffered in the returned iterator for the stats stage: a
    # write task then held its whole input in memory until the stats ran.
    ctx = TaskContext(task_idx=0, op_name="test")
    sink = _RecordingSink()
    refs = []

    def blocks():
        for _ in range(3):
            block = _block(2)
            refs.append(weakref.ref(block))
            yield block

    out = generate_write_fn(sink)(blocks(), ctx)

    # The stats stage has not run yet and `out` is still alive. Nothing may be
    # holding the written blocks at this point.
    gc.collect()
    assert all(ref() is None for ref in refs), "written blocks are still held"
    assert list(out) == []


def test_legacy_datasource_write_collects_stats():
    ctx = TaskContext(task_idx=0, op_name="test")
    source = _RecordingLegacyDatasource()
    blocks = [_block(2), _block(3)]

    out = generate_write_fn(source, mode="x")(iter(blocks), ctx)

    assert list(out) == []
    assert source.num_rows == 5
    assert source.write_args == {"mode": "x"}
    assert _stats_row(ctx)["num_rows"] == 5


def test_write_fn_with_empty_input():
    ctx = TaskContext(task_idx=0, op_name="test")
    sink = _RecordingSink()

    assert list(generate_write_fn(sink)(iter(()), ctx)) == []

    stats = _stats_row(ctx)
    assert stats["num_rows"] == 0
    assert stats["write_return"] is None


def test_collect_write_stats_without_write_fn_counts_blocks():
    # `generate_collect_write_stats_fn` is also driven directly, with no
    # `generate_write_fn` upstream (see test_bigquery). It must still derive
    # the stats from the blocks it is given.
    ctx = TaskContext(task_idx=0, op_name="test")
    blocks = [_block(2), _block(3)]

    stats = _stats_row(ctx, blocks)

    assert stats["num_rows"] == 5
    assert stats["size_bytes"] == sum(
        BlockAccessor.for_block(b).size_bytes() for b in blocks
    )
    assert stats["write_return"] is None


def test_write_stats_read_after_write_stage_runs():
    # The write stage is lazy: it only runs when the stats stage first pulls
    # on its output. So the stats must not be read off `ctx` before that pull,
    # or they are read before the write has counted anything. Drive the real
    # transform chain from its output end, the way a map task does.
    ctx = TaskContext(task_idx=0, op_name="test")
    sink = _RecordingSink(write_return="done")
    transformer = MapTransformer(
        [
            BlockMapTransformFn(generate_write_fn(sink), disable_block_shaping=True),
            generate_collect_write_stats_fn(),
        ]
    )

    out = transformer.apply_transform(
        iter([_block(2), _block(3)]), ctx, clock=TransformClock()
    )
    stats = next(iter(out))

    assert sink.num_rows == 5
    assert dict(stats.iloc[0])["num_rows"] == 5
    assert dict(stats.iloc[0])["write_return"] == "done"


def test_non_atomic_checkpoint_captures_ids_during_write():
    # Non-file sinks write their checkpoint AFTER the data, but the write
    # consumes the blocks without re-emitting them, so the ID columns are
    # picked up on their way past and only they are kept.
    data_context = SimpleNamespace(checkpoint_config=SimpleNamespace(id_column="id"))
    capture_fn = _generate_capture_id_columns_transform(data_context)
    checkpoint_writer = _RecordingCheckpointWriter()
    write_checkpoint_fn = _generate_non_atomic_write_checkpoint_transform(
        checkpoint_writer
    )

    ctx = TaskContext(task_idx=0, op_name="test")
    sink = _RecordingSink(write_return="done")
    write_fn = generate_write_fn(sink)
    blocks = [
        pd.DataFrame({"id": [1, 2], "x": ["a", "b"]}),
        pd.DataFrame({"id": [3], "x": ["c"]}),
    ]

    # Chain them the way the operator does: each stage pulls the one before.
    captured = capture_fn(iter(blocks), ctx)
    written = write_fn(captured, ctx)
    checkpointed = write_checkpoint_fn(written, ctx)

    stats = next(generate_collect_write_stats_fn()(checkpointed, ctx))
    assert dict(stats.iloc[0])["num_rows"] == 3
    assert sink.num_rows == 3
    assert checkpoint_writer.checkpointed_ids == [1, 2, 3]


def test_non_atomic_checkpoint_validates_id_column_before_write():
    data_context = SimpleNamespace(checkpoint_config=SimpleNamespace(id_column="id"))
    capture_fn = _generate_capture_id_columns_transform(data_context)
    ctx = TaskContext(task_idx=0, op_name="test")

    with pytest.raises(ValueError, match="is absent"):
        list(capture_fn(iter([pd.DataFrame({"x": [1, 2]})]), ctx))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
