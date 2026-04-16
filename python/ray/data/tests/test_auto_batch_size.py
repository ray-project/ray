"""Tests for batch_size="auto" in BatchMapTransformFn and map_batches."""
from typing import Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.map_transformer import (
    BatchMapTransformFn,
    MapTransformer,
)
from ray.data._internal.output_buffer import OutputBlockSizeOption
from ray.data._internal.planner.plan_udf_map_op import (
    _generate_transform_fn_for_map_batches,
)
from ray.data.block import BatchFormat, BlockAccessor


def _make_transformer(
    batch_size,
    batch_format: Optional[BatchFormat] = BatchFormat.ARROW,
    received_sizes=None,
):
    def identity(batch):
        if received_sizes is not None:
            received_sizes.append(len(batch))
        return batch

    return MapTransformer(
        [
            BatchMapTransformFn(
                _generate_transform_fn_for_map_batches(identity),
                batch_size=batch_size,
                batch_format=batch_format,
                output_block_size_option=OutputBlockSizeOption.of(
                    target_max_block_size=None
                ),
            )
        ]
    )


def _collect_batches(transformer, blocks):
    ctx = TaskContext(task_idx=0, op_name="test")
    result_blocks = list(transformer.apply_transform(iter(blocks), ctx))
    return [BlockAccessor.for_block(b).to_pandas() for b in result_blocks]


@pytest.mark.parametrize(
    "target_scale,expected",
    [
        pytest.param(100, "batched", id="basic"),
        pytest.param(0.5, 1, id="clamped_to_one"),
    ],
)
def test_compute_auto_batch_size(restore_data_context, target_scale, expected):
    """Batch size scales with target; clamps to 1 when a single row exceeds it."""
    block = pa.table({"x": pa.array([42], type=pa.int64())})
    fn = BatchMapTransformFn(lambda b, c: b, batch_size="auto", batch_format=BatchFormat.ARROW)
    bytes_per_row = BlockAccessor.for_block(block).size_bytes()
    restore_data_context.default_batch_size_bytes = int(bytes_per_row * target_scale)
    computed, _ = fn._compute_auto_batch_size(iter([block]))
    if expected == "batched":
        assert computed is not None
        assert computed >= 1
    else:
        assert computed == expected


@pytest.mark.parametrize(
    "blocks",
    [
        pytest.param([], id="empty_iterator"),
        pytest.param([pa.table({"x": pa.array([], type=pa.int64())})], id="zero_rows"),
    ],
)
def test_compute_auto_batch_size_returns_none(blocks):
    """Empty and zero-row blocks return None (whole block becomes one batch)."""
    fn = BatchMapTransformFn(lambda b, c: b, batch_size="auto")
    computed, _ = fn._compute_auto_batch_size(iter(blocks))
    assert computed is None


def test_auto_batches_respect_target_size(restore_data_context):
    """With 'auto', rows are grouped to approximate the target byte size."""
    n_rows = 1000
    block = pd.DataFrame({"x": list(range(n_rows))})
    bytes_per_row = (
        BlockAccessor.for_block(pa.Table.from_pandas(block)).size_bytes() / n_rows
    )
    restore_data_context.default_batch_size_bytes = int(bytes_per_row * 10)

    batches = _collect_batches(_make_transformer(batch_size="auto"), [block])

    for b in batches[:-1]:
        assert len(b) == 10


def test_explicit_batch_size_unchanged():
    """Explicit integer batch_size produces correctly-sized batches."""
    received_sizes = []
    block = pa.Table.from_pandas(pd.DataFrame({"x": list(range(100))}))
    _collect_batches(
        _make_transformer(batch_size=25, received_sizes=received_sizes), [block]
    )
    assert received_sizes == [25, 25, 25, 25]


def test_none_batch_size_unchanged():
    """batch_size=None yields one batch per block."""
    received_sizes = []
    blocks = [
        pa.Table.from_pandas(pd.DataFrame({"x": list(range(50))})) for _ in range(3)
    ]
    _collect_batches(
        _make_transformer(batch_size=None, received_sizes=received_sizes), blocks
    )
    assert received_sizes == [50, 50, 50]


def test_map_batches_auto_correctness(ray_start_regular_shared, restore_data_context):
    """auto batch_size preserves all rows and actually produces multiple batches.

    Uses wide float32 rows (high bytes/row) + a small target to guarantee
    the dataset is split into several batches rather than processed as one slice.
    """
    n_rows, width = 1_000, 64
    bytes_per_row = width * 4  # float32
    restore_data_context.default_batch_size_bytes = bytes_per_row * 10  # ~10 rows/batch

    def passthrough(batch):
        n = len(batch["data"])
        return {"data": batch["data"], "size": np.full(n, n, dtype=np.int32)}

    rows = (
        ray.data.from_numpy(np.random.rand(n_rows, width).astype(np.float32))
        .map_batches(passthrough, batch_size="auto", batch_format="numpy")
        .take_all()
    )

    assert len(rows) == n_rows
    assert max(r["size"] for r in rows) < n_rows  # proves batching actually occurred


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
