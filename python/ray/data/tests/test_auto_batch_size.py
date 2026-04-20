"""Tests for batch_size="auto" in BatchMapTransformFn and map_batches."""
import numpy as np
import pyarrow as pa
import pytest

import ray
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.map_transformer import (
    BatchMapTransformFn,
    MapTransformer,
    _compute_auto_batch_size,
)
from ray.data._internal.output_buffer import OutputBlockSizeOption
from ray.data._internal.planner.plan_udf_map_op import (
    _generate_transform_fn_for_map_batches,
)
from ray.data.block import BatchFormat
from ray.data.context import DEFAULT_TARGET_MAX_BLOCK_SIZE


def test_compute_auto_batch_size_basic():
    """Batch size is target_bytes / bytes_per_row from the first block."""
    # 10 int64 rows = 80 bytes (8 bytes/row). Target of 800 -> batch_size = 100.
    block = pa.table({"x": pa.array(np.arange(10, dtype=np.int64))})
    batch_size, _ = _compute_auto_batch_size(iter([block]), target_batch_size_bytes=800)
    assert batch_size == 100


def test_compute_auto_batch_size_clamped_to_one():
    """When the target is smaller than one row, batch size clamps to 1."""
    # 10 int64 rows = 80 bytes (8 bytes/row). Target of 1 byte < 1 row -> clamp to 1.
    block = pa.table({"x": pa.array(np.arange(10, dtype=np.int64))})
    batch_size, _ = _compute_auto_batch_size(iter([block]), target_batch_size_bytes=1)
    assert batch_size == 1


@pytest.mark.parametrize(
    "blocks",
    [
        pytest.param([], id="empty_iterator"),
        pytest.param([pa.table({"x": pa.array([], type=pa.int64())})], id="zero_rows"),
    ],
)
def test_compute_auto_batch_size_returns_none(blocks):
    """Empty and zero-row blocks return None (whole block becomes one batch)."""
    batch_size, _ = _compute_auto_batch_size(iter(blocks))
    assert batch_size is None


def test_compute_auto_batch_size_iterator_includes_peeked_block():
    """The returned iterator contains all input blocks, including the peeked block."""
    blocks = [
        pa.table({"x": pa.array(np.arange(10, dtype=np.int64))}),
        pa.table({"x": pa.array(np.arange(10, 20, dtype=np.int64))}),
    ]
    _, returned_blocks = _compute_auto_batch_size(iter(blocks))
    returned = list(returned_blocks)
    assert len(returned) == 2
    assert returned[0].equals(blocks[0])
    assert returned[1].equals(blocks[1])


def test_auto_batches_respect_target_size():
    """With 'auto', rows are grouped to approximate the target byte size."""
    # 1000 int64 rows = 8000 bytes (8 bytes/row). Target of 80 -> batch_size = 10.
    block = pa.table({"x": pa.array(range(1000), type=pa.int64())})

    received_sizes = []

    def identity(batch):
        received_sizes.append(len(batch))
        return batch

    transformer = MapTransformer(
        [
            BatchMapTransformFn(
                _generate_transform_fn_for_map_batches(identity),
                batch_size="auto",
                batch_format=BatchFormat.ARROW,
                output_block_size_option=OutputBlockSizeOption.of(
                    target_max_block_size=DEFAULT_TARGET_MAX_BLOCK_SIZE
                ),
                target_batch_size_bytes=80,
            )
        ]
    )
    ctx = TaskContext(task_idx=0, op_name="test")
    list(transformer.apply_transform(iter([block]), ctx))

    assert sum(received_sizes) == 1000
    assert all(s == 10 for s in received_sizes[:-1])


def test_map_batches_auto_correctness(ray_start_regular_shared):
    """batch_size='auto' preserves all rows in an end-to-end pipeline."""
    n_rows = 100
    rows = (
        ray.data.range(n_rows)
        .map_batches(lambda batch: batch, batch_size="auto")
        .take_all()
    )
    assert len(rows) == n_rows


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
