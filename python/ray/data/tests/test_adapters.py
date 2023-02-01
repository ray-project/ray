import numpy as np
import pandas as pd
import pytest
from pytest_lazyfixture import lazy_fixture

import ray
from ray.data._internal.logical.operators.read_operator import Read
from ray.data._internal.logical.operators.map_operator import (
    MapBatches,
    MapRows,
    FlatMap,
    Filter,
    Write,
)
from ray.data._internal.metrics import MetricsCollector, DataMungingMetrics
from ray.data._internal.planner.transforms.adapters import (
    InputAdapter,
    OutputAdapter,
    OpAdapter,
    BlocksInputAdapter,
    BatchesInputAdapter,
    RowsInputAdapter,
    BlocksOutputAdapter,
    BatchesOutputAdapter,
    RowsOutputAdapter,
    BlocksToBlocksOpAdapter,
    BlocksToBatchesOpAdapter,
    BlocksToRowsOpAdapter,
    BatchesToBlocksOpAdapter,
    BatchesToBatchesOpAdapter,
    BatchesToRowsOpAdapter,
    RowsToBlocksOpAdapter,
    RowsToBatchesOpAdapter,
    RowsToRowsOpAdapter,
    BlocksToBatchesAdapter,
)
from ray.data.datasource import RangeDatasource, DummyOutputDatasource
from ray.tests.conftest import *  # noqa


@pytest.fixture
def small_target_max_block_size():
    ctx = ray.data.context.DatasetContext.get_current()
    original = ctx.target_max_block_size
    # For lists of ints, assume 10 bytes per int (including pickle overhead).
    ctx.target_max_block_size = 30
    yield
    ctx.target_max_block_size = original


@pytest.fixture
def small_target_min_block_size():
    ctx = ray.data.context.DatasetContext.get_current()
    original = ctx.target_min_block_size
    # For lists of ints, assume 10 bytes per int (including pickle overhead).
    ctx.target_min_block_size = 30
    yield
    ctx.target_min_block_size = original


@pytest.fixture
def in_blocks():
    return [[1], [2, 3, 4], [5, 6]]


@pytest.fixture
def in_rows(in_blocks):
    return [row for block in in_blocks for row in block]


@pytest.fixture
def read_op():
    return Read(RangeDatasource())


@pytest.fixture
def write_op(read_op):
    return Write(read_op, DummyOutputDatasource())


@pytest.fixture
def map_batches_op(read_op):
    return MapBatches(read_op, lambda x: x)


@pytest.fixture
def map_rows_op(read_op):
    return MapRows(read_op, lambda x: x)


@pytest.fixture
def flat_map_op(read_op):
    return FlatMap(read_op, lambda x: [x])


@pytest.fixture
def filter_op(read_op):
    return Filter(read_op, lambda x: x % 2 == 0)


def test_blocks_input_adapter(
    ray_start_regular_shared, small_target_max_block_size, in_blocks
):
    # Test that blocks-to-blocks input adapter is a no-op, with no input buffering.
    op = Read(RangeDatasource())
    adapter = BlocksInputAdapter(op)
    out_blocks = adapter.adapt(in_blocks)
    # No buffering of the input.
    assert list(out_blocks) == in_blocks


def test_batches_input_adapter_null_batch(
    ray_start_regular_shared, small_target_max_block_size, in_blocks
):
    # Test that blocks-to-batches input adapter passes input blocks through untouched if
    # no batch size is specified.
    up_op = Read(RangeDatasource())
    down_op = MapBatches(up_op, lambda x: x, batch_size=None)
    adapter = BatchesInputAdapter(down_op)
    metrics_collector = MetricsCollector()
    adapter.register_metrics_collector(metrics_collector)
    out_batches = adapter.adapt(in_blocks)
    # No buffering of the input, and no batching since the batch size is None.
    assert list(out_batches) == in_blocks
    metrics = metrics_collector.get_metrics()
    assert isinstance(metrics, DataMungingMetrics), metrics
    # No copies/slices/concatenations when batch_size=None.
    assert metrics.num_copies == 0
    assert metrics.num_rows_copied == 0
    assert metrics.num_slices == 0
    assert metrics.num_concatenations == 0
    # One format conversion per block.
    assert metrics.num_format_conversions == len(in_blocks)


def test_batches_input_adapter_batch_size(
    ray_start_regular_shared, small_target_max_block_size, in_blocks
):
    # Test that blocks-to-batches input adapter batches to the appropriate size.
    up_op = Read(RangeDatasource())
    batch_size = 2
    down_op = MapBatches(up_op, lambda x: x, batch_size=batch_size)
    adapter = BatchesInputAdapter(down_op)
    metrics_collector = MetricsCollector()
    adapter.register_metrics_collector(metrics_collector)
    out_batches = adapter.adapt(in_blocks)
    # No buffering of the input.
    assert list(out_batches) == [[1, 2], [3, 4], [5, 6]]
    metrics = metrics_collector.get_metrics()
    assert isinstance(metrics, DataMungingMetrics), metrics
    num_rows = sum(len(b) for b in in_blocks)
    # One concatenation -> copy per block slice (4),
    # and one build -> copy per batch (3).
    assert metrics.num_copies == len(in_blocks) + 1 + (num_rows // batch_size)
    # Blocks are copied twice overall.
    assert metrics.num_rows_copied == 2 * num_rows
    # Format conversion per materialized batch.
    assert metrics.num_format_conversions == num_rows // batch_size
    # One slice per input block (3), + extra slices for block split (2).
    assert metrics.num_slices == len(in_blocks) + 2
    # Every block slice when building batches results in a concatenation.
    assert metrics.num_concatenations == 4


def test_batches_input_adapter_batch_format(
    ray_start_regular_shared, small_target_max_block_size, in_blocks
):
    # Test that blocks-to-batches input adapter converts to the correct batch format.
    up_op = Read(RangeDatasource())
    batch_size = 2
    down_op = MapBatches(
        up_op, lambda x: x, batch_size=batch_size, batch_format="numpy"
    )
    adapter = BatchesInputAdapter(down_op)
    metrics_collector = MetricsCollector()
    adapter.register_metrics_collector(metrics_collector)
    out_batches = adapter.adapt(in_blocks)
    expected_batches = np.array([[1, 2], [3, 4], [5, 6]])
    for batch, expected in zip(out_batches, expected_batches):
        # Batch format (not block format) is yielded.
        assert isinstance(batch, np.ndarray)
        np.testing.assert_array_equal(batch, expected)
    metrics = metrics_collector.get_metrics()
    assert isinstance(metrics, DataMungingMetrics), metrics
    num_rows = sum(len(b) for b in in_blocks)
    # One concatenation -> copy per block slice (4),
    # one format conversion -> copy per batch (3).
    # and one build -> copy per batch (3).
    assert metrics.num_copies == len(in_blocks) + 1 + 2 * (num_rows // batch_size)
    # Blocks are copied thrice overall.
    assert metrics.num_rows_copied == 3 * num_rows
    # Format conversion per materialized batch.
    assert metrics.num_format_conversions == num_rows // batch_size
    # One slice per input block (3), + extra slices for block split (2).
    assert metrics.num_slices == len(in_blocks) + 2
    # Every block slice when building batches results in a concatenation.
    assert metrics.num_concatenations == 4


def test_rows_input_adapter(
    ray_start_regular_shared, small_target_max_block_size, in_blocks
):
    # Test that blocks-to-rows input adapter yields rows from blocks without buffering.
    up_op = Read(RangeDatasource())
    down_op = MapRows(up_op, lambda x: x)
    adapter = RowsInputAdapter(down_op)
    metrics_collector = MetricsCollector()
    adapter.register_metrics_collector(metrics_collector)
    out_rows = adapter.adapt(in_blocks)
    assert list(out_rows) == list(range(1, 7))
    metrics = metrics_collector.get_metrics()
    assert isinstance(metrics, DataMungingMetrics), metrics
    # No copies/slices/concatenations/format conversions when going from blocks to rows.
    # This proves that (1) we're not needlessly buffering blocks, (2) we're not doing
    # any unnecessary format conversions.
    assert metrics.num_copies == 0
    assert metrics.num_rows_copied == 0
    assert metrics.num_slices == 0
    assert metrics.num_concatenations == 0
    assert metrics.num_format_conversions == 0


def test_blocks_output_adapter(
    ray_start_regular_shared, small_target_max_block_size, in_blocks
):
    # Test that blocks-to-blocks output adapter buffers the output up to the target max
    # block size.
    op = Read(RangeDatasource())
    adapter = BlocksOutputAdapter(op)
    metrics_collector = MetricsCollector()
    adapter.register_metrics_collector(metrics_collector)
    out_blocks = adapter.adapt(in_blocks)
    # Data should be buffered up to target max block size.
    assert list(out_blocks) == [[1, 2, 3, 4], [5, 6]]
    metrics = metrics_collector.get_metrics()
    num_rows = sum(len(b) for b in in_blocks)
    assert isinstance(metrics, DataMungingMetrics), metrics
    # One concatenation/copy per block, + 2 build copies.
    assert metrics.num_copies == len(in_blocks) + 2
    # Blocks are copied twice overall.
    assert metrics.num_rows_copied == 2 * num_rows
    # No slices on the blocks-to-blocks path.
    assert metrics.num_slices == 0
    # One concatenation per block.
    assert metrics.num_concatenations == len(in_blocks)


def test_batches_output_adapter(
    ray_start_regular_shared, small_target_max_block_size, in_blocks
):
    # Test that batches-to-blocks output adapter buffers the output up to the target max
    # block size.
    op = Read(RangeDatasource())
    batch_size = 2
    up_op = MapBatches(op, lambda x: x, batch_size=batch_size)
    adapter = BatchesOutputAdapter(up_op)
    metrics_collector = MetricsCollector()
    adapter.register_metrics_collector(metrics_collector)
    blocks_to_batches_adapter = BlocksToBatchesAdapter(batch_size, "default", 0, True)
    in_batches = blocks_to_batches_adapter.adapt(in_blocks)
    out_blocks = list(adapter.adapt(in_batches))
    # Data should be buffered up to target max block size.
    assert out_blocks == [[1, 2, 3, 4], [5, 6]]
    metrics = metrics_collector.get_metrics()
    assert isinstance(metrics, DataMungingMetrics), metrics
    num_rows = sum(len(b) for b in in_blocks)
    # One concatenation -> copy per batch (3),
    # and one build -> copy per output block (2).
    assert metrics.num_copies == num_rows // batch_size + len(out_blocks)
    # Blocks are copied twice overall.
    assert metrics.num_rows_copied == 2 * num_rows
    # No slices on the batches-to-blocks path.
    assert metrics.num_slices == 0
    # One concatenation per batch.
    assert metrics.num_concatenations == num_rows // batch_size
    # No format conversions on batches-to-blocks path.
    assert metrics.num_format_conversions == 0


def test_rows_output_adapter(
    ray_start_regular_shared, small_target_max_block_size, in_rows
):
    # Test that rows-to-blocks output adapter buffers the output up to the target max
    # block size.
    op = Read(RangeDatasource())
    up_op = MapRows(op, lambda x: x)
    adapter = RowsOutputAdapter(up_op)
    metrics_collector = MetricsCollector()
    adapter.register_metrics_collector(metrics_collector)
    in_rows = list(in_rows)
    out_blocks = list(adapter.adapt(iter(in_rows)))
    # Data should be buffered up to target max block size.
    assert out_blocks == [[1, 2, 3, 4], [5, 6]]
    metrics = metrics_collector.get_metrics()
    num_rows = len(in_rows)
    assert isinstance(metrics, DataMungingMetrics), metrics
    # One build -> copy per output block.
    assert metrics.num_copies == len(out_blocks)
    # Blocks are copied once, when building the output blocks.
    assert metrics.num_rows_copied == num_rows
    # No slices on the batches-to-blocks path.
    assert metrics.num_slices == 0
    # No concatenations when building the blocks from rows.
    assert metrics.num_concatenations == 0
    # No format conversions on batches-to-blocks path.
    assert metrics.num_format_conversions == 0


def test_blocks_to_blocks_op_adapter(
    ray_start_regular_shared, small_target_max_block_size, in_blocks
):
    # Test that blocks-to-blocks op adapter passes input blocks through untouched,
    # without any buffering.
    up_op = Read(RangeDatasource())
    down_op = Write(up_op, DummyOutputDatasource())
    adapter = BlocksToBlocksOpAdapter(up_op, down_op)
    metrics_collector = MetricsCollector()
    adapter.register_metrics_collector(metrics_collector)
    out_blocks = adapter.adapt(in_blocks)
    # No buffering of the input.
    assert list(out_blocks) == in_blocks
    metrics = metrics_collector.get_metrics()
    assert isinstance(metrics, DataMungingMetrics), metrics
    # No buffering, so no copies; no batching, so no slices/concatenations/format
    # converisons.
    assert metrics.num_copies == 0
    assert metrics.num_rows_copied == 0
    assert metrics.num_slices == 0
    assert metrics.num_concatenations == 0
    assert metrics.num_format_conversions == 0


def test_batches_to_blocks_op_adapter(
    ray_start_regular_shared, small_target_min_block_size, in_blocks
):
    # Test that batches-to-blocks op adapter propagates batches without buffering.
    op = Read(RangeDatasource())
    batch_size = 2
    up_op = MapBatches(op, lambda x: x, batch_size=batch_size)
    down_op = Write(up_op, DummyOutputDatasource())
    adapter = BatchesToBlocksOpAdapter(up_op, down_op)
    metrics_collector = MetricsCollector()
    adapter.register_metrics_collector(metrics_collector)
    blocks_to_batches_adapter = BlocksToBatchesAdapter(batch_size, "default", 0, True)
    in_batches = blocks_to_batches_adapter.adapt(in_blocks)
    out_blocks = list(adapter.adapt(in_batches))
    # Batches should be untouched.
    assert out_blocks == [[1, 2], [3, 4], [5, 6]]
    metrics = metrics_collector.get_metrics()
    assert isinstance(metrics, DataMungingMetrics), metrics
    # No buffering, so no copies; no batching, so no slices/concatenations/format
    # converisons.
    assert metrics.num_copies == 0
    assert metrics.num_rows_copied == 0
    assert metrics.num_slices == 0
    assert metrics.num_concatenations == 0
    assert metrics.num_format_conversions == 0


def test_rows_to_blocks_op_adapter(
    ray_start_regular_shared, small_target_min_block_size, in_rows
):
    # Test that rows-to-blocks op adapter buffers the output up to the target min
    # block size.
    op = Read(RangeDatasource())
    up_op = MapRows(op, lambda x: x)
    down_op = Write(up_op, DummyOutputDatasource())
    adapter = RowsToBlocksOpAdapter(up_op, down_op)
    metrics_collector = MetricsCollector()
    adapter.register_metrics_collector(metrics_collector)
    in_rows = list(in_rows)
    out_blocks = list(adapter.adapt(iter(in_rows)))
    # Data should be buffered up to target min block size.
    assert out_blocks == [[1, 2, 3, 4], [5, 6]]
    metrics = metrics_collector.get_metrics()
    assert isinstance(metrics, DataMungingMetrics), metrics
    num_rows = len(in_rows)
    # One build -> copy per output block.
    assert metrics.num_copies == len(out_blocks)
    # Blocks are copied once, when building the output blocks.
    assert metrics.num_rows_copied == num_rows
    # No slices on the batches-to-blocks path.
    assert metrics.num_slices == 0
    # No concatenations when building the blocks from rows.
    assert metrics.num_concatenations == 0
    # No format conversions on batches-to-blocks path.
    assert metrics.num_format_conversions == 0


def test_blocks_to_batches_op_adapter_null_batch(
    ray_start_regular_shared, small_target_max_block_size, in_blocks
):
    # Test that blocks-to-batches op adapter passes input blocks through untouched if no
    # batch size is specified.
    up_op = Read(RangeDatasource())
    down_op = MapBatches(up_op, lambda x: x, batch_size=None)
    adapter = BlocksToBatchesOpAdapter(up_op, down_op)
    metrics_collector = MetricsCollector()
    adapter.register_metrics_collector(metrics_collector)
    out_batches = adapter.adapt(in_blocks)
    # No buffering of the input, and no batching since the batch size is None.
    assert list(out_batches) == in_blocks
    metrics = metrics_collector.get_metrics()
    assert isinstance(metrics, DataMungingMetrics), metrics
    # No copies/slices/concatenations when batch_size=None.
    assert metrics.num_copies == 0
    assert metrics.num_rows_copied == 0
    assert metrics.num_slices == 0
    assert metrics.num_concatenations == 0
    # One format conversion per block.
    assert metrics.num_format_conversions == len(in_blocks)


def test_blocks_to_batches_op_adapter_batch_size(
    ray_start_regular_shared, small_target_max_block_size, in_blocks
):
    # Test that blocks-to-batches op adapter batches to the appropriate size.
    up_op = Read(RangeDatasource())
    batch_size = 2
    down_op = MapBatches(up_op, lambda x: x, batch_size=batch_size)
    adapter = BlocksToBatchesOpAdapter(up_op, down_op)
    metrics_collector = MetricsCollector()
    adapter.register_metrics_collector(metrics_collector)
    out_batches = adapter.adapt(in_blocks)
    # No buffering of the input.
    assert list(out_batches) == [[1, 2], [3, 4], [5, 6]]
    metrics = metrics_collector.get_metrics()
    assert isinstance(metrics, DataMungingMetrics), metrics
    num_rows = sum(len(b) for b in in_blocks)
    # One concatenation -> copy per block slice (4),
    # and one build -> copy per batch (3).
    assert metrics.num_copies == len(in_blocks) + 1 + (num_rows // batch_size)
    # Blocks are copied twice overall.
    assert metrics.num_rows_copied == 2 * num_rows
    # Format conversion per materialized batch.
    assert metrics.num_format_conversions == num_rows // batch_size
    # One slice per input block (3), + extra slices for block split (2).
    assert metrics.num_slices == len(in_blocks) + 2
    # Every block slice when building batches results in a concatenation.
    assert metrics.num_concatenations == 4


def test_blocks_to_batches_op_adapter_batch_format(
    ray_start_regular_shared, small_target_max_block_size, in_blocks
):
    # Test that blocks-to-batches op adapter converts to the correct batch format.
    up_op = Read(RangeDatasource())
    batch_size = 2
    down_op = MapBatches(
        up_op, lambda x: x, batch_size=batch_size, batch_format="numpy"
    )
    adapter = BlocksToBatchesOpAdapter(up_op, down_op)
    metrics_collector = MetricsCollector()
    adapter.register_metrics_collector(metrics_collector)
    out_batches = adapter.adapt(in_blocks)
    expected_batches = np.array([[1, 2], [3, 4], [5, 6]])
    for batch, expected in zip(out_batches, expected_batches):
        # Batch format (not block format) is yielded.
        assert isinstance(batch, np.ndarray)
        np.testing.assert_array_equal(batch, expected)
    metrics = metrics_collector.get_metrics()
    assert isinstance(metrics, DataMungingMetrics), metrics
    num_rows = sum(len(b) for b in in_blocks)
    # One concatenation -> copy per block slice (4),
    # one format conversion -> copy per batch (3).
    # and one build -> copy per batch (3).
    assert metrics.num_copies == len(in_blocks) + 1 + 2 * (num_rows // batch_size)
    # Blocks are copied thrice overall.
    assert metrics.num_rows_copied == 3 * num_rows
    # Format conversion per materialized batch.
    assert metrics.num_format_conversions == num_rows // batch_size
    # One slice per input block (3), + extra slices for block split (2).
    assert metrics.num_slices == len(in_blocks) + 2
    # Every block slice when building batches results in a concatenation.
    assert metrics.num_concatenations == 4


def test_blocks_to_rows_op_adapter(
    ray_start_regular_shared, small_target_max_block_size, in_blocks
):
    # Test that blocks-to-rows op adapter yields rows from blocks without buffering.
    up_op = Read(RangeDatasource())
    down_op = MapRows(up_op, lambda x: x)
    adapter = BlocksToRowsOpAdapter(up_op, down_op)
    metrics_collector = MetricsCollector()
    adapter.register_metrics_collector(metrics_collector)
    out_rows = adapter.adapt(in_blocks)
    assert list(out_rows) == list(range(1, 7))
    metrics = metrics_collector.get_metrics()
    assert isinstance(metrics, DataMungingMetrics), metrics
    # No copies/slices/concatenations/format conversions when going from blocks to rows.
    # This proves that (1) we're not needlessly buffering blocks, (2) we're not doing
    # any unnecessary format conversions.
    assert metrics.num_copies == 0
    assert metrics.num_rows_copied == 0
    assert metrics.num_slices == 0
    assert metrics.num_concatenations == 0
    assert metrics.num_format_conversions == 0


def test_batches_to_batches_op_adapter_null_batch(
    ray_start_regular_shared, small_target_max_block_size, in_blocks
):
    # Test that batches-to-batches op adapter passes input blocks through untouched if
    # no batch size is specified for either op.
    op = Read(RangeDatasource())
    up_op = MapBatches(op, lambda x: x, batch_size=None)
    down_op = MapBatches(up_op, lambda x: x, batch_size=None)
    adapter = BatchesToBatchesOpAdapter(up_op, down_op)
    metrics_collector = MetricsCollector()
    adapter.register_metrics_collector(metrics_collector)
    out_batches = adapter.adapt(in_blocks)
    # No buffering of the input, and no batching since the batch size is None.
    assert list(out_batches) == in_blocks
    metrics = metrics_collector.get_metrics()
    assert isinstance(metrics, DataMungingMetrics), metrics
    # No copies/slices/concatenations when batch_size=None.
    assert metrics.num_copies == 0
    assert metrics.num_rows_copied == 0
    assert metrics.num_slices == 0
    assert metrics.num_concatenations == 0
    # One format conversion per block.
    assert metrics.num_format_conversions == len(in_blocks)


def test_batches_to_batches_op_adapter_null_and_batch_size(
    ray_start_regular_shared, small_target_max_block_size, in_blocks
):
    # Test that batches-to-batches op adapter batches to the appropriate batch size.
    op = Read(RangeDatasource())
    up_op = MapBatches(op, lambda x: x, batch_size=None)
    batch_size = 2
    down_op = MapBatches(up_op, lambda x: x, batch_size=batch_size)
    adapter = BatchesToBatchesOpAdapter(up_op, down_op)
    metrics_collector = MetricsCollector()
    adapter.register_metrics_collector(metrics_collector)
    out_batches = adapter.adapt(in_blocks)
    # No buffering of the input, but batches of size 2 are constructed.
    assert list(out_batches) == [[1, 2], [3, 4], [5, 6]]
    metrics = metrics_collector.get_metrics()
    assert isinstance(metrics, DataMungingMetrics), metrics
    num_rows = sum(len(b) for b in in_blocks)
    # One concatenation -> copy per block slice (4),
    # and one build -> copy per batch (3).
    assert metrics.num_copies == len(in_blocks) + 1 + (num_rows // batch_size)
    # Blocks are copied twice overall.
    assert metrics.num_rows_copied == 2 * num_rows
    # Format conversion per materialized batch.
    assert metrics.num_format_conversions == num_rows // batch_size
    # One slice per input block (3), + extra slices for block split (2).
    assert metrics.num_slices == len(in_blocks) + 2
    # Every block slice when building batches results in a concatenation.
    assert metrics.num_concatenations == 4


def test_batches_to_batches_op_adapter_batch_size_and_null(
    ray_start_regular_shared, small_target_max_block_size, in_blocks
):
    # Test that batches-to-batches op adapter preserves batch sizes.
    op = Read(RangeDatasource())
    batch_size = 2
    up_op = MapBatches(op, lambda x: x, batch_size=batch_size)
    input_adapter = BatchesInputAdapter(up_op)
    input_adapter.register_metrics_collector(MetricsCollector())
    in_batches = list(input_adapter.adapt(in_blocks))
    down_op = MapBatches(up_op, lambda x: x, batch_size=None)
    adapter = BatchesToBatchesOpAdapter(up_op, down_op)
    # NOTE: These are only the metrics for the op adapter, not the above input adapter.
    metrics_collector = MetricsCollector()
    adapter.register_metrics_collector(metrics_collector)
    out_batches = adapter.adapt(iter(in_batches))
    # No buffering of the input and 2-item batches are maintained.
    assert list(out_batches) == [[1, 2], [3, 4], [5, 6]]
    metrics = metrics_collector.get_metrics()
    assert isinstance(metrics, DataMungingMetrics), metrics
    # No copies/slices/concatenations when batch_size=None.
    assert metrics.num_copies == 0
    assert metrics.num_rows_copied == 0
    assert metrics.num_slices == 0
    assert metrics.num_concatenations == 0
    # One format conversion per input batch.
    assert metrics.num_format_conversions == len(in_batches)


def test_batches_to_batches_op_adapter_same_batch_size(
    ray_start_regular_shared, small_target_max_block_size, in_blocks
):
    # Test that batches-to-batches op adapter preserves batch sizes.
    op = Read(RangeDatasource())
    batch_size = 2
    up_op = MapBatches(op, lambda x: x, batch_size=batch_size)
    input_adapter = BatchesInputAdapter(up_op)
    input_adapter.register_metrics_collector(MetricsCollector())
    in_batches = list(input_adapter.adapt(in_blocks))
    down_op = MapBatches(up_op, lambda x: x, batch_size=batch_size)
    adapter = BatchesToBatchesOpAdapter(up_op, down_op)
    # NOTE: These are only the metrics for the op adapter, not the above input adapter.
    metrics_collector = MetricsCollector()
    adapter.register_metrics_collector(metrics_collector)
    out_batches = adapter.adapt(iter(in_batches))
    # No buffering of the input and 2-item batches are maintained.
    assert list(out_batches) == [[1, 2], [3, 4], [5, 6]]
    metrics = metrics_collector.get_metrics()
    assert isinstance(metrics, DataMungingMetrics), metrics
    num_rows = sum(len(b) for b in in_blocks)
    # One concatenation -> copy per batch (3),
    # and one build -> copy per batch (3).
    assert metrics.num_copies == 2 * (num_rows // batch_size)
    # Input batches are copied twice overall.
    assert metrics.num_rows_copied == 2 * num_rows
    # Format conversion per materialized batch.
    assert metrics.num_format_conversions == num_rows // batch_size
    # One slice per input batch (3).
    assert metrics.num_slices == num_rows // batch_size
    # Every batch slice when building batches results in a concatenation.
    assert metrics.num_concatenations == 3


def test_batches_to_batches_op_adapter_different_batch_sizes(
    ray_start_regular_shared, small_target_max_block_size, in_blocks
):
    # Test that batches-to-batches op adapter preserves batch sizes.
    op = Read(RangeDatasource())
    up_batch_size = 3
    up_op = MapBatches(op, lambda x: x, batch_size=up_batch_size)
    input_adapter = BatchesInputAdapter(up_op)
    input_adapter.register_metrics_collector(MetricsCollector())
    in_batches = list(input_adapter.adapt(in_blocks))
    # Sanity check input batches.
    assert in_batches == [[1, 2, 3], [4, 5, 6]]
    down_batch_size = 2
    down_op = MapBatches(up_op, lambda x: x, batch_size=down_batch_size)
    adapter = BatchesToBatchesOpAdapter(up_op, down_op)
    # NOTE: These are only the metrics for the op adapter, not the above input adapter.
    metrics_collector = MetricsCollector()
    adapter.register_metrics_collector(metrics_collector)
    out_batches = adapter.adapt(iter(in_batches))
    # No buffering of the input and batches are rebatched.
    assert list(out_batches) == [[1, 2], [3, 4], [5, 6]]
    metrics = metrics_collector.get_metrics()
    assert isinstance(metrics, DataMungingMetrics), metrics
    num_rows = sum(len(b) for b in in_blocks)
    # One concatenation -> copy per input batch slice (4),
    # and one build -> copy per batch (3).
    assert metrics.num_copies == (
        2 * (num_rows // up_batch_size) + (num_rows // down_batch_size)
    )
    # Input batches are copied twice overall.
    assert metrics.num_rows_copied == 2 * num_rows
    # Format conversion per materialized batch.
    assert metrics.num_format_conversions == num_rows // down_batch_size
    # Two slices slice per input batch (2), and a slice per batch split (2 * 2).
    assert metrics.num_slices == 3 * (num_rows // up_batch_size)
    # Every batch slice when building batches results in a concatenation.
    assert metrics.num_concatenations == 4


def test_batches_to_batches_op_adapter_different_batch_formats(
    ray_start_regular_shared, small_target_max_block_size, in_blocks
):
    # Test that batches-to-batches op adapter respects differing batch formats.
    op = Read(RangeDatasource())
    up_op = MapBatches(op, lambda x: x, batch_size=None, batch_format="numpy")
    down_op = MapBatches(up_op, lambda x: x, batch_size=None, batch_format="pandas")
    adapter = BatchesToBatchesOpAdapter(up_op, down_op)
    metrics_collector = MetricsCollector()
    adapter.register_metrics_collector(metrics_collector)
    out_batches = adapter.adapt(in_blocks)
    # No buffering of the input, and no batching since the batch size is None.
    expected_dfs = [pd.DataFrame({"value": b}) for b in in_blocks]
    for batch, expected in zip(out_batches, expected_dfs):
        pd.testing.assert_frame_equal(batch, expected)
    metrics = metrics_collector.get_metrics()
    assert isinstance(metrics, DataMungingMetrics), metrics
    num_rows = sum(len(b) for b in in_blocks)
    # Differing (and non-zero-copy) batch formats will result in a format conversion
    # copy per block.
    assert metrics.num_copies == 3
    # No slices/concatenations when batch_size=None.
    assert metrics.num_rows_copied == num_rows
    assert metrics.num_slices == 0
    assert metrics.num_concatenations == 0
    # One format conversion per block.
    assert metrics.num_format_conversions == len(in_blocks)


def test_batches_to_rows_op_adapter(
    ray_start_regular_shared, small_target_max_block_size, in_blocks
):
    # Test that batches-to-rows op adapter streams rows.
    op = Read(RangeDatasource())
    up_op = MapBatches(op, lambda x: x, batch_size=None)
    down_op = MapRows(up_op, lambda x: x)
    adapter = BatchesToRowsOpAdapter(up_op, down_op)
    metrics_collector = MetricsCollector()
    adapter.register_metrics_collector(metrics_collector)
    out_rows = adapter.adapt(in_blocks)
    # No buffering of the input and rows are streamed from input batches.
    assert list(out_rows) == list(range(1, 7))
    metrics = metrics_collector.get_metrics()
    assert isinstance(metrics, DataMungingMetrics), metrics
    # No copies/slices/concatenations/format conversions when going from blocks to rows.
    # This proves that (1) we're not needlessly buffering blocks, (2) we're not doing
    # any unnecessary format conversions.
    assert metrics.num_copies == 0
    assert metrics.num_rows_copied == 0
    assert metrics.num_slices == 0
    assert metrics.num_concatenations == 0
    assert metrics.num_format_conversions == 0


def test_rows_to_batches_op_adapter_null_batch(
    ray_start_regular_shared, small_target_min_block_size, in_rows
):
    # Test that batches-to-batches op adapter passes input blocks through untouched if
    # no batch size is specified.
    op = Read(RangeDatasource())
    up_op = MapRows(op, lambda x: x)
    down_op = MapBatches(up_op, lambda x: x, batch_size=None)
    adapter = RowsToBatchesOpAdapter(up_op, down_op)
    metrics_collector = MetricsCollector()
    adapter.register_metrics_collector(metrics_collector)
    in_rows = list(in_rows)
    out_batches = list(adapter.adapt(iter(in_rows)))
    # Data should be buffered up to target max block size and then batches are yielded.
    # TODO(Clark): Find a better fallback block/batch size than the target max block
    # size.
    assert out_batches == [[1, 2, 3, 4], [5, 6]]
    metrics = metrics_collector.get_metrics()
    assert isinstance(metrics, DataMungingMetrics), metrics
    # One build -> copy per out block (2).
    assert metrics.num_copies == 2
    # Rows are copied once.
    assert metrics.num_rows_copied == len(in_rows)
    # No slicing or concatenation, since we're not going through any batching.
    assert metrics.num_slices == 0
    assert metrics.num_concatenations == 0
    # One format conversion per batch.
    assert metrics.num_format_conversions == len(out_batches)


def test_rows_to_batches_op_adapter_batch_size(
    ray_start_regular_shared, small_target_max_block_size, in_rows
):
    # Test that batches-to-batches op adapter passes input blocks through untouched if
    # no batch size is specified.
    op = Read(RangeDatasource())
    up_op = MapRows(op, lambda x: x)
    batch_size = 2
    down_op = MapBatches(up_op, lambda x: x, batch_size=batch_size)
    adapter = RowsToBatchesOpAdapter(up_op, down_op)
    metrics_collector = MetricsCollector()
    adapter.register_metrics_collector(metrics_collector)
    in_rows = list(in_rows)
    out_batches = list(adapter.adapt(iter(in_rows)))
    # Data should be buffered up to batch size and then batches are yielded.
    assert out_batches == [[1, 2], [3, 4], [5, 6]]
    metrics = metrics_collector.get_metrics()
    assert isinstance(metrics, DataMungingMetrics), metrics
    # One build -> copy per batch when building batch from rows (3),
    # one concatenation -> copy per batch (3)
    # and one build -> copy per batch (3).
    assert metrics.num_copies == 3 * (len(in_rows) // batch_size)
    # Rows are copied thrice overall.
    assert metrics.num_rows_copied == 3 * len(in_rows)
    # Slice per batch.
    assert metrics.num_slices == len(in_rows) // batch_size
    # Concatenation per batch.
    assert metrics.num_concatenations == len(in_rows) // batch_size
    # One format conversion per batch.
    assert metrics.num_format_conversions == len(in_rows) // batch_size


def test_rows_to_rows_op_adapter(
    ray_start_regular_shared, small_target_max_block_size, in_rows
):
    # Test that rows-to-rows op adapter yields rows without buffering.
    op = Read(RangeDatasource())
    up_op = MapRows(op, lambda x: x)
    down_op = MapRows(up_op, lambda x: x)
    adapter = RowsToRowsOpAdapter(up_op, down_op)
    metrics_collector = MetricsCollector()
    adapter.register_metrics_collector(metrics_collector)
    out_rows = adapter.adapt(in_rows)
    assert list(out_rows) == list(range(1, 7))
    metrics = metrics_collector.get_metrics()
    assert isinstance(metrics, DataMungingMetrics), metrics
    # No copies/slices/concatenations/format conversions when going from blocks to rows.
    # This proves that (1) we're not needlessly buffering blocks, (2) we're not doing
    # any unnecessary format conversions.
    assert metrics.num_copies == 0
    assert metrics.num_rows_copied == 0
    assert metrics.num_slices == 0
    assert metrics.num_concatenations == 0
    assert metrics.num_format_conversions == 0


@pytest.mark.parametrize(
    "op,expected_adapter",
    [
        (lazy_fixture("read_op"), BlocksInputAdapter),
        (lazy_fixture("write_op"), BlocksInputAdapter),
        (lazy_fixture("map_batches_op"), BatchesInputAdapter),
        (lazy_fixture("map_rows_op"), RowsInputAdapter),
        (lazy_fixture("filter_op"), RowsInputAdapter),
        (lazy_fixture("flat_map_op"), RowsInputAdapter),
    ],
)
def test_input_adapter_from_op(op, expected_adapter):
    # Test input adapter factory.
    adapter = InputAdapter.from_downstream_op(op)
    assert isinstance(adapter, expected_adapter)
    assert adapter._down_logical_op == op


@pytest.mark.parametrize(
    "op,expected_adapter",
    [
        (lazy_fixture("read_op"), BlocksOutputAdapter),
        (lazy_fixture("write_op"), BlocksOutputAdapter),
        (lazy_fixture("map_batches_op"), BatchesOutputAdapter),
        (lazy_fixture("map_rows_op"), RowsOutputAdapter),
        (lazy_fixture("filter_op"), RowsOutputAdapter),
        (lazy_fixture("flat_map_op"), RowsOutputAdapter),
    ],
)
def test_output_adapter_from_op(op, expected_adapter):
    # Test output adapter factory.
    adapter = OutputAdapter.from_upstream_op(op)
    assert isinstance(adapter, expected_adapter)
    assert adapter._up_logical_op == op


@pytest.mark.parametrize(
    "up,down,expected_adapter",
    [
        (
            lazy_fixture("read_op"),
            lazy_fixture("map_batches_op"),
            BlocksToBatchesOpAdapter,
        ),
        (
            lazy_fixture("read_op"),
            lazy_fixture("write_op"),
            BlocksToBlocksOpAdapter,
        ),
        (
            lazy_fixture("read_op"),
            lazy_fixture("map_rows_op"),
            BlocksToRowsOpAdapter,
        ),
        (
            lazy_fixture("read_op"),
            lazy_fixture("filter_op"),
            BlocksToRowsOpAdapter,
        ),
        (
            lazy_fixture("read_op"),
            lazy_fixture("flat_map_op"),
            BlocksToRowsOpAdapter,
        ),
        (
            lazy_fixture("map_batches_op"),
            lazy_fixture("write_op"),
            BatchesToBlocksOpAdapter,
        ),
        (
            lazy_fixture("map_batches_op"),
            lazy_fixture("map_batches_op"),
            BatchesToBatchesOpAdapter,
        ),
        (
            lazy_fixture("map_batches_op"),
            lazy_fixture("map_rows_op"),
            BatchesToRowsOpAdapter,
        ),
        (
            lazy_fixture("map_batches_op"),
            lazy_fixture("filter_op"),
            BatchesToRowsOpAdapter,
        ),
        (
            lazy_fixture("map_batches_op"),
            lazy_fixture("flat_map_op"),
            BatchesToRowsOpAdapter,
        ),
        (
            lazy_fixture("map_rows_op"),
            lazy_fixture("write_op"),
            RowsToBlocksOpAdapter,
        ),
        (
            lazy_fixture("map_rows_op"),
            lazy_fixture("map_batches_op"),
            RowsToBatchesOpAdapter,
        ),
        (
            lazy_fixture("flat_map_op"),
            lazy_fixture("map_batches_op"),
            RowsToBatchesOpAdapter,
        ),
        (
            lazy_fixture("filter_op"),
            lazy_fixture("map_batches_op"),
            RowsToBatchesOpAdapter,
        ),
        (
            lazy_fixture("map_rows_op"),
            lazy_fixture("filter_op"),
            RowsToRowsOpAdapter,
        ),
    ],
)
def test_op_adapter_from_ops(up, down, expected_adapter):
    # Test op adapter factory.
    adapter = OpAdapter.from_ops(up, down)
    assert isinstance(adapter, expected_adapter)
    assert adapter._up_logical_op == up
    assert adapter._down_logical_op == down
