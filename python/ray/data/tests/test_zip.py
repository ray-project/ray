import itertools

import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data import Schema
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.util import column_udf, named_values
from ray.tests.conftest import *  # noqa


@pytest.mark.parametrize("num_datasets", [2, 3, 4, 5, 10])
def test_zip_multiple_datasets(ray_start_regular_shared, num_datasets):
    # Create multiple datasets with different transformations
    datasets = []
    for i in range(num_datasets):
        ds = ray.data.range(5, override_num_blocks=5)
        if i > 0:  # Apply transformation to all but the first dataset
            ds = ds.map(column_udf("id", lambda x, offset=i: x + offset))
        datasets.append(ds)

    ds = datasets[0].zip(*datasets[1:])

    # Verify schema names
    expected_names = ["id"] + [f"id_{i}" for i in range(1, num_datasets)]
    assert ds.schema().names == expected_names

    # Verify data
    expected_data = []
    for row_idx in range(5):
        row_data = tuple(row_idx + i for i in range(num_datasets))
        expected_data.append(row_data)

    assert ds.take() == named_values(expected_names, expected_data)


@pytest.mark.parametrize(
    "num_blocks1,num_blocks2",
    list(itertools.combinations_with_replacement([1, 2, 4, 16], 2)),
)
def test_zip_different_num_blocks_combinations(
    ray_start_regular_shared, num_blocks1, num_blocks2
):
    n = 12
    ds1 = ray.data.range(n, override_num_blocks=num_blocks1)
    ds2 = ray.data.range(n, override_num_blocks=num_blocks2).map(
        column_udf("id", lambda x: x + 1)
    )
    ds = ds1.zip(ds2)
    assert ds.schema().names == ["id", "id_1"]
    assert ds.take() == named_values(
        ["id", "id_1"], list(zip(range(n), range(1, n + 1)))
    )


def test_zip_pandas(ray_start_regular_shared):
    ds1 = ray.data.from_pandas(pd.DataFrame({"col1": [1, 2], "col2": [4, 5]}))
    ds2 = ray.data.from_pandas(pd.DataFrame({"col3": ["a", "b"], "col4": ["d", "e"]}))
    ds = ds1.zip(ds2)
    assert ds.count() == 2
    result = list(ds.take())
    assert result[0] == {"col1": 1, "col2": 4, "col3": "a", "col4": "d"}

    ds3 = ray.data.from_pandas(pd.DataFrame({"col2": ["a", "b"], "col4": ["d", "e"]}))
    ds = ds1.zip(ds3)
    assert ds.count() == 2
    result = list(ds.take())
    assert result[0] == {"col1": 1, "col2": 4, "col2_1": "a", "col4": "d"}


def test_zip_arrow(ray_start_regular_shared):
    ds1 = ray.data.range(5).map(lambda r: {"id": r["id"]})
    ds2 = ray.data.range(5).map(lambda r: {"a": r["id"] + 1, "b": r["id"] + 2})
    ds = ds1.zip(ds2)
    assert ds.count() == 5
    assert ds.schema() == Schema(
        pa.schema([("id", pa.int64()), ("a", pa.int64()), ("b", pa.int64())])
    )
    result = list(ds.take())
    assert result[0] == {"id": 0, "a": 1, "b": 2}

    # Test duplicate column names.
    ds = ds1.zip(ds1).zip(ds1)
    assert ds.count() == 5
    assert ds.schema() == Schema(
        pa.schema([("id", pa.int64()), ("id_1", pa.int64()), ("id_2", pa.int64())])
    )
    result = list(ds.take())
    assert result[0] == {"id": 0, "id_1": 0, "id_2": 0}


def test_zip_multiple_block_types(ray_start_regular_shared):
    df = pd.DataFrame({"spam": [0]})
    ds_pd = ray.data.from_pandas(df)
    ds2_arrow = ray.data.from_items([{"ham": [0]}])
    assert ds_pd.zip(ds2_arrow).take_all() == [{"spam": 0, "ham": [0]}]


def test_zip_preserve_order(ray_start_regular_shared):
    def foo(x):
        import time

        if x["item"] < 5:
            time.sleep(1)
        return x

    num_items = 10
    items = list(range(num_items))
    ds1 = ray.data.from_items(items, override_num_blocks=num_items)
    ds2 = ray.data.from_items(items, override_num_blocks=num_items)
    ds2 = ds2.map_batches(foo, batch_size=1)
    result = ds1.zip(ds2).take_all()
    assert result == named_values(
        ["item", "item_1"], list(zip(range(num_items), range(num_items)))
    ), result


def test_zip_does_not_free_shared_materialized_blocks(ray_start_regular_shared):
    """Regression test: ZipOperator should not free blocks from a materialized
    dataset that is shared with another consumer.

    Previously, ZipOperator._zip() called _split_at_indices() without specifying
    owned_by_consumer, which defaulted to True. This caused ray.internal.free()
    to be called on blocks that were shared with other operators in the DAG,
    leading to ObjectFreedError.
    """
    # Create a dataset with 3 blocks (rows [7, 7, 6]) and materialize it.
    # The materialized blocks have owns_blocks=False.
    ds = ray.data.range(20, override_num_blocks=3).materialize()
    assert not ds._execute().owns_blocks

    # Consumer 1: a map_batches that uses the same materialized dataset.
    mapped_ds = ds.map_batches(lambda batch: batch, batch_format="pandas")

    # Consumer 2: zip the same materialized dataset with another dataset.
    # This triggers _split_at_indices inside ZipOperator._zip().
    # Use 2 blocks (rows [10, 10]) so that block boundaries are NOT aligned
    # with ds's blocks (rows [7, 7, 6]). This forces actual block splitting
    # (e.g., the first 10-row block gets split at row 7), which exercises
    # the owned_by_consumer code path in _split_all_blocks.
    other_ds = ray.data.range(20, override_num_blocks=2)
    zipped = other_ds.zip(ds)

    # Consuming the zipped result should not raise ObjectFreedError.
    result = zipped.take_all()
    assert len(result) == 20

    # The mapped_ds should also work fine (blocks not freed by the zip).
    result2 = mapped_ds.take_all()
    assert len(result2) == 20


def test_zip_streaming_dispatches_before_inputs_done(ray_start_regular_shared):
    """Verify that ZipOperator submits zip tasks as blocks arrive, rather than
    waiting until all inputs are done (which the old bulk operator did)."""
    from ray.data._internal.execution.interfaces import ExecutionOptions
    from ray.data._internal.execution.interfaces.physical_operator import DataOpTask
    from ray.data._internal.execution.operators.input_data_buffer import (
        InputDataBuffer,
    )
    from ray.data._internal.execution.operators.zip_operator import ZipOperator
    from ray.data._internal.execution.util import make_ref_bundles
    from ray.data.block import BlockAccessor
    from ray.data.context import DataContext
    from ray.data.tests.util import run_op_tasks_sync

    ctx = DataContext.get_current()

    # Two inputs, each with 3 blocks of 2 rows.
    bundles_a = make_ref_bundles([[0, 1], [2, 3], [4, 5]])
    bundles_b = make_ref_bundles([[10, 11], [12, 13], [14, 15]])

    input_a = InputDataBuffer(ctx, bundles_a)
    input_b = InputDataBuffer(ctx, bundles_b)
    zip_op = ZipOperator(ctx, input_a, input_b)

    zip_op.start(ExecutionOptions())
    input_a.start(ExecutionOptions())
    input_b.start(ExecutionOptions())

    # Feed one block from each input — this is enough to align and dispatch a
    # zip task, well before all inputs are done.
    zip_op.add_input(input_a.get_next(), 0)
    zip_op.add_input(input_b.get_next(), 1)

    active = zip_op.get_active_tasks()
    assert (
        len(active) >= 1
    ), "Streaming zip should dispatch a task before all inputs done"
    assert all(isinstance(t, DataOpTask) for t in active)

    # Feed remaining blocks.
    while input_a.has_next():
        zip_op.add_input(input_a.get_next(), 0)
    while input_b.has_next():
        zip_op.add_input(input_b.get_next(), 1)

    zip_op.input_done(0)
    zip_op.input_done(1)
    zip_op.all_inputs_done()

    # Drive the async zip tasks to completion, then drain and verify order.
    run_op_tasks_sync(zip_op)
    assert zip_op.num_active_tasks() == 0

    results = []
    while zip_op.has_next():
        bundle = zip_op.get_next()
        for block_ref in bundle.block_refs:
            df = BlockAccessor.for_block(ray.get(block_ref)).to_pandas()
            results.extend(df["id"].tolist())

    assert results == [0, 1, 2, 3, 4, 5]
    assert zip_op.has_completed()


def test_zip_streaming_with_unaligned_blocks(ray_start_regular_shared):
    """Verify that streaming zip correctly aligns blocks with different row counts
    using the split-and-leftover mechanism."""
    from ray.data._internal.execution.interfaces import ExecutionOptions
    from ray.data._internal.execution.operators.input_data_buffer import (
        InputDataBuffer,
    )
    from ray.data._internal.execution.operators.zip_operator import ZipOperator
    from ray.data._internal.execution.util import make_ref_bundles
    from ray.data.context import DataContext

    ctx = DataContext.get_current()

    # Input A: 3 blocks of [3, 3, 3] rows = 9 rows
    # Input B: 2 blocks of [5, 4] rows = 9 rows
    # Block boundaries don't align — tests the split/leftover logic.
    bundles_a = make_ref_bundles([[0, 1, 2], [3, 4, 5], [6, 7, 8]])
    bundles_b = make_ref_bundles([[10, 11, 12, 13, 14], [15, 16, 17, 18]])

    input_a = InputDataBuffer(ctx, bundles_a)
    input_b = InputDataBuffer(ctx, bundles_b)
    zip_op = ZipOperator(ctx, input_a, input_b)

    zip_op.start(ExecutionOptions())
    input_a.start(ExecutionOptions())
    input_b.start(ExecutionOptions())

    # Feed all blocks.
    while input_a.has_next():
        zip_op.add_input(input_a.get_next(), 0)
    while input_b.has_next():
        zip_op.add_input(input_b.get_next(), 1)

    zip_op.input_done(0)
    zip_op.input_done(1)
    zip_op.all_inputs_done()

    # Drive the async zip tasks to completion, then collect all output rows.
    from ray.data.block import BlockAccessor
    from ray.data.tests.util import run_op_tasks_sync

    run_op_tasks_sync(zip_op)

    id_values = []
    id_1_values = []
    while zip_op.has_next():
        bundle = zip_op.get_next()
        for block_ref in bundle.block_refs:
            df = BlockAccessor.for_block(ray.get(block_ref)).to_pandas()
            id_values.extend(df["id"].tolist())
            id_1_values.extend(df["id_1"].tolist())

    assert id_values == list(range(9))
    assert id_1_values == list(range(10, 19))


def test_zip_streaming_different_row_counts_raises(ray_start_regular_shared):
    """Verify that zipping datasets with different total row counts raises
    ValueError at all_inputs_done."""
    from ray.data._internal.execution.interfaces import ExecutionOptions
    from ray.data._internal.execution.operators.input_data_buffer import (
        InputDataBuffer,
    )
    from ray.data._internal.execution.operators.zip_operator import ZipOperator
    from ray.data._internal.execution.util import make_ref_bundles
    from ray.data.context import DataContext

    ctx = DataContext.get_current()

    bundles_a = make_ref_bundles([[0, 1, 2]])  # 3 rows
    bundles_b = make_ref_bundles([[10, 11]])  # 2 rows

    input_a = InputDataBuffer(ctx, bundles_a)
    input_b = InputDataBuffer(ctx, bundles_b)
    zip_op = ZipOperator(ctx, input_a, input_b)

    zip_op.start(ExecutionOptions())
    input_a.start(ExecutionOptions())
    input_b.start(ExecutionOptions())

    zip_op.add_input(input_a.get_next(), 0)
    zip_op.add_input(input_b.get_next(), 1)

    zip_op.input_done(0)
    zip_op.input_done(1)

    with pytest.raises(ValueError, match="different number of rows"):
        zip_op.all_inputs_done()


def test_zip_streaming_unknown_row_count_resolved_async(ray_start_regular_shared):
    """Verify that when a block's metadata lacks num_rows, ZipOperator fetches it
    via an async MetadataOpTask instead of blocking, and still zips correctly."""
    import pyarrow as pa

    from ray.data._internal.execution.interfaces import (
        BlockEntry,
        ExecutionOptions,
        RefBundle,
    )
    from ray.data._internal.execution.interfaces.physical_operator import (
        MetadataOpTask,
    )
    from ray.data._internal.execution.operators.input_data_buffer import (
        InputDataBuffer,
    )
    from ray.data._internal.execution.operators.zip_operator import ZipOperator
    from ray.data.block import BlockAccessor, BlockMetadata
    from ray.data.context import DataContext
    from ray.data.tests.util import run_op_tasks_sync

    ctx = DataContext.get_current()

    def bundle_with_unknown_rows(values):
        block = pd.DataFrame({"id": values})
        # Build metadata with num_rows=None to exercise the async fetch path.
        metadata = BlockMetadata(
            num_rows=None,
            size_bytes=None,
            exec_stats=None,
            input_files=None,
        )
        return RefBundle(
            [BlockEntry(ray.put(block), metadata)],
            owns_blocks=True,
            schema=pa.lib.Schema.from_pandas(block, preserve_index=False),
        )

    input_a = InputDataBuffer(ctx, [bundle_with_unknown_rows([0, 1, 2])])
    input_b = InputDataBuffer(ctx, [bundle_with_unknown_rows([10, 11, 12])])
    zip_op = ZipOperator(ctx, input_a, input_b)

    zip_op.start(ExecutionOptions())
    input_a.start(ExecutionOptions())
    input_b.start(ExecutionOptions())

    zip_op.add_input(input_a.get_next(), 0)
    zip_op.add_input(input_b.get_next(), 1)

    # With unknown row counts, the operator must defer to async row-count tasks
    # rather than block — so a MetadataOpTask should be active and no output yet.
    active = zip_op.get_active_tasks()
    assert any(isinstance(t, MetadataOpTask) for t in active), active
    assert not zip_op.has_next()

    zip_op.input_done(0)
    zip_op.input_done(1)
    zip_op.all_inputs_done()

    # Drive row-count tasks and the subsequent zip task to completion.
    run_op_tasks_sync(zip_op)
    assert zip_op.num_active_tasks() == 0

    id_values = []
    id_1_values = []
    while zip_op.has_next():
        bundle = zip_op.get_next()
        for block_ref in bundle.block_refs:
            df = BlockAccessor.for_block(ray.get(block_ref)).to_pandas()
            id_values.extend(df["id"].tolist())
            id_1_values.extend(df["id_1"].tolist())

    assert id_values == [0, 1, 2]
    assert id_1_values == [10, 11, 12]


def test_zip_streaming_throttling_enabled(ray_start_regular_shared):
    """Verify that the streaming zip operator has throttling enabled
    (backpressure support)."""
    from ray.data._internal.execution.operators.input_data_buffer import (
        InputDataBuffer,
    )
    from ray.data._internal.execution.operators.zip_operator import ZipOperator
    from ray.data._internal.execution.util import make_ref_bundles
    from ray.data.context import DataContext

    ctx = DataContext.get_current()

    input_a = InputDataBuffer(ctx, make_ref_bundles([[1]]))
    input_b = InputDataBuffer(ctx, make_ref_bundles([[2]]))
    zip_op = ZipOperator(ctx, input_a, input_b)

    assert not zip_op.throttling_disabled()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
