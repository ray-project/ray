import asyncio
import itertools
import math
import os
import time
from typing import Iterator

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ray
from ray.data.context import DataContext
from ray.data.dataset import Dataset
from ray.data.exceptions import UserCodeException
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.test_util import ConcurrencyCounter  # noqa
from ray.data.tests.util import column_udf, extract_values
from ray.tests.conftest import *  # noqa


# Helper function to process timestamp data in nanoseconds
def process_timestamp_data(row):
    # Convert numpy.datetime64 to pd.Timestamp if needed
    if isinstance(row["timestamp"], np.datetime64):
        row["timestamp"] = pd.Timestamp(row["timestamp"])

    # Add 1ns to timestamp
    row["timestamp"] = row["timestamp"] + pd.Timedelta(1, "ns")

    # Ensure the timestamp column is in the expected dtype (datetime64[ns])
    row["timestamp"] = pd.to_datetime(row["timestamp"], errors="raise")

    return row


def process_timestamp_data_batch_arrow(batch: pa.Table) -> pa.Table:
    # Convert pyarrow Table to pandas DataFrame to process the timestamp column
    df = batch.to_pandas()

    df["timestamp"] = df["timestamp"].apply(
        lambda x: pd.Timestamp(x) if isinstance(x, np.datetime64) else x
    )

    # Add 1ns to timestamp
    df["timestamp"] = df["timestamp"] + pd.Timedelta(1, "ns")

    # Convert back to pyarrow Table
    return pa.table(df)


def process_timestamp_data_batch_pandas(batch: pd.DataFrame) -> pd.DataFrame:
    # Add 1ns to timestamp column
    batch["timestamp"] = batch["timestamp"] + pd.Timedelta(1, "ns")
    return batch


def test_map_batches_basic(
    ray_start_regular_shared,
    tmp_path,
    restore_data_context,
    target_max_block_size_infinite_or_default,
):
    ctx = DataContext.get_current()
    ctx.execution_options.preserve_order = True

    # Test input validation
    ds = ray.data.range(5)
    with pytest.raises(ValueError):
        ds.map_batches(
            column_udf("id", lambda x: x + 1), batch_format="pyarrow", batch_size=-1
        ).take()

    # Set up.
    df = pd.DataFrame({"one": [1, 2, 3], "two": [2, 3, 4]})
    table = pa.Table.from_pandas(df)
    pq.write_table(table, os.path.join(tmp_path, "test1.parquet"))

    # Test pandas
    ds = ray.data.read_parquet(str(tmp_path))
    ds2 = ds.map_batches(lambda df: df + 1, batch_size=1, batch_format="pandas")
    ds_list = ds2.take()
    values = [s["one"] for s in ds_list]
    assert values == [2, 3, 4]
    values = [s["two"] for s in ds_list]
    assert values == [3, 4, 5]

    # Test Pyarrow
    ds = ray.data.read_parquet(str(tmp_path))
    ds2 = ds.map_batches(lambda pa: pa, batch_size=1, batch_format="pyarrow")
    ds_list = ds2.take()
    values = [s["one"] for s in ds_list]
    assert values == [1, 2, 3]
    values = [s["two"] for s in ds_list]
    assert values == [2, 3, 4]

    # Test batch
    size = 300
    ds = ray.data.range(size)
    ds2 = ds.map_batches(lambda df: df + 1, batch_size=17, batch_format="pandas")
    ds_list = ds2.take_all()
    for i in range(size):
        # The pandas column is "value", and it originally has rows from 0~299.
        # After the map batch, it should have 1~300.
        row = ds_list[i]
        assert row["id"] == i + 1
    assert ds.count() == 300

    # Test the lambda returns different types than the batch_format
    # pandas => list block
    ds = ray.data.read_parquet(str(tmp_path))
    ds2 = ds.map_batches(lambda df: {"id": np.array([1])}, batch_size=1)
    ds_list = extract_values("id", ds2.take())
    assert ds_list == [1, 1, 1]
    assert ds.count() == 3

    # pyarrow => list block
    ds = ray.data.read_parquet(str(tmp_path))
    ds2 = ds.map_batches(
        lambda df: {"id": np.array([1])}, batch_size=1, batch_format="pyarrow"
    )
    ds_list = extract_values("id", ds2.take())
    assert ds_list == [1, 1, 1]
    assert ds.count() == 3

    # Test the wrong return value raises an exception.
    ds = ray.data.read_parquet(str(tmp_path))
    with pytest.raises(ValueError):
        ds_list = ds.map_batches(
            lambda df: 1, batch_size=2, batch_format="pyarrow"
        ).take()


def test_map_batches_extra_args(
    shutdown_only, tmp_path, target_max_block_size_infinite_or_default
):
    ray.shutdown()
    ray.init(num_cpus=3)

    def put(x):
        # We only support automatic deref in the legacy backend.
        return x

    # Test input validation
    ds = ray.data.range(5)

    class Foo:
        def __call__(self, df):
            return df

    with pytest.raises(ValueError):
        # fn_constructor_args and fn_constructor_kwargs only supported for actor
        # compute strategy.
        ds.map_batches(
            lambda x: x,
            fn_constructor_args=(1,),
            fn_constructor_kwargs={"a": 1},
        )

    with pytest.raises(ValueError):
        # fn_constructor_args and fn_constructor_kwargs only supported for callable
        # class UDFs.
        ds.map_batches(
            lambda x: x,
            fn_constructor_args=(1,),
            fn_constructor_kwargs={"a": 1},
        )

    # Set up.
    df = pd.DataFrame({"one": [1, 2, 3], "two": [2, 3, 4]})
    table = pa.Table.from_pandas(df)
    pq.write_table(table, os.path.join(tmp_path, "test1.parquet"))

    # Test extra UDF args.
    # Test positional.
    def udf(batch, a):
        assert a == 1
        return batch + a

    ds = ray.data.read_parquet(str(tmp_path))
    ds2 = ds.map_batches(
        udf,
        batch_size=1,
        batch_format="pandas",
        fn_args=(put(1),),
    )
    ds_list = ds2.take()
    values = sorted([s["one"] for s in ds_list])
    assert values == [2, 3, 4]
    values = sorted([s["two"] for s in ds_list])
    assert values == [3, 4, 5]

    # Test kwargs.
    def udf(batch, b=None):
        assert b == 2
        return b * batch

    ds = ray.data.read_parquet(str(tmp_path))
    ds2 = ds.map_batches(
        udf,
        batch_size=1,
        batch_format="pandas",
        fn_kwargs={"b": put(2)},
    )
    ds_list = ds2.take()
    values = sorted([s["one"] for s in ds_list])
    assert values == [2, 4, 6]
    values = sorted([s["two"] for s in ds_list])
    assert values == [4, 6, 8]

    # Test both.
    def udf(batch, a, b=None):
        assert a == 1
        assert b == 2
        return b * batch + a

    ds = ray.data.read_parquet(str(tmp_path))
    ds2 = ds.map_batches(
        udf,
        batch_size=1,
        batch_format="pandas",
        fn_args=(put(1),),
        fn_kwargs={"b": put(2)},
    )
    ds_list = ds2.take()
    values = sorted([s["one"] for s in ds_list])
    assert values == [3, 5, 7]
    values = sorted([s["two"] for s in ds_list])
    assert values == [5, 7, 9]

    # Test constructor UDF args.
    # Test positional.
    class CallableFn:
        def __init__(self, a):
            assert a == 1
            self.a = a

        def __call__(self, x):
            return x + self.a

    ds = ray.data.read_parquet(str(tmp_path))
    ds2 = ds.map_batches(
        CallableFn,
        concurrency=1,
        batch_size=1,
        batch_format="pandas",
        fn_constructor_args=(put(1),),
    )
    ds_list = ds2.take()
    values = sorted([s["one"] for s in ds_list])
    assert values == [2, 3, 4]
    values = sorted([s["two"] for s in ds_list])
    assert values == [3, 4, 5]

    # Test kwarg.
    class CallableFn:
        def __init__(self, b=None):
            assert b == 2
            self.b = b

        def __call__(self, x):
            return self.b * x

    ds = ray.data.read_parquet(str(tmp_path))
    ds2 = ds.map_batches(
        CallableFn,
        concurrency=1,
        batch_size=1,
        batch_format="pandas",
        fn_constructor_kwargs={"b": put(2)},
    )
    ds_list = ds2.take()
    values = sorted([s["one"] for s in ds_list])
    assert values == [2, 4, 6]
    values = sorted([s["two"] for s in ds_list])
    assert values == [4, 6, 8]

    # Test both.
    class CallableFn:
        def __init__(self, a, b=None):
            assert a == 1
            assert b == 2
            self.a = a
            self.b = b

        def __call__(self, x):
            return self.b * x + self.a

    ds = ray.data.read_parquet(str(tmp_path))
    ds2 = ds.map_batches(
        CallableFn,
        concurrency=1,
        batch_size=1,
        batch_format="pandas",
        fn_constructor_args=(put(1),),
        fn_constructor_kwargs={"b": put(2)},
    )
    ds_list = ds2.take()
    values = sorted([s["one"] for s in ds_list])
    assert values == [3, 5, 7]
    values = sorted([s["two"] for s in ds_list])
    assert values == [5, 7, 9]

    # Test callable chain.
    ds = ray.data.read_parquet(str(tmp_path))
    fn_constructor_args = (put(1),)
    fn_constructor_kwargs = {"b": put(2)}
    ds2 = ds.map_batches(
        CallableFn,
        concurrency=1,
        batch_size=1,
        batch_format="pandas",
        fn_constructor_args=fn_constructor_args,
        fn_constructor_kwargs=fn_constructor_kwargs,
    ).map_batches(
        CallableFn,
        concurrency=1,
        batch_size=1,
        batch_format="pandas",
        fn_constructor_args=fn_constructor_args,
        fn_constructor_kwargs=fn_constructor_kwargs,
    )
    ds_list = ds2.take()
    values = sorted([s["one"] for s in ds_list])
    assert values == [7, 11, 15]
    values = sorted([s["two"] for s in ds_list])
    assert values == [11, 15, 19]

    # Test function + callable chain.
    ds = ray.data.read_parquet(str(tmp_path))
    fn_constructor_args = (put(1),)
    fn_constructor_kwargs = {"b": put(2)}
    ds2 = ds.map_batches(
        lambda df, a, b=None: b * df + a,
        batch_size=1,
        batch_format="pandas",
        fn_args=(put(1),),
        fn_kwargs={"b": put(2)},
    ).map_batches(
        CallableFn,
        concurrency=1,
        batch_size=1,
        batch_format="pandas",
        fn_constructor_args=fn_constructor_args,
        fn_constructor_kwargs=fn_constructor_kwargs,
    )
    ds_list = ds2.take()
    values = sorted([s["one"] for s in ds_list])
    assert values == [7, 11, 15]
    values = sorted([s["two"] for s in ds_list])
    assert values == [11, 15, 19]


@pytest.mark.parametrize("method", [Dataset.map, Dataset.map_batches, Dataset.flat_map])
def test_map_with_memory_resources(
    method, shutdown_only, target_max_block_size_infinite_or_default
):
    """Test that we can use memory resource to limit the concurrency."""
    num_blocks = 50
    memory_per_task = 100 * 1024**2
    max_concurrency = 5
    ray.init(num_cpus=num_blocks, _memory=memory_per_task * max_concurrency)

    concurrency_counter = ConcurrencyCounter.remote()

    def map_fn(row_or_batch):
        ray.get(concurrency_counter.inc.remote())
        time.sleep(0.5)
        ray.get(concurrency_counter.decr.remote())
        if method is Dataset.flat_map:
            return [row_or_batch]
        else:
            return row_or_batch

    ds = ray.data.range(num_blocks, override_num_blocks=num_blocks)
    if method is Dataset.map:
        ds = ds.map(
            map_fn,
            num_cpus=1,
            memory=memory_per_task,
        )
    elif method is Dataset.map_batches:
        ds = ds.map_batches(
            map_fn,
            batch_size=None,
            num_cpus=1,
            memory=memory_per_task,
        )
    elif method is Dataset.flat_map:
        ds = ds.flat_map(
            map_fn,
            num_cpus=1,
            memory=memory_per_task,
        )
    assert len(ds.take(num_blocks)) == num_blocks

    actual_max_concurrency = ray.get(concurrency_counter.get_max_concurrency.remote())
    assert actual_max_concurrency <= max_concurrency


def test_map_batches_generator(
    ray_start_regular_shared, tmp_path, target_max_block_size_infinite_or_default
):
    # Set up.
    df = pd.DataFrame({"one": [1, 2, 3], "two": [2, 3, 4]})
    table = pa.Table.from_pandas(df)
    pq.write_table(table, os.path.join(tmp_path, "test1.parquet"))

    def pandas_generator(batch: pd.DataFrame) -> Iterator[pd.DataFrame]:
        for i in range(len(batch)):
            yield batch.iloc[[i]] + 1

    ds = ray.data.read_parquet(str(tmp_path))
    ds2 = ds.map_batches(pandas_generator, batch_size=1, batch_format="pandas")
    ds_list = ds2.take()
    values = sorted([s["one"] for s in ds_list])
    assert values == [2, 3, 4]
    values = sorted([s["two"] for s in ds_list])
    assert values == [3, 4, 5]

    def fail_generator(batch):
        for i in range(len(batch)):
            yield i

    # Test the wrong return value raises an exception.
    ds = ray.data.read_parquet(str(tmp_path))
    with pytest.raises(ValueError):
        ds_list = ds.map_batches(
            fail_generator, batch_size=2, batch_format="pyarrow"
        ).take()


def test_map_batches_actors_preserves_order(
    shutdown_only, target_max_block_size_infinite_or_default
):
    class UDFClass:
        def __call__(self, x):
            return x

    ray.shutdown()
    ray.init(num_cpus=2)
    # Test that actor compute model preserves block order.
    ds = ray.data.range(10, override_num_blocks=5)
    assert extract_values("id", ds.map_batches(UDFClass, concurrency=1).take()) == list(
        range(10)
    )


@pytest.mark.parametrize(
    "num_rows,num_blocks,batch_size",
    [
        (10, 5, 2),
        (10, 1, 10),
        (12, 3, 2),
    ],
)
def test_map_batches_batch_mutation(
    ray_start_regular_shared,
    num_rows,
    num_blocks,
    batch_size,
    restore_data_context,
    target_max_block_size_infinite_or_default,
):
    ctx = DataContext.get_current()
    ctx.execution_options.preserve_order = True

    # Test that batch mutation works without encountering a read-only error (e.g. if the
    # batch is a zero-copy view on data in the object store).
    def mutate(df):
        df["id"] += 1
        return df

    ds = ray.data.range(num_rows, override_num_blocks=num_blocks).repartition(
        num_blocks
    )
    # Convert to Pandas blocks.
    ds = ds.map_batches(lambda df: df, batch_format="pandas", batch_size=None)

    # Apply UDF that mutates the batches.
    ds = ds.map_batches(mutate, batch_size=batch_size, zero_copy_batch=False)
    assert [row["id"] for row in ds.iter_rows()] == list(range(1, num_rows + 1))


@pytest.mark.parametrize(
    "num_rows,num_blocks,batch_size",
    [
        (10, 5, 2),
        (10, 1, 10),
        (12, 3, 2),
    ],
)
def test_map_batches_batch_zero_copy(
    ray_start_regular_shared,
    num_rows,
    num_blocks,
    batch_size,
    target_max_block_size_infinite_or_default,
):
    # Test that batches are zero-copy read-only views when zero_copy_batch=True.
    def mutate(df):
        # Check that batch is read-only.
        assert not df.values.flags.writeable
        df["id"] += 1
        return df

    ds = ray.data.range(num_rows, override_num_blocks=num_blocks).repartition(
        num_blocks
    )
    # Convert to Pandas blocks.
    ds = ds.map_batches(lambda df: df, batch_format="pandas", batch_size=None)
    ds = ds.materialize()

    # Apply UDF that mutates the batches, which should fail since the batch is
    # read-only.
    with pytest.raises(UserCodeException):
        with pytest.raises(
            ValueError, match="tried to mutate a zero-copy read-only batch"
        ):
            ds = ds.map_batches(
                mutate,
                batch_format="pandas",
                batch_size=batch_size,
                zero_copy_batch=True,
            )
            ds.materialize()


BLOCK_BUNDLING_TEST_CASES = [
    (block_size, batch_size)
    for batch_size in range(1, 8)
    for block_size in range(1, 2 * batch_size + 1)
]


@pytest.mark.parametrize("block_size,batch_size", BLOCK_BUNDLING_TEST_CASES)
def test_map_batches_block_bundling_auto(
    ray_start_regular_shared,
    block_size,
    batch_size,
    target_max_block_size_infinite_or_default,
):
    # Ensure that we test at least 2 batches worth of blocks.
    num_blocks = max(10, 2 * batch_size // block_size)
    ds = ray.data.range(num_blocks * block_size, override_num_blocks=num_blocks)
    # Confirm that we have the expected number of initial blocks.
    assert ds._plan.initial_num_blocks() == num_blocks

    # Blocks should be bundled up to the batch size.
    ds1 = ds.map_batches(lambda x: x, batch_size=batch_size).materialize()

    num_expected_blocks = math.ceil(
        # If batch_size > block_size, then multiple blocks will be clumped
        # together to make sure there are at least batch_size rows
        num_blocks
        / max(math.ceil(batch_size / block_size), 1)
    )

    assert ds1._plan.initial_num_blocks() == num_expected_blocks

    # Blocks should not be bundled up when batch_size is not specified.
    ds2 = ds.map_batches(lambda x: x).materialize()
    assert ds2._plan.initial_num_blocks() == num_blocks


@pytest.mark.parametrize(
    "block_sizes,batch_size,expected_num_blocks",
    [
        ([1, 2], 3, 1),
        ([2, 2, 1], 3, 2),
        ([1, 2, 3, 4], 4, 2),
        ([3, 1, 1, 3], 4, 2),
        ([2, 4, 1, 8], 4, 2),
        ([1, 1, 1, 1], 4, 1),
        ([1, 0, 3, 2], 4, 2),
        ([4, 4, 4, 4], 4, 4),
    ],
)
def test_map_batches_block_bundling_skewed_manual(
    ray_start_regular_shared,
    block_sizes,
    batch_size,
    expected_num_blocks,
    target_max_block_size_infinite_or_default,
):
    num_blocks = len(block_sizes)
    ds = ray.data.from_blocks(
        [pd.DataFrame({"a": [1] * block_size}) for block_size in block_sizes]
    )
    # Confirm that we have the expected number of initial blocks.
    assert ds._plan.initial_num_blocks() == num_blocks
    ds = ds.map_batches(lambda x: x, batch_size=batch_size).materialize()

    # Blocks should be bundled up to the batch size.
    assert ds._plan.initial_num_blocks() == expected_num_blocks


BLOCK_BUNDLING_SKEWED_TEST_CASES = [
    (block_sizes, batch_size)
    for batch_size in range(1, 4)
    for num_blocks in range(1, batch_size + 1)
    for block_sizes in itertools.product(
        range(1, 2 * batch_size + 1), repeat=num_blocks
    )
]


@pytest.mark.parametrize("block_sizes,batch_size", BLOCK_BUNDLING_SKEWED_TEST_CASES)
def test_map_batches_block_bundling_skewed_auto(
    ray_start_regular_shared,
    block_sizes,
    batch_size,
    target_max_block_size_infinite_or_default,
):
    num_blocks = len(block_sizes)
    ds = ray.data.from_blocks(
        [pd.DataFrame({"a": [1] * block_size}) for block_size in block_sizes]
    )
    # Confirm that we have the expected number of initial blocks.
    assert ds._plan.initial_num_blocks() == num_blocks
    ds = ds.map_batches(lambda x: x, batch_size=batch_size).materialize()

    curr = 0
    num_out_blocks = 0
    for block_size in block_sizes:
        if curr >= batch_size:
            num_out_blocks += 1
            curr = 0
        curr += block_size
    if curr > 0:
        num_out_blocks += 1

    # Blocks should be bundled up to the batch size.
    assert ds._plan.initial_num_blocks() == num_out_blocks


def test_map_batches_preserve_empty_blocks(
    ray_start_regular_shared, target_max_block_size_infinite_or_default
):
    ds = ray.data.range(10, override_num_blocks=10)
    ds = ds.map_batches(lambda x: [])
    ds = ds.map_batches(lambda x: x)
    assert ds._plan.initial_num_blocks() == 10, ds


def test_map_batches_combine_empty_blocks(
    ray_start_regular_shared, target_max_block_size_infinite_or_default
):
    xs = [x % 3 for x in list(range(100))]

    # ds1 has 1 block which contains 100 rows.
    ds1 = ray.data.from_items(xs).repartition(1).sort("item").map_batches(lambda x: x)
    assert ds1._block_num_rows() == [100]

    # ds2 has 30 blocks, but only 3 of them are non-empty
    ds2 = (
        ray.data.from_items(xs)
        .repartition(30)
        .sort("item")
        .map_batches(lambda x: x, batch_size=1)
    )
    assert len(ds2._block_num_rows()) == 3
    count = sum(1 for x in ds2._block_num_rows() if x > 0)
    assert count == 3

    # The number of partitions should not affect the map_batches() result.
    assert ds1.take_all() == ds2.take_all()


# NOTE: All tests above share a Ray cluster, while the tests below do not. These
# tests should only be carefully reordered to retain this invariant!


@pytest.mark.parametrize(
    "df, expected_df",
    [
        pytest.param(
            pd.DataFrame(
                {
                    "id": [1, 2, 3],
                    "timestamp": pd.to_datetime(
                        [
                            "2024-01-01 00:00:00.123456789",
                            "2024-01-02 00:00:00.987654321",
                            "2024-01-03 00:00:00.111222333",
                        ]
                    ),
                    "value": [10.123456789, 20.987654321, 30.111222333],
                }
            ),
            pd.DataFrame(
                {
                    "id": [1, 2, 3],
                    "timestamp": pd.to_datetime(
                        [
                            "2024-01-01 00:00:00.123456790",
                            "2024-01-02 00:00:00.987654322",
                            "2024-01-03 00:00:00.111222334",
                        ]
                    ),
                    "value": [10.123456789, 20.987654321, 30.111222333],
                }
            ),
            id="nanoseconds_increment",
        )
    ],
)
def test_map_batches_timestamp_nanosecs(
    df, expected_df, ray_start_regular_shared, target_max_block_size_infinite_or_default
):
    """Verify handling timestamp with nanosecs in map_batches"""
    ray_data = ray.data.from_pandas(df)

    # Using pyarrow format
    result_arrow = ray_data.map_batches(
        process_timestamp_data_batch_arrow, batch_format="pyarrow"
    )
    processed_df_arrow = result_arrow.to_pandas()
    processed_df_arrow["timestamp"] = processed_df_arrow["timestamp"].astype(
        "datetime64[ns]"
    )
    pd.testing.assert_frame_equal(processed_df_arrow, expected_df)

    # Using pandas format
    result_pandas = ray_data.map_batches(
        process_timestamp_data_batch_pandas, batch_format="pandas"
    )
    processed_df_pandas = result_pandas.to_pandas()
    processed_df_pandas["timestamp"] = processed_df_pandas["timestamp"].astype(
        "datetime64[ns]"
    )
    pd.testing.assert_frame_equal(processed_df_pandas, expected_df)


def test_map_batches_async_exception_propagation(shutdown_only):
    ray.shutdown()
    ray.init(num_cpus=2)

    class MyUDF:
        def __init__(self):
            pass

        async def __call__(self, batch):
            # This will trigger an assertion error.
            assert False
            yield batch

    ds = ray.data.range(20)
    ds = ds.map_batches(MyUDF, concurrency=2)

    with pytest.raises(ray.exceptions.RayTaskError) as exc_info:
        ds.materialize()

    assert "AssertionError" in str(exc_info.value)
    assert "assert False" in str(exc_info.value)


def test_map_batches_async_generator_fast_yield(
    shutdown_only, target_max_block_size_infinite_or_default
):
    # Tests the case where the async generator yields immediately,
    # with a high number of tasks in flight, which results in
    # the internal queue being almost instantaneously filled.
    # This test ensures that the internal queue is completely drained in this scenario.

    ray.shutdown()
    ray.init(num_cpus=4)

    async def task_yield(row):
        return row

    class AsyncActor:
        def __init__(self):
            pass

        async def __call__(self, batch):
            rows = [{"id": np.array([i])} for i in batch["id"]]
            tasks = [asyncio.create_task(task_yield(row)) for row in rows]
            for task in tasks:
                yield await task

    n = 8
    ds = ray.data.range(n, override_num_blocks=n)
    ds = ds.map_batches(
        AsyncActor,
        batch_size=n,
        compute=ray.data.ActorPoolStrategy(size=1, max_tasks_in_flight_per_actor=n),
        concurrency=1,
        max_concurrency=n,
    )

    output = ds.take_all()
    expected_output = [{"id": i} for i in range(n)]
    # Because all tasks are submitted almost simultaneously,
    # the output order may be different compared to the original input.
    assert len(output) == len(expected_output), (len(output), len(expected_output))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
