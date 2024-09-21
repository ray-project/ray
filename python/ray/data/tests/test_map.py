import asyncio
import itertools
import math
import os
import threading
import time
from typing import Iterator

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ray
from ray._private.test_utils import wait_for_condition
from ray.data._internal.execution.interfaces.ref_bundle import (
    _ref_bundles_iterator_to_block_refs_list,
)
from ray.data._internal.execution.operators.actor_pool_map_operator import _MapWorker
from ray.data.context import DataContext
from ray.data.exceptions import UserCodeException
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.test_util import ConcurrencyCounter  # noqa
from ray.data.tests.util import column_udf, column_udf_class, extract_values
from ray.tests.conftest import *  # noqa


def test_basic_actors(shutdown_only):
    ray.init(num_cpus=6)
    n = 5
    ds = ray.data.range(n)
    assert sorted(
        extract_values(
            "id",
            ds.map(
                column_udf_class("id", lambda x: x + 1),
                concurrency=1,
            ).take(),
        )
    ) == list(range(1, n + 1))

    # Should still work even if num actors > num cpus.
    ds = ray.data.range(n)
    assert sorted(
        extract_values(
            "id",
            ds.map(
                column_udf_class("id", lambda x: x + 1),
                concurrency=4,
            ).take(),
        )
    ) == list(range(1, n + 1))

    # Test setting custom max inflight tasks.
    ds = ray.data.range(10, override_num_blocks=5)
    assert sorted(
        extract_values(
            "id",
            ds.map(
                column_udf_class("id", lambda x: x + 1),
                compute=ray.data.ActorPoolStrategy(max_tasks_in_flight_per_actor=3),
            ).take(),
        )
    ) == list(range(1, 11))

    # Test invalid max tasks inflight arg.
    with pytest.raises(ValueError):
        ray.data.range(10).map(
            column_udf_class("id", lambda x: x),
            compute=ray.data.ActorPoolStrategy(max_tasks_in_flight_per_actor=0),
        )

    # Test min no more than max check.
    with pytest.raises(ValueError):
        ray.data.range(10).map(
            column_udf_class("id", lambda x: x),
            concurrency=(8, 4),
        )

    # Make sure all actors are dead after dataset execution finishes.
    def _all_actors_dead():
        actor_table = ray.state.actors()
        actors = {
            id: actor_info
            for actor_info in actor_table.values()
            if actor_info["ActorClassName"] == _MapWorker.__name__
        }
        assert len(actors) > 0
        return all(actor_info["State"] == "DEAD" for actor_info in actors.values())

    wait_for_condition(_all_actors_dead)


def test_callable_classes(shutdown_only):
    ray.init(num_cpus=2)
    ds = ray.data.range(10, override_num_blocks=10)

    class StatefulFn:
        def __init__(self):
            self.num_reuses = 0

        def __call__(self, x):
            r = self.num_reuses
            self.num_reuses += 1
            return {"id": np.array([r])}

    # map
    actor_reuse = ds.map(StatefulFn, concurrency=1).take()
    assert sorted(extract_values("id", actor_reuse)) == list(range(10)), actor_reuse

    class StatefulFn:
        def __init__(self):
            self.num_reuses = 0

        def __call__(self, x):
            r = self.num_reuses
            self.num_reuses += 1
            return [{"id": r}]

    # flat map
    actor_reuse = extract_values("id", ds.flat_map(StatefulFn, concurrency=1).take())
    assert sorted(actor_reuse) == list(range(10)), actor_reuse

    class StatefulFn:
        def __init__(self):
            self.num_reuses = 0

        def __call__(self, x):
            r = self.num_reuses
            self.num_reuses += 1
            return {"id": np.array([r])}

    # map batches
    actor_reuse = extract_values(
        "id",
        ds.map_batches(StatefulFn, batch_size=1, concurrency=1).take(),
    )
    assert sorted(actor_reuse) == list(range(10)), actor_reuse

    class StatefulFn:
        def __init__(self):
            self.num_reuses = 0

        def __call__(self, x):
            r = self.num_reuses
            self.num_reuses += 1
            return r > 0

    # filter
    actor_reuse = ds.filter(StatefulFn, concurrency=1).take()
    assert len(actor_reuse) == 9, actor_reuse

    class StatefulFnWithArgs:
        def __init__(self, arg, kwarg):
            assert arg == 1
            assert kwarg == 2

        def __call__(self, x, arg, kwarg):
            assert arg == 1
            assert kwarg == 2
            return x

    # map_batches & map with args & kwargs
    for ds_map in (ds.map_batches, ds.map):
        result = ds_map(
            StatefulFnWithArgs,
            concurrency=1,
            fn_args=(1,),
            fn_kwargs={"kwarg": 2},
            fn_constructor_args=(1,),
            fn_constructor_kwargs={"kwarg": 2},
        ).take()
        assert sorted(extract_values("id", result)) == list(range(10)), result

    class StatefulFlatMapFnWithArgs:
        def __init__(self, arg, kwarg):
            self._arg = arg
            assert arg == 1
            assert kwarg == 2

        def __call__(self, x, arg, kwarg):
            assert arg == 1
            assert kwarg == 2
            return [x] * self._arg

    # flat_map with args & kwargs
    result = ds.flat_map(
        StatefulFlatMapFnWithArgs,
        concurrency=1,
        fn_args=(1,),
        fn_kwargs={"kwarg": 2},
        fn_constructor_args=(1,),
        fn_constructor_kwargs={"kwarg": 2},
    ).take()
    assert sorted(extract_values("id", result)) == list(range(10)), result


def test_concurrent_callable_classes(shutdown_only):
    """Test that concurrenct actor pool runs user UDF in a separate thread."""
    ray.init(num_cpus=2)
    ds = ray.data.range(10, override_num_blocks=10)

    class StatefulFn:
        def __call__(self, x):
            thread_id = threading.get_ident()
            assert threading.current_thread() is not threading.main_thread()
            return {"tid": np.array([thread_id])}

    thread_ids = extract_values(
        "tid",
        ds.map_batches(StatefulFn, concurrency=1, max_concurrency=2).take_all(),
    )
    # Make sure user's UDF is not running concurrently.
    assert len(set(thread_ids)) == 1

    class ErrorFn:
        def __call__(self, x):
            raise ValueError

    with pytest.raises((UserCodeException, ValueError)):
        ds.map_batches(ErrorFn, concurrency=1, max_concurrency=2).take_all()


def test_transform_failure(shutdown_only):
    ray.init(num_cpus=2)
    ds = ray.data.from_items([0, 10], override_num_blocks=2)

    def mapper(x):
        time.sleep(x)
        raise ValueError("oops")
        return x

    with pytest.raises(ray.exceptions.RayTaskError):
        ds.map(mapper).materialize()


def test_actor_task_failure(shutdown_only, restore_data_context):
    ray.init(num_cpus=2)

    ctx = DataContext.get_current()
    ctx.actor_task_retry_on_errors = [ValueError]

    ds = ray.data.from_items([0, 10], override_num_blocks=2)

    class Mapper:
        def __init__(self):
            self._counter = 0

        def __call__(self, x):
            if self._counter < 2:
                self._counter += 1
                raise ValueError("oops")
            return x

    ds.map_batches(Mapper, concurrency=1).materialize()


def test_concurrency(shutdown_only):
    ray.init(num_cpus=6)
    ds = ray.data.range(10, override_num_blocks=10)

    def udf(x):
        return x

    class UDFClass:
        def __call__(self, x):
            return x

    # Test function and class.
    for fn in [udf, UDFClass]:
        # Test concurrency with None, single integer and a tuple of integers.
        for concurrency in [2, (2, 4)]:
            if fn == udf and concurrency == (2, 4):
                error_message = "``concurrency`` is set as a tuple of integers"
                with pytest.raises(ValueError, match=error_message):
                    ds.map(fn, concurrency=concurrency).take_all()
            else:
                result = ds.map(fn, concurrency=concurrency).take_all()
                assert sorted(extract_values("id", result)) == list(range(10)), result

    # Test concurrency with an illegal value.
    error_message = "``concurrency`` is expected to be set a"
    for concurrency in ["dummy", (1, 3, 5)]:
        with pytest.raises(ValueError, match=error_message):
            ds.map(UDFClass, concurrency=concurrency).take_all()

    # Test concurrency not set.
    result = ds.map(udf).take_all()
    assert sorted(extract_values("id", result)) == list(range(10)), result
    error_message = "``concurrency`` must be specified when using a callable class."
    with pytest.raises(ValueError, match=error_message):
        ds.map(UDFClass).take_all()


def test_flat_map_generator(ray_start_regular_shared):
    ds = ray.data.range(3)

    def map_generator(item: dict) -> Iterator[int]:
        for _ in range(2):
            yield {"id": item["id"] + 1}

    assert sorted(extract_values("id", ds.flat_map(map_generator).take())) == [
        1,
        1,
        2,
        2,
        3,
        3,
    ]


def test_add_column(ray_start_regular_shared):
    ds = ray.data.range(5).add_column("foo", lambda x: 1)
    assert ds.take(1) == [{"id": 0, "foo": 1}]

    ds = ray.data.range(5).add_column("foo", lambda x: x["id"] + 1)
    assert ds.take(1) == [{"id": 0, "foo": 1}]

    ds = ray.data.range(5).add_column("id", lambda x: x["id"] + 1)
    assert ds.take(2) == [{"id": 1}, {"id": 2}]

    with pytest.raises(ValueError):
        ds = ray.data.range(5).add_column("id", 0)


def test_drop_columns(ray_start_regular_shared, tmp_path):
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [2, 3, 4], "col3": [3, 4, 5]})
    ds1 = ray.data.from_pandas(df)
    ds1.write_parquet(str(tmp_path))
    ds2 = ray.data.read_parquet(str(tmp_path))

    for ds in [ds1, ds2]:
        assert ds.drop_columns(["col2"]).take(1) == [{"col1": 1, "col3": 3}]
        assert ds.drop_columns(["col1", "col3"]).take(1) == [{"col2": 2}]
        assert ds.drop_columns([]).take(1) == [{"col1": 1, "col2": 2, "col3": 3}]
        assert ds.drop_columns(["col1", "col2", "col3"]).take(1) == [{}]
        assert ds.drop_columns(["col1", "col1", "col2", "col1"]).take(1) == [
            {"col3": 3}
        ]
        # Test dropping non-existent column
        with pytest.raises((UserCodeException, KeyError)):
            ds.drop_columns(["dummy_col", "col1", "col2"]).materialize()


def test_select_columns(ray_start_regular_shared):
    # Test pandas and arrow
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [2, 3, 4], "col3": [3, 4, 5]})
    ds1 = ray.data.from_pandas(df)

    ds2 = ds1.map_batches(lambda pa: pa, batch_size=1, batch_format="pyarrow")

    for each_ds in [ds1, ds2]:
        assert each_ds.select_columns(cols=[]).take(1) == [{}]
        assert each_ds.select_columns(cols=["col1", "col2", "col3"]).take(1) == [
            {"col1": 1, "col2": 2, "col3": 3}
        ]
        assert each_ds.select_columns(cols=["col1", "col2"]).take(1) == [
            {"col1": 1, "col2": 2}
        ]
        assert each_ds.select_columns(cols=["col2", "col1"]).take(1) == [
            {"col1": 1, "col2": 2}
        ]
        # Test selecting columns with duplicates
        assert each_ds.select_columns(cols=["col1", "col2", "col2"]).schema().names == [
            "col1",
            "col2",
            "col2",
        ]
        # Test selecting a column that is not in the dataset schema
        with pytest.raises((UserCodeException, KeyError)):
            each_ds.select_columns(cols=["col1", "col2", "dummy_col"]).materialize()


def test_map_batches_basic(ray_start_regular_shared, tmp_path, restore_data_context):
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


def test_map_batches_extra_args(shutdown_only, tmp_path):
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


def test_map_with_memory_resources(shutdown_only):
    """Test that we can use memory resource to limit the concurrency."""
    num_blocks = 50
    memory_per_task = 100 * 1024**2
    max_concurrency = 5
    ray.init(num_cpus=num_blocks, _memory=memory_per_task * max_concurrency)

    concurrency_counter = ConcurrencyCounter.remote()

    def map_batches(batch):
        ray.get(concurrency_counter.inc.remote())
        time.sleep(0.5)
        ray.get(concurrency_counter.decr.remote())
        return batch

    ds = ray.data.range(num_blocks, override_num_blocks=num_blocks)
    ds = ds.map_batches(
        map_batches,
        batch_size=None,
        num_cpus=1,
        memory=memory_per_task,
    )
    assert len(ds.take(num_blocks)) == num_blocks

    actual_max_concurrency = ray.get(concurrency_counter.get_max_concurrency.remote())
    assert actual_max_concurrency <= max_concurrency


def test_map_batches_generator(ray_start_regular_shared, tmp_path):
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


def test_map_batches_actors_preserves_order(shutdown_only):
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
    ray_start_regular_shared, num_rows, num_blocks, batch_size, restore_data_context
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
    ds = ds.map_batches(mutate, batch_size=batch_size)
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
    ray_start_regular_shared, num_rows, num_blocks, batch_size
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
    ray_start_regular_shared, block_size, batch_size
):
    # Ensure that we test at least 2 batches worth of blocks.
    num_blocks = max(10, 2 * batch_size // block_size)
    ds = ray.data.range(num_blocks * block_size, override_num_blocks=num_blocks)
    # Confirm that we have the expected number of initial blocks.
    assert ds._plan.initial_num_blocks() == num_blocks

    # Blocks should be bundled up to the batch size.
    ds1 = ds.map_batches(lambda x: x, batch_size=batch_size).materialize()
    assert ds1._plan.initial_num_blocks() == math.ceil(
        num_blocks / max(batch_size // block_size, 1)
    )

    # Blocks should not be bundled up when batch_size is not specified.
    ds2 = ds.map_batches(lambda x: x).materialize()
    assert ds2._plan.initial_num_blocks() == num_blocks


@pytest.mark.parametrize(
    "block_sizes,batch_size,expected_num_blocks",
    [
        ([1, 2], 3, 1),
        ([2, 2, 1], 3, 2),
        ([1, 2, 3, 4], 4, 3),
        ([3, 1, 1, 3], 4, 2),
        ([2, 4, 1, 8], 4, 4),
        ([1, 1, 1, 1], 4, 1),
        ([1, 0, 3, 2], 4, 2),
        ([4, 4, 4, 4], 4, 4),
    ],
)
def test_map_batches_block_bundling_skewed_manual(
    ray_start_regular_shared, block_sizes, batch_size, expected_num_blocks
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
    ray_start_regular_shared, block_sizes, batch_size
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
        if curr > 0 and curr + block_size > batch_size:
            num_out_blocks += 1
            curr = 0
        curr += block_size
    if curr > 0:
        num_out_blocks += 1

    # Blocks should be bundled up to the batch size.
    assert ds._plan.initial_num_blocks() == num_out_blocks


def test_map_with_mismatched_columns(ray_start_regular_shared):
    def bad_fn(row):
        if row["id"] > 5:
            return {"a": "hello1"}
        else:
            return {"b": "hello1"}

    def good_fn(row):
        if row["id"] > 5:
            return {"a": "hello1", "b": "hello2"}
        else:
            return {"b": "hello2", "a": "hello1"}

    ds = ray.data.range(10, override_num_blocks=1)
    error_message = "Current row has different columns compared to previous rows."
    with pytest.raises(ValueError) as e:
        ds.map(bad_fn).materialize()
    assert error_message in str(e.value)
    ds_map = ds.map(good_fn)
    assert ds_map.take() == [{"a": "hello1", "b": "hello2"} for _ in range(10)]


def test_map_batches_preserve_empty_blocks(ray_start_regular_shared):
    ds = ray.data.range(10, override_num_blocks=10)
    ds = ds.map_batches(lambda x: [])
    ds = ds.map_batches(lambda x: x)
    assert ds._plan.initial_num_blocks() == 10, ds


def test_map_batches_combine_empty_blocks(ray_start_regular_shared):
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


def test_map_batches_preserves_empty_block_format(ray_start_regular_shared):
    """Tests that the block format for empty blocks are not modified."""

    def empty_pandas(batch):
        return pd.DataFrame({"x": []})

    df = pd.DataFrame({"x": [1, 2, 3]})

    # First map_batches creates the empty Pandas block.
    # Applying subsequent map_batches should not change the type of the empty block.
    ds = (
        ray.data.from_pandas(df)
        .map_batches(empty_pandas)
        .map_batches(lambda x: x, batch_size=None)
    )

    bundles = ds.iter_internal_ref_bundles()
    block_refs = _ref_bundles_iterator_to_block_refs_list(bundles)

    assert len(block_refs) == 1
    assert type(ray.get(block_refs[0])) == pd.DataFrame


def test_map_with_objects_and_tensors(ray_start_regular_shared):
    # Tests https://github.com/ray-project/ray/issues/45235

    class UnsupportedType:
        pass

    def f(batch):
        batch_size = len(batch["id"])
        return {
            "array": np.zeros((batch_size, 32, 32, 3)),
            "unsupported": [UnsupportedType()] * batch_size,
        }

    ray.data.range(1).map_batches(f).materialize()


def test_random_sample(ray_start_regular_shared):
    import math

    def ensure_sample_size_close(dataset, sample_percent=0.5):
        r1 = dataset.random_sample(sample_percent)
        assert math.isclose(
            r1.count(), int(dataset.count() * sample_percent), rel_tol=2, abs_tol=2
        )

    ds = ray.data.range(10, override_num_blocks=2)
    ensure_sample_size_close(ds)

    ds = ray.data.range_tensor(5, override_num_blocks=2, shape=(2, 2))
    ensure_sample_size_close(ds)

    # imbalanced datasets
    ds1 = ray.data.range(1, override_num_blocks=1)
    ds2 = ray.data.range(2, override_num_blocks=1)
    ds3 = ray.data.range(3, override_num_blocks=1)
    # noinspection PyTypeChecker
    ds = ds1.union(ds2).union(ds3)
    ensure_sample_size_close(ds)
    # Small datasets
    ds1 = ray.data.range(5, override_num_blocks=5)
    ensure_sample_size_close(ds1)


def test_random_sample_checks(ray_start_regular_shared):
    with pytest.raises(ValueError):
        # Cannot sample -1
        ray.data.range(1).random_sample(-1)
    with pytest.raises(ValueError):
        # Cannot sample from empty dataset
        ray.data.range(0).random_sample(0.2)
    with pytest.raises(ValueError):
        # Cannot sample fraction > 1
        ray.data.range(1).random_sample(10)


# NOTE: All tests above share a Ray cluster, while the tests below do not. These
# tests should only be carefully reordered to retain this invariant!
def test_actor_pool_strategy_default_num_actors(shutdown_only):
    import time

    class UDFClass:
        def __call__(self, x):
            time.sleep(1)
            return x

    num_cpus = 5
    ray.shutdown()
    ray.init(num_cpus=num_cpus)
    compute_strategy = ray.data.ActorPoolStrategy()
    ray.data.range(10, override_num_blocks=10).map_batches(
        UDFClass, compute=compute_strategy, batch_size=1
    ).materialize()


def test_actor_pool_strategy_bundles_to_max_actors(shutdown_only):
    """Tests that blocks are bundled up to the specified max number of actors."""

    class UDFClass:
        def __call__(self, x):
            return x

    max_size = 2
    ds = (
        ray.data.range(10, override_num_blocks=10)
        .map_batches(UDFClass, batch_size=None, concurrency=max_size)
        .materialize()
    )

    # Check batch size is still respected.
    ds = (
        ray.data.range(10, override_num_blocks=10)
        .map_batches(UDFClass, batch_size=10, concurrency=max_size)
        .materialize()
    )

    assert "1 blocks" in ds.stats()


def test_nonserializable_map_batches(shutdown_only):
    import threading

    lock = threading.Lock()

    x = ray.data.range(10)
    # Check that the `inspect_serializability` trace was printed
    with pytest.raises(TypeError, match=r".*was found to be non-serializable.*"):
        x.map_batches(lambda _: lock).take(1)


def test_map_batches_async_generator(shutdown_only):
    ray.shutdown()
    ray.init(num_cpus=10)

    async def sleep_and_yield(i):
        await asyncio.sleep(i % 5)
        return {"input": [i], "output": [2**i]}

    class AsyncActor:
        def __init__(self):
            pass

        async def __call__(self, batch):
            tasks = [asyncio.create_task(sleep_and_yield(i)) for i in batch["id"]]
            for task in tasks:
                yield await task

    n = 10
    ds = ray.data.range(n, override_num_blocks=2)
    ds = ds.map(lambda x: x)
    ds = ds.map_batches(AsyncActor, batch_size=1, concurrency=1, max_concurrency=2)

    start_t = time.time()
    output = ds.take_all()
    runtime = time.time() - start_t
    assert runtime < sum(range(n)), runtime

    expected_output = [{"input": i, "output": 2**i} for i in range(n)]
    assert output == expected_output, (output, expected_output)


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


def test_map_batches_async_generator_fast_yield(shutdown_only):
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
