import asyncio
import itertools
import logging
import math
import os
import random
import threading
import time
from asyncio import AbstractEventLoop
from typing import Iterator, Literal
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
from pkg_resources import parse_version

import ray
from ray._common.test_utils import wait_for_condition
from ray._private.arrow_utils import get_pyarrow_version
from ray._private.test_utils import run_string_as_driver
from ray.data._internal.arrow_ops.transform_pyarrow import (
    MIN_PYARROW_VERSION_TYPE_PROMOTION,
)
from ray.data._internal.planner.plan_udf_map_op import (
    _generate_transform_fn_for_async_map,
    _MapActorContext,
)
from ray.data.context import DataContext
from ray.data.dataset import Dataset
from ray.data.datatype import DataType
from ray.data.exceptions import UserCodeException
from ray.data.expressions import col, lit, udf
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.test_util import ConcurrencyCounter  # noqa
from ray.data.tests.util import column_udf, extract_values
from ray.exceptions import RayTaskError
from ray.tests.conftest import *  # noqa


def test_specifying_num_cpus_and_num_gpus_logs_warning(
    shutdown_only, propagate_logs, caplog, target_max_block_size_infinite_or_default
):
    ray.init(num_cpus=1, num_gpus=1)

    with caplog.at_level(logging.WARNING):
        ray.data.range(1).map(lambda x: x, num_cpus=1, num_gpus=1).take(1)

        assert (
            "Specifying both num_cpus and num_gpus for map tasks is experimental"
            in caplog.text
        ), caplog.text


def test_invalid_max_tasks_in_flight_raises_error():
    with pytest.raises(ValueError):
        ray.data.ActorPoolStrategy(max_tasks_in_flight_per_actor=0)


@pytest.mark.parametrize("concurrency", [(2, 1), -1])
def test_invalid_concurrency_raises_error(shutdown_only, concurrency):
    ray.init()

    class UDF:
        def __call__(self, row):
            return row

    with pytest.raises(ValueError):
        ray.data.range(1).map(UDF, concurrency=concurrency)


def test_callable_classes(shutdown_only, target_max_block_size_infinite_or_default):
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
    assert sorted(extract_values("id", actor_reuse)) == [
        [v] for v in list(range(10))
    ], actor_reuse

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

    class StatefulFilterFnWithArgs:
        def __init__(self, arg, kwarg):
            assert arg == 1
            assert kwarg == 2

        def __call__(self, x, arg, kwarg):
            assert arg == 1
            assert kwarg == 2
            return True

    # fiter with args & kwargs
    result = ds.filter(
        StatefulFilterFnWithArgs,
        concurrency=1,
        fn_args=(1,),
        fn_kwargs={"kwarg": 2},
        fn_constructor_args=(1,),
        fn_constructor_kwargs={"kwarg": 2},
    ).take()
    assert sorted(extract_values("id", result)) == list(range(10)), result


def test_concurrent_callable_classes(
    shutdown_only, target_max_block_size_infinite_or_default
):
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


def test_transform_failure(shutdown_only, target_max_block_size_infinite_or_default):
    ray.init(num_cpus=2)
    ds = ray.data.from_items([0, 10], override_num_blocks=2)

    def mapper(x):
        time.sleep(x)
        raise ValueError("oops")
        return x

    with pytest.raises(ray.exceptions.RayTaskError):
        ds.map(mapper).materialize()


def test_actor_task_failure(
    shutdown_only, restore_data_context, target_max_block_size_infinite_or_default
):
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


def test_gpu_workers_not_reused(
    shutdown_only, target_max_block_size_infinite_or_default
):
    """By default, in Ray Core if `num_gpus` is specified workers will not be reused
    for tasks invocation.

    For more context check out https://github.com/ray-project/ray/issues/29624"""

    ray.init(num_gpus=1)

    total_blocks = 5
    ds = ray.data.range(5, override_num_blocks=total_blocks)

    def _get_worker_id(_):
        return {"worker_id": ray.get_runtime_context().get_worker_id()}

    unique_worker_ids = ds.map(_get_worker_id, num_gpus=1).unique("worker_id")

    assert len(unique_worker_ids) == total_blocks


def test_concurrency(shutdown_only, target_max_block_size_infinite_or_default):
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
        for concurrency in [2, (2, 4), (2, 6, 4)]:
            if fn == udf and (concurrency == (2, 4) or concurrency == (2, 6, 4)):
                error_message = "``concurrency`` is set as a tuple of integers"
                with pytest.raises(ValueError, match=error_message):
                    ds.map(fn, concurrency=concurrency).take_all()
            else:
                result = ds.map(fn, concurrency=concurrency).take_all()
                assert sorted(extract_values("id", result)) == list(range(10)), result

    # Test concurrency with an illegal value.
    error_message = "``concurrency`` is expected to be set a"
    for concurrency in ["dummy", (1, 3, 5, 7)]:
        with pytest.raises(ValueError, match=error_message):
            ds.map(UDFClass, concurrency=concurrency).take_all()

    # Test concurrency not set.
    result = ds.map(udf).take_all()
    assert sorted(extract_values("id", result)) == list(range(10)), result
    error_message = "``concurrency`` must be specified when using a callable class."
    with pytest.raises(ValueError, match=error_message):
        ds.map(UDFClass).take_all()


@pytest.mark.parametrize("udf_kind", ["gen", "func"])
def test_flat_map(
    ray_start_regular_shared, udf_kind, target_max_block_size_infinite_or_default
):
    ds = ray.data.range(3)

    if udf_kind == "gen":

        def _udf(item: dict) -> Iterator[int]:
            for _ in range(2):
                yield {"id": item["id"] + 1}

    elif udf_kind == "func":

        def _udf(item: dict) -> dict:
            return [{"id": item["id"] + 1} for _ in range(2)]

    else:
        pytest.fail(f"Invalid udf_kind: {udf_kind}")

    assert sorted(extract_values("id", ds.flat_map(_udf).take())) == [
        1,
        1,
        2,
        2,
        3,
        3,
    ]


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
            id="nanoseconds_increment_map",
        )
    ],
)
def test_map_timestamp_nanosecs(
    df, expected_df, ray_start_regular_shared, target_max_block_size_infinite_or_default
):
    """Verify handling timestamp with nanosecs in map"""
    ray_data = ray.data.from_pandas(df)
    result = ray_data.map(process_timestamp_data)
    processed_df = result.to_pandas()
    processed_df["timestamp"] = processed_df["timestamp"].astype("datetime64[ns]")
    pd.testing.assert_frame_equal(processed_df, expected_df)


def test_add_column(ray_start_regular_shared):
    """Tests the add column API."""

    # Test with pyarrow batch format
    ds = ray.data.range(5).add_column(
        "foo", lambda x: pa.array([1] * x.num_rows), batch_format="pyarrow"
    )
    assert ds.take(1) == [{"id": 0, "foo": 1}]

    # Test with chunked array batch format
    ds = ray.data.range(5).add_column(
        "foo", lambda x: pa.chunked_array([[1] * x.num_rows]), batch_format="pyarrow"
    )
    assert ds.take(1) == [{"id": 0, "foo": 1}]

    ds = ray.data.range(5).add_column(
        "foo", lambda x: pc.add(x["id"], 1), batch_format="pyarrow"
    )
    assert ds.take(1) == [{"id": 0, "foo": 1}]

    # Adding a column that is already there should not result in an error
    ds = ray.data.range(5).add_column(
        "id", lambda x: pc.add(x["id"], 1), batch_format="pyarrow"
    )
    assert ds.take(2) == [{"id": 1}, {"id": 2}]

    # Adding a column in the wrong format should result in an error
    with pytest.raises(
        ray.exceptions.UserCodeException, match="For pyarrow batch format"
    ):
        ds = ray.data.range(5).add_column("id", lambda x: [1], batch_format="pyarrow")
        assert ds.take(2) == [{"id": 1}, {"id": 2}]

    # Test with numpy batch format
    ds = ray.data.range(5).add_column(
        "foo", lambda x: np.array([1] * len(x[list(x.keys())[0]])), batch_format="numpy"
    )
    assert ds.take(1) == [{"id": 0, "foo": 1}]

    ds = ray.data.range(5).add_column(
        "foo", lambda x: np.add(x["id"], 1), batch_format="numpy"
    )
    assert ds.take(1) == [{"id": 0, "foo": 1}]

    # Adding a column that is already there should not result in an error
    ds = ray.data.range(5).add_column(
        "id", lambda x: np.add(x["id"], 1), batch_format="numpy"
    )
    assert ds.take(2) == [{"id": 1}, {"id": 2}]

    # Adding a column in the wrong format should result in an error
    with pytest.raises(
        ray.exceptions.UserCodeException, match="For numpy batch format"
    ):
        ds = ray.data.range(5).add_column("id", lambda x: [1], batch_format="numpy")
        assert ds.take(2) == [{"id": 1}, {"id": 2}]

    # Test with pandas batch format
    ds = ray.data.range(5).add_column("foo", lambda x: pd.Series([1] * x.shape[0]))
    assert ds.take(1) == [{"id": 0, "foo": 1}]

    ds = ray.data.range(5).add_column("foo", lambda x: x["id"] + 1)
    assert ds.take(1) == [{"id": 0, "foo": 1}]

    # Adding a column that is already there should not result in an error
    ds = ray.data.range(5).add_column("id", lambda x: x["id"] + 1)
    assert ds.take(2) == [{"id": 1}, {"id": 2}]

    # Adding a column in the wrong format may result in an error
    with pytest.raises(ray.exceptions.UserCodeException):
        ds = ray.data.range(5).add_column(
            "id", lambda x: range(7), batch_format="pandas"
        )
        assert ds.take(2) == [{"id": 1}, {"id": 2}]

    ds = ray.data.range(5).add_column("const", lambda _: 3, batch_format="pandas")
    assert ds.take(2) == [{"id": 0, "const": 3}, {"id": 1, "const": 3}]

    with pytest.raises(ValueError):
        ds = ray.data.range(5).add_column("id", 0)

    # Test that an invalid batch_format raises an error
    with pytest.raises(ValueError):
        ray.data.range(5).add_column("foo", lambda x: x["id"] + 1, batch_format="foo")


@pytest.mark.parametrize(
    "names, expected_schema",
    [
        ({"spam": "foo", "ham": "bar"}, ["foo", "bar"]),
        ({"spam": "foo"}, ["foo", "ham"]),
        (["foo", "bar"], ["foo", "bar"]),
    ],
)
def test_rename_columns(
    ray_start_regular_shared,
    names,
    expected_schema,
    target_max_block_size_infinite_or_default,
):
    ds = ray.data.from_items([{"spam": 0, "ham": 0}])

    renamed_ds = ds.rename_columns(names)
    renamed_schema_names = renamed_ds.schema().names

    assert sorted(renamed_schema_names) == sorted(expected_schema)


def test_default_batch_size_emits_deprecation_warning(
    ray_start_regular_shared, target_max_block_size_infinite_or_default
):
    with pytest.warns(
        DeprecationWarning,
        match="Passing 'default' to `map_batches` is deprecated and won't be "
        "supported after September 2025. Use `batch_size=None` instead.",
    ):
        ray.data.range(1).map_batches(lambda x: x, batch_size="default")


@pytest.mark.parametrize(
    "names, expected_exception, expected_message",
    [
        # Case 1: Empty dictionary, should raise ValueError
        ({}, ValueError, "rename_columns received 'names' with no entries."),
        # Case 2: Invalid dictionary (duplicate values), should raise ValueError
        (
            {"spam": "foo", "ham": "foo"},
            ValueError,
            "rename_columns received duplicate values in the 'names': "
            "{'spam': 'foo', 'ham': 'foo'}",
        ),
        # Case 3: Dictionary with non-string keys/values, should raise ValueError
        (
            {"spam": 1, "ham": "bar"},
            ValueError,
            "rename_columns requires both keys and values in the 'names' to be "
            "strings.",
        ),
        # Case 4: Empty list, should raise ValueError
        (
            [],
            ValueError,
            "rename_columns requires 'names' with at least one column name.",
        ),
        # Case 5: List with duplicate values, should raise ValueError
        (
            ["foo", "bar", "foo"],
            ValueError,
            "rename_columns received duplicate values in the 'names': "
            "['foo', 'bar', 'foo']",
        ),
        # Case 6: List with non-string values, should raise ValueError
        (
            ["foo", "bar", 1],
            ValueError,
            "rename_columns requires all elements in the 'names' to be strings.",
        ),
        # Case 7: Mismatched length of list and current column names, should raise
        # ValueError
        (
            ["foo", "bar", "baz"],
            ValueError,
            "rename_columns requires 'names': ['foo', 'bar', 'baz'] length match "
            "current schema names: ['spam', 'ham'].",
        ),
        # Case 8: Invalid type for `names` (integer instead of dict or list), should
        # raise TypeError
        (
            42,
            TypeError,
            "rename_columns expected names to be either List[str] or Dict[str, str], "
            "got <class 'int'>.",
        ),
    ],
)
def test_rename_columns_error_cases(
    ray_start_regular_shared,
    names,
    expected_exception,
    expected_message,
    target_max_block_size_infinite_or_default,
):
    # Simulate a dataset with two columns: "spam" and "ham"
    ds = ray.data.from_items([{"spam": 0, "ham": 0}])

    # Test that the correct exception is raised
    with pytest.raises(expected_exception) as exc_info:
        ds.rename_columns(names)

    # Verify that the exception message matches the expected message
    assert str(exc_info.value) == expected_message


def test_drop_columns(
    ray_start_regular_shared, tmp_path, target_max_block_size_infinite_or_default
):
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [2, 3, 4], "col3": [3, 4, 5]})
    ds1 = ray.data.from_pandas(df)
    ds1.write_parquet(str(tmp_path))
    ds2 = ray.data.read_parquet(str(tmp_path))

    for ds in [ds1, ds2]:
        assert ds.drop_columns(["col2"]).take(1) == [{"col1": 1, "col3": 3}]
        assert ds.drop_columns(["col1", "col3"]).take(1) == [{"col2": 2}]
        assert ds.drop_columns([]).take(1) == [{"col1": 1, "col2": 2, "col3": 3}]
        assert ds.drop_columns(["col1", "col2", "col3"]).take(1) == []
        assert ds.drop_columns(["col1", "col2"]).take(1) == [{"col3": 3}]
        # Test dropping non-existent column
        with pytest.raises((UserCodeException, KeyError)):
            ds.drop_columns(["dummy_col", "col1", "col2"]).materialize()

    with pytest.raises(ValueError, match="drop_columns expects unique column names"):
        ds1.drop_columns(["col1", "col2", "col2"])


def test_select_rename_columns(
    ray_start_regular_shared, target_max_block_size_infinite_or_default
):
    ds = ray.data.range(1)

    def map_fn(row):
        return {"a": "a", "b": "b", "c": "c"}

    ds = ds.map(map_fn)
    result = ds.rename_columns({"a": "A"}).select_columns("A").take_all()
    assert result == [{"A": "a"}]
    result = ds.rename_columns({"a": "A"}).select_columns("b").take_all()
    assert result == [{"b": "b"}]
    result = ds.rename_columns({"a": "x", "b": "y"}).select_columns("c").take_all()
    assert result == [{"c": "c"}]
    result = ds.rename_columns({"a": "x", "b": "y"}).select_columns("x").take_all()
    assert result == [{"x": "a"}]
    result = ds.rename_columns({"a": "x", "b": "y"}).select_columns("y").take_all()
    assert result == [{"y": "b"}]
    result = ds.rename_columns({"a": "b", "b": "a"}).select_columns("b").take_all()
    assert result == [{"b": "a"}]
    result = ds.rename_columns({"a": "b", "b": "a"}).select_columns("a").take_all()
    assert result == [{"a": "b"}]


def test_select_columns(
    ray_start_regular_shared, target_max_block_size_infinite_or_default
):
    # Test pandas and arrow
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [2, 3, 4], "col3": [3, 4, 5]})
    ds1 = ray.data.from_pandas(df)

    ds2 = ds1.map_batches(lambda pa: pa, batch_size=1, batch_format="pyarrow")

    for each_ds in [ds1, ds2]:
        # Test selecting with empty columns
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
        with pytest.raises(ValueError, match="expected unique column names"):
            each_ds.select_columns(cols=["col1", "col2", "col2"]).schema()
        # Test selecting a column that is not in the dataset schema
        with pytest.raises((UserCodeException, KeyError)):
            each_ds.select_columns(cols=["col1", "col2", "dummy_col"]).materialize()


@pytest.mark.parametrize(
    "cols, expected_exception, expected_error",
    [
        (
            None,
            TypeError,
            "select_columns requires 'cols' to be a string or a list of strings.",
        ),
        (
            1,
            TypeError,
            "select_columns requires 'cols' to be a string or a list of strings.",
        ),
        (
            [1],
            ValueError,
            "select_columns requires all elements of 'cols' to be strings.",
        ),
    ],
)
def test_select_columns_validation(
    ray_start_regular_shared,
    cols,
    expected_exception,
    expected_error,
    target_max_block_size_infinite_or_default,
):
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [2, 3, 4], "col3": [3, 4, 5]})
    ds1 = ray.data.from_pandas(df)

    with pytest.raises(expected_exception, match=expected_error):
        ds1.select_columns(cols=cols)


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


def test_map_with_objects_and_tensors(
    ray_start_regular_shared, target_max_block_size_infinite_or_default
):
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


def test_random_sample(
    ray_start_regular_shared, target_max_block_size_infinite_or_default
):
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


def test_random_sample_checks(
    ray_start_regular_shared, target_max_block_size_infinite_or_default
):
    with pytest.raises(ValueError):
        # Cannot sample -1
        ray.data.range(1).random_sample(-1)
    with pytest.raises(ValueError):
        # Cannot sample from empty dataset
        ray.data.range(0).random_sample(0.2)
    with pytest.raises(ValueError):
        # Cannot sample fraction > 1
        ray.data.range(1).random_sample(10)


def test_random_sample_fixed_seed_0001(
    ray_start_regular_shared, target_max_block_size_infinite_or_default
):
    """Tests random_sample() with a fixed seed.

    https://github.com/ray-project/ray/pull/51401

    This test is to ensure that the random sampling is reproducible.
    In the following example, we generate a deterministic seed sequence
    for each block. Each block generates 10 ranndom numbers and we pick
    10% of them. The indices from Ray Data should be the same as the
    ones generated by numpy.
    """
    ds = ray.data.range(100, override_num_blocks=10).random_sample(
        fraction=0.1, seed=1234
    )

    result = ds.to_pandas()["id"].to_numpy()

    # Expected:
    expected = np.array([8, 49, 71, 78, 81, 85])

    np.testing.assert_array_equal(result, expected)


@pytest.mark.parametrize("dtype", ["numpy", "pandas", "pyarrow"])
@pytest.mark.parametrize("num_blocks, num_rows_per_block", [(1, 1000), (10, 100)])
@pytest.mark.parametrize("fraction", [0.1, 0.5, 1.0])
@pytest.mark.parametrize("seed", [1234, 4321, 0])
def test_random_sample_fixed_seed_0002(
    ray_start_regular_shared,
    dtype,
    num_blocks,
    num_rows_per_block,
    fraction,
    seed,
    target_max_block_size_infinite_or_default,
):
    """Checks if random_sample() gives the same result across different parameters. This is to
    test whether the result from random_sample() can be computed explicitly using numpy functions.

    The expected result (sampled row indices) is deterministic for a fixed seed and number of blocks.
    """

    def generate_data(n_per_block: int, n_blocks: int):
        for i in range(n_blocks):
            yield {
                "item": np.arange(i * n_per_block, (i + 1) * n_per_block),
            }

    if dtype == "numpy":
        ds = ray.data.from_items(
            np.arange(num_rows_per_block * num_blocks), override_num_blocks=num_blocks
        )
    elif dtype == "pandas":
        data = [pd.DataFrame(b) for b in generate_data(num_rows_per_block, num_blocks)]
        ds = ray.data.from_pandas(data)
    elif dtype == "pyarrow":
        data = [
            pa.Table.from_pydict(b)
            for b in generate_data(num_rows_per_block, num_blocks)
        ]
        ds = ray.data.from_arrow(data)
    else:
        raise ValueError(f"Unknown dtype: {dtype}")

    ds = ds.random_sample(fraction=fraction, seed=seed)

    # Seed sequence for each block: [task_idx, seed]
    expected_raw = np.concatenate(
        [
            np.random.default_rng([i, seed]).random(num_rows_per_block)
            for i in range(num_blocks)
        ]
    )

    # Sample the random numbers and get the indices
    expected = np.where(expected_raw < fraction)[0]

    assert ds.count() == len(expected)
    assert set(ds.to_pandas()["item"].to_list()) == set(expected.tolist())


def test_actor_udf_cleanup(
    ray_start_regular_shared,
    tmp_path,
    restore_data_context,
    target_max_block_size_infinite_or_default,
):
    """Test that for the actor map operator, the UDF object is deleted properly."""
    ctx = DataContext.get_current()
    ctx._enable_actor_pool_on_exit_hook = True

    test_file = tmp_path / "test.txt"

    # Simulate the case that the UDF depends on some external resources that
    # need to be cleaned up.
    class StatefulUDF:
        def __init__(self):
            with open(test_file, "w") as f:
                f.write("test")

        def __call__(self, row):
            return row

        def __del__(self):
            # Delete the file when the UDF is deleted.
            os.remove(test_file)

    ds = ray.data.range(10)
    ds = ds.map(StatefulUDF, concurrency=1)
    assert sorted(extract_values("id", ds.take_all())) == list(range(10))

    wait_for_condition(lambda: not os.path.exists(test_file))


def test_warn_large_udfs(
    ray_start_regular_shared, target_max_block_size_infinite_or_default
):
    driver = """
import ray
import numpy as np
from ray.data._internal.execution.operators.map_operator import MapOperator

large_object = np.zeros(MapOperator.MAP_UDF_WARN_SIZE_THRESHOLD + 1, dtype=np.int8)

class LargeUDF:
    def __init__(self):
        self.data = large_object

    def __call__(self, batch):
        return batch

ds = ray.data.range(1)
ds = ds.map_batches(LargeUDF, concurrency=1)
assert ds.take_all() == [{"id": 0}]
    """
    output = run_string_as_driver(driver)
    assert "The UDF of operator MapBatches(LargeUDF) is too large" in output


# NOTE: All tests above share a Ray cluster, while the tests below do not. These
# tests should only be carefully reordered to retain this invariant!
def test_actor_pool_strategy_default_num_actors(
    shutdown_only, target_max_block_size_infinite_or_default
):
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


def test_actor_pool_strategy_bundles_to_max_actors(
    shutdown_only, target_max_block_size_infinite_or_default
):
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


def test_nonserializable_map_batches(
    shutdown_only, target_max_block_size_infinite_or_default
):
    import threading

    lock = threading.Lock()

    x = ray.data.range(10)
    # Check that the `inspect_serializability` trace was printed
    with pytest.raises(TypeError, match=r".*was found to be non-serializable.*"):
        x.map_batches(lambda _: lock).take(1)


@pytest.mark.parametrize("udf_kind", ["coroutine", "async_gen"])
def test_async_map_batches(
    shutdown_only, udf_kind, target_max_block_size_infinite_or_default
):
    ray.shutdown()
    ray.init(num_cpus=10)

    class AsyncActor:
        def __init__(self):
            pass

        if udf_kind == "async_gen":

            async def __call__(self, batch):
                for i in batch["id"]:
                    await asyncio.sleep((i % 5) / 100)
                    yield {"input": [i], "output": [2**i]}

        elif udf_kind == "coroutine":

            async def __call__(self, batch):
                await asyncio.sleep(random.randint(0, 5) / 100)
                return {
                    "input": list(batch["id"]),
                    "output": [2**i for i in batch["id"]],
                }

        else:
            pytest.fail(f"Unknown udf_kind: {udf_kind}")

    n = 10
    ds = ray.data.range(n, override_num_blocks=2)
    ds = ds.map(lambda x: x)
    ds = ds.map_batches(AsyncActor, batch_size=1, concurrency=1, max_concurrency=2)

    start_t = time.time()
    output = ds.take_all()
    runtime = time.time() - start_t
    assert runtime < sum(range(n)), runtime

    expected_output = [{"input": i, "output": 2**i} for i in range(n)]
    assert sorted(output, key=lambda row: row["input"]) == expected_output, (
        output,
        expected_output,
    )


@pytest.mark.parametrize("udf_kind", ["coroutine", "async_gen"])
def test_async_flat_map(
    shutdown_only, udf_kind, target_max_block_size_infinite_or_default
):
    class AsyncActor:
        def __init__(self):
            pass

        if udf_kind == "async_gen":

            async def __call__(self, row):
                id = row["id"]
                yield {"id": id}
                await asyncio.sleep(random.randint(0, 5) / 100)
                yield {"id": id + 1}

        elif udf_kind == "coroutine":

            async def __call__(self, row):
                id = row["id"]
                await asyncio.sleep(random.randint(0, 5) / 100)
                return [{"id": id}, {"id": id + 1}]

        else:
            pytest.fail(f"Unknown udf_kind: {udf_kind}")

    n = 10
    ds = ray.data.from_items([{"id": i} for i in range(0, n, 2)])
    ds = ds.flat_map(AsyncActor, concurrency=1, max_concurrency=2)
    output = ds.take_all()
    assert sorted(extract_values("id", output)) == list(range(n))


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


class TestGenerateTransformFnForAsyncMap:
    @pytest.fixture
    def mock_actor_async_ctx(self):
        _map_actor_ctx = _MapActorContext(Mock(), Mock(), is_async=True)

        loop: AbstractEventLoop = _map_actor_ctx.udf_map_asyncio_loop
        assert loop is not None

        with patch("ray.data._map_actor_context", _map_actor_ctx):

            yield _map_actor_ctx

            loop.call_soon_threadsafe(loop.stop)
            _map_actor_ctx.udf_map_asyncio_thread.join()

    def test_non_coroutine_function_assertion(
        self, target_max_block_size_infinite_or_default
    ):
        """Test that non-coroutine function raises assertion error."""

        def sync_fn(x):
            return x

        validate_fn = Mock()

        with pytest.raises(ValueError, match="Expected a coroutine function"):
            _generate_transform_fn_for_async_map(
                sync_fn, validate_fn, max_concurrency=1
            )

    def test_zero_max_concurrent_batches_assertion(
        self, target_max_block_size_infinite_or_default
    ):
        """Test that zero max_concurrent_batches raises assertion error."""

        async def async_fn(x):
            yield x

        validate_fn = Mock()

        with pytest.raises(AssertionError):
            _generate_transform_fn_for_async_map(
                async_fn, validate_fn, max_concurrency=0
            )

    def test_empty_input(
        self, mock_actor_async_ctx, target_max_block_size_infinite_or_default
    ):
        """Test with empty input iterator."""

        async def async_fn(x):
            yield x

        validate_fn = Mock()

        transform_fn = _generate_transform_fn_for_async_map(
            async_fn, validate_fn, max_concurrency=2
        )

        task_context = Mock()
        assert list(transform_fn([], task_context)) == []
        validate_fn.assert_not_called()

    @pytest.mark.parametrize("udf_kind", ["coroutine", "async_gen"])
    def test_basic_async_processing(
        self, udf_kind, mock_actor_async_ctx, target_max_block_size_infinite_or_default
    ):
        """Test basic async processing with order preservation."""

        if udf_kind == "async_gen":

            async def async_fn(x):
                # Randomly slow-down UDFs (capped by 5ms)
                delay = random.randint(0, 5) / 1000
                await asyncio.sleep(delay)
                yield x

        elif udf_kind == "coroutine":

            async def async_fn(x):
                # Randomly slow-down UDFs (capped by 5ms)
                delay = random.randint(0, 5) / 1000
                await asyncio.sleep(delay)
                return x

        else:
            pytest.fail(f"Unrecognized udf_kind ({udf_kind})")

        validate_fn = Mock()

        transform_fn = _generate_transform_fn_for_async_map(
            async_fn, validate_fn, max_concurrency=100
        )

        N = 10_000

        task_context = Mock()
        result = list(transform_fn(range(N), task_context))

        assert result == list(range(N))
        assert validate_fn.call_count == N

    @pytest.mark.parametrize("result_len", [0, 5])
    def test_basic_async_processing_with_iterator(
        self,
        result_len: int,
        mock_actor_async_ctx,
        target_max_block_size_infinite_or_default,
    ):
        """Test UDF that yields multiple items per input."""

        async def multi_yield_fn(x):
            for i in range(result_len):
                yield f"processed_{x}_{i}"

        validate_fn = Mock()

        transform_fn = _generate_transform_fn_for_async_map(
            multi_yield_fn, validate_fn, max_concurrency=2
        )

        task_context = Mock()

        input_seq = [1, 2]

        # NOTE: Outputs are expected to match input sequence ordering
        expected = [f"processed_{x}_{i}" for x in input_seq for i in range(result_len)]

        assert list(transform_fn(input_seq, task_context)) == expected

    def test_concurrency_limiting(
        self,
        mock_actor_async_ctx,
        restore_data_context,
        target_max_block_size_infinite_or_default,
    ):
        """Test that concurrency is properly limited."""
        max_concurrency = 10

        concurrent_task_counter = 0

        async def async_fn(x):
            # NOTE: This is safe, since event-loop is single-threaded
            nonlocal concurrent_task_counter
            concurrent_task_counter += 1

            assert concurrent_task_counter <= max_concurrency

            yield x

            # NOTE: We're doing sleep here to interrupt the task and yield
            #       event loop to the next one (otherwise tasks will simply be
            #       completed sequentially)
            await asyncio.sleep(0.001)

            concurrent_task_counter -= 1

        validate_fn = Mock()

        transform_fn = _generate_transform_fn_for_async_map(
            async_fn, validate_fn, max_concurrency=max_concurrency
        )

        task_context = Mock()
        result = list(transform_fn(range(10_000), task_context))
        assert len(result) == 10_000

    @pytest.mark.parametrize("failure_kind", ["udf", "validation"])
    def test_exception_in_udf(
        self,
        failure_kind: str,
        mock_actor_async_ctx,
        target_max_block_size_infinite_or_default,
    ):
        """Test exception handling in UDF."""

        udf_failure_msg = "UDF failure"
        validation_failure_msg = "Validation failure"

        async def failing_async_fn(x):
            if failure_kind == "udf" and x == 2:
                raise ValueError(udf_failure_msg)
            yield x

        def validate_fn(x):
            if failure_kind == "validation" and x == 2:
                raise ValueError(validation_failure_msg)

        transform_fn = _generate_transform_fn_for_async_map(
            failing_async_fn, validate_fn, max_concurrency=2
        )

        task_context = Mock()

        if failure_kind == "udf":
            expected_exception_msg = udf_failure_msg
        elif failure_kind == "validation":
            expected_exception_msg = validation_failure_msg
        else:
            pytest.fail(f"Unexpected failure type ({failure_kind})")

        with pytest.raises(ValueError, match=expected_exception_msg):
            list(transform_fn([1, 2, 3], task_context))


@pytest.mark.parametrize("fn_type", ["func", "class"])
def test_map_operator_warns_on_few_inputs(
    fn_type: Literal["func", "class"],
    shutdown_only,
    target_max_block_size_infinite_or_default,
):
    if fn_type == "func":

        def fn(row):
            return row

    else:

        class fn:
            def __call__(self, row):
                return row

    with pytest.warns(UserWarning, match="can launch at most 1 task"):
        # The user specified `concurrency=2` for the map operator, but the pipeline
        # can only launch one task because there's only one input block. So, Ray Data
        # should emit a warning instructing the user to increase the number of input
        # blocks.
        ray.data.range(2, override_num_blocks=1).map(fn, concurrency=2).materialize()


def test_map_op_backpressure_configured_properly(
    target_max_block_size_infinite_or_default,
):
    """This test asserts that configuration of the MapOperator generator's back-pressure is
    propagated appropriately to the Ray Core
    """

    total = 5

    def _map_raising(r):
        if isinstance(r["item"], Exception):
            raise r["item"]

        return r

    # Reset this to make sure test is invariant of default value changes
    DataContext.get_current()._max_num_blocks_in_streaming_gen_buffer = 2

    # To simulate incremental iteration we are
    #   - Aggressively applying back-pressure (allowing no more than a single block
    #       to be in the queue)
    #   - Restrict Map Operator concurrency to run no more than 1 task at a time
    #
    # At the end of the pipeline we fetch only first 4 elements (instead of 5) to prevent the last 1
    # from executing (1 is going to be a buffered block)
    df = ray.data.from_items(
        list(range(5)) + [ValueError("failed!")], override_num_blocks=6
    )

    # NOTE: Default back-pressure configuration allows 2 blocks in the
    #       generator's buffer, hence default execution will fail as we'd
    #       try map all 6 elements
    with pytest.raises(RayTaskError) as exc_info:
        df.map(_map_raising).materialize()

    assert str(ValueError("failed")) in str(exc_info.value)

    # Reducing number of blocks in the generator buffer, will prevent this pipeline
    # from throwing
    vals = (
        df.map(
            _map_raising,
            concurrency=1,
            ray_remote_args_fn=lambda: {
                "_generator_backpressure_num_objects": 2,  # 1 for block, 1 for metadata
            },
        )
        .limit(total - 1)
        .take_batch()["item"]
        .tolist()
    )

    assert list(range(5))[:-1] == vals


@pytest.mark.skipif(
    get_pyarrow_version() < MIN_PYARROW_VERSION_TYPE_PROMOTION,
    reason="Requires pyarrow>=14 for unify_schemas in OneHotEncoder",
)
def test_map_names(target_max_block_size_infinite_or_default):
    """To test different UDF format such that the operator
    has the correct representation.

    The actual name is handled by
    AbstractUDFMap._get_operator_name()
    """

    ds = ray.data.range(5)

    r = ds.map(lambda x: {"id": str(x["id"])}).__repr__()
    assert r.startswith("Map(<lambda>)"), r

    class C:
        def __call__(self, x):
            return x

    r = ds.map(C, concurrency=4).__repr__()
    assert r.startswith("Map(C)"), r

    # Simple and partial functions
    def func(x, y):
        return x

    r = ds.map(func, fn_args=[0]).__repr__()
    assert r.startswith("Map(func)")

    from functools import partial

    r = ds.map(partial(func, y=1)).__repr__()
    assert r.startswith("Map(func)"), r

    # Preprocessor
    from ray.data.preprocessors import OneHotEncoder

    ds = ray.data.from_items(["a", "b", "c", "a", "b", "c"])
    enc = OneHotEncoder(columns=["item"])
    r = enc.fit_transform(ds).__repr__()
    assert r.startswith("OneHotEncoder"), r


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
@pytest.mark.parametrize(
    "column_name, expr, expected_value",
    [
        # Arithmetic operations
        ("result", col("id") + 1, 1),  # 0 + 1 = 1
        ("result", col("id") + 5, 5),  # 0 + 5 = 5
        ("result", col("id") - 1, -1),  # 0 - 1 = -1
        ("result", col("id") * 2, 0),  # 0 * 2 = 0
        ("result", col("id") * 3, 0),  # 0 * 3 = 0
        ("result", col("id") / 2, 0.0),  # 0 / 2 = 0.0
        # More complex arithmetic
        ("result", (col("id") + 1) * 2, 2),  # (0 + 1) * 2 = 2
        ("result", (col("id") * 2) + 3, 3),  # 0 * 2 + 3 = 3
        # Comparison operations
        ("result", col("id") > 0, False),  # 0 > 0 = False
        ("result", col("id") >= 0, True),  # 0 >= 0 = True
        ("result", col("id") < 1, True),  # 0 < 1 = True
        ("result", col("id") <= 0, True),  # 0 <= 0 = True
        ("result", col("id") == 0, True),  # 0 == 0 = True
        # Operations with literals
        ("result", col("id") + lit(10), 10),  # 0 + 10 = 10
        ("result", col("id") * lit(5), 0),  # 0 * 5 = 0
        ("result", lit(2) + col("id"), 2),  # 2 + 0 = 2
        ("result", lit(10) / (col("id") + 1), 10.0),  # 10 / (0 + 1) = 10.0
    ],
)
def test_with_column(
    ray_start_regular_shared,
    column_name,
    expr,
    expected_value,
    target_max_block_size_infinite_or_default,
):
    """Verify that `with_column` works with various operations."""
    ds = ray.data.range(5).with_column(column_name, expr)
    result = ds.take(1)[0]
    assert result["id"] == 0
    assert result[column_name] == expected_value


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_with_column_nonexistent_column(
    ray_start_regular_shared, target_max_block_size_infinite_or_default
):
    """Verify that referencing a non-existent column with col() raises an exception."""
    # Create a dataset with known column "id"
    ds = ray.data.range(5)

    # Try to reference a non-existent column - this should raise an exception
    with pytest.raises(UserCodeException):
        ds.with_column("result", col("nonexistent_column") + 1).materialize()


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_with_column_multiple_expressions(
    ray_start_regular_shared, target_max_block_size_infinite_or_default
):
    """Verify that `with_column` correctly handles multiple expressions at once."""
    ds = ray.data.range(5)

    ds = ds.with_column("plus_one", col("id") + 1)
    ds = ds.with_column("times_two", col("id") * 2)
    ds = ds.with_column("ten_minus_id", 10 - col("id"))

    first_row = ds.take(1)[0]
    assert first_row["id"] == 0
    assert first_row["plus_one"] == 1
    assert first_row["times_two"] == 0
    assert first_row["ten_minus_id"] == 10

    # Ensure all new columns exist in the schema.
    assert set(ds.schema().names) == {"id", "plus_one", "times_two", "ten_minus_id"}


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
@pytest.mark.parametrize(
    "udf_function, column_name, expected_result",
    [
        # Single column UDF - add one to each value
        pytest.param(
            lambda: udf(DataType.int64())(lambda x: pc.add(x, 1)),
            "add_one",
            1,  # 0 + 1 = 1
            id="single_column_add_one",
        ),
        # Single column UDF - multiply by 2
        pytest.param(
            lambda: udf(DataType.int64())(lambda x: pc.multiply(x, 2)),
            "times_two",
            0,  # 0 * 2 = 0
            id="single_column_multiply",
        ),
        # Single column UDF - square the value
        pytest.param(
            lambda: udf(DataType.int64())(lambda x: pc.multiply(x, x)),
            "squared",
            0,  # 0 * 0 = 0
            id="single_column_square",
        ),
        # Single column UDF with string return type
        pytest.param(
            lambda: udf(DataType.string())(lambda x: pc.cast(x, pa.string())),
            "id_str",
            "0",  # Convert 0 to "0"
            id="single_column_to_string",
        ),
        # Single column UDF with float return type
        pytest.param(
            lambda: udf(DataType.float64())(lambda x: pc.divide(x, 2.0)),
            "half",
            0.0,  # 0 / 2.0 = 0.0
            id="single_column_divide_float",
        ),
    ],
)
def test_with_column_udf_single_column(
    ray_start_regular_shared,
    udf_function,
    column_name,
    expected_result,
    target_max_block_size_infinite_or_default,
):
    """Test UDFExpr functionality with single column operations in with_column."""
    ds = ray.data.range(5)
    udf_fn = udf_function()

    # Apply the UDF to the "id" column
    ds_with_udf = ds.with_column(column_name, udf_fn(col("id")))

    result = ds_with_udf.take(1)[0]
    assert result["id"] == 0
    assert result[column_name] == expected_result


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
@pytest.mark.parametrize(
    "test_scenario",
    [
        # Multi-column UDF - add two columns
        pytest.param(
            {
                "data": [{"a": 1, "b": 2}, {"a": 3, "b": 4}],
                "udf": lambda: udf(DataType.int64())(lambda x, y: pc.add(x, y)),
                "column_name": "sum_ab",
                "expected_first": 3,  # 1 + 2 = 3
                "expected_second": 7,  # 3 + 4 = 7
            },
            id="multi_column_add",
        ),
        # Multi-column UDF - multiply two columns
        pytest.param(
            {
                "data": [{"x": 2, "y": 3}, {"x": 4, "y": 5}],
                "udf": lambda: udf(DataType.int64())(lambda x, y: pc.multiply(x, y)),
                "column_name": "product_xy",
                "expected_first": 6,  # 2 * 3 = 6
                "expected_second": 20,  # 4 * 5 = 20
            },
            id="multi_column_multiply",
        ),
        # Multi-column UDF - string concatenation
        pytest.param(
            {
                "data": [
                    {"first": "John", "last": "Doe"},
                    {"first": "Jane", "last": "Smith"},
                ],
                "udf": lambda: udf(DataType.string())(
                    lambda first, last: pc.binary_join_element_wise(first, last, " ")
                ),
                "column_name": "full_name",
                "expected_first": "John Doe",
                "expected_second": "Jane Smith",
            },
            id="multi_column_string_concat",
        ),
    ],
)
def test_with_column_udf_multi_column(
    ray_start_regular_shared,
    test_scenario,
    target_max_block_size_infinite_or_default,
):
    """Test UDFExpr functionality with multi-column operations in with_column."""
    data = test_scenario["data"]
    udf_fn = test_scenario["udf"]()
    column_name = test_scenario["column_name"]
    expected_first = test_scenario["expected_first"]
    expected_second = test_scenario["expected_second"]

    ds = ray.data.from_items(data)

    # Apply UDF to multiple columns based on the scenario
    if "a" in data[0] and "b" in data[0]:
        ds_with_udf = ds.with_column(column_name, udf_fn(col("a"), col("b")))
    elif "x" in data[0] and "y" in data[0]:
        ds_with_udf = ds.with_column(column_name, udf_fn(col("x"), col("y")))
    else:  # first/last name scenario
        ds_with_udf = ds.with_column(column_name, udf_fn(col("first"), col("last")))

    results = ds_with_udf.take(2)
    assert results[0][column_name] == expected_first
    assert results[1][column_name] == expected_second


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
@pytest.mark.parametrize(
    "expression_scenario",
    [
        # UDF in arithmetic expression
        pytest.param(
            {
                "expression_factory": lambda add_one_udf: add_one_udf(col("id")) * 2,
                "expected": 2,  # (0 + 1) * 2 = 2
                "column_name": "udf_times_two",
            },
            id="udf_in_arithmetic",
        ),
        # UDF with literal addition
        pytest.param(
            {
                "expression_factory": lambda add_one_udf: add_one_udf(col("id"))
                + lit(10),
                "expected": 11,  # (0 + 1) + 10 = 11
                "column_name": "udf_plus_literal",
            },
            id="udf_plus_literal",
        ),
        # UDF in comparison
        pytest.param(
            {
                "expression_factory": lambda add_one_udf: add_one_udf(col("id")) > 0,
                "expected": True,  # (0 + 1) > 0 = True
                "column_name": "udf_comparison",
            },
            id="udf_in_comparison",
        ),
        # Nested UDF operations (UDF + regular expression)
        pytest.param(
            {
                "expression_factory": lambda add_one_udf: add_one_udf(col("id") + 5),
                "expected": 6,  # add_one(0 + 5) = add_one(5) = 6
                "column_name": "nested_udf",
            },
            id="nested_udf_expression",
        ),
    ],
)
def test_with_column_udf_in_complex_expressions(
    ray_start_regular_shared,
    expression_scenario,
    target_max_block_size_infinite_or_default,
):
    """Test UDFExpr functionality in complex expressions with with_column."""
    ds = ray.data.range(5)

    # Create a simple add_one UDF for use in expressions
    @udf(DataType.int64())
    def add_one(x: pa.Array) -> pa.Array:
        return pc.add(x, 1)

    expression = expression_scenario["expression_factory"](add_one)
    expected = expression_scenario["expected"]
    column_name = expression_scenario["column_name"]

    ds_with_expr = ds.with_column(column_name, expression)

    result = ds_with_expr.take(1)[0]
    assert result["id"] == 0
    assert result[column_name] == expected


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_with_column_udf_multiple_udfs(
    ray_start_regular_shared, target_max_block_size_infinite_or_default
):
    """Test applying multiple UDFs in sequence with with_column."""
    ds = ray.data.range(5)

    # Define multiple UDFs
    @udf(DataType.int64())
    def add_one(x: pa.Array) -> pa.Array:
        return pc.add(x, 1)

    @udf(DataType.int64())
    def multiply_by_two(x: pa.Array) -> pa.Array:
        return pc.multiply(x, 2)

    @udf(DataType.float64())
    def divide_by_three(x: pa.Array) -> pa.Array:
        return pc.divide(x, 3.0)

    # Apply UDFs in sequence
    ds = ds.with_column("plus_one", add_one(col("id")))
    ds = ds.with_column("times_two", multiply_by_two(col("plus_one")))
    ds = ds.with_column("div_three", divide_by_three(col("times_two")))

    # Convert to pandas and compare with expected result
    result_df = ds.to_pandas()

    expected_df = pd.DataFrame(
        {
            "id": [0, 1, 2, 3, 4],
            "plus_one": [1, 2, 3, 4, 5],  # id + 1
            "times_two": [2, 4, 6, 8, 10],  # (id + 1) * 2
            "div_three": [
                2.0 / 3.0,
                4.0 / 3.0,
                2.0,
                8.0 / 3.0,
                10.0 / 3.0,
            ],  # ((id + 1) * 2) / 3
        }
    )

    pd.testing.assert_frame_equal(result_df, expected_df)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_with_column_mixed_udf_and_regular_expressions(
    ray_start_regular_shared, target_max_block_size_infinite_or_default
):
    """Test mixing UDF expressions and regular expressions in with_column operations."""
    ds = ray.data.range(5)

    # Define a UDF for testing
    @udf(DataType.int64())
    def multiply_by_three(x: pa.Array) -> pa.Array:
        return pc.multiply(x, 3)

    # Mix regular expressions and UDF expressions
    ds = ds.with_column("plus_ten", col("id") + 10)  # Regular expression
    ds = ds.with_column("times_three", multiply_by_three(col("id")))  # UDF expression
    ds = ds.with_column("minus_five", col("id") - 5)  # Regular expression
    ds = ds.with_column(
        "udf_plus_regular", multiply_by_three(col("id")) + col("plus_ten")
    )  # Mixed: UDF + regular
    ds = ds.with_column(
        "comparison", col("times_three") > col("plus_ten")
    )  # Regular expression using UDF result

    # Convert to pandas and compare with expected result
    result_df = ds.to_pandas()

    expected_df = pd.DataFrame(
        {
            "id": [0, 1, 2, 3, 4],
            "plus_ten": [10, 11, 12, 13, 14],  # id + 10
            "times_three": [0, 3, 6, 9, 12],  # id * 3
            "minus_five": [-5, -4, -3, -2, -1],  # id - 5
            "udf_plus_regular": [10, 14, 18, 22, 26],  # (id * 3) + (id + 10)
            "comparison": [False, False, False, False, False],  # times_three > plus_ten
        }
    )

    pd.testing.assert_frame_equal(result_df, expected_df)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_with_column_udf_invalid_return_type_validation(
    ray_start_regular_shared, target_max_block_size_infinite_or_default
):
    """Test that UDFs returning invalid types raise TypeError with clear message."""
    ds = ray.data.range(3)

    # Test UDF returning invalid type (dict) - expecting string but returning dict
    @udf(DataType.string())
    def invalid_dict_return(x: pa.Array) -> dict:
        return {"invalid": "return_type"}

    # Test UDF returning invalid type (str) - expecting string but returning plain str
    @udf(DataType.string())
    def invalid_str_return(x: pa.Array) -> str:
        return "invalid_string"

    # Test UDF returning invalid type (int) - expecting int64 but returning plain int
    @udf(DataType.int64())
    def invalid_int_return(x: pa.Array) -> int:
        return 42

    # Test each invalid return type
    test_cases = [
        (invalid_dict_return, "dict"),
        (invalid_str_return, "str"),
        (invalid_int_return, "int"),
    ]

    for invalid_udf, expected_type_name in test_cases:
        with pytest.raises((RayTaskError, UserCodeException)) as exc_info:
            ds.with_column("invalid_col", invalid_udf(col("id"))).take(1)

        # The actual TypeError gets wrapped, so we need to check the exception chain
        error_message = str(exc_info.value)
        assert f"returned invalid type {expected_type_name}" in error_message
        assert "Expected type" in error_message
        assert "pandas.Series" in error_message and "numpy.ndarray" in error_message


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
@pytest.mark.parametrize(
    "scenario",
    [
        pytest.param(
            {
                "data": [
                    {"name": "Alice"},
                    {"name": "Bob"},
                    {"name": "Charlie"},
                ],
                "expr_factory": lambda: col("name") + "_X",
                "column_name": "name_with_suffix",
                "expected": ["Alice_X", "Bob_X", "Charlie_X"],
            },
            id="string_col_plus_python_literal_rhs",
        ),
        pytest.param(
            {
                "data": [
                    {"name": "Alice"},
                    {"name": "Bob"},
                    {"name": "Charlie"},
                ],
                "expr_factory": lambda: "_X" + col("name"),
                "column_name": "name_with_prefix",
                "expected": ["_XAlice", "_XBob", "_XCharlie"],
            },
            id="python_literal_lhs_plus_string_col",
        ),
        pytest.param(
            {
                "data": [
                    {"first": "John", "last": "Doe"},
                    {"first": "Jane", "last": "Smith"},
                ],
                "expr_factory": lambda: col("first") + col("last"),
                "column_name": "full_name",
                "expected": ["JohnDoe", "JaneSmith"],
            },
            id="string_col_plus_string_col",
        ),
        pytest.param(
            {
                "arrow_table": pa.table(
                    {"name": pa.array(["Alice", "Bob"]).dictionary_encode()}
                ),
                "expr_factory": lambda: col("name") + "_X",
                "column_name": "name_with_suffix",
                "expected": ["Alice_X", "Bob_X"],
            },
            id="dict_encoded_string_col_plus_literal_rhs",
        ),
        pytest.param(
            {
                "data": [
                    {"name": "Alice"},
                    {"name": "Bob"},
                ],
                "expr_factory": lambda: col("name") + lit("_X"),
                "column_name": "name_with_suffix",
                "expected": ["Alice_X", "Bob_X"],
            },
            id="string_col_plus_lit_literal_rhs",
        ),
    ],
)
def test_with_column_string_concat_combinations(
    ray_start_regular_shared,
    scenario,
):
    if "arrow_table" in scenario:
        ds = ray.data.from_arrow(scenario["arrow_table"])
    else:
        ds = ray.data.from_items(scenario["data"])

    expr = scenario["expr_factory"]()
    column_name = scenario["column_name"]

    ds2 = ds.with_column(column_name, expr)
    out = ds2.to_pandas()
    assert out[column_name].tolist() == scenario["expected"]


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_with_column_string_concat_type_mismatch_raises(
    ray_start_regular_shared,
):
    # int + string should raise a user-facing error
    ds = ray.data.range(3)
    with pytest.raises((RayTaskError, UserCodeException)):
        ds.with_column("bad", col("id") + "_X").materialize()


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
@pytest.mark.parametrize(
    "expression, expected_column_data, test_description",
    [
        # Floor division operations
        pytest.param(
            col("id") // 2,
            [0, 0, 1, 1, 2],  # [0//2, 1//2, 2//2, 3//2, 4//2]
            "floor_division_by_literal",
        ),
        pytest.param(
            lit(10) // (col("id") + 2),
            [5, 3, 2, 2, 1],  # [10//(0+2), 10//(1+2), 10//(2+2), 10//(3+2), 10//(4+2)]
            "literal_floor_division_by_expression",
        ),
        # Not equal operations
        pytest.param(
            col("id") != 2,
            [True, True, False, True, True],  # [0!=2, 1!=2, 2!=2, 3!=2, 4!=2]
            "not_equal_operation",
        ),
        # Null checking operations
        pytest.param(
            col("id").is_null(),
            [False, False, False, False, False],  # None of the values are null
            "is_null_operation",
        ),
        pytest.param(
            col("id").is_not_null(),
            [True, True, True, True, True],  # All values are not null
            "is_not_null_operation",
        ),
        # Logical NOT operations
        pytest.param(
            ~(col("id") == 2),
            [True, True, False, True, True],  # ~[0==2, 1==2, 2==2, 3==2, 4==2]
            "logical_not_operation",
        ),
    ],
)
def test_with_column_floor_division_and_logical_operations(
    ray_start_regular_shared,
    expression,
    expected_column_data,
    test_description,
):
    """Test floor division, not equal, null checks, and logical NOT operations with with_column."""
    ds = ray.data.range(5)
    result_ds = ds.with_column("result", expression)

    # Convert to pandas and assert on the whole dataframe
    result_df = result_ds.to_pandas()
    expected_df = pd.DataFrame({"id": [0, 1, 2, 3, 4], "result": expected_column_data})

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
@pytest.mark.parametrize(
    "test_data, expression, expected_results, test_description",
    [
        # Test with null values
        pytest.param(
            [{"value": 1}, {"value": None}, {"value": 3}],
            col("value").is_null(),
            [False, True, False],
            "is_null_with_actual_nulls",
        ),
        pytest.param(
            [{"value": 1}, {"value": None}, {"value": 3}],
            col("value").is_not_null(),
            [True, False, True],
            "is_not_null_with_actual_nulls",
        ),
        # Test is_in operations
        pytest.param(
            [{"value": 1}, {"value": 2}, {"value": 3}],
            col("value").is_in([1, 3]),
            [True, False, True],
            "isin_operation",
        ),
        pytest.param(
            [{"value": 1}, {"value": 2}, {"value": 3}],
            col("value").not_in([1, 3]),
            [False, True, False],
            "not_in_operation",
        ),
        # Test string operations
        pytest.param(
            [{"name": "Alice"}, {"name": "Bob"}, {"name": "Charlie"}],
            col("name") == "Bob",
            [False, True, False],
            "string_equality",
        ),
        pytest.param(
            [{"name": "Alice"}, {"name": "Bob"}, {"name": "Charlie"}],
            col("name") != "Bob",
            [True, False, True],
            "string_not_equal",
        ),
        # Filter with string operations - accept engine's null propagation
        pytest.param(
            [
                {"name": "included"},
                {"name": "excluded"},
                {"name": None},
            ],
            col("name").is_not_null() & (col("name") != "excluded"),
            [True, False, False],
            "string_filter",
        ),
    ],
)
def test_with_column_null_checks_and_membership_operations(
    ray_start_regular_shared,
    test_data,
    expression,
    expected_results,
    test_description,
    target_max_block_size_infinite_or_default,
):
    """Test null checking, is_in/not_in membership operations, and string comparisons with with_column."""
    ds = ray.data.from_items(test_data)
    result_ds = ds.with_column("result", expression)

    # Convert to pandas and assert on the whole dataframe
    result_df = result_ds.to_pandas()

    # Create expected dataframe from test data
    expected_data = {}
    for key in test_data[0].keys():
        expected_data[key] = [row[key] for row in test_data]
    expected_data["result"] = expected_results

    expected_df = pd.DataFrame(expected_data)

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
@pytest.mark.parametrize(
    "expression_factory, expected_results, test_description",
    [
        # Complex boolean expressions
        pytest.param(
            lambda: (col("age") > 18) & (col("country") == "USA"),
            [
                True,
                False,
                False,
            ],  # [(25>18)&("USA"=="USA"), (17>18)&("Canada"=="USA"), (30>18)&("UK"=="USA")]
            "complex_and_expression",
        ),
        pytest.param(
            lambda: (col("age") < 18) | (col("country") == "USA"),
            [
                True,
                True,
                False,
            ],  # [(25<18)|("USA"=="USA"), (17<18)|("Canada"=="USA"), (30<18)|("UK"=="USA")]
            "complex_or_expression",
        ),
        pytest.param(
            lambda: ~((col("age") < 25) & (col("country") != "USA")),
            [
                True,
                False,
                True,
            ],  # ~[(25<25)&("USA"!="USA"), (17<25)&("Canada"!="USA"), (30<25)&("UK"!="USA")]
            "complex_not_expression",
        ),
        # Age group calculation (common use case)
        pytest.param(
            lambda: col("age") // 10 * 10,
            [20, 10, 30],  # [25//10*10, 17//10*10, 30//10*10]
            "age_group_calculation",
        ),
        # Eligibility flags
        pytest.param(
            lambda: (col("age") >= 21)
            & (col("score") >= 10)
            & col("active").is_not_null()
            & (col("active") == lit(True)),
            [
                True,
                False,
                False,
            ],
            "eligibility_flag",
        ),
    ],
)
def test_with_column_complex_boolean_expressions(
    ray_start_regular_shared,
    expression_factory,
    expected_results,
    test_description,
    target_max_block_size_infinite_or_default,
):
    """Test complex boolean expressions with AND, OR, NOT operations commonly used for filtering and flagging."""
    test_data = [
        {"age": 25, "country": "USA", "active": True, "score": 20},
        {"age": 17, "country": "Canada", "active": False, "score": 10},
        {"age": 30, "country": "UK", "active": None, "score": 20},
    ]

    ds = ray.data.from_items(test_data)
    expression = expression_factory()
    result_ds = ds.with_column("result", expression)

    # Convert to pandas and assert on the whole dataframe
    result_df = result_ds.to_pandas()
    expected_df = pd.DataFrame(
        {
            "age": [25, 17, 30],
            "country": ["USA", "Canada", "UK"],
            "active": [True, False, None],
            "score": [20, 10, 20],
            "result": expected_results,
        }
    )

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_with_column_chained_expression_operations(
    ray_start_regular_shared, target_max_block_size_infinite_or_default
):
    """Test chaining multiple expression operations together in a data transformation pipeline."""
    test_data = [
        {"age": 25, "salary": 50000, "active": True, "score": 20},
        {"age": 17, "salary": 0, "active": False, "score": 10},
        {"age": 35, "salary": 75000, "active": None, "score": 20},
    ]

    ds = ray.data.from_items(test_data)

    # Chain multiple operations
    result_ds = (
        ds.with_column("is_adult", col("age") >= 18)
        .with_column("age_group", (col("age") // 10) * 10)
        .with_column("has_salary", col("salary") != 0)
        .with_column(
            "is_active_adult", (col("age") >= 18) & col("active").is_not_null()
        )
        .with_column("salary_tier", (col("salary") // 25000) * 25000)
        .with_column("score_tier", (col("score") // 20) * 20)
    )

    # Convert to pandas and assert on the whole dataframe
    result_df = result_ds.to_pandas()
    expected_df = pd.DataFrame(
        {
            "age": [25, 17, 35],
            "salary": [50000, 0, 75000],
            "active": [True, False, None],
            "score": [20, 10, 20],  # Add the missing score column
            "is_adult": [True, False, True],
            "age_group": [20, 10, 30],  # age // 10 * 10
            "has_salary": [True, False, True],  # salary != 0
            "is_active_adult": [
                True,
                False,
                False,
            ],  # (age >= 18) & (active is not null)
            "salary_tier": [50000, 0, 75000],  # salary // 25000 * 25000
            "score_tier": [20, 0, 20],  # score // 20 * 20
        }
    )

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
@pytest.mark.parametrize(
    "filter_expr, test_data, expected_flags, test_description",
    [
        # Simple filter expressions
        pytest.param(
            col("age") >= 21,
            [
                {"age": 20, "name": "Alice"},
                {"age": 21, "name": "Bob"},
                {"age": 25, "name": "Charlie"},
            ],
            [False, True, True],
            "age_filter",
        ),
        pytest.param(
            col("score") > 50,
            [
                {"score": 30, "status": "fail"},
                {"score": 50, "status": "pass"},
                {"score": 70, "status": "pass"},
            ],
            [False, False, True],
            "score_filter",
        ),
        # Complex filter with multiple conditions
        pytest.param(
            (col("age") >= 18) & col("active"),
            [
                {"age": 17, "active": True},
                {"age": 18, "active": False},
                {"age": 25, "active": True},
            ],
            [False, False, True],
            "complex_and_filter",
        ),
        pytest.param(
            (col("status") == "approved") | (col("priority") == "high"),
            [
                {"status": "pending", "priority": "low"},
                {"status": "approved", "priority": "low"},
                {"status": "pending", "priority": "high"},
            ],
            [False, True, True],
            "complex_or_filter",
        ),
        # Filter with null handling
        pytest.param(
            col("value").is_not_null() & (col("value") > 0),
            [
                {"value": None},
                {"value": -5},
                {"value": 10},
            ],
            [
                False,
                False,
                True,
            ],
            "null_aware_filter",
        ),
        # Filter with string operations - reorder to check null first
        pytest.param(
            col("name").is_not_null() & (col("name") != "excluded"),
            [
                {"name": "included"},
                {"name": "excluded"},
                {"name": None},
            ],
            [True, False, False],
            "string_filter",
        ),
        # Filter with membership operations
        pytest.param(
            col("category").is_in(["A", "B"]),
            [
                {"category": "A"},
                {"category": "B"},
                {"category": "C"},
                {"category": "D"},
            ],
            [True, True, False, False],
            "membership_filter",
        ),
        # Nested filter expressions
        pytest.param(
            (col("score") >= 50) & (col("grade") != "F"),
            [
                {"score": 45, "grade": "F"},
                {"score": 55, "grade": "D"},
                {"score": 75, "grade": "B"},
                {"score": 30, "grade": "F"},
            ],
            [False, True, True, False],
            "nested_filters",
        ),
    ],
)
def test_with_column_filter_expressions(
    ray_start_regular_shared,
    filter_expr,
    test_data,
    expected_flags,
    test_description,
):
    """Test filter() expression functionality with with_column for creating boolean flag columns."""
    ds = ray.data.from_items(test_data)
    result_ds = ds.with_column("is_filtered", filter_expr)

    # Convert to pandas and verify the filter results
    result_df = result_ds.to_pandas()

    # Build expected dataframe
    expected_df = pd.DataFrame(test_data)
    expected_df["is_filtered"] = expected_flags

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_with_column_filter_in_pipeline(ray_start_regular_shared):
    """Test filter() expressions used in a data processing pipeline with multiple transformations."""
    # Create test data for a sales analysis pipeline
    test_data = [
        {"product": "A", "quantity": 10, "price": 100, "region": "North"},
        {"product": "B", "quantity": 5, "price": 200, "region": "South"},
        {"product": "C", "quantity": 20, "price": 50, "region": "North"},
        {"product": "D", "quantity": 15, "price": 75, "region": "East"},
        {"product": "E", "quantity": 3, "price": 300, "region": "West"},
    ]

    ds = ray.data.from_items(test_data)

    # Build a pipeline with multiple filter expressions
    result_ds = (
        ds
        # Calculate total revenue
        .with_column("revenue", col("quantity") * col("price"))
        # Flag high-value transactions
        .with_column("is_high_value", col("revenue") >= 1000)
        # Flag bulk orders
        .with_column("is_bulk_order", col("quantity") >= 10)
        # Flag premium products
        .with_column("is_premium", col("price") >= 100)
        # Create composite filter for special handling
        .with_column(
            "needs_special_handling",
            (col("is_high_value")) | (col("is_bulk_order") & col("is_premium")),
        )
        # Regional filter
        .with_column("is_north_region", col("region") == "North")
    )

    # Convert to pandas and verify
    result_df = result_ds.to_pandas()

    expected_df = pd.DataFrame(
        {
            "product": ["A", "B", "C", "D", "E"],
            "quantity": [10, 5, 20, 15, 3],
            "price": [100, 200, 50, 75, 300],
            "region": ["North", "South", "North", "East", "West"],
            "revenue": [1000, 1000, 1000, 1125, 900],
            "is_high_value": [True, True, True, True, False],
            "is_bulk_order": [True, False, True, True, False],
            "is_premium": [True, True, False, False, True],
            "needs_special_handling": [True, True, True, True, False],
            "is_north_region": [True, False, True, False, False],
        }
    )

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)


@pytest.mark.parametrize(
    "expr_factory, expected_columns, alias_name, expected_values",
    [
        (
            lambda: col("id").alias("new_id"),
            ["id", "new_id"],
            "new_id",
            [0, 1, 2, 3, 4],  # Copy of id column
        ),
        (
            lambda: (col("id") + 1).alias("id_plus_one"),
            ["id", "id_plus_one"],
            "id_plus_one",
            [1, 2, 3, 4, 5],  # id + 1
        ),
        (
            lambda: (col("id") * 2 + 5).alias("transformed"),
            ["id", "transformed"],
            "transformed",
            [5, 7, 9, 11, 13],  # id * 2 + 5
        ),
        (
            lambda: lit(42).alias("constant"),
            ["id", "constant"],
            "constant",
            [42, 42, 42, 42, 42],  # lit(42)
        ),
        (
            lambda: (col("id") >= 0).alias("is_non_negative"),
            ["id", "is_non_negative"],
            "is_non_negative",
            [True, True, True, True, True],  # id >= 0
        ),
        (
            lambda: (col("id") + 1).alias("id"),
            ["id"],  # Only one column since we're overwriting id
            "id",
            [1, 2, 3, 4, 5],  # id + 1 replaces original id
        ),
    ],
    ids=[
        "col_alias",
        "arithmetic_alias",
        "complex_alias",
        "literal_alias",
        "comparison_alias",
        "overwrite_existing_column",
    ],
)
def test_with_column_alias_expressions(
    ray_start_regular_shared,
    expr_factory,
    expected_columns,
    alias_name,
    expected_values,
):
    """Test that alias expressions work correctly with with_column."""
    expr = expr_factory()

    # Verify the alias name matches what we expect
    assert expr.name == alias_name

    # Apply the aliased expression
    ds = ray.data.range(5).with_column(alias_name, expr)

    # Convert to pandas for comprehensive comparison
    result_df = ds.to_pandas()

    # Create expected DataFrame
    expected_df = pd.DataFrame({"id": [0, 1, 2, 3, 4], alias_name: expected_values})

    # Ensure column order matches expected_columns
    expected_df = expected_df[expected_columns]

    # Assert the entire DataFrame is equal
    pd.testing.assert_frame_equal(result_df, expected_df)
    # Verify the alias expression evaluates the same as the non-aliased version
    non_aliased_expr = expr
    ds_non_aliased = ray.data.range(5).with_column(alias_name, non_aliased_expr)

    non_aliased_df = ds_non_aliased.to_pandas()

    pd.testing.assert_frame_equal(result_df, non_aliased_df)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
