import asyncio
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
import pytest

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
from ray.data.exceptions import UserCodeException
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.test_util import ConcurrencyCounter  # noqa
from ray.data.tests.util import extract_values
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


@pytest.mark.parametrize(
    "concurrency",
    [
        "spam",
        # Two and three-tuples are valid for callable classes but not for functions.
        (1, 2),
        (1, 2, 3),
        (1, 2, 3, 4),
    ],
)
def test_invalid_func_concurrency_raises(ray_start_regular_shared, concurrency):
    ds = ray.data.range(1)
    with pytest.raises(ValueError):
        ds.map(lambda x: x, concurrency=concurrency)


@pytest.mark.parametrize("concurrency", ["spam", (1, 2, 3, 4)])
def test_invalid_class_concurrency_raises(ray_start_regular_shared, concurrency):
    class Fn:
        def __call__(self, row):
            return row

    ds = ray.data.range(1)
    with pytest.raises(ValueError):
        ds.map(Fn, concurrency=concurrency)


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
def test_actor_udf_cleanup(
    shutdown_only,
    tmp_path,
    restore_data_context,
    target_max_block_size_infinite_or_default,
):
    """Test that for the actor map operator, the UDF object is deleted properly."""
    ray.shutdown()
    ray.init(num_cpus=2)
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
    assert "OneHotEncoder" in r, r


def test_map_with_max_calls():

    ds = ray.data.range(10)

    # OK to set 'max_calls' as static option
    ds = ds.map(lambda x: x, max_calls=1)

    assert ds.count() == 10

    ds = ray.data.range(10)

    # Not OK to set 'max_calls' as dynamic option
    with pytest.raises(ValueError):
        ds = ds.map(
            lambda x: x,
            ray_remote_args_fn=lambda: {"max_calls": 1},
        )
        ds.take_all()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
