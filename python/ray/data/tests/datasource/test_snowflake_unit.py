"""Tests for the Snowflake datasource and datasink that don't need a Snowflake
account. `test_snowflake.py` has the tests that run against Snowflake."""

import os
from datetime import datetime
from typing import Any, Iterator, List, Optional
from unittest import mock

import pandas as pd
import pyarrow as pa
import pytest
from snowflake.connector.result_batch import (
    ArrowResultBatch,
    JSONResultBatch,
)

import ray
from ray.data._internal.datasource.snowflake_datasink import (
    SnowflakeDatasink,
    _ObjectName,
    _parse_object_name,
)
from ray.data._internal.datasource.snowflake_datasource import SnowflakeDatasource
from ray.data._internal.object_extensions.arrow import ArrowPythonObjectArray
from ray.tests.conftest import *  # noqa

CONNECTION_PARAMETERS = {"user": "user", "account": "account"}


class FakeResultBatch(ArrowResultBatch):
    """Stands in for `snowflake.connector.result_batch.ArrowResultBatch`."""

    def __init__(self, table: pa.Table):
        self._table = table
        self.rowcount = table.num_rows

    def to_arrow(
        self,
        connection: Optional[Any] = None,
        force_microsecond_precision: bool = False,
    ) -> pa.Table:
        table = self._table
        if force_microsecond_precision:
            # Stand in for the connector's downscale, which only ever hits the
            # `timestamp[ns]` columns it would otherwise produce.
            schema = pa.schema(
                [
                    field.with_type(pa.timestamp("us", field.type.tz))
                    if pa.types.is_timestamp(field.type) and field.type.unit == "ns"
                    else field
                    for field in table.schema
                ]
            )
            table = table.cast(schema, safe=False)
        return table


def make_result_batches(rows_per_batch: List[int]) -> List[FakeResultBatch]:
    batches = []
    start = 0
    for num_rows in rows_per_batch:
        batches.append(
            FakeResultBatch(pa.table({"id": range(start, start + num_rows)}))
        )
        start += num_rows
    return batches


@pytest.fixture
def mock_connect() -> Iterator[mock.MagicMock]:
    with mock.patch("snowflake.connector.connect") as mock_connect:
        yield mock_connect


@pytest.fixture
def mock_cursor(mock_connect: mock.MagicMock) -> mock.MagicMock:
    connection = mock_connect.return_value.__enter__.return_value
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.description = [("id",)]
    return cursor


@pytest.mark.parametrize(
    "rows_per_batch, parallelism, expected_rows_per_task",
    [
        # Each read task gets a contiguous group of batches.
        ([100, 100, 100, 100], 2, [200, 200]),
        ([100, 100, 100, 100], 3, [200, 100, 100]),
        # There's at most one read task per batch.
        ([100, 100], 8, [100, 100]),
        # Batches without rows are dropped.
        ([100, 0, 100], 8, [100, 100]),
        # Each read task gets at least `MIN_ROWS_PER_READ_TASK` rows.
        ([10, 10, 10], 8, [30]),
    ],
)
def test_read_tasks(mock_cursor, rows_per_batch, parallelism, expected_rows_per_task):
    mock_cursor.get_result_batches.return_value = make_result_batches(rows_per_batch)
    datasource = SnowflakeDatasource("SELECT * FROM T", CONNECTION_PARAMETERS)

    read_tasks = datasource.get_read_tasks(parallelism)

    assert [task.metadata.num_rows for task in read_tasks] == expected_rows_per_task
    rows = [
        row
        for task in read_tasks
        for block in task()
        for row in block["id"].to_pylist()
    ]
    assert rows == list(range(sum(rows_per_batch)))


def test_read_asks_for_arrow_result_format(mock_connect, mock_cursor):
    """`to_arrow` only works on Arrow batches, so the query has to ask for them."""
    mock_cursor.get_result_batches.return_value = make_result_batches([100])
    connection_parameters = {
        **CONNECTION_PARAMETERS,
        "session_parameters": {"QUERY_TAG": "ray"},
    }
    datasource = SnowflakeDatasource("SELECT * FROM T", connection_parameters)

    datasource.get_read_tasks(1)

    _, kwargs = mock_connect.call_args
    assert kwargs["session_parameters"] == {
        # The datasource pins the format without dropping what the caller passed.
        "QUERY_TAG": "ray",
        "PYTHON_CONNECTOR_QUERY_RESULT_FORMAT": "ARROW",
    }
    # The caller's parameters aren't mutated.
    assert connection_parameters["session_parameters"] == {"QUERY_TAG": "ray"}


def test_read_rejects_json_result_batches(mock_cursor):
    """A connector that can't use Arrow returns JSON batches whose `to_arrow` raises.

    It overrides the format per statement, so asking for Arrow isn't enough. Fail
    on the driver with a message that says what to do, rather than inside a read
    task with `NotSupportedError`.
    """
    mock_cursor.get_result_batches.return_value = [mock.Mock(spec=JSONResultBatch)]
    datasource = SnowflakeDatasource("SELECT * FROM T", CONNECTION_PARAMETERS)

    with pytest.raises(RuntimeError, match="needs Snowflake's Arrow result format"):
        datasource.get_read_tasks(1)


def test_read_runs_query_once(mock_cursor):
    mock_cursor.get_result_batches.return_value = make_result_batches([100] * 4)
    datasource = SnowflakeDatasource("SELECT * FROM T", CONNECTION_PARAMETERS)

    datasource.get_read_tasks(2)
    datasource.get_read_tasks(4)

    mock_cursor.execute.assert_called_once_with("SELECT * FROM T")


@pytest.mark.parametrize("result_batches", [None, [], make_result_batches([0])])
def test_read_empty_result(mock_cursor, result_batches):
    mock_cursor.get_result_batches.return_value = result_batches
    mock_cursor.description = [("id",), ("name",)]
    datasource = SnowflakeDatasource("SELECT * FROM T", CONNECTION_PARAMETERS)

    read_tasks = datasource.get_read_tasks(2)

    assert len(read_tasks) == 1
    blocks = list(read_tasks[0]())
    assert len(blocks) == 1
    assert blocks[0].num_rows == 0
    assert blocks[0].column_names == ["id", "name"]


# The connector reads a scale 7-9 `TIMESTAMP` as `timestamp[ns]`, but downscales a
# batch holding a value outside the nanosecond range to `timestamp[us]`.
def test_read_pins_timestamps_to_microseconds(mock_cursor):
    """Batches that disagree on a timestamp's unit must still share a schema."""
    nanoseconds = pa.table({"ts": pa.array([1_000_000_123], type=pa.timestamp("ns"))})
    # A `9999-12-31` end date overflows `timestamp[ns]`, so the connector reads this
    # batch as microseconds no matter what the datasource asks for.
    microseconds = pa.table(
        {"ts": pa.array([253_402_214_400_000_000], type=pa.timestamp("us"))}
    )
    mock_cursor.get_result_batches.return_value = [
        FakeResultBatch(nanoseconds),
        FakeResultBatch(microseconds),
    ]
    mock_cursor.description = [("ts",)]
    datasource = SnowflakeDatasource("SELECT * FROM T", CONNECTION_PARAMETERS)

    read_tasks = datasource.get_read_tasks(2)
    blocks = [block for read_task in read_tasks for block in read_task()]

    assert len(blocks) == 2
    assert all(block.schema.field("ts").type == pa.timestamp("us") for block in blocks)
    # The schema the datasource reports has to match the blocks it produces.
    assert all(read_task.schema == blocks[0].schema for read_task in read_tasks)
    assert pa.concat_tables(blocks).column("ts").to_pylist() == [
        datetime(1970, 1, 1, 0, 0, 1),
        datetime(9999, 12, 31),
    ]


def test_read_rejects_old_connector():
    """`force_microsecond_precision` needs connector 4.3.0, so say so up front."""
    with mock.patch("importlib.metadata.version", return_value="4.2.0"):
        with pytest.raises(ImportError, match="4.3.0 or later, but found 4.2.0"):
            SnowflakeDatasource("SELECT * FROM T", CONNECTION_PARAMETERS)


# The driver downloads the first batch to plan the read, and read tasks download
# the rest, so test a planted payload in each.
@pytest.mark.parametrize("poisoned_index", [0, 1])
def test_read_rejects_pickle_object_columns(mock_cursor, tmp_path, poisoned_index):
    """A result batch carrying a pickled-object column must be rejected before
    anything is unpickled."""
    marker = tmp_path / "exploit_marker"

    class Exploit:
        def __reduce__(self):
            return (os.system, (f"touch {marker}",))

    poisoned = pa.table(
        {"id": [1, 2], "evil": ArrowPythonObjectArray.from_objects([Exploit()] * 2)}
    )
    batches = make_result_batches([2, 2])
    batches[poisoned_index] = FakeResultBatch(poisoned)
    mock_cursor.get_result_batches.return_value = batches
    datasource = SnowflakeDatasource("SELECT * FROM T", CONNECTION_PARAMETERS)

    with pytest.raises(ValueError, match="arrow_pickled_object"):
        for read_task in datasource.get_read_tasks(2):
            list(read_task())

    assert not marker.exists(), "pickle.load executed attacker code"


def test_read_snowflake(ray_start_regular_shared, mock_cursor):
    mock_cursor.get_result_batches.return_value = make_result_batches([100] * 4)

    ds = ray.data.read_snowflake(
        "SELECT * FROM T", CONNECTION_PARAMETERS, override_num_blocks=2
    )

    assert ds.schema().names == ["id"]
    assert sorted(row["id"] for row in ds.take_all()) == list(range(400))
    mock_cursor.execute.assert_called_once_with("SELECT * FROM T")


def test_read_snowflake_with_shard_keys_uses_read_sql():
    with mock.patch("ray.data.read_sql") as mock_read_sql:
        ray.data.read_snowflake(
            "SELECT * FROM T", CONNECTION_PARAMETERS, shard_keys=["id"]
        )

    _, kwargs = mock_read_sql.call_args
    assert kwargs["shard_keys"] == ["id"]
    assert kwargs["shard_hash_fn"] == "hash"


@pytest.mark.parametrize(
    "name, expected",
    [
        ("my_table", _ObjectName(None, None, "MY_TABLE")),
        ("my_schema.my_table", _ObjectName(None, "MY_SCHEMA", "MY_TABLE")),
        ("my_db.my_schema.my_table", _ObjectName("MY_DB", "MY_SCHEMA", "MY_TABLE")),
        ('"My Table"', _ObjectName(None, None, "My Table")),
        (
            'my_db."My.Schema"."My ""quoted"" table"',
            _ObjectName("MY_DB", "My.Schema", 'My "quoted" table'),
        ),
        # Unquoted names can hold digits, `_` and `$`, and can lead with `_`.
        ("_my$table1", _ObjectName(None, None, "_MY$TABLE1")),
        # Double-quoting is the escape hatch for everything else.
        ('"my-table"', _ObjectName(None, None, "my-table")),
    ],
)
def test_parse_object_name(name, expected):
    object_name = _parse_object_name(name)

    assert object_name == expected
    assert (object_name.database, object_name.schema, object_name.table) == expected


@pytest.mark.parametrize(
    "name",
    [
        "",
        "a.b.c.d",
        "a..b",
        ".a",
        '"unterminated',
        'a."b',
        '""',
        # `write_pandas` quotes every part it's given, so a name Snowflake would
        # reject unquoted has to be rejected here. Otherwise it silently becomes a
        # different, quoted table that `auto_create_table` creates.
        "my-table",
        "my_schema. my_table",
        "my table",
        "1table",
        "$table",
        "DROP TABLE x",
    ],
)
def test_parse_object_name_rejects_invalid_names(name):
    with pytest.raises(ValueError, match="Invalid Snowflake table name"):
        _parse_object_name(name)


def test_write():
    datasink = SnowflakeDatasink("my_schema.my_table", CONNECTION_PARAMETERS)
    blocks = [pa.table({"id": [0, 1]}), pa.table({"id": [2]})]

    with mock.patch("snowflake.connector.connect") as mock_connect, mock.patch(
        "snowflake.connector.pandas_tools.write_pandas"
    ) as mock_write_pandas:
        datasink.write(blocks, ctx=mock.Mock())

    mock_connect.assert_called_once_with(**CONNECTION_PARAMETERS)
    # Each write task loads all of its blocks with one `write_pandas` call.
    mock_write_pandas.assert_called_once()
    (connection, df, table), kwargs = mock_write_pandas.call_args
    assert connection is mock_connect.return_value.__enter__.return_value
    pd.testing.assert_frame_equal(
        df, pd.DataFrame({"id": [0, 1, 2]}), check_dtype=False
    )
    assert table == "MY_TABLE"
    assert kwargs == {
        "database": None,
        "schema": "MY_SCHEMA",
        "auto_create_table": True,
        "use_logical_type": True,
    }


def test_write_uses_logical_type_for_timezone_aware_timestamps():
    """`write_pandas` needs logical types to write timezone-aware timestamps right.

    Its own warning about this tests `is_datetime64tz_dtype`, which is False for
    the `pd.ArrowDtype` columns an Arrow-backed block produces, so nothing flags
    a missing `use_logical_type` at runtime.
    """
    datasink = SnowflakeDatasink("my_table", CONNECTION_PARAMETERS)
    blocks = [pa.table({"ts": pa.array([0], type=pa.timestamp("us", tz="UTC"))})]

    with mock.patch("snowflake.connector.connect"), mock.patch(
        "snowflake.connector.pandas_tools.write_pandas"
    ) as mock_write_pandas:
        datasink.write(blocks, ctx=mock.Mock())

    (_, df, _), kwargs = mock_write_pandas.call_args
    assert kwargs["use_logical_type"]
    # The dtype the connector's own check misses, which is why we can't rely on it.
    assert isinstance(df["ts"].dtype, pd.ArrowDtype)
    assert not isinstance(df["ts"].dtype, pd.DatetimeTZDtype)


def test_write_without_rows_does_nothing():
    datasink = SnowflakeDatasink("my_table", CONNECTION_PARAMETERS)
    blocks = [pa.table({"id": pa.array([], type=pa.int64())})]

    with mock.patch("snowflake.connector.connect") as mock_connect:
        datasink.write(blocks, ctx=mock.Mock())

    mock_connect.assert_not_called()


def test_write_snowflake_uses_snowflake_datasink():
    ds = ray.data.from_items([{"id": 0}])

    with mock.patch.object(ray.data.Dataset, "write_datasink") as mock_write_datasink:
        ds.write_snowflake("my_table", CONNECTION_PARAMETERS)

    (datasink,), _ = mock_write_datasink.call_args
    assert isinstance(datasink, SnowflakeDatasink)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
