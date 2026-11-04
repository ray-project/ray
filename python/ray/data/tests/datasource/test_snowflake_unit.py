"""Tests for the Snowflake datasource and datasink that don't need a Snowflake
account. `test_snowflake.py` has the tests that run against Snowflake."""

from typing import Iterator, List, Optional
from unittest import mock

import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data._internal.datasource.snowflake_datasink import (
    SnowflakeDatasink,
    _parse_object_name,
)
from ray.data._internal.datasource.snowflake_datasource import SnowflakeDatasource
from ray.tests.conftest import *  # noqa

CONNECTION_PARAMETERS = {"user": "user", "account": "account"}


class FakeResultBatch:
    """Stands in for `snowflake.connector.result_batch.ResultBatch`."""

    def __init__(self, table: pa.Table, num_failures: int = 0):
        self._table = table
        self._num_failures = num_failures

    @property
    def rowcount(self) -> int:
        return self._table.num_rows

    def to_arrow(self) -> Optional[pa.Table]:
        if self._num_failures > 0:
            self._num_failures -= 1
            raise ConnectionError("Failed to download the result batch.")
        # Like connector 5.x, return `None` for a batch without rows.
        return self._table if self._table.num_rows > 0 else None


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
def mock_cursor() -> Iterator[mock.MagicMock]:
    with mock.patch("snowflake.connector.connect") as mock_connect:
        connection = mock_connect.return_value.__enter__.return_value
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.description = [("id",)]
        yield cursor


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


def test_read_retries_failed_download(mock_cursor):
    table = pa.table({"id": range(100)})
    mock_cursor.get_result_batches.return_value = [
        FakeResultBatch(table, num_failures=2)
    ]
    datasource = SnowflakeDatasource("SELECT * FROM T", CONNECTION_PARAMETERS)

    with mock.patch("ray._common.retry.time.sleep"):
        [read_task] = datasource.get_read_tasks(1)
        blocks = list(read_task())

    assert blocks == [table]


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
        ("my_table", (None, None, "MY_TABLE")),
        ("my_schema.my_table", (None, "MY_SCHEMA", "MY_TABLE")),
        ("my_db.my_schema.my_table", ("MY_DB", "MY_SCHEMA", "MY_TABLE")),
        ('"My Table"', (None, None, "My Table")),
        (
            'my_db."My.Schema"."My ""quoted"" table"',
            ("MY_DB", "My.Schema", 'My "quoted" table'),
        ),
    ],
)
def test_parse_object_name(name, expected):
    assert _parse_object_name(name) == expected


@pytest.mark.parametrize(
    "name", ["", "a.b.c.d", "a..b", ".a", '"unterminated', 'a."b', '""']
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
    }


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
