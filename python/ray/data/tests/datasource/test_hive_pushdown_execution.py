"""Exercise table pushdown through real Ray workers with a counting SQL client."""

import json
import sys

import pyarrow as pa
import pytest

import ray
import ray.data
from ray.data.expressions import col

_CLIENT = r"""
import json
import os
import sqlite3

TYPES = {"id": "BIGINT", "name": "STRING", "score": "DOUBLE"}

class Cursor:
    def __init__(self, events, failure):
        self.events, self.failure, self.fetches = events, failure, 0
        self.database = sqlite3.connect(":memory:")
        self.database.execute("CREATE TABLE events (id INTEGER, name TEXT, score REAL)")
        self.database.executemany("INSERT INTO events VALUES (?, ?, ?)", [(i, f"name{i}", i / 2) for i in range(12)])
        self.description = None

    def record(self, kind, value=None):
        with open(self.events, "a") as stream:
            stream.write(json.dumps([kind, value]) + "\n")

    def get_table_schema(self, table, database):
        self.record("metadata")
        return list(TYPES.items())

    def execute(self, statement):
        self.record("execute", statement)
        if self.failure == "execute":
            raise ValueError("injected execute failure")
        self.result = self.database.execute(statement.replace("`default`.", ""))
        self.description = [(column[0], TYPES.get(column[0], "INT"), None, None, None, None, None) for column in self.result.description]

    def fetchmany(self, size):
        self.fetches += 1
        self.record("fetch", self.fetches)
        if self.fetches == 2 and self.failure:
            if self.failure == "worker_loss":
                os._exit(1)
            if self.failure == "convert":
                return [("not an integer", "bad", 0.0)]
            raise ValueError("injected fetch failure")
        return self.result.fetchmany(min(size, 2))

    def cancel_operation(self):
        self.record("cancel")

    def close(self):
        self.database.close()
        self.record("cursor_close")

class Connection:
    def __init__(self, events, failure):
        self.events, self.failure = events, failure

    def cursor(self, *, user):
        return Cursor(self.events, self.failure)

    def close(self):
        pass

def connect(events, failure=None):
    return Connection(events, failure)
"""


@pytest.fixture(scope="module")
def hive_worker_runtime(tmp_path_factory):
    directory = tmp_path_factory.mktemp("hive_client")
    package = directory / "impala"
    package.mkdir()
    (package / "__init__.py").write_text("")
    (package / "dbapi.py").write_text(_CLIENT)
    sys.path.insert(0, str(directory))
    ray.init(
        num_cpus=2, include_dashboard=False, runtime_env={"working_dir": str(directory)}
    )
    try:
        yield
    finally:
        ray.shutdown()
        sys.path.remove(str(directory))
        sys.modules.pop("impala.dbapi", None)
        sys.modules.pop("impala", None)


def _factory(path, failure=None):
    events = str(path)

    def connection_factory():
        from impala.dbapi import connect

        return connect(events, failure)

    return connection_factory


def _statements(path):
    if not path.exists():
        return []
    return [
        value
        for kind, value in map(json.loads, path.read_text().splitlines())
        if kind == "execute"
    ]


@pytest.mark.parametrize(
    "operation",
    [
        "project",
        "mixed",
        "limit_before_filter",
        "source_limit",
        "empty",
        "zero",
        "query",
    ],
)
def test_public_hive_pushdown(
    hive_worker_runtime, tmp_path, restore_data_context, operation
):
    events = tmp_path / "events.jsonl"
    context = ray.data.DataContext.get_current()
    context.enable_progressbars = False
    context.target_max_block_size = 1
    context.max_errored_blocks = 0
    factory = _factory(events)
    sql = "SELECT id, name, score FROM events"
    if operation == "query":
        sql = "SELECT id, name, score FROM events"
        dataset = ray.data.read_hive(
            query=sql,
            schema=pa.schema(
                [("id", pa.int64()), ("name", pa.string()), ("score", pa.float64())]
            ),
            connection_factory=factory,
        )
        result = dataset.filter(expr=col("id") >= 5).select_columns(["name"]).limit(3)
        expected = [{"name": f"name{i}"} for i in range(5, 8)]
    else:
        dataset = ray.data.read_hive(
            "events",
            limit=5 if operation == "source_limit" else None,
            connection_factory=factory,
        )
        if operation == "project":
            result = (
                dataset.filter(expr=col("id") >= 5).select_columns(["name"]).limit(3)
            )
            expected = [{"name": f"name{i}"} for i in range(5, 8)]
        elif operation == "mixed":
            result = (
                dataset.filter(expr=(col("id") >= 2) & (col("score") < 4.0))
                .select_columns(["name"])
                .limit(3)
            )
            expected = [{"name": f"name{i}"} for i in range(2, 5)]
        elif operation in ("limit_before_filter", "source_limit"):
            result = (
                dataset.limit(5) if operation == "limit_before_filter" else dataset
            ).filter(expr=col("id") >= 8)
            expected = []
        elif operation == "empty":
            result = (
                dataset.filter(expr=col("id") >= 2).select_columns([]).materialize()
            )
            assert result.count() == 10
            assert result.schema().names == result.schema().types == []
            statements = _statements(events)
            assert len(statements) == 1
            assert "1 AS `__ray_hive_row`" in statements[0]
            return
        else:
            result = dataset.limit(0)
            expected = []
    assert [dict(row) for row in result.take_all()] == expected
    statements = _statements(events)
    if operation == "zero":
        assert statements == []
        assert result.schema().names == ["id", "name", "score"]
    else:
        assert len(statements) == 1
        if operation == "query":
            assert statements == [sql]
        elif operation == "mixed":
            assert statements[0].startswith("SELECT `name`, `score`")
            assert "WHERE" in statements[0] and "LIMIT" not in statements[0]
        elif operation == "project":
            assert statements[0].startswith("SELECT `name`")
            assert "WHERE" in statements[0] and statements[0].endswith("LIMIT 3")
        else:
            assert "WHERE" not in statements[0] and statements[0].endswith("LIMIT 5")


@pytest.mark.parametrize("failure", ["execute", "fetch", "convert", "worker_loss"])
def test_table_failure_is_not_replayed(
    hive_worker_runtime, tmp_path, restore_data_context, failure
):
    events = tmp_path / "events.jsonl"
    context = ray.data.DataContext.get_current()
    context.target_max_block_size = 1
    context.max_errored_blocks = 0
    context.enable_progressbars = False
    dataset = ray.data.read_hive(
        "events", connection_factory=_factory(events, failure)
    ).filter(expr=col("id") >= 0)
    with pytest.raises(Exception):
        dataset.take_all()
    assert len(_statements(events)) == 1


def test_dataset_branches_and_reexecution_are_independent(
    hive_worker_runtime, tmp_path, restore_data_context
):
    events = tmp_path / "events.jsonl"
    dataset = ray.data.read_hive("events", connection_factory=_factory(events))
    first = dataset.filter(expr=col("id") < 2).select_columns(["id"])
    second = dataset.filter(expr=col("id") > 9).select_columns(["name"])
    assert [dict(row) for row in first.take_all()] == [{"id": 0}, {"id": 1}]
    assert [dict(row) for row in second.take_all()] == [
        {"name": "name10"},
        {"name": "name11"},
    ]
    assert [dict(row) for row in first.take_all()] == [{"id": 0}, {"id": 1}]
    assert len(_statements(events)) == 3


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
