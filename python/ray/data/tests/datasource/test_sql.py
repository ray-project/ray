import sqlite3
import tempfile
from typing import Generator

import pyarrow as pa
import pytest

import ray
from ray.data._internal.datasource.sql_datasink import SQLDatasink
from ray.data._internal.execution.interfaces import TaskContext
from ray.tests.conftest import *  # noqa  # noqa


@pytest.fixture(name="temp_database")
def temp_database_fixture() -> Generator[str, None, None]:
    with tempfile.NamedTemporaryFile(suffix=".db") as file:
        yield file.name


def test_read_sql(temp_database: str):
    connection = sqlite3.connect(temp_database)
    connection.execute("CREATE TABLE movie(title, year, score)")
    expected_values = [
        ("Monty Python and the Holy Grail", 1975, 8.2),
        ("And Now for Something Completely Different", 1971, 7.5),
    ]
    connection.executemany("INSERT INTO movie VALUES (?, ?, ?)", expected_values)
    connection.commit()
    connection.close()

    dataset = ray.data.read_sql(
        "SELECT * FROM movie",
        lambda: sqlite3.connect(temp_database),
    )
    actual_values = [tuple(record.values()) for record in dataset.take_all()]

    assert sorted(actual_values) == sorted(expected_values)


@pytest.mark.parametrize(
    "sql, sql_params",
    [
        ("SELECT * FROM movie WHERE year >= ?", (1975,)),
        ("SELECT * FROM movie WHERE year >= ?", [1975]),
        ("SELECT * FROM movie WHERE year >= :year", {"year": 1975}),
    ],
)
def test_read_sql_with_params(temp_database: str, sql: str, sql_params):
    connection = sqlite3.connect(temp_database)
    connection.execute("CREATE TABLE movie(title, year, score)")
    expected_values = [
        ("Monty Python and the Holy Grail", 1975, 8.2),
        ("And Now for Something Completely Different", 1971, 7.5),
        ("Monty Python's Life of Brian", 1979, 8.0),
    ]
    connection.executemany("INSERT INTO movie VALUES (?, ?, ?)", expected_values)
    connection.commit()
    connection.close()

    dataset = ray.data.read_sql(
        sql,
        lambda: sqlite3.connect(temp_database),
        sql_params=sql_params,
    )
    actual_values = [tuple(record.values()) for record in dataset.take_all()]

    assert sorted(actual_values) == sorted(
        [row for row in expected_values if row[1] >= 1975]
    )


def test_read_sql_with_parallelism_fallback(temp_database: str):
    connection = sqlite3.connect(temp_database)
    connection.execute("CREATE TABLE grade(name, id, score)")
    base_tuple = ("xiaoming", 1, 8.2)
    # Generate 200 elements
    expected_values = [
        (f"{base_tuple[0]}{i}", i, base_tuple[2] + i + 1) for i in range(500)
    ]
    connection.executemany("INSERT INTO grade VALUES (?, ?, ?)", expected_values)
    connection.commit()
    connection.close()

    num_blocks = 2
    dataset = ray.data.read_sql(
        "SELECT * FROM grade",
        lambda: sqlite3.connect(temp_database),
        override_num_blocks=num_blocks,
        shard_hash_fn="unicode",
        shard_keys=["id"],
    )
    dataset = dataset.materialize()
    assert dataset.num_blocks() == num_blocks

    actual_values = [tuple(record.values()) for record in dataset.take_all()]
    assert sorted(actual_values) == sorted(expected_values)


@pytest.mark.parametrize(
    "read_kwargs",
    [
        pytest.param(
            {"override_num_blocks": 1, "shard_keys": ["title"]}, id="single_task"
        ),
        pytest.param({"parallelism": 4}, id="no_shard_keys"),
    ],
)
def test_read_sql_skips_unnecessary_sharding_probe(
    temp_database: str, read_kwargs, caplog, propagate_logs
):
    connection = sqlite3.connect(temp_database)
    connection.execute("CREATE TABLE movie(title, year, score)")
    expected_values = [
        ("Monty Python and the Holy Grail", 1975, 8.2),
        ("And Now for Something Completely Different", 1971, 7.5),
    ]
    connection.executemany("INSERT INTO movie VALUES (?, ?, ?)", expected_values)
    connection.commit()
    connection.close()

    def connection_factory():
        connection = sqlite3.connect(temp_database)

        def fail_if_probed(value):
            raise sqlite3.OperationalError("unexpected sharding probe")

        connection.create_function("fail_if_probed", 1, fail_if_probed)
        return connection

    with caplog.at_level("INFO", logger="ray.data._internal.datasource.sql_datasource"):
        dataset = ray.data.read_sql(
            "SELECT * FROM movie",
            connection_factory,
            shard_hash_fn="fail_if_probed",
            **read_kwargs,
        )
        actual_values = [tuple(record.values()) for record in dataset.take_all()]

    assert sorted(actual_values) == sorted(expected_values)
    assert not any(
        "Database does not support sharding" in r.message
        or "Sharding probe failed" in r.message
        or "Sharding is not supported" in r.message
        for r in caplog.records
    )


def test_read_sql_with_parallelism_supported(temp_database: str):
    connection = sqlite3.connect(temp_database)
    connection.execute("CREATE TABLE events (id INTEGER)")
    expected_values = [(i,) for i in range(200)]
    connection.executemany("INSERT INTO events VALUES (?)", expected_values)
    connection.commit()
    connection.close()

    def connection_factory():
        connection = sqlite3.connect(temp_database)
        connection.create_function("MOD", 2, lambda value, divisor: value % divisor)
        connection.create_function("stable_hash", 1, lambda value: value)
        return connection

    dataset = ray.data.read_sql(
        "SELECT * FROM events",
        connection_factory,
        override_num_blocks=4,
        shard_hash_fn="stable_hash",
        shard_keys=["id"],
    )
    dataset = dataset.materialize()

    assert dataset.num_blocks() == 4
    actual_values = [(record["id"],) for record in dataset.take_all()]
    assert sorted(actual_values) == expected_values


# for mysql test
@pytest.mark.skip(reason="skip this test because mysql env is not ready")
def test_read_sql_with_parallelism_mysql(temp_database: str):
    # connect mysql
    import pymysql

    connection = pymysql.connect(
        host="10.10.xx.xx", user="root", password="22222", database="test"
    )
    cursor = connection.cursor()

    cursor.execute(
        "CREATE TABLE IF NOT EXISTS grade (name VARCHAR(255), id INT, score FLOAT)"
    )

    base_tuple = ("xiaoming", 1, 8.2)
    expected_values = [
        (f"{base_tuple[0]}{i}", i, base_tuple[2] + i + 1) for i in range(200)
    ]

    cursor.executemany(
        "INSERT INTO grade (name, id, score) VALUES (%s, %s, %s)", expected_values
    )
    connection.commit()

    cursor.close()
    connection.close()

    dataset = ray.data.read_sql(
        "SELECT * FROM grade",
        lambda: pymysql.connect(host="xxxxx", user="xx", password="xx", database="xx"),
        parallelism=4,
        shard_keys=["id"],
    )
    actual_values = [tuple(record.values()) for record in dataset.take_all()]

    assert sorted(actual_values) == sorted(expected_values)
    assert dataset.materialize().num_blocks() == 4


def test_write_sql(temp_database: str):
    connection = sqlite3.connect(temp_database)
    connection.cursor().execute("CREATE TABLE test(string, number)")
    dataset = ray.data.from_items(
        [{"string": "spam", "number": 0}, {"number": 1, "string": "ham"}],
        override_num_blocks=2,
    )

    dataset.write_sql(
        "INSERT INTO test VALUES(?, ?)", lambda: sqlite3.connect(temp_database)
    )

    result = connection.cursor().execute("SELECT * FROM test ORDER BY number")
    assert result.fetchall() == [("spam", 0), ("ham", 1)]


def test_write_sql_unknown_order_runs_udf_once(temp_database):
    with sqlite3.connect(temp_database) as db:
        db.execute("CREATE TABLE output(a, b)")
        db.execute("CREATE TABLE calls(marker)")

    def counted(batch):
        with sqlite3.connect(temp_database) as db:
            db.execute("INSERT INTO calls VALUES (?)", (int(batch["a"][0]),))
        return batch

    ds = ray.data.from_items(
        [{"a": 11, "b": 12}, {"a": 21, "b": 22}], override_num_blocks=2
    ).map_batches(counted)
    assert ds.columns(fetch_if_missing=False) is None
    ds.write_sql(
        "INSERT INTO output VALUES (?, ?)", lambda: sqlite3.connect(temp_database)
    )
    with sqlite3.connect(temp_database) as db:
        assert db.execute("SELECT * FROM calls ORDER BY marker").fetchall() == [
            (11,),
            (21,),
        ]
        assert db.execute("SELECT * FROM output ORDER BY a").fetchall() == [
            (11, 12),
            (21, 22),
        ]


def test_write_sql_inconsistent_order_across_tasks(temp_database):
    with sqlite3.connect(temp_database) as db:
        db.execute("CREATE TABLE output(a, b)")
    ds = ray.data.from_items(
        [{"a": 11, "b": 12}, {"b": 22, "a": 21}], override_num_blocks=2
    ).map_batches(lambda batch: batch)
    with pytest.raises(ValueError, match=r"select_columns.*committed"):
        ds.write_sql(
            "INSERT INTO output VALUES (?, ?)", lambda: sqlite3.connect(temp_database)
        )
    with sqlite3.connect(temp_database) as db:
        assert db.execute("SELECT * FROM output ORDER BY a").fetchall() == [
            (11, 12),
            (22, 21),
        ]


def test_write_sql_inconsistent_order_in_one_task(temp_database):
    with sqlite3.connect(temp_database) as db:
        db.execute("CREATE TABLE output(a, b)")
    sink = SQLDatasink(
        "INSERT INTO output VALUES (?, ?)",
        lambda: sqlite3.connect(temp_database),
        column_names=None,
    )
    blocks = [
        pa.table({}),
        pa.table({"a": [11], "b": [12]}),
        pa.table({"b": [22], "a": [21]}),
    ]
    with pytest.raises(ValueError, match="select_columns"):
        sink.write(blocks, TaskContext(task_idx=0, op_name="Write"))
    with sqlite3.connect(temp_database) as db:
        assert db.execute("SELECT * FROM output").fetchall() == []


def test_write_sql_known_columns_mismatch(temp_database):
    with sqlite3.connect(temp_database) as db:
        db.execute("CREATE TABLE output(a, b)")
    sink = SQLDatasink(
        "INSERT INTO output VALUES (?, ?)",
        lambda: sqlite3.connect(temp_database),
        column_names=["a", "b"],
    )
    with pytest.raises(ValueError) as error:
        sink.write(
            [pa.table({"a": [1], "c": [2]})], TaskContext(task_idx=0, op_name="Write")
        )
    message = str(error.value)
    assert "expected columns ['a', 'b'], got ['a', 'c']" in message
    assert "Missing columns: ['b']" in message
    assert "Unexpected columns: ['c']" in message


@pytest.mark.parametrize("num_blocks", (1, 20))
def test_write_sql_many_rows(num_blocks: int, temp_database: str):
    connection = sqlite3.connect(temp_database)
    connection.cursor().execute("CREATE TABLE test(id)")
    dataset = ray.data.range(1000).repartition(num_blocks)

    dataset.write_sql(
        "INSERT INTO test VALUES(?)", lambda: sqlite3.connect(temp_database)
    )

    result = connection.cursor().execute("SELECT * FROM test ORDER BY id")
    assert result.fetchall() == [(i,) for i in range(1000)]


def test_write_sql_nonexistant_table(temp_database: str):
    dataset = ray.data.range(1)
    with pytest.raises(sqlite3.OperationalError):
        dataset.write_sql(
            "INSERT INTO test VALUES(?)", lambda: sqlite3.connect(temp_database)
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
