import sqlite3
import tempfile
from typing import Generator

import pytest

import ray
from ray.data._internal.datasource.sql_datasource import SQLDatasource
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
        [{"string": "spam", "number": 0}, {"string": "ham", "number": 1}]
    )

    dataset.write_sql(
        "INSERT INTO test VALUES(?, ?)", lambda: sqlite3.connect(temp_database)
    )

    result = connection.cursor().execute("SELECT * FROM test ORDER BY number")
    assert result.fetchall() == [("spam", 0), ("ham", 1)]


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


@pytest.mark.parametrize("num_blocks", (2, 4))
def test_read_sql_with_null_shard_keys(num_blocks: int, temp_database: str):
    connection = sqlite3.connect(temp_database)
    connection.execute("CREATE TABLE events (id INTEGER, customer_id TEXT)")

    expected_values = [(i, None if i < 10 else str(i)) for i in range(200)]
    connection.executemany("INSERT INTO events VALUES (?, ?)", expected_values)
    connection.commit()
    connection.close()

    dataset = ray.data.read_sql(
        "SELECT * FROM events",
        lambda: sqlite3.connect(temp_database),
        override_num_blocks=num_blocks,
        shard_hash_fn="unicode",
        shard_keys=["customer_id"],
    )

    actual_values = [
        (record["id"], record["customer_id"]) for record in dataset.take_all()
    ]

    assert len(actual_values) == len(expected_values)
    assert sorted(actual_values, key=lambda row: row[0]) == expected_values


@pytest.mark.parametrize("num_blocks", (2, 4))
def test_read_sql_with_multiple_null_shard_keys(num_blocks: int, temp_database: str):
    connection = sqlite3.connect(temp_database)
    connection.execute(
        "CREATE TABLE events (id INTEGER, customer_id TEXT, region TEXT)"
    )

    # Cover every NULL combination across the two shard keys: both non-NULL,
    # only customer_id NULL, only region NULL, and both NULL.
    expected_values = []
    for i in range(200):
        customer_id = None if i % 4 in (0, 1) else f"c{i}"
        region = None if i % 4 in (0, 2) else f"r{i}"
        expected_values.append((i, customer_id, region))
    connection.executemany("INSERT INTO events VALUES (?, ?, ?)", expected_values)
    connection.commit()
    connection.close()

    dataset = ray.data.read_sql(
        "SELECT * FROM events",
        lambda: sqlite3.connect(temp_database),
        override_num_blocks=num_blocks,
        shard_hash_fn="unicode",
        shard_keys=["customer_id", "region"],
    )

    actual_values = [
        (record["id"], record["customer_id"], record["region"])
        for record in dataset.take_all()
    ]

    # Every row must come back exactly once, including rows where one or both
    # shard keys are NULL.
    assert len(actual_values) == len(expected_values)
    assert sorted(actual_values, key=lambda row: row[0]) == expected_values


class _QueryRecordingCursor:
    """A DB-API2 cursor double that records executed SQL and returns no rows."""

    def __init__(self, executed: list):
        self._executed = executed
        self.description = [("id",), ("customer_id",), ("region",)]

    def execute(self, sql, params=None):
        self._executed.append(sql)

    def executemany(self, sql, *args):
        pass

    def fetchone(self):
        return None

    def fetchall(self):
        return []


class _QueryRecordingConnection:
    """A DB-API2 connection double backed by ``_QueryRecordingCursor``."""

    def __init__(self, executed: list):
        self._executed = executed

    def cursor(self):
        return _QueryRecordingCursor(self._executed)

    def commit(self):
        pass

    def close(self):
        pass

    def rollback(self):
        pass


def test_multi_key_null_shard_predicates():
    """Multi-key sharding must build a NOT(non-null) / AND(non-null) predicate.

    Whether an end-to-end ``read_sql`` builds these predicates depends on the
    backend supporting the MOD/ABS/CONCAT sharding probe (SQLite only supports
    MOD when compiled with math functions); otherwise it falls back to a single
    read task. This test captures the SQL each read task generates directly, so
    the multi-key predicate logic is covered regardless of the backend.
    """
    executed = []
    datasource = SQLDatasource(
        "SELECT * FROM events",
        lambda: _QueryRecordingConnection(executed),
        "unicode",
        shard_keys=["customer_id", "region"],
    )

    # Task 0 absorbs every row the other tasks reject, via the exact boolean
    # complement of the non-NULL guard (not an "any key IS NULL" test). This
    # keeps rows whose row-valued key has mixed NULL fields -- FALSE for both
    # IS NULL and IS NOT NULL in PostgreSQL -- from being dropped.
    datasource._create_parallel_read_fn(task_id=0, parallelism=4)()
    assert "NOT (customer_id IS NOT NULL AND region IS NOT NULL)" in executed[-1]

    # Every other task only takes rows where *all* shard keys are non-NULL,
    # AND-ed together.
    datasource._create_parallel_read_fn(task_id=1, parallelism=4)()
    assert "(customer_id IS NOT NULL AND region IS NOT NULL)" in executed[-1]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
