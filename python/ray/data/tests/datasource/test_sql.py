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


class _ModuloProbingCursor:
    """A DB-API2 cursor double that simulates a database's modulo support.

    ``MOD()`` and the ``%`` operator are not portable across databases, so
    these tests control which syntax the fake "database" accepts. Using an
    unsupported syntax raises the same kind of error a real driver would, and
    every statement is recorded so tests can assert on the generated SQL.
    """

    def __init__(self, mod_supported: bool, percent_supported: bool):
        self._mod_supported = mod_supported
        self._percent_supported = percent_supported
        self.executed = []
        self.description = [("id",)]

    def execute(self, sql, params=None):
        self.executed.append(sql)
        if "MOD(" in sql and not self._mod_supported:
            raise sqlite3.OperationalError("no such function: MOD")
        if " % " in sql and not self._percent_supported:
            raise sqlite3.OperationalError('near "%": syntax error')

    def executemany(self, sql, *args):
        pass

    def fetchone(self):
        return (0,)

    def fetchall(self):
        return []


class _ModuloProbingConnection:
    """A DB-API2 connection double that hands out `_ModuloProbingCursor`s."""

    def __init__(self, mod_supported: bool, percent_supported: bool):
        self._mod_supported = mod_supported
        self._percent_supported = percent_supported
        self.cursors = []

    def cursor(self):
        cursor = _ModuloProbingCursor(self._mod_supported, self._percent_supported)
        self.cursors.append(cursor)
        return cursor

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


def _make_probe_datasource(mod_supported: bool, percent_supported: bool):
    """Build a SQLDatasource backed by a fake connection with given modulo support."""
    connection = _ModuloProbingConnection(mod_supported, percent_supported)
    datasource = SQLDatasource(
        sql="SELECT * FROM events",
        connection_factory=lambda: connection,
        shard_hash_fn="MD5",
        shard_keys=["customer_id"],
    )
    return datasource, connection


def _all_queries(connection) -> list:
    return [sql for cursor in connection.cursors for sql in cursor.executed]


def test_supports_sharding_prefers_mod_when_available():
    # When MOD() works, it wins the probe and the % fallback is never reached.
    datasource, connection = _make_probe_datasource(
        mod_supported=True, percent_supported=True
    )
    assert datasource.supports_sharding(4) is True
    assert datasource._modulo_syntax == "MOD"
    queries = _all_queries(connection)
    assert any("MOD(" in q for q in queries)
    assert not any(" % " in q for q in queries)


def test_supports_sharding_falls_back_to_percent_operator():
    # MOD() is unavailable (e.g. SQLite without math functions, SQL Server),
    # so the probe should fall back to the % operator and cache that choice.
    datasource, connection = _make_probe_datasource(
        mod_supported=False, percent_supported=True
    )
    assert datasource.supports_sharding(4) is True
    assert datasource._modulo_syntax == "%"
    queries = _all_queries(connection)
    # MOD() is tried first (and fails), then % wins.
    assert any("MOD(" in q for q in queries)
    assert any(" % " in q for q in queries)


def test_supports_sharding_false_when_no_modulo_available():
    # Neither syntax works: sharding is genuinely unsupported.
    datasource, connection = _make_probe_datasource(
        mod_supported=False, percent_supported=False
    )
    assert datasource.supports_sharding(4) is False
    assert datasource._modulo_syntax is None
    queries = _all_queries(connection)
    # Both syntaxes were probed before giving up.
    assert any("MOD(" in q for q in queries)
    assert any(" % " in q for q in queries)


def test_parallel_read_fn_uses_detected_modulo_syntax():
    # The per-task read query must reuse the syntax that won the probe;
    # otherwise the probe and the actual read would disagree and fail.
    datasource, connection = _make_probe_datasource(
        mod_supported=False, percent_supported=True
    )
    assert datasource.supports_sharding(4) is True
    assert datasource._modulo_syntax == "%"

    read_fn = datasource._create_parallel_read_fn(1, 4)
    read_fn()

    read_queries = [
        q for q in _all_queries(connection) if q.startswith("SELECT * FROM (")
    ]
    assert read_queries, "expected a sharded read query to be executed"
    assert all(" % " in q for q in read_queries)
    assert not any("MOD(" in q for q in read_queries)


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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
