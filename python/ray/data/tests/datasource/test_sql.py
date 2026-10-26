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


def _sqlite_connection_factory(database: str, denied_functions=()):
    def connect() -> sqlite3.Connection:
        def stable_hash(value):
            if value is None or value == "":
                return None
            return sum((index + 1) * ord(char) for index, char in enumerate(str(value)))

        def authorize(action, arg1, arg2, db_name, trigger):
            if action == sqlite3.SQLITE_FUNCTION:
                name = arg2.decode() if isinstance(arg2, bytes) else (arg2 or "")
                if name.lower() in denied_functions:
                    return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        connection = sqlite3.connect(database)
        # SQLite only enables MOD() when compiled with math functions, and CONCAT()
        # was added in SQLite 3.44.0. Provide both so sharding tests exercise the
        # datasource logic independently of the installed SQLite version.
        connection.create_function(
            "MOD", 2, lambda value, divisor: None if value is None else value % divisor
        )
        connection.create_function(
            "CONCAT",
            -1,
            lambda *values: "".join(
                "" if value is None else str(value) for value in values
            ),
        )
        connection.create_function("stable_hash", 1, stable_hash)
        connection.set_authorizer(authorize)
        return connection

    return connect


def _sqlite_connection(database: str) -> sqlite3.Connection:
    return _sqlite_connection_factory(database)()


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


@pytest.mark.parametrize(
    "case", ["single_null", "single_empty_string", "multiple_null"]
)
def test_read_sql_with_parallelism_handles_null_hashes(case: str, temp_database: str):
    connection = _sqlite_connection(temp_database)
    connection.execute(
        "CREATE TABLE events (id INTEGER, customer_id TEXT, region TEXT)"
    )

    expected_values = []
    for i in range(200):
        if case == "single_null":
            customer_id = None if i < 80 else f"c{i}"
            region = "r"
        elif case == "single_empty_string":
            customer_id = "" if i < 20 else f"c{i}"
            region = "r"
        else:
            # Cover all four NULL combinations across the two shard keys.
            customer_id = None if i % 4 in (0, 1) else f"c{i}"
            region = None if i % 4 in (0, 2) else f"r{i}"
        expected_values.append((i, customer_id, region))

    shard_keys = ["customer_id"]
    shard_hash_fn = "unicode"
    if case == "multiple_null":
        shard_keys.append("region")
        shard_hash_fn = "stable_hash"

    connection.executemany("INSERT INTO events VALUES (?, ?, ?)", expected_values)
    connection.commit()
    connection.close()

    num_blocks = 4
    dataset = ray.data.read_sql(
        "SELECT * FROM events",
        _sqlite_connection_factory(temp_database),
        override_num_blocks=num_blocks,
        shard_hash_fn=shard_hash_fn,
        shard_keys=shard_keys,
    )
    dataset = dataset.materialize()
    assert dataset.num_blocks() == num_blocks

    actual_values = [
        (record["id"], record["customer_id"], record["region"])
        for record in dataset.take_all()
    ]
    assert sorted(actual_values, key=lambda row: row[0]) == expected_values


def test_read_sql_override_detects_unsupported_coalesce(temp_database: str):
    connection = _sqlite_connection(temp_database)
    connection.execute("CREATE TABLE events (id INTEGER, customer_id TEXT)")
    connection.executemany(
        "INSERT INTO events VALUES (?, ?)",
        [(i, None if i % 10 == 0 else f"c{i}") for i in range(200)],
    )
    connection.commit()
    connection.close()

    # Probe the complete shard predicate so unsupported COALESCE is detected
    # before the read tasks execute.
    with pytest.raises(ValueError, match="sharding"):
        ray.data.read_sql(
            "SELECT * FROM events",
            _sqlite_connection_factory(temp_database, denied_functions=("coalesce",)),
            override_num_blocks=4,
            shard_hash_fn="stable_hash",
            shard_keys=["customer_id"],
        )


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


def test_sql_sharding_null_bucket_distribution(temp_database: str):
    connection = _sqlite_connection(temp_database)
    connection.execute(
        "CREATE TABLE events (id INTEGER, customer_id TEXT, region TEXT)"
    )
    expected_values = [(i, None, None if i % 5 == 0 else f"r{i}") for i in range(200)]
    connection.executemany("INSERT INTO events VALUES (?, ?, ?)", expected_values)
    connection.commit()
    connection.close()

    datasource = SQLDatasource(
        "SELECT * FROM events",
        lambda: _sqlite_connection(temp_database),
        "stable_hash",
        shard_keys=["customer_id", "region"],
    )
    tasks = datasource.get_read_tasks(4)
    assert len(tasks) == 4

    rows_per_task = []
    for task in tasks:
        rows = []
        for block in task():
            rows.extend(
                (row["id"], row["customer_id"], row["region"])
                for row in block.to_pylist()
            )
        rows_per_task.append(rows)

    # Partially NULL keys with usable hashes keep their normal shard assignments.
    assert all(rows_per_task)
    actual_values = [row for rows in rows_per_task for row in rows]
    assert len(actual_values) == len(expected_values)
    assert sorted(actual_values, key=lambda row: row[0]) == expected_values

    def expected_task_id(region):
        if region is None:
            return 0
        value = sum((index + 1) * ord(char) for index, char in enumerate(region))
        return value % 4

    for task_id, rows in enumerate(rows_per_task):
        expected_ids = {
            i for i, _, region in expected_values if expected_task_id(region) == task_id
        }
        assert {row[0] for row in rows} == expected_ids


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
