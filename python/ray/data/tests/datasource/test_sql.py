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


def _sqlite_connection_factory(database: str, denied_functions=(), query_log=None):
    def connect() -> sqlite3.Connection:
        def signed_hash(value):
            return value

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
        connection.create_function("signed_hash", 1, signed_hash)
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
        if query_log is not None:

            def record_query(sql):
                with open(query_log, "a", encoding="utf-8") as log:
                    log.write(sql + "\n")

            connection.set_trace_callback(record_query)
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


@pytest.mark.parametrize("denied_functions", [(), ("mod",)])
@pytest.mark.parametrize("override_num_blocks", [None, 4])
def test_read_sql_with_parallelism_fallback(
    temp_database: str, denied_functions, override_num_blocks, tmp_path
):
    connection = sqlite3.connect(temp_database)
    connection.execute(
        "CREATE TABLE events (id INTEGER, shard_key INTEGER, score REAL)"
    )
    # Include negative keys, for which databases differ on the sign of modulo,
    # and NULL keys, whose computed bucket is routed to task 0.
    expected_values = [
        (i, None if i % 20 == 0 else i - 250, i + 0.5) for i in range(500)
    ]
    connection.executemany("INSERT INTO events VALUES (?, ?, ?)", expected_values)
    connection.commit()
    connection.close()

    query_log = tmp_path / "queries.log"
    kwargs = {}
    if override_num_blocks is not None:
        kwargs["override_num_blocks"] = override_num_blocks
    dataset = ray.data.read_sql(
        "SELECT * FROM events",
        _sqlite_connection_factory(temp_database, denied_functions, str(query_log)),
        shard_hash_fn="signed_hash",
        shard_keys=["shard_key"],
        **kwargs,
    )
    dataset = dataset.materialize()
    if override_num_blocks is None:
        # Automatic parallelism uses multiple shards when resources allow.
        assert dataset.num_blocks() > 1
    else:
        assert dataset.num_blocks() == override_num_blocks

    actual_values = [
        (record["id"], record["shard_key"], record["score"])
        for record in dataset.take_all()
    ]
    assert sorted(actual_values, key=lambda row: row[0]) == expected_values

    # num_blocks() alone can also be produced by repartitioning a single-task
    # fallback. Verify that distinct shard queries ran.
    executed_queries = query_log.read_text(encoding="utf-8").splitlines()
    shard_queries = {
        query
        for query in executed_queries
        if query.lower().startswith("select * from (select * from events) as t where")
    }
    if override_num_blocks is None:
        assert len(shard_queries) > 1
    else:
        assert len(shard_queries) == override_num_blocks
    if denied_functions:
        assert all(" % " in query for query in shard_queries)
    else:
        assert all("MOD(" in query for query in shard_queries)


@pytest.mark.parametrize(
    "sql, sql_params",
    [
        ("SELECT * FROM events WHERE id >= %s", (100,)),
        ("SELECT * FROM events WHERE id >= %(min_id)s", {"min_id": 100}),
    ],
)
@pytest.mark.parametrize(
    "denied_functions, expected_syntax",
    [
        pytest.param((), "MOD", id="mod_supported"),
        pytest.param(("mod",), "%%", id="percent_fallback"),
    ],
)
def test_modulo_probe_with_format_params(
    temp_database: str, sql: str, sql_params, denied_functions, expected_syntax
):
    connection = sqlite3.connect(temp_database)
    connection.execute("CREATE TABLE events (id INTEGER, shard_key INTEGER)")
    expected_values = [(i, i - 100) for i in range(200)]
    connection.executemany("INSERT INTO events VALUES (?, ?)", expected_values)
    connection.commit()
    connection.close()

    class FormatStyleCursor:
        def __init__(self, cursor):
            self.cursor = cursor

        def execute(self, query, params=None):
            if params is None:
                return self.cursor.execute(query)
            try:
                # Emulate DB API format/pyformat parameter processing.
                query = query % params
            except (TypeError, ValueError) as e:
                raise sqlite3.OperationalError(str(e)) from e
            return self.cursor.execute(query)

        def __getattr__(self, name):
            return getattr(self.cursor, name)

    class FormatStyleConnection:
        def __init__(self):
            self.connection = _sqlite_connection_factory(
                temp_database, denied_functions=denied_functions
            )()

        def cursor(self):
            return FormatStyleCursor(self.connection.cursor())

        def commit(self):
            self.connection.commit()

        def rollback(self):
            self.connection.rollback()

        def close(self):
            self.connection.close()

    datasource = SQLDatasource(
        sql,
        FormatStyleConnection,
        "signed_hash",
        shard_keys=["shard_key"],
        sql_params=sql_params,
    )
    tasks = datasource.get_read_tasks(4)
    assert datasource._modulo_syntax == expected_syntax

    actual_values = [
        (row["id"], row["shard_key"])
        for task in tasks
        for block in task()
        for row in block.to_pylist()
    ]
    assert sorted(actual_values) == expected_values[100:]


@pytest.mark.parametrize(
    "sql, sql_params",
    [
        ("SELECT * FROM events WHERE id >= ?", (100,)),
        ("SELECT * FROM events WHERE id >= :min_id", {"min_id": 100}),
    ],
)
def test_read_sql_with_params_uses_percent_fallback(
    temp_database: str, sql: str, sql_params
):
    connection = sqlite3.connect(temp_database)
    connection.execute("CREATE TABLE events (id INTEGER, shard_key INTEGER)")
    expected_values = [(i, i - 100) for i in range(200)]
    connection.executemany("INSERT INTO events VALUES (?, ?)", expected_values)
    connection.commit()
    connection.close()

    dataset = ray.data.read_sql(
        sql,
        _sqlite_connection_factory(temp_database, denied_functions=("mod",)),
        sql_params=sql_params,
        override_num_blocks=4,
        shard_hash_fn="signed_hash",
        shard_keys=["shard_key"],
    )
    dataset = dataset.materialize()

    assert dataset.num_blocks() == 4
    actual_values = [
        (record["id"], record["shard_key"]) for record in dataset.take_all()
    ]
    assert sorted(actual_values) == expected_values[100:]


def test_build_modulo_expr_rejects_unknown_syntax():
    datasource = SQLDatasource(
        "SELECT * FROM events",
        lambda: sqlite3.connect(":memory:"),
        "MD5",
        shard_keys=["id"],
    )
    with pytest.raises(ValueError, match="Unsupported modulo syntax: unknown"):
        datasource._build_modulo_expr("unknown", "MD5", 4)


def test_supports_sharding_propagates_connection_error():
    def connection_factory():
        raise sqlite3.OperationalError("connection failed")

    datasource = SQLDatasource(
        "SELECT * FROM events",
        connection_factory,
        "MD5",
        shard_keys=["id"],
    )
    with pytest.raises(sqlite3.OperationalError, match="connection failed"):
        datasource.supports_sharding(4)


@pytest.mark.parametrize("failing_method", ["cursor", "commit", "close"])
def test_supports_sharding_propagates_acquisition_and_cleanup_errors(
    temp_database: str, failing_method: str
):
    class FailingConnection:
        def __init__(self):
            self.connection = _sqlite_connection_factory(temp_database)()

        def cursor(self):
            if failing_method == "cursor":
                raise RuntimeError("cursor failed")
            return self.connection.cursor()

        def commit(self):
            self.connection.commit()
            if failing_method == "commit":
                raise RuntimeError("commit failed")

        def rollback(self):
            self.connection.rollback()

        def close(self):
            self.connection.close()
            if failing_method == "close":
                raise RuntimeError("close failed")

    calls = []

    def connection_factory():
        calls.append(True)
        return FailingConnection()

    datasource = SQLDatasource(
        "SELECT 1 AS id",
        connection_factory,
        "signed_hash",
        shard_keys=["id"],
    )
    with pytest.raises(RuntimeError, match=f"{failing_method} failed"):
        datasource.supports_sharding(4)
    # Acquisition and cleanup errors propagate without trying another modulo
    # syntax.
    assert len(calls) == 1


def test_read_sql_override_includes_last_probe_error(
    temp_database: str, caplog, propagate_logs
):
    connection = sqlite3.connect(temp_database)
    connection.execute("CREATE TABLE events (id INTEGER)")
    connection.execute("INSERT INTO events VALUES (1)")
    connection.commit()
    connection.close()

    caplog.set_level("INFO")
    with pytest.raises(ValueError, match="Last sharding probe error.*missing_hash"):
        ray.data.read_sql(
            "SELECT * FROM events",
            lambda: sqlite3.connect(temp_database),
            override_num_blocks=4,
            shard_hash_fn="missing_hash",
            shard_keys=["id"],
        )

    assert "Sharding probe failed (tried modulo syntaxes)" in caplog.text
    assert "no such function: missing_hash" in caplog.text


def test_get_read_tasks_logs_neutral_probe_failure(
    temp_database: str, caplog, propagate_logs
):
    connection = sqlite3.connect(temp_database)
    connection.execute("CREATE TABLE events (id INTEGER)")
    connection.execute("INSERT INTO events VALUES (1)")
    connection.commit()
    connection.close()

    datasource = SQLDatasource(
        "SELECT * FROM events",
        lambda: sqlite3.connect(temp_database),
        "missing_hash",
        shard_keys=["id"],
    )
    caplog.set_level("INFO")
    tasks = datasource.get_read_tasks(4)
    assert len(tasks) == 1
    rows = [row for block in tasks[0]() for row in block.to_pylist()]
    assert rows == [{"id": 1}]
    assert "Sharding probe failed" in caplog.text
    assert "neither the MOD() function" not in caplog.text


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
