import sqlite3
import tempfile
from typing import Generator

import pytest

import ray
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
