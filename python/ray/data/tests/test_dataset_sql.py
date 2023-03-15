import sqlite3
import tempfile
from typing import Any, Generator, List, Tuple

import pytest

import ray


@pytest.fixture(name="temp_database")
def temp_database_fixture() -> Generator[str, None, None]:
    with tempfile.NamedTemporaryFile(suffix=".db") as file:
        yield file.name


def test_read_sql(temp_database: str):
    connection = sqlite3.connect(temp_database)
    connection.execute(f"CREATE TABLE movie(title, year, score)")
    values = [
        ("Monty Python and the Holy Grail", 1975, 8.2),
        ("And Now for Something Completely Different", 1971, 7.5),
    ]
    connection.executemany("INSERT INTO movie VALUES (?, ?, ?)", values)
    connection.close()

    dataset = ray.data.read_sql(
        "SELECT * FROM movies", lambda: sqlite3.connect(temp_database)
    )

    assert [record.values() for record in dataset.take_all()] == values
