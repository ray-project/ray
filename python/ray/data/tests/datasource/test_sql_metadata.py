import sqlite3

import pytest

from ray.data._internal.datasource.sql_datasource import SQLDatasource


@pytest.mark.parametrize("first_shard_rows", [0, 50, 91, 100])
def test_hash_shard_metadata_matches_rows(tmp_path, first_shard_rows):
    database = str(tmp_path / "items.db")
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE items(id INTEGER, shard INTEGER)")
        connection.executemany(
            "INSERT INTO items VALUES (?, ?)",
            [(i, int(i >= first_shard_rows)) for i in range(100)],
        )

    datasource = SQLDatasource(
        "SELECT * FROM items",
        lambda: sqlite3.connect(database),
        shard_hash_fn="abs",
        shard_keys=["shard"],
    )
    tasks = datasource.get_read_tasks(2)
    assert len(tasks) == 2
    actual_counts = []
    for task in tasks:
        actual_count = sum(block.num_rows for block in task())
        actual_counts.append(actual_count)
        assert task.metadata.num_rows is None or task.metadata.num_rows == actual_count
    assert actual_counts == [first_shard_rows, 100 - first_shard_rows]
