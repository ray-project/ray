
import ray
import sqlite3
import tempfile


connection = sqlite3.connect("/tmp/asdf3.db")
connection.cursor().execute("CREATE TABLE test(id)")
dataset = ray.data.range(1000).repartition(1)

dataset.write_sql(
    "INSERT INTO test VALUES(?)", lambda: sqlite3.connect("/tmp/asdf3.db")
)

result = connection.cursor().execute("SELECT * FROM test ORDER BY id")
# assert result.fetchall() == [(i,) for i in range(1000)]

