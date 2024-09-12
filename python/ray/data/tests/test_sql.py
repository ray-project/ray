import os
import re
import sqlite3
import tempfile
import uuid
from collections import namedtuple
from contextlib import contextmanager
from typing import Generator
from unittest import mock

import pandas as pd
import pyarrow as pa
import pytest

import ray
import ray.cloudpickle as pickle


@pytest.fixture(name="temp_database")
def temp_database_fixture() -> Generator[str, None, None]:
    with tempfile.NamedTemporaryFile(suffix=".db") as file:
        yield file.name


@pytest.mark.parametrize("parallelism", [-1, 1])
def test_read_sql(temp_database: str, parallelism: int):
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
        override_num_blocks=parallelism,
    )
    actual_values = [tuple(record.values()) for record in dataset.take_all()]

    assert sorted(actual_values) == sorted(expected_values)


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


def test_databricks_uc_datasource():
    MockResponse = namedtuple("Response", "raise_for_status json content")

    MockChunk = namedtuple("Chunk", "index, row_count byte_count data")

    token = "test_token"
    warehouse_id = "test_warehouse_id"
    catalog = "catalog1"
    schema = "db1"
    query = "select * from table1"
    expected_result_df = pd.DataFrame(
        {
            "c1": range(10000),
            "c2": map(lambda x: "str" + str(x), range(10000)),
        }
    )
    rows_per_chunk = 700

    @contextmanager
    def setup_mock():
        mock_chunks = []

        num_rows = len(expected_result_df)
        cur_pos = 0
        index = 0

        while cur_pos < num_rows:
            if cur_pos + rows_per_chunk <= num_rows:
                chunk_rows = rows_per_chunk
            else:
                chunk_rows = num_rows - cur_pos

            chunk_df = expected_result_df[cur_pos : (cur_pos + chunk_rows)]
            chunk_pa_table = pa.Table.from_pandas(chunk_df)
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, chunk_pa_table.schema) as writer:
                writer.write_table(chunk_pa_table)

            chunk_data = sink.getvalue()

            mock_chunks.append(
                MockChunk(
                    index=index,
                    row_count=chunk_rows,
                    byte_count=len(chunk_data),
                    data=chunk_data,
                )
            )
            index += 1
            cur_pos += rows_per_chunk

        chunk_meta_json = [
            {
                "chunk_index": index,
                "row_count": mock_chunk.row_count,
                "byte_count": mock_chunk.byte_count,
            }
            for index, mock_chunk in enumerate(mock_chunks)
        ]
        chunk_meta_json.reverse()
        valid_statement_ids = set()

        def request_post_mock(url, data=None, json=None, **kwargs):
            import json as jsonlib

            headers = kwargs["headers"]

            if url == "https://test_shard/api/2.0/sql/statements/":
                assert headers == {
                    "Content-Type": "application/json",
                    "Authorization": "Bearer " + token,
                }
                assert jsonlib.loads(data) == {
                    "statement": query,
                    "warehouse_id": warehouse_id,
                    "wait_timeout": "0s",
                    "disposition": "EXTERNAL_LINKS",
                    "format": "ARROW_STREAM",
                    "catalog": catalog,
                    "schema": schema,
                }

                statement_id = uuid.uuid4().hex
                valid_statement_ids.add(statement_id)

                return MockResponse(
                    raise_for_status=lambda: None,
                    json=lambda: {
                        "statement_id": statement_id,
                        "status": {"state": "PENDING"},
                    },
                    content=b"",
                )

            assert False, "Invalid request."

        def request_get_mock(url, params=None, **kwargs):
            headers = kwargs["headers"]

            if match := re.match(
                r"^https://test_shard/api/2\.0/sql/statements/([^/]*)/$", url
            ):
                statement_id = match.group(1)
                assert headers == {
                    "Content-Type": "application/json",
                    "Authorization": "Bearer " + token,
                }

                assert statement_id in valid_statement_ids

                return MockResponse(
                    raise_for_status=lambda: None,
                    json=lambda: {
                        "status": {"state": "SUCCEEDED"},
                        "manifest": {
                            "truncated": False,
                            "chunks": chunk_meta_json,
                        },
                    },
                    content=None,
                )

            if match := re.match(
                r"^https://test_shard/api/2\.0/sql/"
                r"statements/([^/]*)/result/chunks/([^/]*)$",
                url,
            ):
                assert headers == {
                    "Content-Type": "application/json",
                    "Authorization": "Bearer " + token,
                }

                chunk_index = match.group(2)

                external_link = f"https://test_external_link/{chunk_index}"
                return MockResponse(
                    raise_for_status=lambda: None,
                    json=lambda: {
                        "external_links": [
                            {
                                "external_link": external_link,
                            }
                        ]
                    },
                    content=None,
                )

            if match := re.match(r"^https://test_external_link/([^/]*)$", url):
                assert headers is None

                chunk_index = int(match.group(1))

                return MockResponse(
                    raise_for_status=lambda: None,
                    json=lambda: None,
                    content=mock_chunks[chunk_index].data,
                )

            assert False, "Invalid request."

        with mock.patch("requests.get", request_get_mock), mock.patch(
            "requests.post", request_post_mock
        ), mock.patch.dict(
            os.environ,
            {
                "DATABRICKS_HOST": "test_shard",
                "DATABRICKS_TOKEN": token,
            },
        ):
            yield

    setup_mock_fn_path = os.path.join(tempfile.mkdtemp(), "setup_mock_fn.pkl")
    with open(setup_mock_fn_path, "wb") as fp:
        pickle.dump(setup_mock, fp)

    MOCK_ENV = "RAY_DATABRICKS_UC_DATASOURCE_READ_FN_MOCK_TEST_SETUP_FN_PATH"
    with setup_mock(), mock.patch.dict(os.environ, {MOCK_ENV: setup_mock_fn_path}):
        # shut down existing Ray local cluster and
        # recreate Ray local cluster so that Ray head node could get the
        # "MOCK_ENV"
        ray.shutdown()
        ray.init()

        # test query with a table name
        result = (
            ray.data.read_databricks_tables(
                warehouse_id=warehouse_id,
                table="table1",
                catalog="catalog1",
                schema="db1",
                override_num_blocks=5,
            )
            .to_pandas()
            .sort_values("c1")
            .reset_index(drop=True)
        )
        pd.testing.assert_frame_equal(result, expected_result_df)

        # test query with SQL
        result = (
            ray.data.read_databricks_tables(
                warehouse_id=warehouse_id,
                query="select * from table1",
                catalog="catalog1",
                schema="db1",
                override_num_blocks=5,
            )
            .to_pandas()
            .sort_values("c1")
            .reset_index(drop=True)
        )

        pd.testing.assert_frame_equal(result, expected_result_df)

        # test larger parallelism
        result = (
            ray.data.read_databricks_tables(
                warehouse_id=warehouse_id,
                query="select * from table1",
                catalog="catalog1",
                schema="db1",
                override_num_blocks=100,
            )
            .to_pandas()
            .sort_values("c1")
            .reset_index(drop=True)
        )

        pd.testing.assert_frame_equal(result, expected_result_df)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
