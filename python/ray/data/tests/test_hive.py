import tempfile
from typing import Generator
from unittest import mock

import pyarrow as pa
import pytest

import ray
from ray.tests.conftest import *  # noqa


@pytest.fixture(name="temp_database")
def temp_database_fixture() -> Generator[str, None, None]:
    """Create a temporary SQLite database to simulate Hive behavior."""
    with tempfile.NamedTemporaryFile(suffix=".db") as file:
        yield file.name


def _mock_hive_describe_formatted(table_location: str, file_format: str = "parquet"):
    """
    Mock the output of DESCRIBE FORMATTED command.

    Returns a list of tuples simulating Hive's DESCRIBE FORMATTED output.
    """
    return [
        ("# col_name", "data_type", "comment"),
        ("", "", ""),
        ("id", "int", ""),
        ("name", "string", ""),
        ("", "", ""),
        ("# Detailed Table Information", "", ""),
        ("Database:", "default", ""),
        ("Owner:", "test_user", ""),
        ("Location:", table_location, ""),
        ("Table Type:", "MANAGED_TABLE", ""),
        (
            "InputFormat:",
            f"org.apache.hadoop.mapred.{file_format.capitalize()}InputFormat",
            "",
        ),
        ("", "", ""),
    ]


def test_read_hive_basic(temp_database: str, tmp_path):
    """Test basic Hive table reading by querying metastore and reading from storage."""
    # Create mock parquet files
    parquet_path = str(tmp_path / "test_table")

    # Create some test data as parquet
    import pandas as pd
    import pyarrow.parquet as pq

    df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
    pq.write_table(pa.table(df), f"{parquet_path}/data.parquet")

    # Mock PyHive connection and DESCRIBE FORMATTED output
    with mock.patch("pyhive.hive.connect") as mock_connect:
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        # Mock DESCRIBE FORMATTED to return table location
        mock_cursor.fetchall.return_value = _mock_hive_describe_formatted(parquet_path)

        mock_connect.return_value = mock_conn

        dataset = ray.data.read_hive(
            table="test_table",
            host="test-hive-host",
            database="default",
        )

        result_df = dataset.to_pandas()
        pd.testing.assert_frame_equal(result_df, df)


def test_read_hive_with_authentication(tmp_path):
    """Test Hive reading with LDAP authentication parameters."""
    parquet_path = str(tmp_path / "users_table")

    import pandas as pd
    import pyarrow.parquet as pq

    df = pd.DataFrame({"username": ["alice", "bob"], "age": [30, 25]})
    pq.write_table(pa.table(df), f"{parquet_path}/data.parquet")

    with mock.patch("pyhive.hive.connect") as mock_connect:
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = _mock_hive_describe_formatted(parquet_path)
        mock_connect.return_value = mock_conn

        dataset = ray.data.read_hive(
            table="users",
            host="test-hive-host",
            auth="LDAP",
            username="test_user",
            password="test_pass",
        )

        # Verify authentication parameters were passed
        mock_connect.assert_called_once()
        call_kwargs = mock_connect.call_args.kwargs
        assert call_kwargs["auth"] == "LDAP"
        assert call_kwargs["username"] == "test_user"
        assert call_kwargs["password"] == "test_pass"

        result_df = dataset.to_pandas()
        pd.testing.assert_frame_equal(result_df, df)


def test_read_hive_with_column_selection(tmp_path):
    """Test reading specific columns from Hive table."""
    parquet_path = str(tmp_path / "transactions_table")

    import pandas as pd
    import pyarrow.parquet as pq

    df = pd.DataFrame(
        {
            "id": list(range(10)),
            "amount": [i * 100.0 for i in range(10)],
            "category": ["A", "B"] * 5,
        }
    )
    pq.write_table(pa.table(df), f"{parquet_path}/data.parquet")

    with mock.patch("pyhive.hive.connect") as mock_connect:
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = _mock_hive_describe_formatted(parquet_path)
        mock_connect.return_value = mock_conn

        # Read only specific columns
        dataset = ray.data.read_hive(
            table="transactions",
            host="test-hive-host",
            columns=["id", "amount"],
        )

        result_df = dataset.to_pandas()
        expected_df = df[["id", "amount"]]
        pd.testing.assert_frame_equal(result_df, expected_df)


def test_read_hive_kerberos_params(tmp_path):
    """Test Hive reading with Kerberos authentication parameters."""
    parquet_path = str(tmp_path / "secure_table")

    import pandas as pd
    import pyarrow.parquet as pq

    df = pd.DataFrame({"id": [1, 2, 3]})
    pq.write_table(pa.table(df), f"{parquet_path}/data.parquet")

    with mock.patch("pyhive.hive.connect") as mock_connect:
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = _mock_hive_describe_formatted(parquet_path)
        mock_connect.return_value = mock_conn

        dataset = ray.data.read_hive(
            table="secure_data",
            host="test-hive-host",
            auth="KERBEROS",
            kerberos_service_name="hive-kerb",
        )

        # Verify Kerberos parameters were passed
        call_kwargs = mock_connect.call_args.kwargs
        assert call_kwargs["auth"] == "KERBEROS"
        assert call_kwargs["kerberos_service_name"] == "hive-kerb"

        result_df = dataset.to_pandas()
        pd.testing.assert_frame_equal(result_df, df)


def test_read_hive_with_configuration(tmp_path):
    """Test Hive reading with configuration parameters."""
    parquet_path = str(tmp_path / "config_table")

    import pandas as pd
    import pyarrow.parquet as pq

    df = pd.DataFrame({"key": ["param1", "param2"], "value": ["value1", "value2"]})
    pq.write_table(pa.table(df), f"{parquet_path}/data.parquet")

    hive_config = {
        "hive.execution.engine": "tez",
        "hive.vectorized.execution.enabled": "true",
    }

    with mock.patch("pyhive.hive.connect") as mock_connect:
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = _mock_hive_describe_formatted(parquet_path)
        mock_connect.return_value = mock_conn

        dataset = ray.data.read_hive(
            table="config_test",
            host="test-hive-host",
            configuration=hive_config,
        )

        # Verify configuration was passed
        call_kwargs = mock_connect.call_args.kwargs
        assert call_kwargs["configuration"] == hive_config

        result_df = dataset.to_pandas()
        pd.testing.assert_frame_equal(result_df, df)


def test_read_hive_custom_port(tmp_path):
    """Test Hive reading with custom port."""
    parquet_path = str(tmp_path / "test_port_table")

    import pandas as pd
    import pyarrow.parquet as pq

    df = pd.DataFrame({"id": [1]})
    pq.write_table(pa.table(df), f"{parquet_path}/data.parquet")

    with mock.patch("pyhive.hive.connect") as mock_connect:
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = _mock_hive_describe_formatted(parquet_path)
        mock_connect.return_value = mock_conn

        custom_port = 10001
        dataset = ray.data.read_hive(
            table="test",
            host="test-hive-host",
            port=custom_port,
        )

        # Verify custom port was used
        call_kwargs = mock_connect.call_args.kwargs
        assert call_kwargs["port"] == custom_port

        result_df = dataset.to_pandas()
        pd.testing.assert_frame_equal(result_df, df)


def test_read_hive_avro_format(tmp_path):
    """Test reading Hive table stored in Avro format."""
    avro_path = str(tmp_path / "avro_table")

    import pandas as pd
    import pyarrow as pa
    from pyarrow import avro

    df = pd.DataFrame({"name": ["Alice", "Bob"], "score": [95, 87]})
    table = pa.table(df)

    # Write as Avro
    with open(f"{avro_path}/data.avro", "wb") as f:
        avro.write_avro(f, table)

    with mock.patch("pyhive.hive.connect") as mock_connect:
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = _mock_hive_describe_formatted(
            avro_path, "avro"
        )
        mock_connect.return_value = mock_conn

        dataset = ray.data.read_hive(
            table="avro_test",
            host="test-hive-host",
        )

        result_df = dataset.to_pandas()
        pd.testing.assert_frame_equal(result_df, df)


def test_read_hive_missing_location():
    """Test error handling when table location cannot be determined."""
    with mock.patch("pyhive.hive.connect") as mock_connect:
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        # Return DESCRIBE output without location
        mock_cursor.fetchall.return_value = [
            ("# col_name", "data_type", "comment"),
            ("id", "int", ""),
        ]

        mock_connect.return_value = mock_conn

        with pytest.raises(ValueError, match="Could not determine storage location"):
            ray.data.read_hive(
                table="missing_location",
                host="test-hive-host",
            )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
