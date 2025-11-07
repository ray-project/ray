from unittest import mock

import pandas as pd
import pyarrow as pa
import pyarrow.avro
import pyarrow.parquet as pq
import pytest

import ray
from ray.tests.conftest import *  # noqa


def _mock_hive_describe_formatted(table_location: str, file_format: str = "parquet"):
    """Mock the output of DESCRIBE FORMATTED command.

    Returns a list of tuples simulating Hive's DESCRIBE FORMATTED output.

    Args:
        table_location: The storage location of the table
        file_format: The file format (parquet, avro, etc.)

    Returns:
        List of tuples representing DESCRIBE FORMATTED output
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


def test_read_hive_basic(tmp_path):
    """Test basic Hive table reading by querying metastore and reading from storage."""
    parquet_path = str(tmp_path / "test_table")

    df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
    pq.write_table(pa.table(df), f"{parquet_path}/data.parquet")

    with mock.patch("pyhive.hive.connect") as mock_connect:
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
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

        call_kwargs = mock_connect.call_args.kwargs
        assert call_kwargs["auth"] == "KERBEROS"
        assert call_kwargs["kerberos_service_name"] == "hive-kerb"

        result_df = dataset.to_pandas()
        pd.testing.assert_frame_equal(result_df, df)


def test_read_hive_with_configuration(tmp_path):
    """Test Hive reading with configuration parameters."""
    parquet_path = str(tmp_path / "config_table")

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

        call_kwargs = mock_connect.call_args.kwargs
        assert call_kwargs["configuration"] == hive_config

        result_df = dataset.to_pandas()
        pd.testing.assert_frame_equal(result_df, df)


def test_read_hive_custom_port(tmp_path):
    """Test Hive reading with custom port."""
    parquet_path = str(tmp_path / "test_port_table")

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

        call_kwargs = mock_connect.call_args.kwargs
        assert call_kwargs["port"] == custom_port

        result_df = dataset.to_pandas()
        pd.testing.assert_frame_equal(result_df, df)


def test_read_hive_avro_format(tmp_path):
    """Test reading Hive table stored in Avro format."""
    avro_path = str(tmp_path / "avro_table")

    df = pd.DataFrame({"name": ["Alice", "Bob"], "score": [95, 87]})
    table = pa.table(df)

    with open(f"{avro_path}/data.avro", "wb") as f:
        pyarrow.avro.write_avro(f, table)

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


def test_read_hive_invalid_host():
    """Test validation error for invalid host."""
    with pytest.raises(ValueError, match="Host cannot be empty"):
        ray.data.read_hive(table="test", host="")

    with pytest.raises(ValueError, match="Host should not include protocol"):
        ray.data.read_hive(table="test", host="http://example.com")

    with pytest.raises(ValueError, match="Host should not include protocol"):
        ray.data.read_hive(table="test", host="https://example.com")


def test_read_hive_invalid_port():
    """Test validation error for invalid port."""
    with pytest.raises(ValueError, match="Port must be between"):
        ray.data.read_hive(table="test", host="test-host", port=0)

    with pytest.raises(ValueError, match="Port must be between"):
        ray.data.read_hive(table="test", host="test-host", port=65536)

    with pytest.raises(TypeError, match="Port must be an integer"):
        ray.data.read_hive(table="test", host="test-host", port="10000")


def test_read_hive_invalid_table_name():
    """Test validation error for invalid table name."""
    with pytest.raises(ValueError, match="Table cannot be empty"):
        ray.data.read_hive(table="", host="test-host")

    with pytest.raises(ValueError, match="Table contains invalid character"):
        ray.data.read_hive(table="test; DROP TABLE", host="test-host")

    with pytest.raises(ValueError, match="Table contains invalid character"):
        ray.data.read_hive(table="test--", host="test-host")


def test_read_hive_invalid_database_name():
    """Test validation error for invalid database name."""
    with pytest.raises(ValueError, match="Database cannot be empty"):
        ray.data.read_hive(table="test", host="test-host", database="")

    with pytest.raises(ValueError, match="Database contains invalid character"):
        ray.data.read_hive(table="test", host="test-host", database="db; DROP")


def test_read_hive_invalid_auth_params():
    """Test validation error for invalid authentication parameters."""
    with pytest.raises(ValueError, match="Username is required for LDAP"):
        ray.data.read_hive(
            table="test",
            host="test-host",
            auth="LDAP",
            password="pass",
        )

    with pytest.raises(ValueError, match="Password is required for LDAP"):
        ray.data.read_hive(
            table="test",
            host="test-host",
            auth="LDAP",
            username="user",
        )


def test_read_hive_invalid_kerberos_service_name():
    """Test validation error for invalid Kerberos service name."""
    with pytest.raises(ValueError, match="Kerberos service name cannot be empty"):
        ray.data.read_hive(
            table="test",
            host="test-host",
            auth="KERBEROS",
            kerberos_service_name="",
        )

    with pytest.raises(ValueError, match="Kerberos service name contains invalid"):
        ray.data.read_hive(
            table="test",
            host="test-host",
            auth="KERBEROS",
            kerberos_service_name="hive@service",
        )


def test_read_hive_invalid_configuration():
    """Test validation error for invalid configuration."""
    with pytest.raises(TypeError, match="Configuration must be a dictionary"):
        ray.data.read_hive(
            table="test",
            host="test-host",
            configuration="not-a-dict",
        )

    with pytest.raises(TypeError, match="Configuration keys must be strings"):
        ray.data.read_hive(
            table="test",
            host="test-host",
            configuration={123: "value"},
        )

    with pytest.raises(TypeError, match="Configuration values must be strings"):
        ray.data.read_hive(
            table="test",
            host="test-host",
            configuration={"key": 123},
        )


def test_read_hive_host_normalization(tmp_path):
    """Test that host values are normalized (whitespace stripped)."""
    parquet_path = str(tmp_path / "normalized_table")

    df = pd.DataFrame({"id": [1]})
    pq.write_table(pa.table(df), f"{parquet_path}/data.parquet")

    with mock.patch("pyhive.hive.connect") as mock_connect:
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = _mock_hive_describe_formatted(parquet_path)
        mock_connect.return_value = mock_conn

        # Host with leading/trailing whitespace should be normalized
        dataset = ray.data.read_hive(
            table="test",
            host="  test-host  ",
        )

        # Verify normalized host was used (no spaces)
        call_kwargs = mock_connect.call_args.kwargs
        assert call_kwargs["host"] == "test-host"

        result_df = dataset.to_pandas()
        pd.testing.assert_frame_equal(result_df, df)


def test_read_hive_table_name_normalization(tmp_path):
    """Test that table names are normalized (whitespace stripped)."""
    parquet_path = str(tmp_path / "normalized_table")

    df = pd.DataFrame({"id": [1]})
    pq.write_table(pa.table(df), f"{parquet_path}/data.parquet")

    with mock.patch("pyhive.hive.connect") as mock_connect:
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = _mock_hive_describe_formatted(parquet_path)
        mock_connect.return_value = mock_conn

        # Table name with leading/trailing whitespace should be normalized
        dataset = ray.data.read_hive(
            table="  test_table  ",
            host="test-host",
        )

        result_df = dataset.to_pandas()
        pd.testing.assert_frame_equal(result_df, df)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
