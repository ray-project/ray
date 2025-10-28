import sqlite3
import tempfile
from typing import Generator
from unittest import mock

import pytest

import ray
from ray.tests.conftest import *  # noqa


@pytest.fixture(name="temp_database")
def temp_database_fixture() -> Generator[str, None, None]:
    """Create a temporary SQLite database to simulate Hive behavior."""
    with tempfile.NamedTemporaryFile(suffix=".db") as file:
        yield file.name


def test_read_hive_basic(temp_database: str):
    """Test basic Hive reading using mocked PyHive connection."""
    # Setup test database with SQLite (simulating Hive structure)
    connection = sqlite3.connect(temp_database)
    connection.execute("CREATE TABLE employee(name, id, salary)")
    expected_values = [
        ("Alice", 1, 50000.0),
        ("Bob", 2, 60000.0),
        ("Charlie", 3, 55000.0),
    ]
    connection.executemany("INSERT INTO employee VALUES (?, ?, ?)", expected_values)
    connection.commit()
    connection.close()

    # Mock PyHive to use SQLite connection instead
    with mock.patch("pyhive.hive.connect") as mock_connect:
        mock_connect.return_value = sqlite3.connect(temp_database)

        dataset = ray.data.read_hive(
            sql="SELECT * FROM employee",
            host="test-hive-host",
            port=10000,
            database="default",
        )
        actual_values = [tuple(record.values()) for record in dataset.take_all()]

        assert sorted(actual_values) == sorted(expected_values)


def test_read_hive_with_authentication(temp_database: str):
    """Test Hive reading with LDAP authentication parameters."""
    connection = sqlite3.connect(temp_database)
    connection.execute("CREATE TABLE users(username, age)")
    expected_values = [("alice", 30), ("bob", 25)]
    connection.executemany("INSERT INTO users VALUES (?, ?)", expected_values)
    connection.commit()
    connection.close()

    with mock.patch("pyhive.hive.connect") as mock_connect:
        mock_connect.return_value = sqlite3.connect(temp_database)

        dataset = ray.data.read_hive(
            sql="SELECT * FROM users",
            host="test-hive-host",
            auth="LDAP",
            username="test_user",
            password="test_pass",
        )
        actual_values = [tuple(record.values()) for record in dataset.take_all()]

        # Verify authentication parameters were passed
        mock_connect.assert_called_once()
        call_kwargs = mock_connect.call_args.kwargs
        assert call_kwargs["auth"] == "LDAP"
        assert call_kwargs["username"] == "test_user"
        assert call_kwargs["password"] == "test_pass"

        assert sorted(actual_values) == sorted(expected_values)


def test_read_hive_with_sharding(temp_database: str):
    """Test parallel reading with sharding enabled."""
    connection = sqlite3.connect(temp_database)
    connection.execute("CREATE TABLE transactions(id, amount)")
    # Create 100 records for meaningful sharding
    expected_values = [(i, i * 100.0) for i in range(100)]
    connection.executemany("INSERT INTO transactions VALUES (?, ?)", expected_values)
    connection.commit()
    connection.close()

    with mock.patch("pyhive.hive.connect") as mock_connect:
        mock_connect.return_value = sqlite3.connect(temp_database)

        num_blocks = 4
        dataset = ray.data.read_hive(
            sql="SELECT * FROM transactions",
            host="test-hive-host",
            shard_keys=["id"],
            shard_hash_fn="unicode",
            override_num_blocks=num_blocks,
        )
        dataset = dataset.materialize()

        assert dataset.num_blocks() == num_blocks
        actual_values = [tuple(record.values()) for record in dataset.take_all()]
        assert sorted(actual_values) == sorted(expected_values)


def test_read_hive_with_configuration(temp_database: str):
    """Test Hive reading with configuration parameters."""
    connection = sqlite3.connect(temp_database)
    connection.execute("CREATE TABLE config_test(key, value)")
    expected_values = [("param1", "value1"), ("param2", "value2")]
    connection.executemany("INSERT INTO config_test VALUES (?, ?)", expected_values)
    connection.commit()
    connection.close()

    hive_config = {
        "hive.execution.engine": "tez",
        "hive.vectorized.execution.enabled": "true",
    }

    with mock.patch("pyhive.hive.connect") as mock_connect:
        mock_connect.return_value = sqlite3.connect(temp_database)

        dataset = ray.data.read_hive(
            sql="SELECT * FROM config_test",
            host="test-hive-host",
            configuration=hive_config,
        )
        actual_values = [tuple(record.values()) for record in dataset.take_all()]

        # Verify configuration was passed
        call_kwargs = mock_connect.call_args.kwargs
        assert call_kwargs["configuration"] == hive_config

        assert sorted(actual_values) == sorted(expected_values)


def test_read_hive_kerberos_params(temp_database: str):
    """Test Hive reading with Kerberos authentication parameters."""
    connection = sqlite3.connect(temp_database)
    connection.execute("CREATE TABLE secure_data(id)")
    expected_values = [(1,), (2,), (3,)]
    connection.executemany("INSERT INTO secure_data VALUES (?)", expected_values)
    connection.commit()
    connection.close()

    with mock.patch("pyhive.hive.connect") as mock_connect:
        mock_connect.return_value = sqlite3.connect(temp_database)

        dataset = ray.data.read_hive(
            sql="SELECT * FROM secure_data",
            host="test-hive-host",
            auth="KERBEROS",
            kerberos_service_name="hive-kerb",
        )
        actual_values = [tuple(record.values()) for record in dataset.take_all()]

        # Verify Kerberos parameters were passed
        call_kwargs = mock_connect.call_args.kwargs
        assert call_kwargs["auth"] == "KERBEROS"
        assert call_kwargs["kerberos_service_name"] == "hive-kerb"

        assert sorted(actual_values) == sorted(expected_values)


def test_read_hive_empty_result(temp_database: str):
    """Test reading from empty Hive table."""
    connection = sqlite3.connect(temp_database)
    connection.execute("CREATE TABLE empty_table(col1, col2)")
    connection.commit()
    connection.close()

    with mock.patch("pyhive.hive.connect") as mock_connect:
        mock_connect.return_value = sqlite3.connect(temp_database)

        dataset = ray.data.read_hive(
            sql="SELECT * FROM empty_table",
            host="test-hive-host",
        )

        assert dataset.count() == 0


def test_read_hive_sharding_validation():
    """Test that sharding validation works correctly."""
    with mock.patch("pyhive.hive.connect") as mock_connect:
        # Create a mock connection that will fail sharding check
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        # Make the sharding test query fail
        mock_cursor.execute.side_effect = Exception("MOD not supported")
        mock_connect.return_value = mock_conn

        # Should raise error when override_num_blocks > 1 but no shard_keys
        with pytest.raises(ValueError, match="shard_keys must be provided"):
            ray.data.read_hive(
                sql="SELECT * FROM test",
                host="test-hive-host",
                override_num_blocks=2,
            )


def test_read_hive_with_ray_remote_args(temp_database: str):
    """Test Hive reading with Ray remote arguments."""
    connection = sqlite3.connect(temp_database)
    connection.execute("CREATE TABLE test_resources(id)")
    expected_values = [(1,), (2,), (3,)]
    connection.executemany("INSERT INTO test_resources VALUES (?)", expected_values)
    connection.commit()
    connection.close()

    with mock.patch("pyhive.hive.connect") as mock_connect:
        mock_connect.return_value = sqlite3.connect(temp_database)

        dataset = ray.data.read_hive(
            sql="SELECT * FROM test_resources",
            host="test-hive-host",
            num_cpus=2,
            memory=1024 * 1024 * 1024,  # 1GB
        )
        actual_values = [tuple(record.values()) for record in dataset.take_all()]

        assert sorted(actual_values) == sorted(expected_values)


def test_read_hive_complex_query(temp_database: str):
    """Test Hive reading with complex SQL query."""
    connection = sqlite3.connect(temp_database)
    connection.execute("CREATE TABLE sales(product, quantity, price)")
    expected_values = [
        ("laptop", 5, 1000.0),
        ("mouse", 20, 25.0),
        ("keyboard", 15, 75.0),
    ]
    connection.executemany("INSERT INTO sales VALUES (?, ?, ?)", expected_values)
    connection.commit()
    connection.close()

    with mock.patch("pyhive.hive.connect") as mock_connect:
        mock_connect.return_value = sqlite3.connect(temp_database)

        # Test with WHERE clause and aggregation
        dataset = ray.data.read_hive(
            sql="SELECT product, quantity * price as revenue FROM sales WHERE quantity > 10",
            host="test-hive-host",
        )
        actual_values = [tuple(record.values()) for record in dataset.take_all()]

        expected_results = [("mouse", 500.0), ("keyboard", 1125.0)]
        assert sorted(actual_values) == sorted(expected_results)


def test_read_hive_with_custom_port(temp_database: str):
    """Test Hive reading with custom port."""
    connection = sqlite3.connect(temp_database)
    connection.execute("CREATE TABLE test(id)")
    expected_values = [(1,)]
    connection.executemany("INSERT INTO test VALUES (?)", expected_values)
    connection.commit()
    connection.close()

    with mock.patch("pyhive.hive.connect") as mock_connect:
        mock_connect.return_value = sqlite3.connect(temp_database)

        custom_port = 10001
        dataset = ray.data.read_hive(
            sql="SELECT * FROM test",
            host="test-hive-host",
            port=custom_port,
        )
        actual_values = [tuple(record.values()) for record in dataset.take_all()]

        # Verify custom port was used
        call_kwargs = mock_connect.call_args.kwargs
        assert call_kwargs["port"] == custom_port

        assert actual_values == expected_values


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
