from unittest import mock

import pyarrow as pa
import pytest
from psycopg import sql

from ray.data._internal.datasource.hologres_datasource import HologresDatasource
from ray.data.read_api import read_hologres


class TestHologresDatasource:
    """Tests for HologresDatasource."""

    @pytest.fixture
    def datasource(self):
        with mock.patch("psycopg.connect") as mock_connect:
            # Setup mock connection to bypass actual DB connection during init
            mock_conn = mock.MagicMock()
            mock_cursor = mock.MagicMock()
            mock_connect.return_value = mock_conn
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

            # Mock the table existence check to return True
            mock_cursor.fetchone.return_value = [1]  # Table exists

            datasource = HologresDatasource(
                table="table_name",
                connection_uri="postgresql://user:password@localhost:5432/testdb",
                columns=["column1", "column2"],
                connection_options={"connect_timeout": 10},
            )
            return datasource

    def test_init(self, datasource):
        # Test query generation with columns
        expected_query = 'SELECT "column1", "column2" FROM "public"."table_name"'
        assert datasource._query.as_string() == expected_query
        assert datasource._schema == "public"
        assert datasource._table == "table_name"

    @mock.patch("psycopg.connect")
    def test_init_without_schema(self, mock_connect):
        # Setup mock connection to bypass actual DB connection during init
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Mock the table existence check to return True
        mock_cursor.fetchone.return_value = [1]  # Table exists

        datasource = HologresDatasource(
            table="my_table",
            connection_uri="postgresql://user:password@localhost:5432/testdb",
        )
        expected_query = 'SELECT * FROM "public"."my_table"'
        assert datasource._query.as_string() == expected_query
        assert datasource._schema == "public"
        assert datasource._table == "my_table"

    @mock.patch("psycopg.connect")
    def test_init_with_schema(self, mock_connect):
        # Setup mock connection to bypass actual DB connection during init
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Mock the table existence check to return True
        mock_cursor.fetchone.return_value = [1]  # Table exists

        datasource = HologresDatasource(
            schema="myschema",
            table="my_table",
            connection_uri="postgresql://user:password@localhost:5432/testdb",
        )
        expected_query = 'SELECT * FROM "myschema"."my_table"'
        assert datasource._query.as_string() == expected_query
        assert datasource._schema == "myschema"
        assert datasource._table == "my_table"

        datasource = HologresDatasource(
            schema="MYSCHEMA",
            table='TABL"E',
            connection_uri="postgresql://user:password@localhost:5432/testdb",
        )
        expected_query = 'SELECT * FROM "MYSCHEMA"."TABL""E"'
        assert datasource._query.as_string() == expected_query
        assert datasource._schema == "MYSCHEMA"
        assert datasource._table == 'TABL"E'

    def test_generate_query_columns(self, datasource):
        # Test with specific columns
        datasource._columns = ["field1"]
        query = datasource._generate_query().as_string()
        assert 'SELECT "field1" FROM' in query

        datasource._columns = ["field1", "field2"]
        query = datasource._generate_query().as_string()
        assert 'SELECT "field1", "field2" FROM' in query

        datasource._columns = ["field1", "FIELD2"]
        query = datasource._generate_query().as_string()
        assert 'SELECT "field1", "FIELD2" FROM' in query

        # Test with no columns (should select *)
        datasource._columns = None
        query = datasource._generate_query().as_string()
        assert "SELECT * FROM" in query

    def test_generate_query_with_where_filter(self):
        with mock.patch("psycopg.connect") as mock_connect:
            # Setup mock connection to bypass actual DB connection during init
            mock_conn = mock.MagicMock()
            mock_cursor = mock.MagicMock()
            mock_connect.return_value = mock_conn
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

            # Mock the table existence check to return True
            mock_cursor.fetchone.return_value = [1]  # Table exists

            datasource = HologresDatasource(
                table="table_name",
                connection_uri="postgresql://user:password@localhost:5432/testdb",
                columns=["column1", "column2"],
                where_filter="status = 'active'",
            )

            query = datasource._generate_query().as_string()
            expected_query = 'SELECT "column1", "column2" FROM "public"."table_name" WHERE status = \'active\''
            assert query == expected_query

            # Test with no where filter
            datasource._where_filter = None
            query = datasource._generate_query().as_string()
            expected_query = 'SELECT "column1", "column2" FROM "public"."table_name"'
            assert query == expected_query

            # Test with wildcard selection and where filter
            datasource._columns = None
            datasource._where_filter = '"AGE" > 18'
            query = datasource._generate_query().as_string()
            expected_query = 'SELECT * FROM "public"."table_name" WHERE "AGE" > 18'
            assert query == expected_query

    def test_validate_select_query_accepts_valid_queries(self):
        valid_queries = [
            'SELECT * FROM "public"."table"',
            'SELECT "col1", "col2" FROM "public"."table"',
            'SELECT * FROM "public"."table" WHERE age > 18',
            'SELECT * FROM "public"."table" WHERE status = \'active\'',
            'SELECT * FROM "public"."table" WHERE age > 18 AND status = \'active\'',
            'SELECT * FROM "public"."table" WHERE id IN (1, 2, 3)',
            'SELECT * FROM "public"."table" WHERE id BETWEEN 1 AND 100',
            'SELECT * FROM "public"."table" WHERE value IS NOT NULL',
            # Subqueries are fine
            'SELECT * FROM "public"."table" WHERE id IN (SELECT id FROM other)',
            'SELECT * FROM "public"."table" WHERE EXISTS (SELECT 1 FROM other)',
            # String literal containing a dangerous keyword is fine
            'SELECT * FROM "public"."table" WHERE status = \'DROP\'',
        ]
        for q in valid_queries:
            HologresDatasource._validate_select_query(q)

    def test_validate_select_query_rejects_multiple_statements(self):
        with pytest.raises(ValueError, match="multiple statements"):
            HologresDatasource._validate_select_query(
                'SELECT * FROM "public"."t" WHERE 1=1; DROP TABLE users'
            )

    def test_validate_select_query_rejects_non_select(self):
        with pytest.raises(ValueError, match="Only SELECT"):
            HologresDatasource._validate_select_query(
                'DELETE FROM "public"."users" WHERE 1=1'
            )
        with pytest.raises(ValueError, match="Only SELECT"):
            HologresDatasource._validate_select_query('DROP TABLE "public"."users"')
        with pytest.raises(ValueError, match="Only SELECT"):
            HologresDatasource._validate_select_query(
                'INSERT INTO "public"."users" VALUES (1)'
            )
        with pytest.raises(ValueError, match="Only SELECT"):
            HologresDatasource._validate_select_query(
                'UPDATE "public"."users" SET admin = true'
            )

    def test_init_rejects_malicious_where_filter(self):
        # Validation runs before _table_exists(), so no DB mock needed
        with pytest.raises(ValueError):
            HologresDatasource(
                table="test",
                connection_uri="postgresql://user:pass@host/db",
                where_filter="1=1; DROP TABLE users",
            )

    def test_build_read_sql_with_shard_list(self, datasource):
        original_query = sql.SQL('SELECT * FROM "public"."table_name"')
        result = datasource._build_read_sql([0, 1, 2], original_query)
        result_str = result.as_string()

        # Check that it adds hg_shard_id IN clause and includes the original query
        assert "hg_shard_id IN (0, 1, 2)" in result_str
        assert 'SELECT * FROM "public"."table_name"' in result_str
        # Verify that the WHERE clause was added
        assert "WHERE" in result_str

    def test_build_read_sql_with_existing_where_clause(self, datasource):
        original_query = sql.SQL('SELECT * FROM "public"."table_name" WHERE age > 18')
        datasource._where_filter = "age > 18"
        result = datasource._build_read_sql([0, 1, 2], original_query)
        result_str = result.as_string()

        # Check that it appends to existing WHERE clause with AND
        assert "hg_shard_id IN (0, 1, 2)" in result_str
        assert (
            'SELECT * FROM "public"."table_name" WHERE age > 18 AND hg_shard_id IN (0, 1, 2)'
            in result_str
        )

    @mock.patch("psycopg.connect")
    def test_get_shard_count_success(self, mock_connect):
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Mock the table existence check first (called during __init__)
        # During init, _table_exists() is called which calls fetchone() returning [1] (table exists)
        # Then during _get_shard_count(), fetchone() is called again returning [5] (shard count)
        mock_cursor.fetchone.side_effect = [
            [1],
            [5],
        ]  # Table exists, then return 5 shards

        datasource = HologresDatasource(
            table="test_table",
            connection_uri="postgresql://user:password@localhost:5432/testdb",
        )

        shard_count = datasource._get_shard_count()

        assert shard_count == 5
        # Two calls expected: one for table existence check during init, one for shard count
        assert mock_cursor.execute.call_count == 2
        # Check that the shard count query was called in the second call
        second_call_args = mock_cursor.execute.call_args_list[1]
        assert "hg_table_properties" in second_call_args[0][0]

    @mock.patch("psycopg.connect")
    def test_get_shard_count_failure(self, mock_connect):
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Mock the table existence check first (called during __init__) to return True
        # During init, _table_exists() is called which calls fetchone() returning [1] (table exists)
        # Then during _get_shard_count(), fetchone() is called again raising exception
        mock_cursor.fetchone.side_effect = [
            [1],
            Exception("Database error"),
        ]  # Table exists, then exception when getting shard count

        datasource = HologresDatasource(
            table="test_table",
            connection_uri="postgresql://user:password@localhost:5432/testdb",
        )

        shard_count = datasource._get_shard_count()

        assert shard_count is None

    @mock.patch("psycopg.connect")
    def test_execute_read_sql(self, mock_connect):
        # Setup mock connection and cursor
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Mock the results from the database
        mock_cursor.fetchall.return_value = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        mock_cursor.description = [("id",), ("name",)]

        datasource = HologresDatasource(
            table="test_table",
            connection_uri="postgresql://user:password@localhost:5432/testdb",
            read_mode="select",
        )

        # Test basic query execution
        result = datasource._execute_read_sql(
            sql.SQL("SELECT * FROM public.test_table")
        )

        # Verify the query was executed
        mock_cursor.execute.assert_called()
        assert result is not None  # Should return a PyArrow table

    @mock.patch("psycopg.connect")
    def test_get_read_tasks_with_shards(self, mock_connect):
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Mock the table existence check and shard count
        mock_cursor.fetchone.return_value = [50]  # 50 shards

        # Create datasource instance
        datasource = HologresDatasource(
            table="test_table",
            connection_uri="postgresql://user:password@localhost:5432/testdb",
        )

        # Mock the _execute_read_sql method to prevent actual db calls
        batch = pa.record_batch(
            [pa.array([1, 2]), pa.array(["a", "b"])], names=["id", "name"]
        )
        with mock.patch.object(datasource, "_execute_read_sql", return_value=batch):
            read_tasks = datasource.get_read_tasks(parallelism=4)

            # Should create 4 tasks since parallelism=4 and we have 50 shards
            assert len(read_tasks) == 4

            # Verify each task has appropriate shard assignments
            # With 50 shards and 4 tasks, they should be distributed as [13, 13, 12, 12] or similar
            # Each task's get_shard_list() returns a list of shard ids
            total_shard_nums = []
            for task in read_tasks:
                shard_list = task.get_shard_list()
                assert shard_list is not None
                total_shard_nums.extend(shard_list)

            total_shard_nums.sort()
            assert total_shard_nums == list(
                range(50)
            )  # Should have shards 0 through 49

    @mock.patch("psycopg.connect")
    def test_get_read_tasks_when_no_shards_found(self, mock_connect):
        mock_conn = mock.MagicMock()
        mock_cursor = mock.MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Mock the table existence check first (called during __init__) to return True, then None for shard count
        # During init, _table_exists() fetchone() returns [1] (table exists)
        # Then during get_read_tasks, _get_shard_count() fetchone() returns None (no shards found)
        mock_cursor.fetchone.side_effect = [
            [1],
            None,
        ]  # Table exists, then None for shard count

        datasource = HologresDatasource(
            table="test_table",
            connection_uri="postgresql://user:password@localhost:5432/testdb",
        )

        # When no shards are found, get_read_tasks creates a ReadTask with a lambda that calls _execute_read_sql
        # with the original query (no shard filter applied)
        read_tasks = datasource.get_read_tasks(parallelism=4)

        # Should fallback to single task when shard info unavailable
        assert len(read_tasks) == 1

        # Verify the task contains the correct lambda function that will call _execute_read_sql
        # with the original query (no hg_shard_id filter)
        task = read_tasks[0]

        # Import the actual function implementation to validate
        batch = pa.record_batch(
            [pa.array([1, 2]), pa.array(["a", "b"])], names=["id", "name"]
        )

        # Create a mock that captures the query passed to _execute_read_sql
        captured_queries = []

        def mock_execute_read_sql(query_param):
            captured_queries.append(query_param)
            return batch

        # Patch the method temporarily and execute the task's read function
        with mock.patch.object(
            datasource, "_execute_read_sql", side_effect=mock_execute_read_sql
        ):
            # Execute the task's read function to trigger the lambda
            task._read_fn()

            # Check that _execute_read_sql was called and captured the query
            assert len(captured_queries) >= 1
            called_query = captured_queries[0]

            # The query should be the original query without hg_shard_id filter
            assert "hg_shard_id" not in called_query.as_string()
            assert datasource._query.as_string() == called_query.as_string()


class TestHologresReadApi:
    """Tests for the read_hologres function parameters."""

    @mock.patch("ray.data.datasource.ReadTask")
    @mock.patch("ray.data.read_api.HologresDatasource")
    def test_read_hologres_with_schema_and_table_params(
        self, mock_datasource_class, mock_read_task
    ):
        # Setup mocks
        mock_dataset = mock.MagicMock()

        # Mock the HologresDatasource instance to avoid database connection during init
        mock_datasource_instance = mock.MagicMock()
        # Configure the return value of the mock constructor to return our pre-configured mock instance
        mock_datasource_class.return_value = mock_datasource_instance

        # Mock the return value of get_read_tasks
        mock_read_task_instance = mock.MagicMock()
        mock_datasource_instance.get_read_tasks.return_value = [mock_read_task_instance]

        # Test calling read_hologres with schema_name and table_name with default read_mode
        with mock.patch("ray.data.read_api.read_datasource") as mock_read_datasource:
            mock_read_datasource.return_value = mock_dataset

            result = read_hologres(
                schema_name="myschema",
                table_name="mytable",
                connection_uri="postgresql://user:pass@host:port/db",
            )

            # Verify that HologresDatasource was called with the correct table parameter with default read_mode
            mock_datasource_class.assert_called_once_with(
                schema="myschema",
                table="mytable",
                connection_uri="postgresql://user:pass@host:port/db",
                columns=None,
                connection_options=None,
                where_filter=None,
                read_mode="copy_to",
                is_compressed=True,
            )

            # Verify that read_datasource was called
            mock_read_datasource.assert_called_once()

            # Verify the result
            assert result == mock_dataset

        # Reset the mock to test the second call
        mock_datasource_class.reset_mock()
        mock_read_datasource.reset_mock()

        # Test calling read_hologres with schema_name, table_name and select read mode
        with mock.patch("ray.data.read_api.read_datasource") as mock_read_datasource:
            mock_read_datasource.return_value = mock_dataset

            result = read_hologres(
                schema_name="myschema",
                table_name="mytable",
                connection_uri="postgresql://user:pass@host:port/db",
                read_mode="select",
            )

            # Verify that HologresDatasource was called with the correct parameters including copy_to read_mode
            mock_datasource_class.assert_called_once_with(
                schema="myschema",
                table="mytable",
                connection_uri="postgresql://user:pass@host:port/db",
                columns=None,
                connection_options=None,
                where_filter=None,
                read_mode="select",
                is_compressed=True,
            )

            # Verify that read_datasource was called
            mock_read_datasource.assert_called_once()

            # Verify the result
            assert result == mock_dataset

    @mock.patch("ray.data.datasource.ReadTask")
    @mock.patch("ray.data.read_api.HologresDatasource")
    def test_read_hologres_with_default_schema(
        self, mock_datasource_class, mock_read_task
    ):
        # Setup mocks
        mock_dataset = mock.MagicMock()

        # Mock the HologresDatasource instance to avoid database connection during init
        mock_datasource_instance = mock.MagicMock()
        # Configure the return value of the mock constructor to return our pre-configured mock instance
        mock_datasource_class.return_value = mock_datasource_instance

        # Mock the return value of get_read_tasks
        mock_read_task_instance = mock.MagicMock()
        mock_datasource_instance.get_read_tasks.return_value = [mock_read_task_instance]

        # Test calling read_hologres with only table_name (using default schema "public")
        with mock.patch("ray.data.read_api.read_datasource") as mock_read_datasource:
            mock_read_datasource.return_value = mock_dataset

            result = read_hologres(
                table_name="mytable",
                connection_uri="postgresql://user:pass@host:port/db",
            )

            # Verify that HologresDatasource was called with the correct table parameter using default schema
            mock_datasource_class.assert_called_once_with(
                schema="public",
                table="mytable",
                connection_uri="postgresql://user:pass@host:port/db",
                columns=None,
                connection_options=None,
                where_filter=None,
                read_mode="copy_to",
                is_compressed=True,
            )

            # Verify that read_datasource was called
            mock_read_datasource.assert_called_once()

            # Verify the result
            assert result == mock_dataset

    @mock.patch("ray.data.datasource.ReadTask")
    @mock.patch("ray.data.read_api.HologresDatasource")
    def test_read_hologres_with_custom_schema_and_columns(
        self, mock_datasource_class, mock_read_task
    ):
        # Setup mocks
        mock_dataset = mock.MagicMock()

        # Mock the HologresDatasource instance to avoid database connection during init
        mock_datasource_instance = mock.MagicMock()
        # Configure the return value of the mock constructor to return our pre-configured mock instance
        mock_datasource_class.return_value = mock_datasource_instance

        # Mock the return value of get_read_tasks
        mock_read_task_instance = mock.MagicMock()
        mock_datasource_instance.get_read_tasks.return_value = [mock_read_task_instance]

        # Test calling read_hologres with custom schema, table, and columns
        with mock.patch("ray.data.read_api.read_datasource") as mock_read_datasource:
            mock_read_datasource.return_value = mock_dataset

            result = read_hologres(
                schema_name="analytics",
                table_name="users",
                connection_uri="postgresql://user:pass@host:port/db",
                columns=["id", "name", "email"],
            )

            # Verify that HologresDatasource was called with the correct parameters
            mock_datasource_class.assert_called_once_with(
                schema="analytics",
                table="users",
                connection_uri="postgresql://user:pass@host:port/db",
                columns=["id", "name", "email"],
                connection_options=None,
                where_filter=None,
                read_mode="copy_to",
                is_compressed=True,
            )

            # Verify that read_datasource was called
            mock_read_datasource.assert_called_once()

            # Verify the result
            assert result == mock_dataset

    @mock.patch("ray.data.datasource.ReadTask")
    @mock.patch("ray.data.read_api.HologresDatasource")
    def test_read_hologres_with_where_filter(
        self, mock_datasource_class, mock_read_task
    ):
        # Setup mocks
        mock_dataset = mock.MagicMock()

        # Mock the HologresDatasource instance to avoid database connection during init
        mock_datasource_instance = mock.MagicMock()
        # Configure the return value of the mock constructor to return our pre-configured mock instance
        mock_datasource_class.return_value = mock_datasource_instance

        # Mock the return value of get_read_tasks
        mock_read_task_instance = mock.MagicMock()
        mock_datasource_instance.get_read_tasks.return_value = [mock_read_task_instance]

        # Test calling read_hologres with where_filter parameter
        with mock.patch("ray.data.read_api.read_datasource") as mock_read_datasource:
            mock_read_datasource.return_value = mock_dataset

            result = read_hologres(
                table_name="mytable",
                connection_uri="postgresql://user:pass@host:port/db",
                where_filter="age > 18 AND status = 'active'",
            )

            # Verify that HologresDatasource was called with the correct parameters including where_filter
            mock_datasource_class.assert_called_once_with(
                schema="public",
                table="mytable",
                connection_uri="postgresql://user:pass@host:port/db",
                columns=None,
                connection_options=None,
                where_filter="age > 18 AND status = 'active'",
                read_mode="copy_to",
                is_compressed=True,
            )

            # Verify that read_datasource was called
            mock_read_datasource.assert_called_once()

            # Verify the result
            assert result == mock_dataset


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
