"""
Tests for the Kinetica Ray Data integration.

These tests use mocks to verify the KineticaDatasource and KineticaDatasink
work correctly without requiring a running Kinetica server.
"""

import json
import os
import pytest
from unittest import mock
from unittest.mock import Mock, MagicMock, patch

import pyarrow as pa

from ray.data._internal.datasource.kinetica_datasource import (
    KineticaDatasource,
    _is_filter_safe,
)
from ray.data._internal.datasource.kinetica_datasink import (
    KineticaDatasink,
    KineticaSinkMode,
    KineticaTableSettings,
)
from ray.data._internal.execution.interfaces.task_context import TaskContext


# ============================================================================
# Fixtures for Mocking GPUdb Client
# ============================================================================


@pytest.fixture
def mock_gpudb_client():
    """Mock GPUdb client for datasource tests."""
    client = MagicMock()

    # Mock show_table response
    client.show_table.return_value = {
        "type_schemas": [json.dumps({
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "value", "type": "double"},
            ]
        })],
        "properties": [{"id": [], "name": [], "value": []}],
        "total_size": 100,
    }

    # Mock get_records response
    client.get_records.return_value = {
        "records_json": [
            json.dumps({"id": 1, "name": "Alice", "value": 100.5}),
            json.dumps({"id": 2, "name": "Bob", "value": 200.75}),
        ],
        "total_number_of_records": 100,
    }

    return client


@pytest.fixture
def mock_gpudb_sink_client():
    """Mock GPUdb client for datasink tests."""
    client = MagicMock()

    # Mock table existence check
    client.has_table.return_value = {"table_exists": False}

    # Mock insert_records response
    client.insert_records.return_value = {
        "count_inserted": 3,
        "count_updated": 0,
        "info": {},
    }

    # Mock show_table response
    client.show_table.return_value = {
        "type_ids": ["type_123"],
        "type_schemas": [json.dumps({
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
            ]
        })],
        "properties": [{"id": [], "name": []}],
    }

    return client


@pytest.fixture(autouse=True)
def patch_gpudb():
    """Automatically patch GPUdb for all tests."""
    with patch("gpudb.GPUdb") as mock_gpudb_class:
        mock_instance = MagicMock()
        mock_gpudb_class.return_value = mock_instance
        yield mock_instance


# ============================================================================
# Filter Safety Tests
# ============================================================================


class TestFilterSafety:
    """Tests for filter expression safety validation."""

    @pytest.mark.parametrize(
        "filter_expr, is_safe",
        [
            ("id > 100", True),
            ("name = 'Alice' AND value > 50", True),
            ("id > 100 AND name IS NOT NULL", True),
            ("id = 1; DROP TABLE test;", False),
            ("id > 100; SELECT * FROM users", False),
            ("id IN {1, 2, 3}", False),
            ("name = '{malicious}'", False),
        ],
    )
    def test_is_filter_safe(self, filter_expr, is_safe):
        """Test filter safety validation."""
        assert _is_filter_safe(filter_expr) == is_safe


# ============================================================================
# KineticaDatasource Tests
# ============================================================================


class TestKineticaDatasource:
    """Tests for KineticaDatasource."""

    @pytest.fixture
    def datasource(self):
        """Create a KineticaDatasource with test parameters."""
        with patch("ray.data._internal.datasource.kinetica_datasource.KineticaDatasource._init_client"):
            ds = KineticaDatasource(
                url="http://localhost:9191",
                table_name="test_table",
                username="admin",
                password="password",
                columns=["id", "name"],
                filter_expression="id > 100",
                batch_size=5000,
            )
            return ds

    def test_init(self, datasource):
        """Test datasource initialization."""
        assert datasource._url == "http://localhost:9191"
        assert datasource._table_name == "test_table"
        assert datasource._username == "admin"
        assert datasource._columns == ["id", "name"]
        assert datasource._filter_expression == "id > 100"
        assert datasource._batch_size == 5000

    def test_default_batch_size(self):
        """Test default batch size."""
        with patch("ray.data._internal.datasource.kinetica_datasource.KineticaDatasource._init_client"):
            ds = KineticaDatasource(
                url="http://localhost:9191",
                table_name="test_table",
            )
            assert ds._batch_size == 10000

    def test_unsafe_filter_rejected(self):
        """Test that unsafe filter expressions are rejected."""
        with pytest.raises(ValueError, match="unsafe characters"):
            KineticaDatasource(
                url="http://localhost:9191",
                table_name="test_table",
                filter_expression="id = 1; DROP TABLE test;",
            )

    def test_get_name(self, datasource):
        """Test datasource name generation."""
        assert datasource.get_name() == "Kinetica(test_table)"

    @patch.object(KineticaDatasource, "_init_client")
    def test_get_table_info(self, mock_init_client, mock_gpudb_client):
        """Test _get_table_info method."""
        mock_init_client.return_value = mock_gpudb_client

        ds = KineticaDatasource(
            url="http://localhost:9191",
            table_name="test_table",
        )

        record_type, total_count, arrow_schema = ds._get_table_info(mock_gpudb_client)

        assert total_count == 100
        assert arrow_schema is not None
        assert len(arrow_schema) == 3
        mock_gpudb_client.show_table.assert_called_once()

    @patch.object(KineticaDatasource, "_init_client")
    def test_get_table_info_with_filter(self, mock_init_client, mock_gpudb_client):
        """Test _get_table_info with filter expression."""
        mock_init_client.return_value = mock_gpudb_client

        ds = KineticaDatasource(
            url="http://localhost:9191",
            table_name="test_table",
            filter_expression="id > 50",
        )

        record_type, total_count, arrow_schema = ds._get_table_info(mock_gpudb_client)

        # Should call get_records to get filtered count
        mock_gpudb_client.get_records.assert_called_with(
            table_name="test_table",
            offset=0,
            limit=0,
            options={"expression": "id > 50"},
        )

    @patch.object(KineticaDatasource, "_init_client")
    def test_estimate_row_size(self, mock_init_client, mock_gpudb_client):
        """Test _estimate_row_size method."""
        mock_init_client.return_value = mock_gpudb_client

        ds = KineticaDatasource(
            url="http://localhost:9191",
            table_name="test_table",
            columns=["id", "name"],
        )

        row_size = ds._estimate_row_size(mock_gpudb_client, sample_size=100)

        assert row_size > 0
        mock_gpudb_client.get_records.assert_called()

    @patch.object(KineticaDatasource, "_init_client")
    @pytest.mark.parametrize("parallelism", [1, 2, 4])
    def test_get_read_tasks(self, mock_init_client, mock_gpudb_client, parallelism):
        """Test get_read_tasks with different parallelism levels."""
        mock_init_client.return_value = mock_gpudb_client

        ds = KineticaDatasource(
            url="http://localhost:9191",
            table_name="test_table",
        )

        read_tasks = ds.get_read_tasks(parallelism)

        assert len(read_tasks) <= parallelism
        assert all(task.metadata.num_rows > 0 for task in read_tasks)

    @patch.object(KineticaDatasource, "_init_client")
    def test_get_read_tasks_empty_table(self, mock_init_client, mock_gpudb_client):
        """Test get_read_tasks with empty table."""
        mock_init_client.return_value = mock_gpudb_client
        mock_gpudb_client.show_table.return_value = {
            "type_schemas": [json.dumps({"fields": []})],
            "properties": [{}],
            "total_size": 0,
        }

        ds = KineticaDatasource(
            url="http://localhost:9191",
            table_name="empty_table",
        )

        read_tasks = ds.get_read_tasks(parallelism=2)

        assert len(read_tasks) == 0

    @patch.object(KineticaDatasource, "_init_client")
    def test_estimate_inmemory_data_size(self, mock_init_client, mock_gpudb_client):
        """Test estimate_inmemory_data_size method."""
        mock_init_client.return_value = mock_gpudb_client

        ds = KineticaDatasource(
            url="http://localhost:9191",
            table_name="test_table",
        )

        size = ds.estimate_inmemory_data_size()

        assert size is not None
        assert size > 0


# ============================================================================
# KineticaDatasink Tests
# ============================================================================


class TestKineticaDatasink:
    """Tests for KineticaDatasink."""

    @pytest.fixture
    def datasink(self):
        """Create a KineticaDatasink with test parameters."""
        with patch("ray.data._internal.datasource.kinetica_datasink.KineticaDatasink._init_client"):
            ds = KineticaDatasink(
                url="http://localhost:9191",
                table_name="test_table",
                username="admin",
                password="password",
                mode=KineticaSinkMode.APPEND,
                batch_size=5000,
            )
            return ds

    def test_init(self, datasink):
        """Test datasink initialization."""
        assert datasink._url == "http://localhost:9191"
        assert datasink._table_name == "test_table"
        assert datasink._username == "admin"
        assert datasink._mode == KineticaSinkMode.APPEND
        assert datasink._batch_size == 5000

    def test_string_mode(self):
        """Test datasink accepts string mode values."""
        with patch("ray.data._internal.datasource.kinetica_datasink.KineticaDatasink._init_client"):
            ds = KineticaDatasink(
                url="http://localhost:9191",
                table_name="test_table",
                mode="overwrite",
            )
            assert ds._mode == KineticaSinkMode.OVERWRITE

    def test_table_settings(self):
        """Test KineticaTableSettings configuration."""
        settings = KineticaTableSettings(
            is_replicated=True,
            chunk_size=1000000,
            ttl=60,
            primary_keys=["id"],
            shard_keys=["region"],
        )

        with patch("ray.data._internal.datasource.kinetica_datasink.KineticaDatasink._init_client"):
            ds = KineticaDatasink(
                url="http://localhost:9191",
                table_name="test_table",
                table_settings=settings,
            )

            assert ds._table_settings.is_replicated is True
            assert ds._table_settings.chunk_size == 1000000
            assert ds._table_settings.ttl == 60
            assert ds._table_settings.primary_keys == ["id"]
            assert ds._table_settings.shard_keys == ["region"]

    def test_get_name(self, datasink):
        """Test datasink name generation."""
        assert datasink.get_name() == "Kinetica(test_table)"

    def test_supports_distributed_writes(self, datasink):
        """Test datasink reports distributed write support."""
        assert datasink.supports_distributed_writes is True

    def test_num_rows_per_write(self, datasink):
        """Test num_rows_per_write property."""
        assert datasink.num_rows_per_write == 5000

    @patch.object(KineticaDatasink, "_init_client")
    def test_table_exists(self, mock_init_client, mock_gpudb_sink_client):
        """Test _table_exists method."""
        mock_init_client.return_value = mock_gpudb_sink_client
        mock_gpudb_sink_client.has_table.return_value = {"table_exists": True}

        ds = KineticaDatasink(
            url="http://localhost:9191",
            table_name="test_table",
        )

        exists = ds._table_exists(mock_gpudb_sink_client)

        assert exists is True
        mock_gpudb_sink_client.has_table.assert_called_once()

    @patch.object(KineticaDatasink, "_init_client")
    def test_drop_table(self, mock_init_client, mock_gpudb_sink_client):
        """Test _drop_table method."""
        mock_init_client.return_value = mock_gpudb_sink_client

        ds = KineticaDatasink(
            url="http://localhost:9191",
            table_name="test_table",
        )

        ds._drop_table(mock_gpudb_sink_client)

        mock_gpudb_sink_client.clear_table.assert_called_once_with(table_name="test_table")

    @patch.object(KineticaDatasink, "_init_client")
    @patch("ray.data._internal.datasource.kinetica_type_utils.arrow_schema_to_kinetica_columns")
    def test_create_table(self, mock_arrow_to_kinetica, mock_init_client, mock_gpudb_sink_client):
        """Test _create_table method."""
        from gpudb import GPUdbRecordColumn, GPUdbRecordType

        mock_init_client.return_value = mock_gpudb_sink_client

        # Mock columns
        mock_columns = [
            GPUdbRecordColumn(
                name="id",
                column_type=GPUdbRecordColumn._ColumnType.LONG,
                column_properties=[],
                is_nullable=False,
            ),
        ]

        # Mock record type
        mock_record_type = MagicMock(spec=GPUdbRecordType)
        mock_record_type.create_type.return_value = "type_123"
        mock_record_type.schema_string = "schema_string"
        mock_record_type.column_properties = {}

        with patch("gpudb.GPUdbRecordType", return_value=mock_record_type):
            ds = KineticaDatasink(
                url="http://localhost:9191",
                table_name="test_table",
            )

            ds._create_table(mock_gpudb_sink_client, mock_columns)

            mock_gpudb_sink_client.create_table.assert_called_once()

    @patch.object(KineticaDatasink, "_init_client")
    @pytest.mark.parametrize(
        "mode, table_exists, should_create",
        [
            (KineticaSinkMode.CREATE, False, True),
            (KineticaSinkMode.APPEND, False, True),
            (KineticaSinkMode.APPEND, True, False),
            (KineticaSinkMode.OVERWRITE, False, True),
            (KineticaSinkMode.OVERWRITE, True, True),
        ],
    )
    def test_on_write_start_modes(
        self, mock_init_client, mock_gpudb_sink_client, mode, table_exists, should_create
    ):
        """Test on_write_start with different modes."""
        mock_init_client.return_value = mock_gpudb_sink_client
        mock_gpudb_sink_client.has_table.return_value = {"table_exists": table_exists}

        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
        ])

        with patch.object(KineticaDatasink, "_create_table") as mock_create, \
             patch.object(KineticaDatasink, "_drop_table") as mock_drop, \
             patch.object(KineticaDatasink, "_get_existing_record_type") as mock_get_type, \
             patch("ray.data._internal.datasource.kinetica_type_utils.arrow_schema_to_kinetica_columns") as mock_arrow_to_kinetica:

            mock_arrow_to_kinetica.return_value = []

            # Mock existing record type
            mock_record_type = MagicMock()
            mock_record_type.columns = []
            mock_get_type.return_value = mock_record_type

            ds = KineticaDatasink(
                url="http://localhost:9191",
                table_name="test_table",
                mode=mode,
                schema=schema,
            )

            ds.on_write_start(schema)

            if mode == KineticaSinkMode.OVERWRITE and table_exists:
                mock_drop.assert_called_once()

            if should_create:
                if mode == KineticaSinkMode.APPEND and table_exists:
                    mock_get_type.assert_called_once()
                else:
                    # CREATE or OVERWRITE should create table
                    if mode != KineticaSinkMode.APPEND or not table_exists:
                        mock_create.assert_called()

    @patch.object(KineticaDatasink, "_init_client")
    def test_on_write_start_create_existing_table_fails(self, mock_init_client, mock_gpudb_sink_client):
        """Test that CREATE mode fails if table already exists."""
        from gpudb import GPUdbException

        mock_init_client.return_value = mock_gpudb_sink_client
        mock_gpudb_sink_client.has_table.return_value = {"table_exists": True}

        schema = pa.schema([pa.field("id", pa.int64())])

        ds = KineticaDatasink(
            url="http://localhost:9191",
            table_name="test_table",
            mode=KineticaSinkMode.CREATE,
            schema=schema,
        )

        with pytest.raises(GPUdbException, match="already exists"):
            ds.on_write_start(schema)

    @patch.object(KineticaDatasink, "_init_client")
    @patch("ray.data._internal.datasource.kinetica_type_utils.arrow_schema_to_kinetica_columns")
    @patch("ray.data._internal.datasource.kinetica_type_utils.convert_arrow_batch_to_records")
    def test_write(
        self,
        mock_convert_batch,
        mock_arrow_to_kinetica,
        mock_init_client,
        mock_gpudb_sink_client,
    ):
        """Test write method."""
        mock_init_client.return_value = mock_gpudb_sink_client
        mock_gpudb_sink_client.has_table.return_value = {"table_exists": False}

        # Mock conversion functions
        mock_arrow_to_kinetica.return_value = []
        mock_convert_batch.return_value = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]

        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
        ])

        # Create test data
        rb = pa.record_batch(
            [pa.array([1, 2]), pa.array(["Alice", "Bob"])],
            names=["id", "name"],
        )
        block_data = pa.Table.from_batches([rb])

        with patch.object(KineticaDatasink, "_create_table"):
            ds = KineticaDatasink(
                url="http://localhost:9191",
                table_name="test_table",
                mode=KineticaSinkMode.CREATE,
                schema=schema,
            )
            ds._column_defs = []

            ctx = TaskContext(task_idx=0, target_max_block_size=1024)
            result = ds.write([block_data], ctx=ctx)

            assert "num_inserted" in result
            assert "num_updated" in result
            assert "errors" in result


# ============================================================================
# Type Utils Tests
# ============================================================================


class TestKineticaTypeUtils:
    """Tests for type conversion utilities."""

    def test_arrow_schema_conversion(self):
        """Test converting Arrow schema to Kinetica columns."""
        from ray.data._internal.datasource.kinetica_type_utils import (
            arrow_schema_to_kinetica_columns,
        )

        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("value", pa.float64()),
            pa.field("active", pa.bool_()),
        ])

        columns = arrow_schema_to_kinetica_columns(schema)

        assert len(columns) == 4
        assert columns[0].name == "id"
        assert columns[1].name == "name"
        assert columns[2].name == "value"
        assert columns[3].name == "active"

    def test_arrow_schema_with_keys(self):
        """Test converting Arrow schema with primary/shard keys."""
        from ray.data._internal.datasource.kinetica_type_utils import (
            arrow_schema_to_kinetica_columns,
        )
        from gpudb import GPUdbColumnProperty

        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("region", pa.string()),
            pa.field("value", pa.float64()),
        ])

        columns = arrow_schema_to_kinetica_columns(
            schema,
            primary_keys=["id"],
            shard_keys=["region"],
        )

        assert GPUdbColumnProperty.PRIMARY_KEY in columns[0].column_properties
        assert GPUdbColumnProperty.SHARD_KEY in columns[1].column_properties


# ============================================================================
# Module Import Tests
# ============================================================================


class TestModuleImports:
    """Tests for module imports and API exposure."""

    def test_read_kinetica_importable(self):
        """Test read_kinetica is importable from ray.data."""
        from ray.data import read_kinetica
        assert callable(read_kinetica)

    def test_read_kinetica_sql_importable(self):
        """Test read_kinetica_sql is importable from ray.data."""
        from ray.data import read_kinetica_sql
        assert callable(read_kinetica_sql)

    def test_datasource_importable(self):
        """Test KineticaDatasource is importable."""
        from ray.data._internal.datasource.kinetica_datasource import (
            KineticaDatasource,
        )
        assert KineticaDatasource is not None

    def test_datasink_importable(self):
        """Test KineticaDatasink is importable."""
        from ray.data._internal.datasource.kinetica_datasink import (
            KineticaDatasink,
            KineticaSinkMode,
            KineticaTableSettings,
        )
        assert KineticaDatasink is not None
        assert KineticaSinkMode is not None
        assert KineticaTableSettings is not None

    def test_sql_connection_factory_importable(self):
        """Test SQL connection factory is importable."""
        from ray.data._internal.datasource.kinetica_sql_connection import (
            KineticaConnectionFactory,
            create_kinetica_connection_factory,
        )
        assert KineticaConnectionFactory is not None
        assert callable(create_kinetica_connection_factory)


# ============================================================================
# Integration Tests (require Kinetica server)
# ============================================================================


@pytest.mark.skipif(
    not os.environ.get("KINETICA_URL"),
    reason="Integration tests require KINETICA_URL environment variable"
)
class TestKineticaIntegration:
    """Integration tests requiring a running Kinetica server."""

    @pytest.fixture
    def connection_params(self):
        """Get connection parameters from environment."""
        return {
            "url": os.environ.get("KINETICA_URL", "http://localhost:9191"),
            "username": os.environ.get("KINETICA_USER", "admin"),
            "password": os.environ.get("KINETICA_PASS", ""),
        }

    def test_read_simple_query(self, connection_params):
        """Test reading data from a Kinetica table."""
        import ray

        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True)

        try:
            ds = ray.data.read_kinetica(
                table_name="ki_home.ki_catalog_ddl",  # System table that should exist
                **connection_params,
                limit=10,
            )

            count = ds.count()
            assert count >= 0  # Table might be empty but query should work

        finally:
            pass

    def test_write_and_read_roundtrip(self, connection_params):
        """Test writing and reading back data."""
        import ray

        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True)

        table_name = "test_ray_roundtrip"

        try:
            # Create test data
            data = [
                {"id": 1, "name": "Alice", "value": 100.5},
                {"id": 2, "name": "Bob", "value": 200.75},
                {"id": 3, "name": "Charlie", "value": 300.25},
            ]
            ds = ray.data.from_items(data)

            # Write to Kinetica
            ds.write_kinetica(
                table_name=table_name,
                mode="overwrite",
                **connection_params,
            )

            # Read back
            read_ds = ray.data.read_kinetica(
                table_name=table_name,
                **connection_params,
            )

            # Verify
            assert read_ds.count() == 3

        finally:
            # Cleanup: drop the test table
            try:
                from gpudb import GPUdb
                client = GPUdb(
                    host=connection_params["url"],
                    username=connection_params.get("username"),
                    password=connection_params.get("password"),
                )
                client.clear_table(table_name=table_name)
            except Exception:
                pass

    def test_read_with_filter(self, connection_params):
        """Test reading with a filter expression."""
        import ray

        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True)

        table_name = "test_ray_filter"

        try:
            # Create test data
            data = [
                {"id": i, "value": i * 10}
                for i in range(100)
            ]
            ds = ray.data.from_items(data)

            # Write to Kinetica
            ds.write_kinetica(
                table_name=table_name,
                mode="overwrite",
                **connection_params,
            )

            # Read with filter
            read_ds = ray.data.read_kinetica(
                table_name=table_name,
                filter_expression="value >= 500",
                **connection_params,
            )

            # Verify filter worked
            count = read_ds.count()
            assert count == 50  # ids 50-99 have values >= 500

        finally:
            # Cleanup
            try:
                from gpudb import GPUdb
                client = GPUdb(
                    host=connection_params["url"],
                    username=connection_params.get("username"),
                    password=connection_params.get("password"),
                )
                client.clear_table(table_name=table_name)
            except Exception:
                pass

    def test_read_specific_columns(self, connection_params):
        """Test reading specific columns."""
        import ray

        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True)

        table_name = "test_ray_columns"

        try:
            # Create test data
            data = [
                {"id": 1, "name": "Alice", "value": 100.5, "extra": "data"},
            ]
            ds = ray.data.from_items(data)

            # Write to Kinetica
            ds.write_kinetica(
                table_name=table_name,
                mode="overwrite",
                **connection_params,
            )

            # Read specific columns
            read_ds = ray.data.read_kinetica(
                table_name=table_name,
                columns=["id", "name"],
                **connection_params,
            )

            # Verify only requested columns present
            row = read_ds.take(1)[0]
            assert "id" in row
            assert "name" in row
            assert "value" not in row
            assert "extra" not in row

        finally:
            # Cleanup
            try:
                from gpudb import GPUdb
                client = GPUdb(
                    host=connection_params["url"],
                    username=connection_params.get("username"),
                    password=connection_params.get("password"),
                )
                client.clear_table(table_name=table_name)
            except Exception:
                pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
