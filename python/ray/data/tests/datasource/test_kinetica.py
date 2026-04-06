"""
Tests for the Kinetica Ray Data integration.

These tests verify the KineticaDatasource and KineticaDatasink work correctly.
Integration tests require a running Kinetica server.
"""

import os
import pytest
from unittest.mock import Mock, patch, MagicMock


# ============================================================================
# KineticaDatasource Tests
# ============================================================================

class TestKineticaDatasource:
    """Tests for the KineticaDatasource class."""

    def test_datasource_initialization(self):
        """Test KineticaDatasource can be created with parameters."""
        from ray.data._internal.datasource.kinetica_datasource import (
            KineticaDatasource,
        )

        ds = KineticaDatasource(
            url="http://localhost:9191",
            table_name="test_table",
            username="admin",
            password="password",
            columns=["id", "name"],
            filter_expression="id > 100",
            batch_size=5000,
        )

        assert ds._url == "http://localhost:9191"
        assert ds._table_name == "test_table"
        assert ds._username == "admin"
        assert ds._columns == ["id", "name"]
        assert ds._filter_expression == "id > 100"
        assert ds._batch_size == 5000

    def test_datasource_default_batch_size(self):
        """Test KineticaDatasource uses default batch size."""
        from ray.data._internal.datasource.kinetica_datasource import (
            KineticaDatasource,
        )

        ds = KineticaDatasource(
            url="http://localhost:9191",
            table_name="test_table",
        )

        assert ds._batch_size == 10000  # Default

    def test_unsafe_filter_rejected(self):
        """Test that unsafe filter expressions are rejected."""
        from ray.data._internal.datasource.kinetica_datasource import (
            KineticaDatasource,
        )

        with pytest.raises(ValueError, match="unsafe characters"):
            KineticaDatasource(
                url="http://localhost:9191",
                table_name="test_table",
                filter_expression="id = 1; DROP TABLE test;",
            )

    def test_get_name(self):
        """Test datasource name generation."""
        from ray.data._internal.datasource.kinetica_datasource import (
            KineticaDatasource,
        )

        ds = KineticaDatasource(
            url="http://localhost:9191",
            table_name="my_table",
        )

        assert ds.get_name() == "Kinetica(my_table)"


# ============================================================================
# KineticaDatasink Tests
# ============================================================================

class TestKineticaDatasink:
    """Tests for the KineticaDatasink class."""

    def test_datasink_initialization(self):
        """Test KineticaDatasink can be created with parameters."""
        from ray.data._internal.datasource.kinetica_datasink import (
            KineticaDatasink,
            KineticaSinkMode,
        )

        ds = KineticaDatasink(
            url="http://localhost:9191",
            table_name="test_table",
            username="admin",
            password="password",
            mode=KineticaSinkMode.APPEND,
            batch_size=5000,
        )

        assert ds._url == "http://localhost:9191"
        assert ds._table_name == "test_table"
        assert ds._username == "admin"
        assert ds._mode == KineticaSinkMode.APPEND
        assert ds._batch_size == 5000

    def test_datasink_string_mode(self):
        """Test KineticaDatasink accepts string mode values."""
        from ray.data._internal.datasource.kinetica_datasink import (
            KineticaDatasink,
            KineticaSinkMode,
        )

        ds = KineticaDatasink(
            url="http://localhost:9191",
            table_name="test_table",
            mode="overwrite",
        )

        assert ds._mode == KineticaSinkMode.OVERWRITE

    def test_table_settings(self):
        """Test KineticaTableSettings configuration."""
        from ray.data._internal.datasource.kinetica_datasink import (
            KineticaDatasink,
            KineticaTableSettings,
        )

        settings = KineticaTableSettings(
            is_replicated=True,
            chunk_size=1000000,
            ttl=60,
            primary_keys=["id"],
            shard_keys=["region"],
        )

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

    def test_get_name(self):
        """Test datasink name generation."""
        from ray.data._internal.datasource.kinetica_datasink import KineticaDatasink

        ds = KineticaDatasink(
            url="http://localhost:9191",
            table_name="my_table",
        )

        assert ds.get_name() == "Kinetica(my_table)"

    def test_supports_distributed_writes(self):
        """Test datasink reports distributed write support."""
        from ray.data._internal.datasource.kinetica_datasink import KineticaDatasink

        ds = KineticaDatasink(
            url="http://localhost:9191",
            table_name="test_table",
        )

        assert ds.supports_distributed_writes is True


# ============================================================================
# Type Utils Tests
# ============================================================================

class TestKineticaTypeUtils:
    """Tests for type conversion utilities."""

    def test_arrow_schema_conversion(self):
        """Test converting Arrow schema to Kinetica columns."""
        import pyarrow as pa
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
        import pyarrow as pa
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
