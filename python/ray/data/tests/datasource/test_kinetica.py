"""
Tests for the Kinetica Ray Data integration.

These tests use mocks to verify the KineticaDatasource and KineticaDatasink
work correctly without requiring a running Kinetica server.
"""

import json
import os
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from ray.data._internal.datasource.kinetica_datasink import (
    KineticaDatasink,
    KineticaSinkMode,
    KineticaTableSettings,
)
from ray.data._internal.datasource.kinetica_datasource import (
    KineticaDatasource,
    _has_balanced_quotes,
    _is_filter_safe,
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
        "type_schemas": [
            json.dumps(
                {
                    "fields": [
                        {"name": "id", "type": "long"},
                        {"name": "name", "type": "string"},
                        {"name": "value", "type": "double"},
                    ]
                }
            )
        ],
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
        "type_schemas": [
            json.dumps(
                {
                    "fields": [
                        {"name": "id", "type": "long"},
                        {"name": "name", "type": "string"},
                    ]
                }
            )
        ],
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
            # Safe expressions
            ("id > 100", True),
            ("name = 'Alice' AND value > 50", True),
            ("id > 100 AND name IS NOT NULL", True),
            # Keywords inside string literals are allowed (not injection)
            ("city = 'Union City'", True),
            ("status = 'DROP_PENDING'", True),
            ("name = 'Delete Me'", True),
            ('comment = "ALTER this later"', True),
            # Escaped quotes in strings
            ("name = 'O''Brien'", True),
            # Unsafe expressions - keywords outside strings
            ("id = 1; DROP TABLE test;", False),
            ("id > 100; SELECT * FROM users", False),
            ("id IN {1, 2, 3}", False),
            # Keywords outside of string context
            ("id = 1 UNION SELECT * FROM secrets", False),
            ("id = 1 -- comment injection", False),
            ("id = 1 /* block comment */", False),
            # Unclosed string literals (could bypass stripping)
            ("x = ''' ; DROP TABLE t", False),
            ('x = """ ; DROP TABLE t', False),
            ("name = 'value", False),
            ('comment = "test', False),
        ],
    )
    def test_is_filter_safe(self, filter_expr, is_safe):
        """Test filter safety validation."""
        assert _is_filter_safe(filter_expr) == is_safe

    def test_balanced_quotes_detection(self):
        """Test the _has_balanced_quotes helper function."""
        # Balanced quotes
        assert _has_balanced_quotes("name = 'Alice'") is True
        assert _has_balanced_quotes('status = "active"') is True
        assert _has_balanced_quotes("name = 'O''Brien'") is True
        assert _has_balanced_quotes('msg = "He said ""hi"""') is True
        assert _has_balanced_quotes("id = 1") is True

        # Unbalanced quotes
        assert _has_balanced_quotes("name = 'Alice") is False
        assert _has_balanced_quotes('status = "active') is False
        assert _has_balanced_quotes("x = '''") is False


# ============================================================================
# KineticaDatasource Tests
# ============================================================================


class TestKineticaDatasource:
    """Tests for KineticaDatasource."""

    @pytest.fixture
    def datasource(self):
        """Create a KineticaDatasource with test parameters."""
        with patch(
            "ray.data._internal.datasource.kinetica_datasource.KineticaDatasource._init_client"
        ):
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
        with patch(
            "ray.data._internal.datasource.kinetica_datasource.KineticaDatasource._init_client"
        ):
            ds = KineticaDatasource(
                url="http://localhost:9191",
                table_name="test_table",
            )
            assert ds._batch_size == 10000

    def test_unsafe_filter_rejected(self):
        """Test that unsafe filter expressions are rejected without leaking input."""
        with pytest.raises(ValueError, match="unsafe patterns") as exc_info:
            KineticaDatasource(
                url="http://localhost:9191",
                table_name="test_table",
                filter_expression="id = 1; DROP TABLE test;",
            )
        # Verify the error message does NOT include the raw user input
        # (to prevent log injection)
        assert "DROP TABLE" not in str(exc_info.value)

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

        total_count, arrow_schema = ds._get_table_info(mock_gpudb_client)

        assert total_count == 100
        assert arrow_schema is not None
        assert len(arrow_schema) == 3

    @patch.object(KineticaDatasource, "_init_client")
    def test_get_table_info_with_filter(self, mock_init_client, mock_gpudb_client):
        """Test _get_table_info with filter expression."""
        mock_init_client.return_value = mock_gpudb_client

        ds = KineticaDatasource(
            url="http://localhost:9191",
            table_name="test_table",
            filter_expression="id > 50",
        )

        total_count, arrow_schema = ds._get_table_info(mock_gpudb_client)

        assert total_count >= 0
        assert arrow_schema is not None

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
        with patch(
            "ray.data._internal.datasource.kinetica_datasink.KineticaDatasink._init_client"
        ):
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
        with patch(
            "ray.data._internal.datasource.kinetica_datasink.KineticaDatasink._init_client"
        ):
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

        with patch(
            "ray.data._internal.datasource.kinetica_datasink.KineticaDatasink._init_client"
        ):
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
        """Test datasink reports distributed write support based on table readiness."""
        # Before on_write_start is called, _table_ready is False
        # so distributed writes should be disabled to prevent race conditions
        assert datasink.supports_distributed_writes is False

        # After table is ready, distributed writes should be enabled
        datasink._table_ready = True
        assert datasink.supports_distributed_writes is True

    def test_min_rows_per_write(self, datasink):
        """Test min_rows_per_write property (used by Ray Data framework)."""
        assert datasink.min_rows_per_write == 5000

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
        """Test _drop_table method uses no_error_if_not_exists option."""
        mock_init_client.return_value = mock_gpudb_sink_client

        ds = KineticaDatasink(
            url="http://localhost:9191",
            table_name="test_table",
        )

        ds._drop_table(mock_gpudb_sink_client)

        mock_gpudb_sink_client.clear_table.assert_called_once_with(
            table_name="test_table",
            options={"no_error_if_not_exists": "true"},
        )

    @patch.object(KineticaDatasink, "_init_client")
    @patch(
        "ray.data._internal.datasource.kinetica_type_utils.arrow_schema_to_kinetica_columns"
    )
    def test_create_table(
        self, mock_arrow_to_kinetica, mock_init_client, mock_gpudb_sink_client
    ):
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
        self,
        mock_init_client,
        mock_gpudb_sink_client,
        mode,
        table_exists,
        should_create,
    ):
        """Test on_write_start with different modes."""
        mock_init_client.return_value = mock_gpudb_sink_client
        mock_gpudb_sink_client.has_table.return_value = {"table_exists": table_exists}

        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
            ]
        )

        with (
            patch.object(KineticaDatasink, "_create_table") as mock_create,
            patch.object(KineticaDatasink, "_drop_table") as mock_drop,
            patch.object(
                KineticaDatasink, "_get_existing_record_type"
            ) as mock_get_type,
            patch(
                "ray.data._internal.datasource.kinetica_type_utils.arrow_schema_to_kinetica_columns"
            ) as mock_arrow_to_kinetica,
        ):
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
    def test_on_write_start_create_existing_table_fails(
        self, mock_init_client, mock_gpudb_sink_client
    ):
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
    @patch(
        "ray.data._internal.datasource.kinetica_type_utils.arrow_schema_to_kinetica_columns"
    )
    @patch(
        "ray.data._internal.datasource.kinetica_type_utils.convert_arrow_batch_to_records"
    )
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

        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
            ]
        )

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

            ctx = TaskContext(task_idx=0, op_name="test_write")
            result = ds.write([block_data], ctx=ctx)

            assert "num_inserted" in result
            assert "num_updated" in result
            # Note: write raises RuntimeError on errors instead of returning them


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

        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
                pa.field("value", pa.float64()),
                pa.field("active", pa.bool_()),
            ]
        )

        columns = arrow_schema_to_kinetica_columns(schema)

        assert len(columns) == 4
        assert columns[0].name == "id"
        assert columns[1].name == "name"
        assert columns[2].name == "value"
        assert columns[3].name == "active"

    def test_arrow_schema_with_keys(self):
        """Test converting Arrow schema with primary/shard keys."""
        from gpudb import GPUdbColumnProperty

        from ray.data._internal.datasource.kinetica_type_utils import (
            arrow_schema_to_kinetica_columns,
        )

        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("region", pa.string()),
                pa.field("value", pa.float64()),
            ]
        )

        columns = arrow_schema_to_kinetica_columns(
            schema,
            primary_keys=["id"],
            shard_keys=["region"],
        )

        assert GPUdbColumnProperty.PRIMARY_KEY in columns[0].column_properties
        assert GPUdbColumnProperty.SHARD_KEY in columns[1].column_properties

    def test_arrow_schema_with_invalid_keys_rejected(self):
        """Test that invalid primary/shard keys raise ValueError."""
        from ray.data._internal.datasource.kinetica_type_utils import (
            arrow_schema_to_kinetica_columns,
        )

        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
            ]
        )

        # Invalid primary key
        with pytest.raises(ValueError, match="non-existent columns"):
            arrow_schema_to_kinetica_columns(
                schema,
                primary_keys=["nonexistent_column"],
            )

        # Invalid shard key
        with pytest.raises(ValueError, match="non-existent columns"):
            arrow_schema_to_kinetica_columns(
                schema,
                shard_keys=["also_nonexistent"],
            )

        # Both invalid
        with pytest.raises(ValueError, match="non-existent columns"):
            arrow_schema_to_kinetica_columns(
                schema,
                primary_keys=["bad_pk"],
                shard_keys=["bad_sk"],
            )

    def test_arrow_to_kinetica_integer_types(self):
        """Test Arrow integer types convert correctly to Kinetica."""
        from gpudb import GPUdbColumnProperty

        from ray.data._internal.datasource.kinetica_type_utils import (
            arrow_to_kinetica_type,
        )

        # Signed integers
        assert arrow_to_kinetica_type(pa.int8()) == (
            "int",
            [GPUdbColumnProperty.INT8],
        )
        assert arrow_to_kinetica_type(pa.int16()) == (
            "int",
            [GPUdbColumnProperty.INT16],
        )
        assert arrow_to_kinetica_type(pa.int32()) == ("int", [])
        assert arrow_to_kinetica_type(pa.int64()) == ("long", [])

        # Unsigned integers
        assert arrow_to_kinetica_type(pa.uint8()) == (
            "int",
            [GPUdbColumnProperty.INT16],
        )
        assert arrow_to_kinetica_type(pa.uint16()) == ("int", [])
        assert arrow_to_kinetica_type(pa.uint32()) == ("long", [])
        assert arrow_to_kinetica_type(pa.uint64()) == (
            "string",
            [GPUdbColumnProperty.ULONG],
        )

    def test_arrow_to_kinetica_float_types(self):
        """Test Arrow float types convert correctly to Kinetica."""
        from ray.data._internal.datasource.kinetica_type_utils import (
            arrow_to_kinetica_type,
        )

        assert arrow_to_kinetica_type(pa.float32()) == ("float", [])
        assert arrow_to_kinetica_type(pa.float64()) == ("double", [])

    def test_arrow_to_kinetica_datetime_types(self):
        """Test Arrow date/time types convert correctly to Kinetica."""
        from gpudb import GPUdbColumnProperty

        from ray.data._internal.datasource.kinetica_type_utils import (
            arrow_to_kinetica_type,
        )

        assert arrow_to_kinetica_type(pa.date32()) == (
            "string",
            [GPUdbColumnProperty.DATE],
        )
        assert arrow_to_kinetica_type(pa.date64()) == (
            "string",
            [GPUdbColumnProperty.DATE],
        )
        assert arrow_to_kinetica_type(pa.time32("ms")) == (
            "string",
            [GPUdbColumnProperty.TIME],
        )
        assert arrow_to_kinetica_type(pa.time64("us")) == (
            "string",
            [GPUdbColumnProperty.TIME],
        )
        assert arrow_to_kinetica_type(pa.timestamp("us")) == (
            "string",
            [GPUdbColumnProperty.DATETIME],
        )

    def test_arrow_to_kinetica_other_types(self):
        """Test Arrow boolean, string, binary types convert correctly."""
        from gpudb import GPUdbColumnProperty

        from ray.data._internal.datasource.kinetica_type_utils import (
            arrow_to_kinetica_type,
        )

        assert arrow_to_kinetica_type(pa.bool_()) == (
            "int",
            [GPUdbColumnProperty.BOOLEAN],
        )
        assert arrow_to_kinetica_type(pa.string()) == ("string", [])
        assert arrow_to_kinetica_type(pa.binary()) == ("bytes", [])

    def test_kinetica_to_arrow_case_insensitive(self):
        """Test Kinetica to Arrow conversion is case-insensitive."""
        from gpudb import GPUdbRecordColumn

        from ray.data._internal.datasource.kinetica_type_utils import (
            kinetica_to_arrow_type,
        )

        # Helper to create mock column
        def make_col(col_type, props):
            col = GPUdbRecordColumn(
                name="test",
                column_type=col_type,
                column_properties=props,
            )
            return col

        # Test lowercase properties
        assert (
            kinetica_to_arrow_type(
                make_col(GPUdbRecordColumn._ColumnType.INT, ["boolean"])
            )
            == pa.bool_()
        )
        assert (
            kinetica_to_arrow_type(
                make_col(GPUdbRecordColumn._ColumnType.INT, ["int8"])
            )
            == pa.int8()
        )
        assert (
            kinetica_to_arrow_type(
                make_col(GPUdbRecordColumn._ColumnType.STRING, ["date"])
            )
            == pa.date32()
        )
        assert kinetica_to_arrow_type(
            make_col(GPUdbRecordColumn._ColumnType.STRING, ["time"])
        ) == pa.time64("us")
        assert kinetica_to_arrow_type(
            make_col(GPUdbRecordColumn._ColumnType.STRING, ["datetime"])
        ) == pa.timestamp("us")

        # Test uppercase properties
        assert (
            kinetica_to_arrow_type(
                make_col(GPUdbRecordColumn._ColumnType.INT, ["BOOLEAN"])
            )
            == pa.bool_()
        )
        assert (
            kinetica_to_arrow_type(
                make_col(GPUdbRecordColumn._ColumnType.INT, ["INT8"])
            )
            == pa.int8()
        )
        assert (
            kinetica_to_arrow_type(
                make_col(GPUdbRecordColumn._ColumnType.STRING, ["DATE"])
            )
            == pa.date32()
        )
        assert kinetica_to_arrow_type(
            make_col(GPUdbRecordColumn._ColumnType.STRING, ["DATETIME"])
        ) == pa.timestamp("us")

        # Test mixed case properties
        assert (
            kinetica_to_arrow_type(
                make_col(GPUdbRecordColumn._ColumnType.INT, ["Boolean"])
            )
            == pa.bool_()
        )
        assert kinetica_to_arrow_type(
            make_col(GPUdbRecordColumn._ColumnType.STRING, ["DateTime"])
        ) == pa.timestamp("us")

    def test_convert_arrow_batch_datetime_serialization(self):
        """Test date/time values serialize to ISO format strings."""
        from datetime import date, datetime, time

        from gpudb import GPUdbColumnProperty, GPUdbRecordColumn

        from ray.data._internal.datasource.kinetica_type_utils import (
            convert_arrow_batch_to_records,
        )

        # Create test data with date/time columns
        data = {
            "id": [1, 2],
            "date_col": [date(2024, 1, 15), date(2024, 12, 31)],
            "time_col": [time(10, 30, 45), time(23, 59, 59)],
            "datetime_col": [
                datetime(2024, 1, 15, 10, 30, 45),
                datetime(2024, 12, 31, 23, 59, 59),
            ],
        }
        batch = pa.RecordBatch.from_pydict(data)

        # Create column definitions
        columns = [
            GPUdbRecordColumn(
                name="id",
                column_type=GPUdbRecordColumn._ColumnType.INT,
                column_properties=[],
            ),
            GPUdbRecordColumn(
                name="date_col",
                column_type=GPUdbRecordColumn._ColumnType.STRING,
                column_properties=[GPUdbColumnProperty.DATE],
            ),
            GPUdbRecordColumn(
                name="time_col",
                column_type=GPUdbRecordColumn._ColumnType.STRING,
                column_properties=[GPUdbColumnProperty.TIME],
            ),
            GPUdbRecordColumn(
                name="datetime_col",
                column_type=GPUdbRecordColumn._ColumnType.STRING,
                column_properties=[GPUdbColumnProperty.DATETIME],
            ),
        ]

        records = convert_arrow_batch_to_records(batch, columns)

        # Verify date serialization
        assert records[0]["date_col"] == "2024-01-15"
        assert records[1]["date_col"] == "2024-12-31"

        # Verify time serialization
        assert records[0]["time_col"] == "10:30:45"
        assert records[1]["time_col"] == "23:59:59"

        # Verify datetime serialization (ISO format)
        assert "2024-01-15" in records[0]["datetime_col"]
        assert "10:30:45" in records[0]["datetime_col"]

    def test_convert_arrow_batch_datetime_json_serializable(self):
        """Test that records with date/time are JSON serializable."""
        import json
        from datetime import date, datetime, time

        from gpudb import GPUdbColumnProperty, GPUdbRecordColumn

        from ray.data._internal.datasource.kinetica_type_utils import (
            convert_arrow_batch_to_records,
        )

        data = {
            "id": [1],
            "date_col": [date(2024, 1, 15)],
            "time_col": [time(10, 30, 45)],
            "datetime_col": [datetime(2024, 1, 15, 10, 30, 45)],
        }
        batch = pa.RecordBatch.from_pydict(data)

        columns = [
            GPUdbRecordColumn(
                name="id",
                column_type=GPUdbRecordColumn._ColumnType.INT,
                column_properties=[],
            ),
            GPUdbRecordColumn(
                name="date_col",
                column_type=GPUdbRecordColumn._ColumnType.STRING,
                column_properties=[GPUdbColumnProperty.DATE],
            ),
            GPUdbRecordColumn(
                name="time_col",
                column_type=GPUdbRecordColumn._ColumnType.STRING,
                column_properties=[GPUdbColumnProperty.TIME],
            ),
            GPUdbRecordColumn(
                name="datetime_col",
                column_type=GPUdbRecordColumn._ColumnType.STRING,
                column_properties=[GPUdbColumnProperty.DATETIME],
            ),
        ]

        records = convert_arrow_batch_to_records(batch, columns)

        # This should not raise TypeError
        json_str = json.dumps(records[0])
        parsed = json.loads(json_str)

        assert isinstance(parsed["date_col"], str)
        assert isinstance(parsed["time_col"], str)
        assert isinstance(parsed["datetime_col"], str)

    def test_convert_arrow_batch_null_datetime(self):
        """Test that null date/time values are handled correctly."""
        from datetime import date

        from gpudb import GPUdbColumnProperty, GPUdbRecordColumn

        from ray.data._internal.datasource.kinetica_type_utils import (
            convert_arrow_batch_to_records,
        )

        data = {
            "id": [1, 2],
            "date_col": [date(2024, 1, 15), None],
        }
        batch = pa.RecordBatch.from_pydict(data)

        columns = [
            GPUdbRecordColumn(
                name="id",
                column_type=GPUdbRecordColumn._ColumnType.INT,
                column_properties=[],
            ),
            GPUdbRecordColumn(
                name="date_col",
                column_type=GPUdbRecordColumn._ColumnType.STRING,
                column_properties=[GPUdbColumnProperty.DATE],
            ),
        ]

        records = convert_arrow_batch_to_records(batch, columns)

        assert records[0]["date_col"] == "2024-01-15"
        assert records[1]["date_col"] is None

    def test_convert_records_to_arrow_error_handling(self):
        """Test that type conversion errors include column name."""
        from ray.data._internal.datasource.kinetica_type_utils import (
            convert_records_to_arrow_table,
        )

        schema = pa.schema([pa.field("int_col", pa.int64())])
        bad_records = [{"int_col": "not_an_integer"}]

        with pytest.raises(pa.ArrowTypeError) as exc_info:
            convert_records_to_arrow_table(bad_records, schema)

        error_msg = str(exc_info.value)
        assert "int_col" in error_msg

    def test_vector_bytes_json_serialization(self):
        """Test that vector (bytes) can be JSON serialized via base64."""
        import base64
        import json
        import struct

        # Simulate the custom serializer used in _write_simple
        def json_serializer(obj):
            if isinstance(obj, bytes):
                return base64.b64encode(obj).decode("ascii")
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

        # Create a 3D vector
        vector_bytes = struct.pack("3f", 1.0, 2.0, 3.0)
        record = {"id": 1, "embedding": vector_bytes}

        # Should serialize without error
        json_str = json.dumps(record, default=json_serializer)
        parsed = json.loads(json_str)

        assert isinstance(parsed["embedding"], str)

        # Verify we can decode back to original bytes
        decoded = base64.b64decode(parsed["embedding"])
        assert decoded == vector_bytes

    def test_decimal_scale_zero_preserved(self):
        """Test that decimal scale=0 is preserved, not treated as falsy."""
        from gpudb import GPUdbRecordColumn

        from ray.data._internal.datasource.kinetica_type_utils import (
            kinetica_to_arrow_type,
        )

        # Create a decimal column with explicit scale=0
        col = GPUdbRecordColumn(
            name="amount",
            column_type=GPUdbRecordColumn._ColumnType.STRING,
            column_properties=["decimal"],
            precision=10,
            scale=0,  # Integer decimal (no decimal places)
        )

        arrow_type = kinetica_to_arrow_type(col)

        # Verify it's a decimal type
        assert pa.types.is_decimal(arrow_type), f"Expected decimal, got {arrow_type}"
        # Verify scale is 0, not the default (4)
        assert arrow_type.scale == 0, (
            f"Expected scale=0, got {arrow_type.scale}. "
            "Scale=0 should not be treated as falsy."
        )
        assert arrow_type.precision == 10

    def test_decimal_scale_none_uses_default(self):
        """Test that decimal with scale=None uses the default scale."""
        from gpudb import GPUdbRecordColumn

        from ray.data._internal.datasource.kinetica_type_utils import (
            kinetica_to_arrow_type,
        )

        # Create a decimal column without explicit scale
        col = GPUdbRecordColumn(
            name="amount",
            column_type=GPUdbRecordColumn._ColumnType.STRING,
            column_properties=["decimal"],
            precision=18,
            scale=None,  # Should use default
        )

        arrow_type = kinetica_to_arrow_type(col)

        assert pa.types.is_decimal(arrow_type)
        # Default scale is 4
        assert arrow_type.scale == GPUdbRecordColumn.DEFAULT_DECIMAL_SCALE

    def test_fixed_size_list_float_to_vector(self):
        """Test fixed-size list of floats maps to Kinetica vector type."""
        from ray.data._internal.datasource.kinetica_type_utils import (
            arrow_to_kinetica_type,
        )

        # 3D float vector
        arrow_type = pa.list_(pa.float32(), 3)
        kinetica_type, props = arrow_to_kinetica_type(arrow_type)

        assert kinetica_type == "bytes", f"Expected 'bytes', got {kinetica_type}"
        assert "vector(3)" in props, f"Expected 'vector(3)' in props, got {props}"

    def test_fixed_size_list_double_to_vector(self):
        """Test fixed-size list of doubles maps to Kinetica vector type."""
        from ray.data._internal.datasource.kinetica_type_utils import (
            arrow_to_kinetica_type,
        )

        # 128D double vector
        arrow_type = pa.list_(pa.float64(), 128)
        kinetica_type, props = arrow_to_kinetica_type(arrow_type)

        assert kinetica_type == "bytes"
        assert "vector(128)" in props

    def test_fixed_size_list_int_to_array(self):
        """Test fixed-size list of non-floats maps to Kinetica array type."""
        from ray.data._internal.datasource.kinetica_type_utils import (
            arrow_to_kinetica_type,
        )

        # Fixed-size int array
        arrow_type = pa.list_(pa.int32(), 4)
        kinetica_type, props = arrow_to_kinetica_type(arrow_type)

        assert kinetica_type == "string", f"Expected 'string', got {kinetica_type}"
        assert "array(int,4)" in props, f"Expected 'array(int,4)' in props, got {props}"

    def test_variable_list_to_array(self):
        """Test variable-length list maps to Kinetica array type without size."""
        from ray.data._internal.datasource.kinetica_type_utils import (
            arrow_to_kinetica_type,
        )

        # Variable-length list
        arrow_type = pa.list_(pa.float64())
        kinetica_type, props = arrow_to_kinetica_type(arrow_type)

        assert kinetica_type == "string"
        # Should be array(double) without size
        assert (
            "array(double)" in props
        ), f"Expected 'array(double)' in props, got {props}"

    def test_convert_arrow_batch_null_columns(self):
        """Test convert_arrow_batch_to_records handles None columns gracefully."""
        from ray.data._internal.datasource.kinetica_type_utils import (
            convert_arrow_batch_to_records,
        )

        # Create a simple batch
        batch = pa.RecordBatch.from_pydict(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
            }
        )

        # Should not raise TypeError when columns is None
        records = convert_arrow_batch_to_records(batch, None)

        assert len(records) == 3
        assert records[0]["id"] == 1
        assert records[0]["name"] == "Alice"

    def test_convert_arrow_batch_vector_invalid_values_error(self):
        """Test that vector serialization with invalid values raises ValueError."""
        from gpudb import GPUdbRecordColumn

        from ray.data._internal.datasource.kinetica_type_utils import (
            convert_arrow_batch_to_records,
        )

        # Create a batch with a list that should be treated as a vector
        # but contains non-float values
        batch = pa.RecordBatch.from_pydict(
            {
                "id": [1],
                "embedding": [["not", "floats", "here"]],  # Strings instead of floats
            }
        )

        # Create column definitions with vector type
        columns = [
            GPUdbRecordColumn(
                name="id",
                column_type=GPUdbRecordColumn._ColumnType.INT,
                column_properties=[],
            ),
            GPUdbRecordColumn(
                name="embedding",
                column_type=GPUdbRecordColumn._ColumnType.BYTES,
                column_properties=["vector(3)"],
            ),
        ]

        # Should raise ValueError with helpful message including column name
        with pytest.raises(ValueError, match="embedding"):
            convert_arrow_batch_to_records(batch, columns)


# ============================================================================
# Datasource Validation Tests
# ============================================================================


class TestKineticaDatasourceValidation:
    """Tests for input validation in KineticaDatasource."""

    def test_base_class_attributes_initialized(self):
        """Test that base class mixin attributes are properly initialized.

        KineticaDatasource must call super().__init__() to initialize
        _predicate_expr from _DatasourcePredicatePushdownMixin.
        """
        with patch(
            "ray.data._internal.datasource.kinetica_datasource."
            "KineticaDatasource._init_client"
        ):
            ds = KineticaDatasource(
                url="http://localhost:9191",
                table_name="test_table",
            )

            # These attributes are set by base class __init__
            # If super().__init__() wasn't called, these would raise AttributeError
            assert hasattr(ds, "_predicate_expr")
            assert ds._predicate_expr is None  # Initial value

    def test_invalid_sort_order_rejected(self):
        """Test that invalid sort_order values are rejected."""
        with pytest.raises(ValueError, match="Invalid sort_order"):
            KineticaDatasource(
                url="http://localhost:9191",
                table_name="test_table",
                sort_order="invalid",
            )

    def test_valid_sort_orders_accepted(self):
        """Test that valid sort_order values are accepted."""
        with patch(
            "ray.data._internal.datasource.kinetica_datasource."
            "KineticaDatasource._init_client"
        ):
            # ascending should work
            ds1 = KineticaDatasource(
                url="http://localhost:9191",
                table_name="test_table",
                sort_order="ascending",
            )
            assert ds1._sort_order == "ascending"

            # descending should work
            ds2 = KineticaDatasource(
                url="http://localhost:9191",
                table_name="test_table",
                sort_order="descending",
            )
            assert ds2._sort_order == "descending"


# ============================================================================
# Datasink Validation Tests
# ============================================================================


class TestKineticaDatasinkValidation:
    """Tests for input validation in KineticaDatasink."""

    def test_base_class_attributes_initialized(self):
        """Test that base class is properly initialized.

        KineticaDatasink must call super().__init__() to ensure
        the Datasink base class initializes any required state.
        """
        with patch(
            "ray.data._internal.datasource.kinetica_datasink."
            "KineticaDatasink._init_client"
        ):
            sink = KineticaDatasink(
                url="http://localhost:9191",
                table_name="test_table",
            )

            # Verify the datasink is a proper Datasink instance
            # and that super().__init__() was called (no AttributeError)
            from ray.data.datasource.datasink import Datasink

            assert isinstance(sink, Datasink)


# ============================================================================
# Datasink Serialization Tests
# ============================================================================


class TestKineticaDatasinkSerialization:
    """Tests for column serialization in KineticaDatasink."""

    def test_decimal_columns_preserve_precision_scale(self):
        """Test that decimal column precision/scale survives serialization."""
        from gpudb import GPUdbRecordColumn

        with patch(
            "ray.data._internal.datasource.kinetica_datasink."
            "KineticaDatasink._init_client"
        ):
            ds = KineticaDatasink(
                url="http://localhost:9191",
                table_name="test_table",
            )

            # Create a decimal column with specific precision and scale
            decimal_col = GPUdbRecordColumn(
                name="amount",
                column_type=GPUdbRecordColumn._ColumnType.STRING,
                column_properties=["decimal"],
                is_nullable=False,
                precision=10,
                scale=2,
            )

            # Serialize and deserialize
            dicts = ds._columns_to_dicts([decimal_col])
            restored = ds._dicts_to_columns(dicts)

            # Verify precision and scale are preserved
            assert restored[0].precision == 10
            assert restored[0].scale == 2

    def test_non_decimal_columns_no_precision_scale(self):
        """Test that non-decimal columns don't include precision/scale."""
        from gpudb import GPUdbRecordColumn

        with patch(
            "ray.data._internal.datasource.kinetica_datasink."
            "KineticaDatasink._init_client"
        ):
            ds = KineticaDatasink(
                url="http://localhost:9191",
                table_name="test_table",
            )

            # Create a regular string column
            string_col = GPUdbRecordColumn(
                name="name",
                column_type=GPUdbRecordColumn._ColumnType.STRING,
                column_properties=[],
                is_nullable=True,
            )

            # Serialize
            dicts = ds._columns_to_dicts([string_col])

            # Verify no precision/scale in dict (they weren't set)
            assert "precision" not in dicts[0] or dicts[0].get("precision") is None
            assert "scale" not in dicts[0] or dicts[0].get("scale") is None


# ============================================================================
# GPUdbTable Creation Helper Tests
# ============================================================================


class TestTryCreateGpudbTable:
    """Tests for _try_create_gpudb_table helper method."""

    @patch.object(KineticaDatasink, "_init_client")
    @patch.object(KineticaDatasink, "_create_gpudb_table")
    def test_success_returns_gpudb_table(
        self, mock_create_gpudb_table, mock_init_client
    ):
        """Test that successful creation returns the GPUdbTable."""
        mock_client = MagicMock()
        mock_init_client.return_value = mock_client

        mock_gpudb_table = MagicMock()
        mock_create_gpudb_table.return_value = mock_gpudb_table

        ds = KineticaDatasink(
            url="http://localhost:9191",
            table_name="test_table",
            use_multihead=False,
        )

        result = ds._try_create_gpudb_table(mock_client)

        assert result == mock_gpudb_table
        mock_create_gpudb_table.assert_called_once_with(mock_client, table_exists=False)

    @patch.object(KineticaDatasink, "_init_client")
    @patch.object(KineticaDatasink, "_create_gpudb_table")
    def test_failure_with_multihead_raises(
        self, mock_create_gpudb_table, mock_init_client
    ):
        """Test that failure with multihead=True raises RuntimeError."""
        mock_client = MagicMock()
        mock_init_client.return_value = mock_client

        mock_create_gpudb_table.side_effect = Exception("Connection failed")

        ds = KineticaDatasink(
            url="http://localhost:9191",
            table_name="test_table",
            use_multihead=True,
        )

        with pytest.raises(RuntimeError, match="multihead ingest"):
            ds._try_create_gpudb_table(mock_client)

    @patch.object(KineticaDatasink, "_init_client")
    @patch.object(KineticaDatasink, "_create_gpudb_table")
    def test_failure_without_multihead_returns_none(
        self, mock_create_gpudb_table, mock_init_client
    ):
        """Test that failure with multihead=False returns None and logs warning."""
        mock_client = MagicMock()
        mock_init_client.return_value = mock_client

        mock_create_gpudb_table.side_effect = Exception("Connection failed")

        ds = KineticaDatasink(
            url="http://localhost:9191",
            table_name="test_table",
            use_multihead=False,
        )

        result = ds._try_create_gpudb_table(mock_client)

        assert result is None


# ============================================================================
# Deferred Table Creation Tests
# ============================================================================


class TestKineticaDatasinkDeferredCreation:
    """Tests for deferred table creation in KineticaDatasink.

    Note: Since supports_distributed_writes returns False for deferred creation,
    only a single worker runs - no race condition handling is needed.
    """

    @patch.object(KineticaDatasink, "_init_client")
    @patch.object(KineticaDatasink, "_create_table")
    @patch.object(KineticaDatasink, "_create_gpudb_table")
    @patch(
        "ray.data._internal.datasource.kinetica_type_utils."
        "arrow_schema_to_kinetica_columns"
    )
    @patch(
        "ray.data._internal.datasource.kinetica_type_utils."
        "convert_arrow_batch_to_records"
    )
    def test_deferred_creation_creates_table(
        self,
        mock_convert,
        mock_arrow_to_kinetica,
        mock_create_gpudb_table,
        mock_create_table,
        mock_init_client,
    ):
        """Test deferred creation creates table.

        Note: _create_table sets _schema_string and _column_properties,
        so no separate _get_existing_record_type call is needed.
        """
        mock_client = MagicMock()
        mock_init_client.return_value = mock_client

        mock_arrow_to_kinetica.return_value = []
        mock_convert.return_value = [{"id": 1}]
        mock_create_gpudb_table.return_value = None

        # Create datasink without schema (deferred creation)
        ds = KineticaDatasink(
            url="http://localhost:9191",
            table_name="test_table",
            mode=KineticaSinkMode.APPEND,
            use_multihead=False,
        )
        ds._column_defs = None  # Simulate deferred creation

        # Create test data
        rb = pa.record_batch([pa.array([1])], names=["id"])
        block_data = pa.Table.from_batches([rb])

        ctx = TaskContext(task_idx=0, op_name="test_write")
        ds.write([block_data], ctx=ctx)

        # Verify table was created
        mock_create_table.assert_called_once()
        # _create_table sets _schema_string/_column_properties,
        # so _get_existing_record_type is not called (no redundant network call)

    @patch.object(KineticaDatasink, "_init_client")
    @patch.object(KineticaDatasink, "_create_table")
    @patch(
        "ray.data._internal.datasource.kinetica_type_utils."
        "arrow_schema_to_kinetica_columns"
    )
    def test_deferred_creation_error_propagated(
        self,
        mock_arrow_to_kinetica,
        mock_create_table,
        mock_init_client,
    ):
        """Test that errors during deferred creation are propagated."""
        mock_client = MagicMock()
        mock_init_client.return_value = mock_client

        # Simulate a real error
        mock_create_table.side_effect = Exception("Connection refused")

        mock_arrow_to_kinetica.return_value = []

        # Create datasink without schema
        ds = KineticaDatasink(
            url="http://localhost:9191",
            table_name="test_table",
            mode=KineticaSinkMode.APPEND,
            use_multihead=False,
        )
        ds._column_defs = None

        # Create test data
        rb = pa.record_batch([pa.array([1])], names=["id"])
        block_data = pa.Table.from_batches([rb])

        ctx = TaskContext(task_idx=0, op_name="test_write")

        # Should raise the real error
        with pytest.raises(Exception, match="Connection refused"):
            ds.write([block_data], ctx=ctx)

    def test_supports_distributed_writes_false_for_deferred(self):
        """Test that distributed writes are disabled for deferred creation.

        This ensures only a single worker runs when the table doesn't exist
        and schema is unknown, preventing race conditions.
        """
        with patch(
            "ray.data._internal.datasource.kinetica_datasink."
            "KineticaDatasink._init_client"
        ):
            ds = KineticaDatasink(
                url="http://localhost:9191",
                table_name="test_table",
                mode=KineticaSinkMode.APPEND,
            )
            # _table_ready is False when deferred
            assert ds._table_ready is False
            assert ds.supports_distributed_writes is False


# ============================================================================
# Module Import Tests
# ============================================================================


class TestModuleImports:
    """Tests for module imports and API exposure."""

    def test_read_kinetica_importable(self):
        """Test read_kinetica is importable from ray.data."""
        try:
            from ray.data import read_kinetica

            assert callable(read_kinetica)
        except ImportError:
            pytest.skip("read_kinetica not available in this Ray build")

    def test_read_kinetica_sql_importable(self):
        """Test read_kinetica_sql is importable from ray.data."""
        try:
            from ray.data import read_kinetica_sql

            assert callable(read_kinetica_sql)
        except ImportError:
            pytest.skip("read_kinetica_sql not available in this Ray build")

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

    def test_create_gpudb_client_importable(self):
        """Test create_gpudb_client shared factory is importable."""
        from ray.data._internal.datasource.kinetica_type_utils import (
            create_gpudb_client,
        )

        assert callable(create_gpudb_client)


# ============================================================================
# Client Factory Tests
# ============================================================================


class TestCreateGpudbClient:
    """Tests for the shared create_gpudb_client factory function."""

    def test_create_client_basic(self, patch_gpudb):
        """Test basic client creation with minimal parameters."""
        from ray.data._internal.datasource.kinetica_type_utils import (
            create_gpudb_client,
        )

        client = create_gpudb_client(url="http://localhost:9191")
        assert client is not None

    def test_create_client_with_auth(self, patch_gpudb):
        """Test client creation with authentication."""
        from ray.data._internal.datasource.kinetica_type_utils import (
            create_gpudb_client,
        )

        client = create_gpudb_client(
            url="http://localhost:9191",
            username="admin",
            password="password123",
        )
        assert client is not None

    def test_create_client_with_options(self, patch_gpudb):
        """Test client creation with additional options."""
        from ray.data._internal.datasource.kinetica_type_utils import (
            create_gpudb_client,
        )

        client = create_gpudb_client(
            url="http://localhost:9191",
            username="admin",
            password="password",
            options={"timeout": 30000},
        )
        assert client is not None

    def test_datasource_uses_shared_factory(self):
        """Test that KineticaDatasource uses the shared factory."""
        with patch(
            "ray.data._internal.datasource.kinetica_type_utils.create_gpudb_client"
        ) as mock_factory:
            mock_factory.return_value = MagicMock()

            ds = KineticaDatasource(
                url="http://localhost:9191",
                table_name="test_table",
                username="admin",
                password="password",
            )

            ds._init_client()

            mock_factory.assert_called_once_with(
                url="http://localhost:9191",
                username="admin",
                password="password",
                options={},
            )

    def test_datasink_uses_shared_factory(self):
        """Test that KineticaDatasink uses the shared factory."""
        with patch(
            "ray.data._internal.datasource.kinetica_type_utils.create_gpudb_client"
        ) as mock_factory:
            mock_factory.return_value = MagicMock()

            ds = KineticaDatasink(
                url="http://localhost:9191",
                table_name="test_table",
                username="admin",
                password="password",
            )

            ds._init_client()

            mock_factory.assert_called_once_with(
                url="http://localhost:9191",
                username="admin",
                password="password",
                options={},
            )


# ============================================================================
# Integration Tests (require Kinetica server)
# ============================================================================


@pytest.mark.skipif(
    not os.environ.get("KINETICA_URL"),
    reason="Integration tests require KINETICA_URL environment variable",
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
            data = [{"id": i, "value": i * 10} for i in range(100)]
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
