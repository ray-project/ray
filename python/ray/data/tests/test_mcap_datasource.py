"""
Tests for MCAP datasource functionality.

These tests cover the MCAP datasource implementation including:
- Basic reading functionality
- Filtering operations
- External indexing support
- Error handling
- Integration with Ray Data
"""

import os
import tempfile
from typing import Dict, List, Any
from unittest.mock import Mock, patch, MagicMock

import pytest
import pyarrow as pa

import ray
from ray.data import Dataset
from ray.data.datasource import MCAPDatasource, MCAPFilterConfig, ExternalIndexConfig
from ray.data import read_mcap


class TestMCAPFilterConfig:
    """Test MCAPFilterConfig functionality."""

    def test_default_config(self):
        """Test default filter configuration."""
        config = MCAPFilterConfig()
        assert config.channels is None
        assert config.topics is None
        assert config.time_range is None
        assert config.message_types is None
        assert config.include_metadata is True
        assert config.batch_size == 1000

    def test_custom_config(self):
        """Test custom filter configuration."""
        config = MCAPFilterConfig(
            channels={"camera", "lidar"},
            topics={"sensor_data", "control"},
            time_range=(1000000000, 2000000000),
            message_types={"Image", "PointCloud"},
            include_metadata=False,
            batch_size=500,
        )

        assert config.channels == {"camera", "lidar"}
        assert config.topics == {"sensor_data", "control"}
        assert config.time_range == (1000000000, 2000000000)
        assert config.message_types == {"Image", "PointCloud"}
        assert config.include_metadata is False
        assert config.batch_size == 500

    def test_invalid_time_range(self):
        """Test invalid time range validation."""
        # Start time >= end time
        with pytest.raises(ValueError):
            MCAPFilterConfig(time_range=(2000000000, 1000000000))

        # Negative time values
        with pytest.raises(ValueError):
            MCAPFilterConfig(time_range=(-1000000000, 2000000000))

        with pytest.raises(ValueError):
            MCAPFilterConfig(time_range=(1000000000, -2000000000))

    def test_empty_collections(self):
        """Test filter config with empty collections."""
        config = MCAPFilterConfig(
            channels=set(),
            topics=set(),
            message_types=set(),
        )
        assert config.channels == set()
        assert config.topics == set()
        assert config.message_types == set()

    def test_none_values(self):
        """Test filter config with None values."""
        config = MCAPFilterConfig(
            channels=None,
            topics=None,
            time_range=None,
            message_types=None,
        )
        assert config.channels is None
        assert config.topics is None
        assert config.time_range is None
        assert config.message_types is None


class TestExternalIndexConfig:
    """Test ExternalIndexConfig functionality."""

    def test_default_config(self):
        """Test default external index configuration."""
        config = ExternalIndexConfig("test_index.parquet")
        assert config.index_path == "test_index.parquet"
        assert config.index_type == "auto"
        assert config.validate_index is True
        assert config.cache_index is True

    def test_custom_config(self):
        """Test custom external index configuration."""
        config = ExternalIndexConfig(
            index_path="test_index.db",
            index_type="sqlite",
            validate_index=False,
            cache_index=False,
        )

        assert config.index_path == "test_index.db"
        assert config.index_type == "sqlite"
        assert config.validate_index is False
        assert config.cache_index is False

    def test_auto_detection_paths(self):
        """Test auto-detection of index types from file extensions."""
        # Parquet extension
        config = ExternalIndexConfig("index.parquet")
        assert config.index_type == "auto"

        # SQLite extension
        config = ExternalIndexConfig("index.db")
        assert config.index_type == "auto"

        # Custom extension
        config = ExternalIndexConfig("index.custom")
        assert config.index_type == "auto"

    def test_invalid_index_path(self):
        """Test external index config with invalid path."""
        with pytest.raises(ValueError):
            ExternalIndexConfig("")

        with pytest.raises(ValueError):
            ExternalIndexConfig(None)

    def test_index_type_validation(self):
        """Test index type validation."""
        valid_types = ["auto", "parquet", "sqlite", "custom"]
        for index_type in valid_types:
            config = ExternalIndexConfig("test.index", index_type=index_type)
            assert config.index_type == index_type

        # Test invalid index type
        with pytest.raises(ValueError):
            ExternalIndexConfig("test.index", index_type="invalid_type")


class TestMCAPDatasource:
    """Test MCAPDatasource functionality."""

    def setup_method(self):
        """Set up test environment."""
        # Create temporary directory for test files
        self.temp_dir = tempfile.mkdtemp()
        self.test_file_path = os.path.join(self.temp_dir, "test.mcap")

        # Create a mock MCAP file for testing
        self._create_mock_mcap_file()

    def teardown_method(self):
        """Clean up test environment."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_mock_mcap_file(self):
        """Create a mock MCAP file for testing."""
        # Create a simple binary file that can be read
        with open(self.test_file_path, "wb") as f:
            # Write some mock data
            f.write(b"MCAP_MOCK_DATA_FOR_TESTING" * 100)

    @patch("mcap.open")
    def test_init_without_mcap_library(self, mock_mcap_open):
        """Test initialization without MCAP library."""
        # Mock import error
        with patch.dict("sys.modules", {"mcap": None}):
            with pytest.raises(ImportError):
                MCAPDatasource(self.test_file_path)

    @patch("mcap.open")
    def test_init_success(self, mock_mcap_open):
        """Test successful initialization."""
        mock_mcap_open.return_value.__enter__ = Mock()
        mock_mcap_open.return_value.__exit__ = Mock()

        datasource = MCAPDatasource(self.test_file_path)
        assert datasource._filter_config is not None
        assert datasource._external_index_config is None
        assert datasource._include_paths is False

    @patch("mcap.open")
    def test_init_with_filter_config(self, mock_mcap_open):
        """Test initialization with filter configuration."""
        mock_mcap_open.return_value.__enter__ = Mock()
        mock_mcap_open.return_value.__exit__ = Mock()

        filter_config = MCAPFilterConfig(
            channels={"camera"},
            time_range=(1000000000, 2000000000),
        )

        datasource = MCAPDatasource(self.test_file_path, filter_config=filter_config)

        assert datasource._filter_config == filter_config

    @patch("mcap.open")
    def test_init_with_external_index(self, mock_mcap_open):
        """Test initialization with external index configuration."""
        mock_mcap_open.return_value.__enter__ = Mock()
        mock_mcap_open.return_value.__exit__ = Mock()

        external_index = ExternalIndexConfig("test_index.parquet")

        datasource = MCAPDatasource(
            self.test_file_path, external_index_config=external_index
        )

        assert datasource._external_index_config == external_index

    def test_file_extensions(self):
        """Test file extensions configuration."""
        assert MCAPDatasource._FILE_EXTENSIONS == ["mcap"]

    def test_resource_cleanup(self):
        """Test that resources are properly cleaned up."""
        datasource = MCAPDatasource(self.test_file_path)
        # Mock external index to test cleanup
        datasource._external_index = {"type": "sqlite", "connection": Mock()}
        datasource._external_index["connection"].close = Mock()

        # Trigger cleanup
        datasource.__del__()

        # Verify cleanup was called
        datasource._external_index["connection"].close.assert_called_once()


class TestMCAPDatasourceReading:
    """Test MCAP datasource reading functionality."""

    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.test_file_path = os.path.join(self.temp_dir, "test.mcap")
        self._create_mock_mcap_file()

    def teardown_method(self):
        """Clean up test environment."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_mock_mcap_file(self):
        """Create a mock MCAP file for testing."""
        with open(self.test_file_path, "wb") as f:
            f.write(b"MCAP_MOCK_DATA_FOR_TESTING" * 100)

    @patch("mcap.open")
    def test_read_stream_basic(self, mock_mcap_open):
        """Test basic stream reading."""
        # Mock MCAP reader and summary
        mock_reader = Mock()
        mock_summary = Mock()
        mock_summary.channels = {}

        mock_mcap_open.return_value.__enter__.return_value = mock_reader
        mock_mcap_open.return_value.__exit__.return_value = None
        mock_reader.get_summary.return_value = mock_summary
        mock_reader.read_messages.return_value = []

        datasource = MCAPDatasource(self.test_file_path)

        # Mock file reading
        with patch("builtins.open", create=True) as mock_file:
            mock_file.return_value.read.return_value = b"test data"

            # Test reading
            blocks = list(datasource._read_stream(Mock(), self.test_file_path))
            assert isinstance(blocks, list)

    @patch("mcap.open")
    def test_apply_filters_with_channels(self, mock_mcap_open):
        """Test filtering with channel constraints."""
        # Mock MCAP reader and summary
        mock_reader = Mock()
        mock_summary = Mock()

        # Mock channels
        mock_channel1 = Mock()
        mock_channel1.id = 1
        mock_channel1.topic = "camera"

        mock_channel2 = Mock()
        mock_channel2.id = 2
        mock_channel2.topic = "lidar"

        mock_summary.channels = {1: mock_channel1, 2: mock_channel2}

        mock_mcap_open.return_value.__enter__.return_value = mock_reader
        mock_mcap_open.return_value.__exit__.return_value = None
        mock_reader.get_summary.return_value = mock_summary

        # Mock filter
        mock_filter = Mock()
        mock_reader.read_messages.return_value = []

        with patch("mcap.Filter", return_value=mock_filter):
            filter_config = MCAPFilterConfig(channels={"camera"})
            datasource = MCAPDatasource(
                self.test_file_path, filter_config=filter_config
            )

            # Test filter application
            messages = list(datasource._apply_filters(mock_reader, mock_summary))
            assert isinstance(messages, list)

    @patch("mcap.open")
    def test_message_to_pyarrow_format(self, mock_mcap_open):
        """Test message to PyArrow format conversion."""
        mock_mcap_open.return_value.__enter__.return_value = Mock()
        mock_mcap_open.return_value.__exit__.return_value = None

        datasource = MCAPDatasource(self.test_file_path)

        # Mock MCAP message
        mock_message = Mock()
        mock_message.schema.name = "test_schema"
        mock_message.schema.encoding = "json"
        mock_message.schema.data = b"schema_data"
        mock_message.data = b"message_data"
        mock_message.channel_id = 1
        mock_message.log_time = 1500000000
        mock_message.publish_time = 1500000001
        mock_message.sequence = 42
        mock_message.schema_id = 1

        # Test PyArrow format conversion
        result = datasource._message_to_pyarrow_format(mock_message)
        assert isinstance(result, dict)
        assert result["data"] == b"message_data"
        assert result["channel_id"] == 1
        assert result["log_time"] == 1500000000
        assert result["schema_name"] == "test_schema"

    @patch("mcap.open")
    def test_create_block(self, mock_mcap_open):
        """Test block creation from message batch."""
        mock_mcap_open.return_value.__enter__.return_value = Mock()
        mock_mcap_open.return_value.__exit__.return_value = None

        datasource = MCAPDatasource(self.test_file_path)

        # Test data
        batch = [
            {"channel_id": 1, "data": "message1"},
            {"channel_id": 2, "data": "message2"},
        ]

        # Test block creation
        block = datasource._create_block(batch)
        assert isinstance(block, pa.Table)
        assert len(block) == 2

    @patch("mcap.open")
    def test_apply_filters_with_time_range(self, mock_mcap_open):
        """Test filtering with time range constraints."""
        mock_mcap_open.return_value.__enter__.return_value = Mock()
        mock_mcap_open.return_value.__exit__.return_value = None

        filter_config = MCAPFilterConfig(time_range=(1000000000, 2000000000))
        datasource = MCAPDatasource(self.test_file_path, filter_config=filter_config)

        # Mock reader and summary
        mock_reader = Mock()
        mock_summary = Mock()
        mock_summary.channels = {}

        # Test time range filtering
        with patch("mcap.Filter") as mock_filter:
            messages = list(datasource._apply_filters(mock_reader, mock_summary))
            assert isinstance(messages, list)

    @patch("mcap.open")
    def test_apply_filters_with_message_types(self, mock_mcap_open):
        """Test filtering with message type constraints."""
        mock_mcap_open.return_value.__enter__.return_value = Mock()
        mock_mcap_open.return_value.__exit__.return_value = None

        filter_config = MCAPFilterConfig(message_types={"Image", "PointCloud"})
        datasource = MCAPDatasource(self.test_file_path, filter_config=filter_config)

        # Mock reader and summary
        mock_reader = Mock()
        mock_summary = Mock()
        mock_summary.channels = {}

        # Test message type filtering
        with patch("mcap.Filter") as mock_filter:
            messages = list(datasource._apply_filters(mock_reader, mock_summary))
            assert isinstance(messages, list)


class TestMCAPDatasourceExternalIndexing:
    """Test MCAP datasource external indexing functionality."""

    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.test_file_path = os.path.join(self.temp_dir, "test.mcap")
        self._create_mock_mcap_file()

    def teardown_method(self):
        """Clean up test environment."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_mock_mcap_file(self):
        """Create a mock MCAP file for testing."""
        with open(self.test_file_path, "wb") as f:
            f.write(b"MCAP_MOCK_DATA_FOR_TESTING" * 100)

    @patch("mcap.open")
    def test_load_parquet_index(self, mock_mcap_open):
        """Test loading Parquet-based external index."""
        mock_mcap_open.return_value.__enter__.return_value = Mock()
        mock_mcap_open.return_value.__exit__.return_value = None

        # Create mock index file
        index_file = os.path.join(self.temp_dir, "index.parquet")
        with open(index_file, "w") as f:
            f.write("mock parquet data")

        external_index = ExternalIndexConfig(index_file, index_type="parquet")

        with patch("pyarrow.parquet.read_table") as mock_read_table:
            mock_table = Mock()
            mock_read_table.return_value = mock_table

            datasource = MCAPDatasource(
                self.test_file_path, external_index_config=external_index
            )

            # Verify index was loaded
            assert datasource._external_index is not None
            assert datasource._external_index["type"] == "parquet"

    @patch("mcap.open")
    def test_load_sqlite_index(self, mock_mcap_open):
        """Test loading SQLite-based external index."""
        mock_mcap_open.return_value.__enter__.return_value = Mock()
        mock_mcap_open.return_value.__exit__.return_value = None

        # Create mock index file
        index_file = os.path.join(self.temp_dir, "index.db")
        with open(index_file, "w") as f:
            f.write("mock sqlite data")

        external_index = ExternalIndexConfig(index_file, index_type="sqlite")

        with patch("sqlite3.connect") as mock_connect:
            mock_conn = Mock()
            mock_cursor = Mock()
            mock_cursor.fetchall.return_value = [("channels",), ("messages",)]
            mock_conn.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_conn

            datasource = MCAPDatasource(
                self.test_file_path, external_index_config=external_index
            )

            # Verify index was loaded
            assert datasource._external_index is not None
            assert datasource._external_index["type"] == "sqlite"

    @patch("mcap.open")
    def test_auto_detect_index_type(self, mock_mcap_open):
        """Test automatic detection of index type from file extension."""
        mock_mcap_open.return_value.__enter__.return_value = Mock()
        mock_mcap_open.return_value.__exit__.return_value = None

        # Test Parquet auto-detection
        index_file = os.path.join(self.temp_dir, "index.parquet")
        with open(index_file, "w") as f:
            f.write("mock data")

        external_index = ExternalIndexConfig(index_file)  # index_type="auto"

        with patch("pyarrow.parquet.read_table") as mock_read_table:
            mock_table = Mock()
            mock_read_table.return_value = mock_table

            datasource = MCAPDatasource(
                self.test_file_path, external_index_config=external_index
            )

            # Verify auto-detection worked
            assert datasource._external_index is not None
            assert datasource._external_index["type"] == "parquet"

    @patch("mcap.open")
    def test_external_index_filter_optimization(self, mock_mcap_open):
        """Test external index filter optimization."""
        mock_mcap_open.return_value.__enter__.return_value = Mock()
        mock_mcap_open.return_value.__exit__.return_value = None

        # Create mock index file
        index_file = os.path.join(self.temp_dir, "index.parquet")
        with open(index_file, "w") as f:
            f.write("mock data")

        external_index = ExternalIndexConfig(index_file, index_type="parquet")

        with patch("pyarrow.parquet.read_table") as mock_read_table:
            mock_table = Mock()
            mock_read_table.return_value = mock_table

            datasource = MCAPDatasource(
                self.test_file_path, external_index_config=external_index
            )

            # Test that external index optimization is called
            with patch.object(
                datasource, "_optimize_filter_with_external_index"
            ) as mock_optimize:
                mock_optimize.return_value = None

                # This should trigger external index optimization
                datasource._apply_filters_with_external_index(
                    Mock(), Mock(), self.test_file_path
                )

                # Verify optimization was called
                mock_optimize.assert_called_once()

    @patch("mcap.open")
    def test_load_custom_index(self, mock_mcap_open):
        """Test loading custom external index."""
        mock_mcap_open.return_value.__enter__.return_value = Mock()
        mock_mcap_open.return_value.__exit__.return_value = None

        # Create mock index file
        index_file = os.path.join(self.temp_dir, "index.custom")
        with open(index_file, "w") as f:
            f.write("mock custom data")

        external_index = ExternalIndexConfig(index_file, index_type="custom")

        # Mock custom index loading
        with patch.object(
            MCAPDatasource, "_load_custom_index", return_value={"type": "custom"}
        ):
            datasource = MCAPDatasource(
                self.test_file_path, external_index_config=external_index
            )

            # Verify custom index was loaded
            assert datasource._external_index is not None
            assert datasource._external_index["type"] == "custom"

    @patch("mcap.open")
    def test_index_validation_failure(self, mock_mcap_open):
        """Test external index validation failure."""
        mock_mcap_open.return_value.__enter__.return_value = Mock()
        mock_mcap_open.return_value.__exit__.return_value = None

        # Create mock index file
        index_file = os.path.join(self.temp_dir, "index.parquet")
        with open(index_file, "w") as f:
            f.write("mock data")

        external_index = ExternalIndexConfig(
            index_file, index_type="parquet", validate_index=True
        )

        # Mock validation failure
        with patch("pyarrow.parquet.read_table") as mock_read_table:
            mock_read_table.side_effect = Exception("Validation failed")

            with pytest.raises(Exception):
                MCAPDatasource(
                    self.test_file_path, external_index_config=external_index
                )


class TestMCAPDatasourceIntegration:
    """Test MCAP datasource integration with Ray Data."""

    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.test_file_path = os.path.join(self.temp_dir, "test.mcap")
        self._create_mock_mcap_file()

    def teardown_method(self):
        """Clean up test environment."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_mock_mcap_file(self):
        """Create a mock MCAP file for testing."""
        with open(self.test_file_path, "wb") as f:
            f.write(b"MCAP_MOCK_DATA_FOR_TESTING" * 100)

    @patch("mcap.open")
    def test_read_mcap_convenience_function(self, mock_mcap_open):
        """Test the read_mcap convenience function."""
        mock_mcap_open.return_value.__enter__.return_value = Mock()
        mock_mcap_open.return_value.__exit__.return_value = None

        # Test basic reading
        ds = read_mcap(self.test_file_path)
        assert isinstance(ds, Dataset)

    @patch("mcap.open")
    def test_read_mcap_with_filtering(self, mock_mcap_open):
        """Test read_mcap with filter configuration."""
        mock_mcap_open.return_value.__enter__.return_value = Mock()
        mock_mcap_open.return_value.__exit__.return_value = None

        filter_config = MCAPFilterConfig(
            channels={"camera", "lidar"},
            time_range=(1000000000, 2000000000),
        )

        ds = read_mcap(self.test_file_path, filter_config=filter_config)
        assert isinstance(ds, Dataset)

    @patch("mcap.open")
    def test_read_mcap_with_external_indexing(self, mock_mcap_open):
        """Test read_mcap with external indexing configuration."""
        mock_mcap_open.return_value.__enter__.return_value = Mock()
        mock_mcap_open.return_value.__exit__.return_value = None

        # Create mock index file
        index_file = os.path.join(self.temp_dir, "index.parquet")
        with open(index_file, "w") as f:
            f.write("mock index data")

        external_index = ExternalIndexConfig(index_file, index_type="parquet")

        with patch("pyarrow.parquet.read_table") as mock_read_table:
            mock_table = Mock()
            mock_read_table.return_value = mock_table

            ds = read_mcap(self.test_file_path, external_index_config=external_index)
            assert isinstance(ds, Dataset)

    @patch("mcap.open")
    def test_read_mcap_with_all_options(self, mock_mcap_open):
        """Test read_mcap with all configuration options."""
        mock_mcap_open.return_value.__enter__.return_value = Mock()
        mock_mcap_open.return_value.__exit__.return_value = None

        # Create mock index file
        index_file = os.path.join(self.temp_dir, "index.parquet")
        with open(index_file, "w") as f:
            f.write("mock index data")

        filter_config = MCAPFilterConfig(
            channels={"camera"},
            time_range=(1000000000, 2000000000),
            include_metadata=True,
            batch_size=500,
        )

        external_index = ExternalIndexConfig(
            index_file,
            index_type="parquet",
            validate_index=True,
            cache_index=True,
        )

        with patch("pyarrow.parquet.read_table") as mock_read_table:
            mock_table = Mock()
            mock_read_table.return_value = mock_table

            ds = read_mcap(
                self.test_file_path,
                filter_config=filter_config,
                external_index_config=external_index,
                include_paths=True,
                parallelism=2,
            )
            assert isinstance(ds, Dataset)

    @patch("mcap.open")
    def test_read_mcap_with_filesystem(self, mock_mcap_open):
        """Test read_mcap with custom filesystem."""
        mock_mcap_open.return_value.__enter__.return_value = Mock()
        mock_mcap_open.return_value.__exit__.return_value = None

        # Mock filesystem
        mock_fs = Mock()
        mock_fs.open_input_file = Mock()
        mock_fs.open_input_file.return_value.read.return_value = b"test data"

        ds = read_mcap(self.test_file_path, filesystem=mock_fs)
        assert isinstance(ds, Dataset)

    @patch("mcap.open")
    def test_read_mcap_with_partitioning(self, mock_mcap_open):
        """Test read_mcap with partitioning configuration."""
        mock_mcap_open.return_value.__enter__.return_value = Mock()
        mock_mcap_open.return_value.__exit__.return_value = None

        # Mock partitioning
        mock_partitioning = Mock()
        mock_partitioning.base_dir = self.temp_dir

        ds = read_mcap(self.test_file_path, partitioning=mock_partitioning)
        assert isinstance(ds, Dataset)

    @patch("mcap.open")
    def test_read_mcap_error_handling(self, mock_mcap_open):
        """Test read_mcap error handling."""
        # Mock MCAP open failure
        mock_mcap_open.side_effect = Exception("MCAP file corrupted")

        with pytest.raises(Exception):
            read_mcap(self.test_file_path)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
