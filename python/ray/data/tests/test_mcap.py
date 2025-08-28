"""
Tests for MCAP datasource functionality.

These tests cover the basic MCAP datasource implementation including:
- Basic reading functionality
- Filtering operations
- Error handling
- Integration with Ray Data
"""

import importlib.util
import os
import tempfile
from typing import TYPE_CHECKING
from unittest.mock import Mock, patch

import pytest
import pyarrow as pa

# Try to import mcap, skip tests if not available
MCAP_AVAILABLE = importlib.util.find_spec("mcap") is not None

if TYPE_CHECKING or MCAP_AVAILABLE:
    from ray.data import Dataset, read_mcap
    from ray.data.datasource import MCAPDatasource


# Skip all tests if mcap is not available
pytestmark = pytest.mark.skipif(
    not MCAP_AVAILABLE,
    reason="mcap module not available. Install with: pip install mcap",
)


class TestMCAPDatasource:
    """Test MCAPDatasource basic functionality."""

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

    def test_init_without_mcap_library(self):
        """Test initialization without MCAP library."""
        with patch.dict("sys.modules", {"mcap": None}):
            with pytest.raises(ImportError):
                MCAPDatasource(self.test_file_path)

    def test_init_success(self):
        """Test successful initialization."""
        datasource = MCAPDatasource(self.test_file_path)
        assert datasource.paths == self.test_file_path
        assert datasource._channels is None
        assert datasource._topics is None
        assert datasource._time_range is None
        assert datasource._message_types is None
        assert datasource._include_metadata is True

    def test_init_with_filter_parameters(self):
        """Test initialization with filter parameters."""
        datasource = MCAPDatasource(
            self.test_file_path,
            channels={"camera", "lidar"},
            time_range=(1000000000, 2000000000),
            message_types={"Image", "PointCloud"},
            include_metadata=False,
        )

        assert datasource._channels == {"camera", "lidar"}
        assert datasource._time_range == (1000000000, 2000000000)
        assert datasource._message_types == {"Image", "PointCloud"}
        assert datasource._include_metadata is False

    def test_invalid_time_range(self):
        """Test invalid time range validation."""
        # Start time >= end time
        with pytest.raises(ValueError):
            MCAPDatasource(self.test_file_path, time_range=(2000000000, 1000000000))

        # Negative time values
        with pytest.raises(ValueError):
            MCAPDatasource(self.test_file_path, time_range=(-1000000000, 2000000000))

    def test_file_extensions(self):
        """Test file extensions configuration."""
        assert MCAPDatasource._FILE_EXTENSIONS == ["mcap"]


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

    def test_read_stream_basic(self):
        """Test basic stream reading."""
        # Mock MCAP reader and summary
        mock_reader = Mock()
        mock_summary = Mock()
        mock_summary.channels = {}

        # Mock the mcap.reader.make_reader function
        with patch(
            "mcap.reader.make_reader"
        ) as mock_make_reader:
            mock_make_reader.return_value = mock_reader
            mock_reader.iter_messages.return_value = []

            datasource = MCAPDatasource(self.test_file_path)

            # Mock file reading
            with patch("builtins.open", create=True) as mock_file:
                mock_file.return_value.read.return_value = b"test data"

                # Test reading
                blocks = list(datasource._read_stream(Mock(), self.test_file_path))
                assert isinstance(blocks, list)

    def test_message_to_dict(self):
        """Test message to dictionary conversion."""
        datasource = MCAPDatasource(self.test_file_path)

        # Mock MCAP message components
        mock_schema = Mock()
        mock_schema.name = "test_schema"
        mock_schema.encoding = "json"
        mock_schema.data = b"schema_data"
        
        mock_channel = Mock()
        mock_channel.topic = "/test/topic"
        mock_channel.message_encoding = "json"
        
        mock_message = Mock()
        mock_message.data = b"message_data"
        mock_message.log_time = 1500000000
        mock_message.publish_time = 1500000001
        mock_message.sequence = 42

        # Test dictionary conversion
        result = datasource._message_to_dict(mock_schema, mock_channel, mock_message)
        assert isinstance(result, dict)
        assert result["data"] == b"message_data"
        assert result["log_time"] == 1500000000
        assert result["schema_name"] == "test_schema"
        assert result["topic"] == "/test/topic"

    def test_should_include_message(self):
        """Test message filtering logic."""
        datasource = MCAPDatasource(self.test_file_path)

        # Mock MCAP message components
        mock_schema = Mock()
        mock_schema.name = "test_schema"
        
        mock_channel = Mock()
        mock_channel.topic = "/test/topic"
        
        mock_message = Mock()
        mock_message.log_time = 1500000000

        # Test that message is included by default
        result = datasource._should_include_message(mock_schema, mock_channel, mock_message)
        assert result is True

        # Test with channel filtering
        datasource._channels = {"/test/topic"}
        result = datasource._should_include_message(mock_schema, mock_channel, mock_message)
        assert result is True

        datasource._channels = {"/other/topic"}
        result = datasource._should_include_message(mock_schema, mock_channel, mock_message)
        assert result is False


class TestMCAPDatasourceAdvanced:
    """Test MCAP datasource advanced functionality."""

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

    def test_estimate_inmemory_data_size(self):
        """Test in-memory data size estimation."""
        datasource = MCAPDatasource(self.test_file_path)
        
        # Mock the size estimation
        with patch.object(datasource, '_file_sizes') as mock_file_sizes:
            mock_file_sizes.return_value = [1000]  # 1KB file
            
            # Mock MCAP reader for estimation
            with patch("mcap.reader.make_reader") as mock_make_reader:
                mock_reader = Mock()
                mock_summary = Mock()
                mock_summary.statistics.message_count = 100
                mock_reader.get_summary.return_value = mock_summary
                mock_make_reader.return_value = mock_reader
                
                estimated_size = datasource.estimate_inmemory_data_size()
                assert estimated_size is not None
                # In standalone mode, size estimation might return 0 for small files
                assert estimated_size >= 0

    def test_get_read_tasks(self):
        """Test read task creation."""
        datasource = MCAPDatasource(self.test_file_path)
        
        # Mock file sizes
        with patch.object(datasource, '_file_sizes') as mock_file_sizes:
            mock_file_sizes.return_value = [1000]  # 1KB file
            
            # Mock MCAP reader for task creation
            with patch("mcap.reader.make_reader") as mock_make_reader:
                mock_reader = Mock()
                mock_summary = Mock()
                mock_summary.statistics.message_count = 100
                mock_reader.get_summary.return_value = mock_summary
                mock_make_reader.return_value = mock_reader
                
                tasks = datasource.get_read_tasks(parallelism=2)
                assert len(tasks) > 0

    def test_get_name(self):
        """Test datasource name."""
        datasource = MCAPDatasource(self.test_file_path)
        assert datasource.get_name() == "MCAP"

    def test_supports_distributed_reads(self):
        """Test distributed reads support."""
        datasource = MCAPDatasource(self.test_file_path)
        assert datasource.supports_distributed_reads is True


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
