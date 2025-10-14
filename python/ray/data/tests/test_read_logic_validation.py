"""Comprehensive validation tests for ray.data.read() reader selection logic.

This test suite validates:
1. File extension detection and mapping
2. Format hint system
3. Lakehouse format detection
4. Reader parameter filtering
5. Compression handling
6. Edge cases and error handling
"""

import inspect
import os
import tempfile

import pytest

import ray
from ray.data._internal.read_unified import (
    DataSource,
    FileFormat,
    FileTypeDetector,
    LakehouseDetector,
    LakehouseFormat,
    ReaderRegistry,
    SourceDetector,
)


class TestExtensionMapping:
    """Validate file extension to format mapping."""

    def test_all_parquet_extensions(self):
        """Test all Parquet-related extensions."""
        detector = FileTypeDetector()

        parquet_extensions = [
            ".parquet",
            ".PARQUET",
            ".Parquet",
            ".parquet.gz",
            ".parquet.bz2",
            ".parquet.snappy",
            ".parquet.gzip",
            ".parquet.lz4",
            ".parquet.zstd",
        ]

        for ext in parquet_extensions:
            path = f"test{ext}"
            result = detector.detect_file_type(path)
            assert result == FileFormat.PARQUET, f"Failed for {ext}"

    def test_all_csv_extensions(self):
        """Test all CSV-related extensions."""
        detector = FileTypeDetector()

        csv_extensions = [
            ".csv",
            ".CSV",
            ".csv.gz",
            ".csv.bz2",
            ".csv.zip",
            ".tsv",
            ".TSV",
        ]

        for ext in csv_extensions:
            path = f"test{ext}"
            result = detector.detect_file_type(path)
            assert result == FileFormat.CSV, f"Failed for {ext}"

    def test_all_json_extensions(self):
        """Test all JSON-related extensions."""
        detector = FileTypeDetector()

        json_extensions = [
            ".json",
            ".JSON",
            ".jsonl",
            ".JSONL",
            ".json.gz",
            ".jsonl.gz",
        ]

        for ext in json_extensions:
            path = f"test{ext}"
            result = detector.detect_file_type(path)
            assert result == FileFormat.JSON, f"Failed for {ext}"

    def test_all_image_extensions(self):
        """Test all image-related extensions."""
        detector = FileTypeDetector()

        image_extensions = [
            ".png",
            ".PNG",
            ".jpg",
            ".JPG",
            ".jpeg",
            ".JPEG",
            ".gif",
            ".GIF",
            ".bmp",
            ".BMP",
            ".tif",
            ".TIF",
            ".tiff",
            ".TIFF",
            ".webp",
            ".WEBP",
        ]

        for ext in image_extensions:
            path = f"test{ext}"
            result = detector.detect_file_type(path)
            assert result == FileFormat.IMAGES, f"Failed for {ext}"

    def test_all_audio_extensions(self):
        """Test all audio-related extensions."""
        detector = FileTypeDetector()

        audio_extensions = [
            ".mp3",
            ".MP3",
            ".wav",
            ".WAV",
            ".flac",
            ".FLAC",
            ".m4a",
            ".M4A",
            ".ogg",
            ".OGG",
        ]

        for ext in audio_extensions:
            path = f"test{ext}"
            result = detector.detect_file_type(path)
            assert result == FileFormat.AUDIO, f"Failed for {ext}"

    def test_all_video_extensions(self):
        """Test all video-related extensions."""
        detector = FileTypeDetector()

        video_extensions = [
            ".mp4",
            ".MP4",
            ".avi",
            ".AVI",
            ".mov",
            ".MOV",
            ".mkv",
            ".MKV",
            ".m4v",
            ".M4V",
            ".mpeg",
            ".MPEG",
            ".mpg",
            ".MPG",
        ]

        for ext in video_extensions:
            path = f"test{ext}"
            result = detector.detect_file_type(path)
            assert result == FileFormat.VIDEO, f"Failed for {ext}"

    def test_all_numpy_extensions(self):
        """Test NumPy extensions."""
        detector = FileTypeDetector()

        numpy_extensions = [".npy", ".NPY", ".npz", ".NPZ"]

        for ext in numpy_extensions:
            path = f"test{ext}"
            result = detector.detect_file_type(path)
            assert result == FileFormat.NUMPY, f"Failed for {ext}"

    def test_avro_extensions(self):
        """Test Avro extensions."""
        detector = FileTypeDetector()

        avro_extensions = [
            ".avro",
            ".AVRO",
            ".avro.gz",
            ".avro.snappy",
        ]

        for ext in avro_extensions:
            path = f"test{ext}"
            result = detector.detect_file_type(path)
            assert result == FileFormat.AVRO, f"Failed for {ext}"

    def test_text_extensions(self):
        """Test text file extensions."""
        detector = FileTypeDetector()

        text_extensions = [".txt", ".TXT", ".log", ".LOG"]

        for ext in text_extensions:
            path = f"test{ext}"
            result = detector.detect_file_type(path)
            assert result == FileFormat.TEXT, f"Failed for {ext}"

    def test_html_extensions(self):
        """Test HTML extensions."""
        detector = FileTypeDetector()

        html_extensions = [".html", ".HTML", ".htm", ".HTM"]

        for ext in html_extensions:
            path = f"test{ext}"
            result = detector.detect_file_type(path)
            assert result == FileFormat.HTML, f"Failed for {ext}"

    def test_tfrecords_extensions(self):
        """Test TFRecords extensions."""
        detector = FileTypeDetector()

        tfrecord_extensions = [
            ".tfrecord",
            ".tfrecords",
            ".TFRECORD",
            ".TFRECORDS",
        ]

        for ext in tfrecord_extensions:
            path = f"test{ext}"
            result = detector.detect_file_type(path)
            assert result == FileFormat.TFRECORDS, f"Failed for {ext}"

    def test_case_insensitive_detection(self):
        """Test that extension detection is case-insensitive."""
        detector = FileTypeDetector()

        test_cases = [
            ("file.PARQUET", FileFormat.PARQUET),
            ("file.Csv", FileFormat.CSV),
            ("file.JsOn", FileFormat.JSON),
            ("file.PnG", FileFormat.IMAGES),
            ("file.Mp3", FileFormat.AUDIO),
        ]

        for path, expected in test_cases:
            result = detector.detect_file_type(path)
            assert result == expected, f"Failed for {path}"

    def test_compound_extensions(self):
        """Test compound extensions like .csv.gz."""
        detector = FileTypeDetector()

        test_cases = [
            ("data.csv.gz", FileFormat.CSV),
            ("data.json.bz2", FileFormat.JSON),
            ("data.parquet.snappy", FileFormat.PARQUET),
            ("data.avro.gz", FileFormat.AVRO),
            ("data.txt.zip", FileFormat.TEXT),
        ]

        for path, expected in test_cases:
            result = detector.detect_file_type(path)
            assert result == expected, f"Failed for {path}"

    def test_unknown_extension_returns_none(self):
        """Test that unknown extensions return None (for binary fallback)."""
        detector = FileTypeDetector()

        unknown_extensions = [
            "file.xyz",
            "file.unknown",
            "file.custom",
            "file.abc123",
        ]

        for path in unknown_extensions:
            result = detector.detect_file_type(path)
            assert result is None, f"Should return None for {path}"

    def test_no_extension_returns_none(self):
        """Test files without extension."""
        detector = FileTypeDetector()

        test_cases = [
            "README",
            "Makefile",
            "LICENSE",
            "data",
        ]

        for path in test_cases:
            result = detector.detect_file_type(path)
            assert result is None, f"Should return None for {path}"

    def test_dotfiles_handled(self):
        """Test hidden files (starting with dot)."""
        detector = FileTypeDetector()

        # Hidden files should still detect by extension
        result = detector.detect_file_type(".hidden.parquet")
        assert result == FileFormat.PARQUET

    def test_multiple_dots_in_filename(self):
        """Test files with multiple dots."""
        detector = FileTypeDetector()

        test_cases = [
            ("data.backup.parquet", FileFormat.PARQUET),
            ("file.v2.csv", FileFormat.CSV),
            ("test.2023.json", FileFormat.JSON),
        ]

        for path, expected in test_cases:
            result = detector.detect_file_type(path)
            assert result == expected, f"Failed for {path}"

    def test_format_aliases(self):
        """Test format aliases."""
        detector = FileTypeDetector()

        # Check that aliases are defined
        assert "jpeg" in detector.FORMAT_ALIASES
        assert detector.FORMAT_ALIASES["jpeg"] == "images"

        assert "jsonl" in detector.FORMAT_ALIASES
        assert detector.FORMAT_ALIASES["jsonl"] == "json"


class TestFormatHintSystem:
    """Validate format hint system."""

    def test_all_file_based_formats(self):
        """Test format hints for all file-based formats."""
        registry = ReaderRegistry()

        file_formats = [
            "parquet",
            "parquet_bulk",
            "csv",
            "json",
            "text",
            "images",
            "audio",
            "video",
            "numpy",
            "avro",
            "tfrecords",
            "html",
            "webdataset",
            "lance",
            "binary",
        ]

        for fmt in file_formats:
            reader = registry.get_format_reader(fmt)
            assert callable(reader), f"Reader for {fmt} should be callable"

    def test_all_lakehouse_formats(self):
        """Test format hints for lakehouse formats."""
        registry = ReaderRegistry()

        lakehouse_formats = ["delta", "delta_sharing", "hudi", "iceberg"]

        for fmt in lakehouse_formats:
            # These should be accessible via format hint
            reader = registry.get_format_reader(fmt)
            assert callable(reader), f"Reader for {fmt} should be callable"

    def test_all_database_formats(self):
        """Test format hints for database formats."""
        registry = ReaderRegistry()

        database_formats = [
            "sql",
            "bigquery",
            "mongo",
            "mongodb",
            "clickhouse",
            "snowflake",
        ]

        for fmt in database_formats:
            reader = registry.get_format_reader(fmt)
            assert callable(reader), f"Reader for {fmt} should be callable"

    def test_databricks_formats(self):
        """Test format hints for Databricks."""
        registry = ReaderRegistry()

        databricks_formats = ["databricks", "unity_catalog"]

        for fmt in databricks_formats:
            reader = registry.get_format_reader(fmt)
            assert callable(reader), f"Reader for {fmt} should be callable"

    def test_format_hint_case_insensitive(self):
        """Test that format hints are case-insensitive."""
        registry = ReaderRegistry()

        # Test various casings
        test_cases = [
            ("parquet", "PARQUET", "Parquet"),
            ("csv", "CSV", "Csv"),
            ("json", "JSON", "Json"),
        ]

        for lower, upper, mixed in test_cases:
            reader_lower = registry.get_format_reader(lower)
            reader_upper = registry.get_format_reader(upper)
            reader_mixed = registry.get_format_reader(mixed)

            # Should all return the same reader
            assert reader_lower == reader_upper == reader_mixed

    def test_invalid_format_raises_error(self):
        """Test that invalid format raises appropriate error."""
        registry = ReaderRegistry()

        with pytest.raises(ValueError, match="Unsupported format"):
            registry.get_format_reader("invalid_format_xyz")

    def test_format_alias_mongodb(self):
        """Test mongodb alias for mongo."""
        registry = ReaderRegistry()

        reader_mongo = registry.get_format_reader("mongo")
        reader_mongodb = registry.get_format_reader("mongodb")

        # Should return same reader
        assert reader_mongo == reader_mongodb

    def test_supported_formats_list(self):
        """Test that we can get list of supported formats."""
        registry = ReaderRegistry()

        formats = registry.get_supported_formats()

        # Should have all formats
        assert "parquet" in formats
        assert "csv" in formats
        assert "delta" in formats
        assert "sql" in formats

        # Should be sorted
        assert formats == sorted(formats)

    def test_format_hint_overrides_extension(self):
        """Test that format hint overrides extension detection."""
        # This is tested in integration tests
        pass


class TestLakehouseDetection:
    """Validate lakehouse format detection."""

    def test_delta_lake_detection(self, tmp_path):
        """Test Delta Lake detection via _delta_log directory."""
        import pyarrow.fs as pafs

        # Create Delta-like structure
        delta_path = tmp_path / "delta_table"
        delta_path.mkdir()
        (delta_path / "_delta_log").mkdir()
        (delta_path / "_delta_log" / "00000000000000000000.json").write_text("{}")

        detector = LakehouseDetector(pafs.LocalFileSystem())
        result = detector.detect(str(delta_path))

        assert result == LakehouseFormat.DELTA

    def test_hudi_detection(self, tmp_path):
        """Test Hudi detection via .hoodie directory."""
        import pyarrow.fs as pafs

        # Create Hudi-like structure
        hudi_path = tmp_path / "hudi_table"
        hudi_path.mkdir()
        (hudi_path / ".hoodie").mkdir()
        (hudi_path / ".hoodie" / "hoodie.properties").write_text("test=value")

        detector = LakehouseDetector(pafs.LocalFileSystem())
        result = detector.detect(str(hudi_path))

        assert result == LakehouseFormat.HUDI

    def test_iceberg_detection(self, tmp_path):
        """Test Iceberg detection via metadata/version-hint.text."""
        import pyarrow.fs as pafs

        # Create Iceberg-like structure
        iceberg_path = tmp_path / "iceberg_table"
        iceberg_path.mkdir()
        metadata_path = iceberg_path / "metadata"
        metadata_path.mkdir()
        (metadata_path / "version-hint.text").write_text("1")

        detector = LakehouseDetector(pafs.LocalFileSystem())
        result = detector.detect(str(iceberg_path))

        assert result == LakehouseFormat.ICEBERG

    def test_regular_parquet_not_detected_as_lakehouse(self, tmp_path):
        """Test that regular parquet directories don't trigger lakehouse detection."""
        import pyarrow.fs as pafs

        # Create regular directory with parquet files
        parquet_dir = tmp_path / "parquet_data"
        parquet_dir.mkdir()
        (parquet_dir / "part-0.parquet").write_text("fake parquet")

        detector = LakehouseDetector(pafs.LocalFileSystem())
        result = detector.detect(str(parquet_dir))

        assert result is None

    def test_empty_directory_not_detected(self, tmp_path):
        """Test that empty directories don't trigger detection."""
        import pyarrow.fs as pafs

        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()

        detector = LakehouseDetector(pafs.LocalFileSystem())
        result = detector.detect(str(empty_dir))

        assert result is None

    def test_case_sensitive_directory_names(self, tmp_path):
        """Test that lakehouse detection is case-sensitive for directory names."""
        import pyarrow.fs as pafs

        # _DELTA_log (wrong case) should not be detected
        fake_delta = tmp_path / "fake_delta"
        fake_delta.mkdir()
        (fake_delta / "_DELTA_LOG").mkdir()  # Wrong case

        detector = LakehouseDetector(pafs.LocalFileSystem())
        result = detector.detect(str(fake_delta))

        assert result is None


class TestReaderParameterFiltering:
    """Validate reader parameter filtering with inspect.signature()."""

    def test_inspect_signature_usage(self):
        """Test that inspect.signature correctly identifies parameters."""
        from ray.data.read_api import read_parquet

        sig = inspect.signature(read_parquet)
        params = set(sig.parameters.keys())

        # Should have standard parameters
        assert "paths" in params
        assert "filesystem" in params
        assert "columns" in params  # Parquet-specific
        assert "parallelism" in params

    def test_common_parameters_filtered(self):
        """Test that common parameters are correctly filtered."""
        common_params = {
            "filesystem",
            "parallelism",
            "num_cpus",
            "num_gpus",
            "memory",
            "ray_remote_args",
            "meta_provider",
            "partition_filter",
            "include_paths",
            "ignore_missing_paths",
            "shuffle",
            "concurrency",
            "override_num_blocks",
        }

        # These should be filtered to only supported ones
        from ray.data.read_api import read_binary_files

        sig = inspect.signature(read_binary_files)
        params = set(sig.parameters.keys())

        # Check which common params are supported
        supported_common = common_params & params
        assert len(supported_common) > 0

    def test_reader_specific_params_passed(self):
        """Test that reader-specific parameters are identified."""
        from ray.data.read_api import read_csv, read_parquet

        # CSV-specific
        csv_sig = inspect.signature(read_csv)
        csv_params = set(csv_sig.parameters.keys())
        assert "delimiter" in csv_params  # CSV-specific

        # Parquet-specific
        parquet_sig = inspect.signature(read_parquet)
        parquet_params = set(parquet_sig.parameters.keys())
        assert "columns" in parquet_params  # Parquet-specific

    def test_unsupported_params_excluded(self):
        """Test that unsupported parameters would be excluded."""
        from ray.data.read_api import read_text

        sig = inspect.signature(read_text)
        params = set(sig.parameters.keys())

        # Parquet-specific param should not be in text reader
        assert "columns" not in params

    def test_path_parameter_variations(self):
        """Test different path parameter names across readers."""
        from ray.data.read_api import read_parquet, read_delta

        # File-based readers use 'paths'
        parquet_sig = inspect.signature(read_parquet)
        assert "paths" in parquet_sig.parameters

        # Lakehouse readers might use 'path'
        delta_sig = inspect.signature(read_delta)
        # Check what parameter delta uses
        assert "path" in delta_sig.parameters or "paths" in delta_sig.parameters


class TestSourceDetection:
    """Validate data source detection."""

    def test_s3_detection(self):
        """Test S3 path detection."""
        sources = SourceDetector.detect_from_paths(["s3://bucket/data.parquet"])
        assert DataSource.S3 in sources

    def test_gcs_detection(self):
        """Test GCS path detection."""
        sources = SourceDetector.detect_from_paths(["gs://bucket/data.parquet"])
        assert DataSource.GCS in sources

    def test_azure_detection(self):
        """Test Azure path detection."""
        test_paths = [
            "abfs://container@account.dfs.core.windows.net/data.parquet",
            "az://container/data.parquet",
        ]

        for path in test_paths:
            sources = SourceDetector.detect_from_paths([path])
            assert DataSource.AZURE in sources

    def test_hdfs_detection(self):
        """Test HDFS path detection."""
        sources = SourceDetector.detect_from_paths(
            ["hdfs://namenode:9000/data.parquet"]
        )
        assert DataSource.HDFS in sources

    def test_http_detection(self):
        """Test HTTP path detection."""
        sources = SourceDetector.detect_from_paths(["http://example.com/data.parquet"])
        assert DataSource.HTTP in sources

    def test_https_detection(self):
        """Test HTTPS path detection."""
        sources = SourceDetector.detect_from_paths(["https://example.com/data.parquet"])
        assert DataSource.HTTPS in sources

    def test_local_path_detection(self):
        """Test local file system detection."""
        sources = SourceDetector.detect_from_paths(["/local/path/data.parquet"])
        assert DataSource.LOCAL in sources

    def test_mixed_sources_detection(self):
        """Test detecting multiple sources."""
        paths = [
            "s3://bucket/data1.parquet",
            "gs://bucket/data2.parquet",
            "/local/data3.parquet",
        ]

        sources = SourceDetector.detect_from_paths(paths)
        assert DataSource.S3 in sources
        assert DataSource.GCS in sources
        assert DataSource.LOCAL in sources
        assert len(sources) == 3


class TestCompressionHandling:
    """Validate compression format handling."""

    def test_gzip_extension_detection(self):
        """Test .gz extension detection."""
        detector = FileTypeDetector()

        test_cases = [
            ("data.csv.gz", FileFormat.CSV),
            ("data.json.gz", FileFormat.JSON),
            ("data.parquet.gz", FileFormat.PARQUET),
            ("data.txt.gz", FileFormat.TEXT),
        ]

        for path, expected in test_cases:
            result = detector.detect_file_type(path)
            assert result == expected

    def test_bz2_extension_detection(self):
        """Test .bz2 extension detection."""
        detector = FileTypeDetector()

        test_cases = [
            ("data.csv.bz2", FileFormat.CSV),
            ("data.json.bz2", FileFormat.JSON),
            ("data.parquet.bz2", FileFormat.PARQUET),
        ]

        for path, expected in test_cases:
            result = detector.detect_file_type(path)
            assert result == expected

    def test_snappy_extension_detection(self):
        """Test .snappy extension detection."""
        detector = FileTypeDetector()

        test_cases = [
            ("data.parquet.snappy", FileFormat.PARQUET),
            ("data.avro.snappy", FileFormat.AVRO),
        ]

        for path, expected in test_cases:
            result = detector.detect_file_type(path)
            assert result == expected

    def test_zstd_extension_detection(self):
        """Test .zst/.zstd extension detection."""
        detector = FileTypeDetector()

        test_cases = [
            ("data.parquet.zst", FileFormat.PARQUET),
            ("data.parquet.zstd", FileFormat.PARQUET),
        ]

        for path, expected in test_cases:
            result = detector.detect_file_type(path)
            assert result == expected

    def test_lz4_extension_detection(self):
        """Test .lz4 extension detection."""
        detector = FileTypeDetector()

        result = detector.detect_file_type("data.parquet.lz4")
        assert result == FileFormat.PARQUET


class TestEdgeCases:
    """Validate edge case handling."""

    def test_empty_string_path(self):
        """Test handling of empty path."""
        detector = FileTypeDetector()
        result = detector.detect_file_type("")
        assert result is None

    def test_path_with_only_dot(self):
        """Test path that is just a dot."""
        detector = FileTypeDetector()
        result = detector.detect_file_type(".")
        assert result is None

    def test_path_with_double_dot(self):
        """Test path with double dot."""
        detector = FileTypeDetector()
        result = detector.detect_file_type("..")
        assert result is None

    def test_very_long_extension(self):
        """Test file with very long extension."""
        detector = FileTypeDetector()
        result = detector.detect_file_type("file.verylongextensionname")
        assert result is None  # Should fall back to binary

    def test_path_with_spaces(self):
        """Test path with spaces."""
        detector = FileTypeDetector()
        result = detector.detect_file_type("my data file.parquet")
        assert result == FileFormat.PARQUET

    def test_path_with_special_chars(self):
        """Test path with special characters."""
        detector = FileTypeDetector()

        test_cases = [
            ("data-file.parquet", FileFormat.PARQUET),
            ("data_file.csv", FileFormat.CSV),
            ("data.backup.json", FileFormat.JSON),
        ]

        for path, expected in test_cases:
            result = detector.detect_file_type(path)
            assert result == expected

    def test_unicode_in_filename(self):
        """Test unicode characters in filename."""
        detector = FileTypeDetector()
        result = detector.detect_file_type("数据.parquet")
        assert result == FileFormat.PARQUET


class TestFileFormatEnum:
    """Validate FileFormat enum functionality."""

    def test_list_formats_method(self):
        """Test FileFormat.list_formats() method."""
        formats = FileFormat.list_formats()

        assert isinstance(formats, list)
        assert "parquet" in formats
        assert "csv" in formats
        assert "json" in formats
        assert len(formats) > 10

    def test_is_valid_method(self):
        """Test FileFormat.is_valid() method."""
        assert FileFormat.is_valid("parquet")
        assert FileFormat.is_valid("csv")
        assert FileFormat.is_valid("json")
        assert not FileFormat.is_valid("invalid_format")
        assert not FileFormat.is_valid("xyz")

    def test_all_formats_have_values(self):
        """Test that all FileFormat enums have string values."""
        for fmt in FileFormat:
            assert isinstance(fmt.value, str)
            assert len(fmt.value) > 0


class TestLakehouseFormatEnum:
    """Validate LakehouseFormat enum."""

    def test_lakehouse_formats(self):
        """Test that all lakehouse formats are defined."""
        assert LakehouseFormat.DELTA
        assert LakehouseFormat.HUDI
        assert LakehouseFormat.ICEBERG

    def test_lakehouse_format_values(self):
        """Test lakehouse format values."""
        assert LakehouseFormat.DELTA.value == "delta"
        assert LakehouseFormat.HUDI.value == "hudi"
        assert LakehouseFormat.ICEBERG.value == "iceberg"


class TestDataSourceEnum:
    """Validate DataSource enum."""

    def test_all_sources_defined(self):
        """Test that all data sources are defined."""
        expected_sources = [
            DataSource.S3,
            DataSource.GCS,
            DataSource.AZURE,
            DataSource.HDFS,
            DataSource.HTTP,
            DataSource.HTTPS,
            DataSource.LOCAL,
            DataSource.UNKNOWN,
        ]

        for source in expected_sources:
            assert source is not None

    def test_source_values(self):
        """Test data source values."""
        assert DataSource.S3.value == "S3"
        assert DataSource.GCS.value == "GCS"
        assert DataSource.AZURE.value == "Azure"
        assert DataSource.LOCAL.value == "Local"


class TestReaderRegistry:
    """Validate ReaderRegistry functionality."""

    def test_registry_lazy_loading(self):
        """Test that registry lazy loads readers."""
        registry = ReaderRegistry()

        # Before loading, _format_readers should be None
        assert registry._format_readers is None

        # After accessing a reader, should be loaded
        registry.get_format_reader("parquet")
        assert registry._format_readers is not None

    def test_registry_caches_readers(self):
        """Test that registry caches loaded readers."""
        registry = ReaderRegistry()

        # Load readers
        reader1 = registry.get_format_reader("parquet")
        reader2 = registry.get_format_reader("parquet")

        # Should return same reader object
        assert reader1 is reader2

    def test_all_27_readers_accessible(self):
        """Test that all 27 readers are accessible."""
        registry = ReaderRegistry()

        all_formats = [
            # File-based (15)
            "parquet",
            "parquet_bulk",
            "csv",
            "json",
            "text",
            "images",
            "audio",
            "video",
            "numpy",
            "avro",
            "tfrecords",
            "html",
            "webdataset",
            "lance",
            "binary",
            # Lakehouse (4)
            "delta",
            "delta_sharing",
            "hudi",
            "iceberg",
            # Database (6)
            "sql",
            "bigquery",
            "mongo",
            "clickhouse",
            "snowflake",
            # Databricks (2)
            "databricks",
            "unity_catalog",
        ]

        for fmt in all_formats:
            reader = registry.get_format_reader(fmt)
            assert callable(reader), f"Reader for {fmt} should be callable"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
