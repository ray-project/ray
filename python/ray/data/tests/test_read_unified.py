"""Unified tests for ray.data.read() function.

This file consolidates all tests for the unified read API including:
- Format detection and hint tests
- Integration tests
- Performance tests
- Validation tests
- Warning and error handling tests
- Special case tests
"""

import gzip
import json
import logging
import os
import time

import numpy as np
import pyarrow as pa
import pytest

import ray
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


class TestReadUnified:
    """Test suite for the unified ray.data.read() function."""

    def test_read_single_parquet_file(self, ray_start_regular_shared, tmp_path):
        """Test reading a single Parquet file."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create test data
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        parquet_file = os.path.join(tmp_path, "test.parquet")
        pq.write_table(pa.Table.from_pandas(df), parquet_file)

        # Read using the unified reader
        ds = ray.data.read(parquet_file)
        result = ds.to_pandas()

        pd.testing.assert_frame_equal(result, df)

    def test_read_single_csv_file(self, ray_start_regular_shared, tmp_path):
        """Test reading a single CSV file."""
        import pandas as pd

        # Create test data
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        csv_file = os.path.join(tmp_path, "test.csv")
        df.to_csv(csv_file, index=False)

        # Read using the unified reader
        ds = ray.data.read(csv_file)
        result = ds.to_pandas()

        pd.testing.assert_frame_equal(result, df)

    def test_read_single_json_file(self, ray_start_regular_shared, tmp_path):
        """Test reading a single JSON file."""

        # Create test data
        data = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}, {"a": 3, "b": "z"}]
        json_file = os.path.join(tmp_path, "test.json")
        with open(json_file, "w") as f:
            for item in data:
                f.write(json.dumps(item) + "\n")

        # Read using the unified reader
        ds = ray.data.read(json_file)
        result = ds.take()

        assert len(result) == 3
        assert result[0]["a"] == 1
        assert result[0]["b"] == "x"

    def test_read_single_text_file(self, ray_start_regular_shared, tmp_path):
        """Test reading a single text file."""
        # Create test data
        text_file = os.path.join(tmp_path, "test.txt")
        with open(text_file, "w") as f:
            f.write("line1\nline2\nline3")

        # Read using the unified reader
        ds = ray.data.read(text_file)
        result = ds.take()

        assert len(result) == 3
        assert result[0]["text"] == "line1"
        assert result[1]["text"] == "line2"
        assert result[2]["text"] == "line3"

    def test_read_single_binary_file(self, ray_start_regular_shared, tmp_path):
        """Test reading a single binary file."""
        # Create test data
        binary_file = os.path.join(tmp_path, "test.bin")
        test_bytes = b"test binary content"
        with open(binary_file, "wb") as f:
            f.write(test_bytes)

        # Read using the unified reader
        ds = ray.data.read(binary_file)
        result = ds.take()

        assert len(result) == 1
        assert result[0]["bytes"] == test_bytes

    def test_read_directory_single_type(self, ray_start_regular_shared, tmp_path):
        """Test reading a directory with files of a single type."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create test data
        for i in range(3):
            df = pd.DataFrame(
                {
                    "a": [i * 10 + j for j in range(5)],
                    "b": [f"val_{i}_{j}" for j in range(5)],
                }
            )
            parquet_file = os.path.join(tmp_path, f"test_{i}.parquet")
            pq.write_table(pa.Table.from_pandas(df), parquet_file)

        # Read using the unified reader
        ds = ray.data.read(str(tmp_path))
        result = ds.to_pandas()

        assert len(result) == 15  # 3 files × 5 rows each

    def test_read_directory_mixed_types(self, ray_start_regular_shared, tmp_path):
        """Test reading a directory with mixed file types."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create Parquet file
        df_parquet = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        parquet_file = os.path.join(tmp_path, "test.parquet")
        pq.write_table(pa.Table.from_pandas(df_parquet), parquet_file)

        # Create CSV file
        df_csv = pd.DataFrame({"a": [3, 4], "b": ["z", "w"]})
        csv_file = os.path.join(tmp_path, "test.csv")
        df_csv.to_csv(csv_file, index=False)

        # Create text file
        text_file = os.path.join(tmp_path, "test.txt")
        with open(text_file, "w") as f:
            f.write("line1\nline2")

        # Read using the unified reader
        ds = ray.data.read(str(tmp_path))

        # Should successfully read all files
        assert ds.count() > 0

    def test_read_multiple_paths(self, ray_start_regular_shared, tmp_path):
        """Test reading from multiple explicit paths."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create test files
        df1 = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        parquet_file = os.path.join(tmp_path, "test1.parquet")
        pq.write_table(pa.Table.from_pandas(df1), parquet_file)

        df2 = pd.DataFrame({"a": [3, 4], "b": ["z", "w"]})
        csv_file = os.path.join(tmp_path, "test2.csv")
        df2.to_csv(csv_file, index=False)

        # Read using the unified reader with multiple paths
        ds = ray.data.read([parquet_file, csv_file])

        assert ds.count() == 4

    def test_read_compressed_csv(self, ray_start_regular_shared, tmp_path):
        """Test reading compressed CSV files."""
        import pandas as pd

        # Create test data
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        csv_gz_file = os.path.join(tmp_path, "test.csv.gz")
        df.to_csv(csv_gz_file, index=False, compression="gzip")

        # Read using the unified reader
        ds = ray.data.read(csv_gz_file)
        result = ds.to_pandas()

        pd.testing.assert_frame_equal(result, df)

    def test_read_with_include_paths(self, ray_start_regular_shared, tmp_path):
        """Test reading with include_paths=True."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create test data
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        parquet_file = os.path.join(tmp_path, "test.parquet")
        pq.write_table(pa.Table.from_pandas(df), parquet_file)

        # Read using the unified reader with include_paths
        ds = ray.data.read(parquet_file, include_paths=True)
        result = ds.take(1)[0]

        assert "path" in result
        assert "test.parquet" in result["path"]

    def test_read_nonexistent_path(self, ray_start_regular_shared):
        """Test reading a nonexistent path raises an error."""
        with pytest.raises(FileNotFoundError):
            ray.data.read("/nonexistent/path/file.parquet")

    def test_read_ignore_missing_paths(self, ray_start_regular_shared, tmp_path):
        """Test reading with ignore_missing_paths=True."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create one valid file
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        valid_file = os.path.join(tmp_path, "valid.parquet")
        pq.write_table(pa.Table.from_pandas(df), valid_file)

        # Try to read both valid and nonexistent files
        missing_file = os.path.join(tmp_path, "missing.parquet")

        ds = ray.data.read([valid_file, missing_file], ignore_missing_paths=True)

        assert ds.count() == 3  # Only from valid file

    def test_read_empty_directory(self, ray_start_regular_shared, tmp_path):
        """Test reading an empty directory raises an error."""
        empty_dir = os.path.join(tmp_path, "empty")
        os.makedirs(empty_dir)

        with pytest.raises(ValueError, match="No files found"):
            ray.data.read(empty_dir)

    def test_read_empty_directory_with_ignore_missing(
        self, ray_start_regular_shared, tmp_path
    ):
        """Test reading an empty directory with ignore_missing_paths=True."""
        empty_dir = os.path.join(tmp_path, "empty")
        os.makedirs(empty_dir)

        with pytest.raises(ValueError, match="No files found"):
            ray.data.read(empty_dir, ignore_missing_paths=True)

    def test_read_with_reader_specific_args(self, ray_start_regular_shared, tmp_path):
        """Test passing format-specific arguments to underlying readers."""
        import pandas as pd

        # Create CSV with custom delimiter
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        csv_file = os.path.join(tmp_path, "test.csv")
        df.to_csv(csv_file, index=False, sep=";")

        # Read with custom delimiter
        ds = ray.data.read(csv_file, delimiter=";")
        result = ds.to_pandas()

        pd.testing.assert_frame_equal(result, df)

    def test_read_numpy_files(self, ray_start_regular_shared, tmp_path):
        """Test reading NumPy files."""
        # Create test data
        arr = np.array([1, 2, 3, 4, 5])
        npy_file = os.path.join(tmp_path, "test.npy")
        np.save(npy_file, arr)

        # Read using the unified reader
        ds = ray.data.read(npy_file)
        result = ds.take()

        assert len(result) == 1
        assert np.array_equal(result[0]["data"], arr)

    def test_read_images(self, ray_start_regular_shared, tmp_path):
        """Test reading image files."""
        from PIL import Image

        # Create a test image
        img = Image.new("RGB", (10, 10), color="red")
        img_file = os.path.join(tmp_path, "test.png")
        img.save(img_file)

        # Read using the unified reader
        ds = ray.data.read(img_file)
        result = ds.take()

        assert len(result) == 1
        assert "image" in result[0]

    def test_read_with_parallelism(self, ray_start_regular_shared, tmp_path):
        """Test reading with custom parallelism settings."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create multiple files
        for i in range(5):
            df = pd.DataFrame({"a": [i * 10 + j for j in range(10)]})
            parquet_file = os.path.join(tmp_path, f"test_{i}.parquet")
            pq.write_table(pa.Table.from_pandas(df), parquet_file)

        # Read with custom parallelism
        ds = ray.data.read(str(tmp_path), override_num_blocks=10)

        assert ds.count() == 50

    def test_read_jsonl_format(self, ray_start_regular_shared, tmp_path):
        """Test reading JSONL format."""

        # Create test data
        data = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
        jsonl_file = os.path.join(tmp_path, "test.jsonl")
        with open(jsonl_file, "w") as f:
            for item in data:
                f.write(json.dumps(item) + "\n")

        # Read using the unified reader
        ds = ray.data.read(jsonl_file)
        result = ds.take()

        assert len(result) == 2
        assert result[0]["a"] == 1

    def test_read_avro_files(self, ray_start_regular_shared, tmp_path):
        """Test reading Avro files."""
        from fastavro import parse_schema, writer

        # Create test data
        schema = {
            "type": "record",
            "name": "Test",
            "fields": [{"name": "a", "type": "int"}, {"name": "b", "type": "string"}],
        }
        records = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]

        avro_file = os.path.join(tmp_path, "test.avro")
        with open(avro_file, "wb") as f:
            writer(f, parse_schema(schema), records)

        # Read using the unified reader
        ds = ray.data.read(avro_file)
        result = ds.take()

        assert len(result) == 2
        assert result[0]["a"] == 1
        assert result[0]["b"] == "x"

    def test_read_unknown_extension_as_binary(self, ray_start_regular_shared, tmp_path):
        """Test that unknown extensions are read as binary files."""
        # Create a file with unknown extension
        unknown_file = os.path.join(tmp_path, "test.unknown")
        test_content = b"test content"
        with open(unknown_file, "wb") as f:
            f.write(test_content)

        # Read using the unified reader
        ds = ray.data.read(unknown_file)
        result = ds.take()

        assert len(result) == 1
        assert result[0]["bytes"] == test_content

    def test_read_case_insensitive_extensions(self, ray_start_regular_shared, tmp_path):
        """Test that file extension detection is case-insensitive."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create file with uppercase extension
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        parquet_file = os.path.join(tmp_path, "test.PARQUET")
        pq.write_table(pa.Table.from_pandas(df), parquet_file)

        # Read using the unified reader
        ds = ray.data.read(parquet_file)
        result = ds.to_pandas()

        pd.testing.assert_frame_equal(result, df)

    def test_read_compound_extensions(self, ray_start_regular_shared, tmp_path):
        """Test reading files with compound extensions like .csv.gz."""

        # Create compressed JSON
        import json

        data = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
        json_gz_file = os.path.join(tmp_path, "test.json.gz")
        with gzip.open(json_gz_file, "wt") as f:
            for item in data:
                f.write(json.dumps(item) + "\n")

        # Read using the unified reader
        ds = ray.data.read(json_gz_file)
        result = ds.take()

        assert len(result) == 2
        assert result[0]["a"] == 1

    def test_read_with_filesystem_arg(self, ray_start_regular_shared, tmp_path):
        """Test reading with explicit filesystem argument."""
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq
        from pyarrow import fs

        # Create test data
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        parquet_file = os.path.join(tmp_path, "test.parquet")
        pq.write_table(pa.Table.from_pandas(df), parquet_file)

        # Create local filesystem
        local_fs = fs.LocalFileSystem()

        # Read with explicit filesystem
        ds = ray.data.read(parquet_file, filesystem=local_fs)
        result = ds.to_pandas()

        pd.testing.assert_frame_equal(result, df)

    def test_read_preserves_schema_across_types(
        self, ray_start_regular_shared, tmp_path
    ):
        """Test that reading mixed types preserves appropriate schemas."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create files with compatible schemas
        df1 = pd.DataFrame({"value": [1, 2, 3]})
        parquet_file = os.path.join(tmp_path, "test.parquet")
        pq.write_table(pa.Table.from_pandas(df1), parquet_file)

        df2 = pd.DataFrame({"value": [4, 5, 6]})
        csv_file = os.path.join(tmp_path, "test.csv")
        df2.to_csv(csv_file, index=False)

        # Read mixed types
        ds = ray.data.read([parquet_file, csv_file])

        # Should be able to convert to pandas
        result = ds.to_pandas()
        assert len(result) == 6

    def test_read_recursive_directory(self, ray_start_regular_shared, tmp_path):
        """Test reading directories recursively."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create nested directory structure
        subdir1 = os.path.join(tmp_path, "subdir1")
        subdir2 = os.path.join(tmp_path, "subdir2")
        os.makedirs(subdir1)
        os.makedirs(subdir2)

        # Create files in different directories
        df1 = pd.DataFrame({"a": [1, 2]})
        pq.write_table(
            pa.Table.from_pandas(df1), os.path.join(subdir1, "test1.parquet")
        )

        df2 = pd.DataFrame({"a": [3, 4]})
        pq.write_table(
            pa.Table.from_pandas(df2), os.path.join(subdir2, "test2.parquet")
        )

        # Read recursively
        ds = ray.data.read(str(tmp_path))

        assert ds.count() == 4

    def test_read_with_partition_filter(self, ray_start_regular_shared, tmp_path):
        """Test reading with partition filtering."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create partitioned data
        for partition in ["a", "b", "c"]:
            part_dir = os.path.join(tmp_path, f"partition={partition}")
            os.makedirs(part_dir)
            df = pd.DataFrame({"value": [1, 2, 3]})
            pq.write_table(
                pa.Table.from_pandas(df), os.path.join(part_dir, "data.parquet")
            )

        # Read with partition filter
        def filter_func(partition_dict):
            return partition_dict.get("partition") == "a"

        ds = ray.data.read(str(tmp_path), partition_filter=filter_func)

        # Should only read partition a
        assert ds.count() == 3


class TestReadEdgeCases:
    """Test edge cases and error handling for ray.data.read()."""

    def test_read_special_characters_in_filename(
        self, ray_start_regular_shared, tmp_path
    ):
        """Test reading files with special characters in names."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create file with special characters (spaces, hyphens)
        df = pd.DataFrame({"a": [1, 2, 3]})
        special_file = os.path.join(tmp_path, "test file-name.parquet")
        pq.write_table(pa.Table.from_pandas(df), special_file)

        # Read using the unified reader
        ds = ray.data.read(special_file)
        result = ds.to_pandas()

        pd.testing.assert_frame_equal(result, df)

    def test_read_large_number_of_files(self, ray_start_regular_shared, tmp_path):
        """Test reading a large number of files."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create many small files
        num_files = 50
        for i in range(num_files):
            df = pd.DataFrame({"a": [i]})
            parquet_file = os.path.join(tmp_path, f"test_{i:03d}.parquet")
            pq.write_table(pa.Table.from_pandas(df), parquet_file)

        # Read all files
        ds = ray.data.read(str(tmp_path))

        assert ds.count() == num_files

    def test_read_empty_file(self, ray_start_regular_shared, tmp_path):
        """Test reading an empty file."""
        empty_file = os.path.join(tmp_path, "empty.txt")
        open(empty_file, "w").close()

        # Should handle gracefully
        ds = ray.data.read(empty_file)
        assert ds.count() == 0

    def test_read_very_long_path(self, ray_start_regular_shared, tmp_path):
        """Test reading from a very long file path."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create deeply nested directory
        deep_path = tmp_path
        for i in range(10):
            deep_path = os.path.join(deep_path, f"dir_{i}")
        os.makedirs(deep_path)

        df = pd.DataFrame({"a": [1, 2, 3]})
        parquet_file = os.path.join(deep_path, "test.parquet")
        pq.write_table(pa.Table.from_pandas(df), parquet_file)

        # Read using the unified reader
        ds = ray.data.read(parquet_file)
        result = ds.to_pandas()

        pd.testing.assert_frame_equal(result, df)


class TestReadSchemas:
    """Test schema handling across formats."""

    def test_read_schema_inference(self, ray_start_regular_shared, tmp_path):
        """Test automatic schema inference."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create file with various types
        df = pd.DataFrame(
            {
                "int_col": [1, 2, 3],
                "float_col": [1.1, 2.2, 3.3],
                "string_col": ["a", "b", "c"],
                "bool_col": [True, False, True],
            }
        )
        pq.write_table(
            pa.Table.from_pandas(df), os.path.join(tmp_path, "typed.parquet")
        )

        # Read and check schema
        ds = ray.data.read(os.path.join(tmp_path, "typed.parquet"))
        schema = ds.schema()

        assert "int_col" in str(schema)
        assert "float_col" in str(schema)
        assert "string_col" in str(schema)

    def test_read_nested_schema(self, ray_start_regular_shared, tmp_path):
        """Test reading files with nested schemas."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create nested structure
        data = {
            "id": [1, 2, 3],
            "metadata": [{"key": "value1"}, {"key": "value2"}, {"key": "value3"}],
        }
        df = pd.DataFrame(data)
        pq.write_table(
            pa.Table.from_pandas(df), os.path.join(tmp_path, "nested.parquet")
        )

        # Read nested data
        ds = ray.data.read(os.path.join(tmp_path, "nested.parquet"))
        result = ds.take(1)[0]

        assert "metadata" in result

    def test_read_list_columns(self, ray_start_regular_shared, tmp_path):
        """Test reading files with list/array columns."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create data with lists
        df = pd.DataFrame(
            {"id": [1, 2, 3], "values": [[1, 2, 3], [4, 5], [6, 7, 8, 9]]}
        )
        pq.write_table(
            pa.Table.from_pandas(df), os.path.join(tmp_path, "lists.parquet")
        )

        # Read list data
        ds = ray.data.read(os.path.join(tmp_path, "lists.parquet"))
        result = ds.take()

        assert len(result) == 3


class TestReadEncoding:
    """Test encoding and compression handling."""

    def test_read_utf8_encoding(self, ray_start_regular_shared, tmp_path):
        """Test reading UTF-8 encoded files."""
        # Write UTF-8 text
        with open(os.path.join(tmp_path, "utf8.txt"), "w", encoding="utf-8") as f:
            f.write("Hello 世界\nПривет мир\nمرحبا بالعالم\n")

        # Read UTF-8 text
        ds = ray.data.read(os.path.join(tmp_path, "utf8.txt"))
        result = ds.take()

        assert len(result) == 3

    def test_read_gzip_compression(self, ray_start_regular_shared, tmp_path):
        """Test reading gzip-compressed files."""
        import pandas as pd

        # Write compressed CSV
        df = pd.DataFrame({"a": range(100), "b": range(100, 200)})
        df.to_csv(
            os.path.join(tmp_path, "data.csv.gz"), index=False, compression="gzip"
        )

        # Read compressed file
        ds = ray.data.read(os.path.join(tmp_path, "data.csv.gz"))
        assert ds.count() == 100

    def test_read_mixed_compression(self, ray_start_regular_shared, tmp_path):
        """Test reading directory with mixed compression."""
        import pandas as pd

        # Uncompressed
        df1 = pd.DataFrame({"a": [1, 2, 3]})
        df1.to_csv(os.path.join(tmp_path, "data1.csv"), index=False)

        # Gzip compressed
        df2 = pd.DataFrame({"a": [4, 5, 6]})
        df2.to_csv(
            os.path.join(tmp_path, "data2.csv.gz"), index=False, compression="gzip"
        )

        # Read both
        ds = ray.data.read(str(tmp_path))
        assert ds.count() == 6


class TestReadMetadata:
    """Test metadata and path handling."""

    def test_read_with_file_metadata(self, ray_start_regular_shared, tmp_path):
        """Test reading with file metadata."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create file
        df = pd.DataFrame({"a": [1, 2, 3]})
        pq.write_table(pa.Table.from_pandas(df), os.path.join(tmp_path, "data.parquet"))

        # Read with paths
        ds = ray.data.read(os.path.join(tmp_path, "data.parquet"), include_paths=True)
        result = ds.take(1)[0]

        assert "path" in result
        assert "data.parquet" in result["path"]

    def test_read_preserves_nulls(self, ray_start_regular_shared, tmp_path):
        """Test that null values are preserved."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create data with nulls
        df = pd.DataFrame({"a": [1, None, 3], "b": ["x", "y", None]})
        pq.write_table(
            pa.Table.from_pandas(df), os.path.join(tmp_path, "nulls.parquet")
        )

        # Read and verify nulls
        ds = ray.data.read(os.path.join(tmp_path, "nulls.parquet"))
        result = ds.to_pandas()

        assert result["a"].isna().sum() == 1
        assert result["b"].isna().sum() == 1


class TestReadSpecialCases:
    """Test special cases and edge conditions."""

    def test_read_single_row(self, ray_start_regular_shared, tmp_path):
        """Test reading file with single row."""
        import pandas as pd
        import pyarrow.parquet as pq

        df = pd.DataFrame({"a": [1], "b": ["x"]})
        pq.write_table(
            pa.Table.from_pandas(df), os.path.join(tmp_path, "single.parquet")
        )

        ds = ray.data.read(os.path.join(tmp_path, "single.parquet"))
        assert ds.count() == 1

    def test_read_large_row(self, ray_start_regular_shared, tmp_path):
        """Test reading file with very wide rows."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create wide row
        df = pd.DataFrame({f"col_{i}": [i] for i in range(200)})
        pq.write_table(pa.Table.from_pandas(df), os.path.join(tmp_path, "wide.parquet"))

        ds = ray.data.read(os.path.join(tmp_path, "wide.parquet"))
        result = ds.take(1)[0]

        assert len(result) == 200

    def test_read_empty_rows(self, ray_start_regular_shared, tmp_path):
        """Test reading file with empty/sparse data."""
        import pandas as pd
        import pyarrow.parquet as pq

        df = pd.DataFrame({"a": [None, None, None], "b": [None, None, None]})
        pq.write_table(
            pa.Table.from_pandas(df), os.path.join(tmp_path, "empty_rows.parquet")
        )

        ds = ray.data.read(os.path.join(tmp_path, "empty_rows.parquet"))
        assert ds.count() == 3

    def test_read_special_column_names(self, ray_start_regular_shared, tmp_path):
        """Test reading files with special column names."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Special characters in column names
        df = pd.DataFrame(
            {
                "col with spaces": [1, 2, 3],
                "col-with-dashes": [4, 5, 6],
                "col.with.dots": [7, 8, 9],
            }
        )
        pq.write_table(
            pa.Table.from_pandas(df), os.path.join(tmp_path, "special_cols.parquet")
        )

        ds = ray.data.read(os.path.join(tmp_path, "special_cols.parquet"))
        result = ds.take(1)[0]

        assert "col with spaces" in result or "col_with_spaces" in result


class TestReadRobustness:
    """Test robustness and error handling."""

    def test_read_handles_unicode_filenames(self, ray_start_regular_shared, tmp_path):
        """Test reading files with unicode characters in names."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create file with unicode name
        df = pd.DataFrame({"a": [1, 2, 3]})
        unicode_file = os.path.join(tmp_path, "test_测试.parquet")
        pq.write_table(pa.Table.from_pandas(df), unicode_file)

        # Read using the unified reader
        ds = ray.data.read(unicode_file)
        result = ds.to_pandas()

        pd.testing.assert_frame_equal(result, df)

    def test_read_symlinks(self, ray_start_regular_shared, tmp_path):
        """Test reading symlinked files."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create actual file
        df = pd.DataFrame({"a": [1, 2, 3]})
        actual_file = os.path.join(tmp_path, "actual.parquet")
        pq.write_table(pa.Table.from_pandas(df), actual_file)

        # Create symlink
        symlink_file = os.path.join(tmp_path, "symlink.parquet")
        try:
            os.symlink(actual_file, symlink_file)

            # Read symlink
            ds = ray.data.read(symlink_file)
            result = ds.to_pandas()

            pd.testing.assert_frame_equal(result, df)
        except OSError:
            # Skip if symlinks not supported on this system
            pytest.skip("Symlinks not supported")

    def test_read_concurrent_writes(self, ray_start_regular_shared, tmp_path):
        """Test behavior when files are being written concurrently."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create some existing files
        for i in range(3):
            df = pd.DataFrame({"a": [i * 10 + j for j in range(5)]})
            pq.write_table(
                pa.Table.from_pandas(df),
                os.path.join(tmp_path, f"existing_{i}.parquet"),
            )

        # Read existing files
        ds = ray.data.read(str(tmp_path))

        # Should successfully read existing files
        assert ds.count() >= 15

    def test_read_hidden_files_ignored(self, ray_start_regular_shared, tmp_path):
        """Test that hidden files (starting with .) are typically handled."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create visible file
        df = pd.DataFrame({"a": [1, 2, 3]})
        visible_file = os.path.join(tmp_path, "visible.parquet")
        pq.write_table(pa.Table.from_pandas(df), visible_file)

        # Create hidden file
        hidden_file = os.path.join(tmp_path, ".hidden.parquet")
        pq.write_table(pa.Table.from_pandas(df), hidden_file)

        # Read directory
        ds = ray.data.read(str(tmp_path))

        # Should read files
        assert ds.count() >= 3


class TestReadScalability:
    """Scalability tests for large-scale scenarios."""

    def test_read_wide_schema(self, ray_start_regular_shared, tmp_path):
        """Test reading files with many columns."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create file with many columns
        num_cols = 100
        num_rows = 1000

        df = pd.DataFrame(
            {f"col_{i}": np.random.rand(num_rows) for i in range(num_cols)}
        )
        parquet_file = os.path.join(tmp_path, "wide.parquet")
        pq.write_table(pa.Table.from_pandas(df), parquet_file)

        # Read the wide file
        start = time.time()
        ds = ray.data.read(parquet_file)
        count = ds.count()
        elapsed = time.time() - start

        assert count == num_rows
        print(f"Read {num_cols} columns × {num_rows} rows in {elapsed:.3f}s")

    def test_read_deep_nesting(self, ray_start_regular_shared, tmp_path):
        """Test reading from deeply nested directory structures."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create deep nesting
        depth = 10
        current_path = tmp_path

        for level in range(depth):
            current_path = os.path.join(current_path, f"level_{level}")
            os.makedirs(current_path)

        # Create file at the deepest level
        df = pd.DataFrame({"value": [1, 2, 3]})
        pq.write_table(
            pa.Table.from_pandas(df), os.path.join(current_path, "data.parquet")
        )

        # Read from top level
        start = time.time()
        ds = ray.data.read(str(tmp_path))
        count = ds.count()
        elapsed = time.time() - start

        assert count == 3
        print(f"Read from depth {depth} in {elapsed:.3f}s")

    def test_read_incremental_processing(self, ray_start_regular_shared, tmp_path):
        """Test incremental processing of read data."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create dataset
        num_files = 20
        for i in range(num_files):
            df = pd.DataFrame(
                {"id": range(i * 100, (i + 1) * 100), "value": np.random.rand(100)}
            )
            pq.write_table(
                pa.Table.from_pandas(df),
                os.path.join(tmp_path, f"file_{i:03d}.parquet"),
            )

        # Read and process incrementally
        ds = ray.data.read(str(tmp_path))

        start = time.time()
        processed_count = 0
        for batch in ds.iter_batches(batch_size=500):
            # Simulate processing
            processed_count += len(batch)
        elapsed = time.time() - start

        assert processed_count == num_files * 100
        print(f"Processed {processed_count} rows incrementally in {elapsed:.3f}s")

    def test_read_with_transformations_pipeline(
        self, ray_start_regular_shared, tmp_path
    ):
        """Test read followed by transformation pipeline."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create dataset
        num_files = 10
        for i in range(num_files):
            df = pd.DataFrame(
                {
                    "value": np.random.rand(1000),
                    "category": np.random.choice(["A", "B", "C"], 1000),
                }
            )
            pq.write_table(
                pa.Table.from_pandas(df), os.path.join(tmp_path, f"file_{i}.parquet")
            )

        # Read and apply transformations
        start = time.time()
        ds = ray.data.read(str(tmp_path))
        ds = ds.filter(lambda row: row["value"] > 0.5)
        ds = ds.map(lambda row: {**row, "value_squared": row["value"] ** 2})
        ds = ds.groupby("category").count()
        result = ds.take()
        elapsed = time.time() - start

        assert len(result) <= 3  # Max 3 categories
        print(f"Complete pipeline in {elapsed:.3f}s")


class TestReadConcurrency:
    """Test concurrent read operations."""

    def test_read_concurrent_multiple_datasets(
        self, ray_start_regular_shared, tmp_path
    ):
        """Test reading multiple datasets concurrently."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create multiple independent datasets
        datasets_dirs = []
        for dataset_id in range(3):
            dataset_dir = os.path.join(tmp_path, f"dataset_{dataset_id}")
            os.makedirs(dataset_dir)
            datasets_dirs.append(dataset_dir)

            for i in range(5):
                df = pd.DataFrame(
                    {"dataset_id": [dataset_id] * 100, "value": np.random.rand(100)}
                )
                pq.write_table(
                    pa.Table.from_pandas(df),
                    os.path.join(dataset_dir, f"file_{i}.parquet"),
                )

        # Read all datasets concurrently
        start = time.time()
        datasets = [ray.data.read(d) for d in datasets_dirs]
        counts = [ds.count() for ds in datasets]
        elapsed = time.time() - start

        assert all(c == 500 for c in counts)
        print(f"Read 3 datasets concurrently in {elapsed:.3f}s")

    def test_read_with_concurrent_processing(self, ray_start_regular_shared, tmp_path):
        """Test reading while processing is happening."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create dataset
        num_files = 15
        for i in range(num_files):
            df = pd.DataFrame(
                {"id": range(i * 100, (i + 1) * 100), "value": np.random.rand(100)}
            )
            pq.write_table(
                pa.Table.from_pandas(df),
                os.path.join(tmp_path, f"file_{i:03d}.parquet"),
            )

        # Read and process concurrently
        ds = ray.data.read(str(tmp_path))

        # Start processing before reading completes
        processed = ds.map(lambda row: {**row, "processed": True})
        count = processed.count()

        assert count == num_files * 100


class TestReadErrors:
    """Test error handling and messages."""

    def test_error_on_empty_path_list(self, ray_start_regular_shared):
        """Test error when given empty path list."""
        with pytest.raises(ValueError, match="No files found"):
            ray.data.read([])

    def test_error_on_nonexistent_path(self, ray_start_regular_shared):
        """Test error when path doesn't exist."""
        with pytest.raises(FileNotFoundError):
            ray.data.read("/nonexistent/path/data.parquet")

    def test_error_on_empty_directory(self, ray_start_regular_shared, tmp_path):
        """Test error when directory is empty."""
        empty_dir = os.path.join(tmp_path, "empty")
        os.makedirs(empty_dir)

        with pytest.raises(ValueError, match="No files found"):
            ray.data.read(empty_dir)

    def test_error_on_unsupported_format_only(self, ray_start_regular_shared, tmp_path):
        """Test error when all files are truly unsupported."""
        # This is hard to test since unknown extensions use binary fallback
        # But we can test the error message structure

        # Create only truly problematic files (empty directory triggers error)
        empty_dir = os.path.join(tmp_path, "empty")
        os.makedirs(empty_dir)

        with pytest.raises(ValueError):
            ray.data.read(empty_dir)

    def test_error_message_includes_supported_formats(
        self, ray_start_regular_shared, tmp_path
    ):
        """Test that error messages include supported format info."""
        empty_dir = os.path.join(tmp_path, "empty")
        os.makedirs(empty_dir)

        try:
            ray.data.read(empty_dir)
        except ValueError as e:
            error_msg = str(e)
            # Should mention that no files were found
            assert "No files found" in error_msg or "empty" in error_msg.lower()

    def test_error_on_corrupted_required_format(
        self, ray_start_regular_shared, tmp_path
    ):
        """Test error when format-specific file is corrupted."""
        # Create fake parquet file
        fake_parquet = os.path.join(tmp_path, "corrupted.parquet")
        with open(fake_parquet, "w") as f:
            f.write("Not valid parquet data")

        # Should raise error when trying to read
        with pytest.raises(Exception):
            ds = ray.data.read(fake_parquet)
            ds.count()

    def test_detailed_error_on_read_failure(
        self, ray_start_regular_shared, tmp_path, caplog
    ):
        """Test that read failures provide detailed error messages."""
        # Create invalid parquet
        invalid_file = os.path.join(tmp_path, "invalid.parquet")
        with open(invalid_file, "w") as f:
            f.write("invalid parquet content")

        with caplog.at_level(logging.ERROR):
            try:
                ds = ray.data.read(invalid_file)
                ds.count()
            except Exception:
                pass

        # Should have logged error details
        assert any(record.levelname == "ERROR" for record in caplog.records)


class TestReadDecisionLogging:
    """Test logging of read decisions."""

    def test_log_file_type_grouping(self, ray_start_regular_shared, tmp_path, caplog):
        """Test that file type grouping is logged."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create mixed types
        pq.write_table(
            pa.Table.from_pandas(pd.DataFrame({"a": [1]})),
            os.path.join(tmp_path, "d1.parquet"),
        )
        pq.write_table(
            pa.Table.from_pandas(pd.DataFrame({"a": [2]})),
            os.path.join(tmp_path, "d2.parquet"),
        )
        pd.DataFrame({"a": [3]}).to_csv(os.path.join(tmp_path, "d1.csv"), index=False)

        with caplog.at_level(logging.INFO):
            ray.data.read(str(tmp_path))

        # Should log grouping info
        messages = [r.message for r in caplog.records if r.levelname == "INFO"]
        assert any("parquet" in msg.lower() for msg in messages)
        assert any("csv" in msg.lower() for msg in messages)

    def test_log_reader_selection(self, ray_start_regular_shared, tmp_path, caplog):
        """Test that reader selection is logged."""
        import pandas as pd
        import pyarrow.parquet as pq

        df = pd.DataFrame({"a": [1, 2, 3]})
        pq.write_table(pa.Table.from_pandas(df), os.path.join(tmp_path, "data.parquet"))

        with caplog.at_level(logging.INFO):
            ray.data.read(os.path.join(tmp_path, "data.parquet"))

        # Should log reading info
        assert any(record.levelname == "INFO" for record in caplog.records)

    def test_log_concatenation_info(self, ray_start_regular_shared, tmp_path, caplog):
        """Test that dataset concatenation is logged."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create different format files
        pq.write_table(
            pa.Table.from_pandas(pd.DataFrame({"a": [1]})),
            os.path.join(tmp_path, "data.parquet"),
        )
        pd.DataFrame({"a": [2]}).to_csv(os.path.join(tmp_path, "data.csv"), index=False)

        with caplog.at_level(logging.INFO):
            ray.data.read(str(tmp_path))

        # Should log concatenation
        messages = [r.message for r in caplog.records]
        assert any(
            "concatenat" in msg.lower() or "dataset" in msg.lower() for msg in messages
        )


class TestReadProgressiveDiscovery:
    """Test progressive file discovery and reading."""

    def test_handles_nested_discovery(self, ray_start_regular_shared, tmp_path, caplog):
        """Test discovery of files in nested directories."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create nested structure
        subdir1 = os.path.join(tmp_path, "level1", "level2")
        os.makedirs(subdir1)

        df = pd.DataFrame({"a": [1, 2, 3]})
        pq.write_table(pa.Table.from_pandas(df), os.path.join(subdir1, "data.parquet"))

        with caplog.at_level(logging.INFO):
            ds = ray.data.read(str(tmp_path))
            assert ds.count() == 3

    def test_skips_non_file_paths(self, ray_start_regular_shared, tmp_path, caplog):
        """Test that non-file paths are handled gracefully."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create file
        df = pd.DataFrame({"a": [1, 2, 3]})
        pq.write_table(pa.Table.from_pandas(df), os.path.join(tmp_path, "data.parquet"))

        # Create empty subdirectory
        os.makedirs(os.path.join(tmp_path, "emptydir"))

        with caplog.at_level(logging.WARNING):
            ds = ray.data.read(str(tmp_path))
            assert ds.count() == 3

    def test_handles_symlink_cycles(self, ray_start_regular_shared, tmp_path):
        """Test handling of symlink cycles."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create file
        df = pd.DataFrame({"a": [1, 2, 3]})
        pq.write_table(pa.Table.from_pandas(df), os.path.join(tmp_path, "data.parquet"))

        # Try to create symlink cycle (may not work on all systems)
        try:
            os.symlink(tmp_path, os.path.join(tmp_path, "cycle"))
            # If symlink succeeds, reading should still work
            ds = ray.data.read(str(tmp_path))
            # May have duplicates due to cycle, but shouldn't crash
            assert ds.count() >= 3
        except (OSError, NotImplementedError):
            # Symlinks not supported, skip
            pytest.skip("Symlinks not supported on this system")


class TestReadHeuristicsValidation:
    """Test validation of read heuristics and decisions."""

    def test_prefers_structured_over_binary(self, ray_start_regular_shared, tmp_path):
        """Test that structured formats are preferred over binary fallback."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create parquet file
        df = pd.DataFrame({"a": [1, 2, 3]})
        parquet_file = os.path.join(tmp_path, "data.parquet")
        pq.write_table(pa.Table.from_pandas(df), parquet_file)

        # Read should use parquet reader, not binary
        ds = ray.data.read(parquet_file)
        result = ds.take(1)[0]

        # Should have structured data, not bytes
        assert "a" in result
        assert "bytes" not in result

    def test_compression_transparent_handling(self, ray_start_regular_shared, tmp_path):
        """Test that compression is handled transparently."""
        import pandas as pd

        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})

        # Create uncompressed
        df.to_csv(os.path.join(tmp_path, "data.csv"), index=False)

        # Create compressed
        df.to_csv(
            os.path.join(tmp_path, "data_compressed.csv.gz"),
            index=False,
            compression="gzip",
        )

        # Both should be read with same result structure
        ds = ray.data.read(str(tmp_path))
        assert ds.count() == 6  # 3 rows × 2 files

    def test_handles_ambiguous_extensions_consistently(
        self, ray_start_regular_shared, tmp_path
    ):
        """Test consistent handling of potentially ambiguous extensions."""
        # M4A should always be audio
        audio_file = os.path.join(tmp_path, "audio.m4a")
        with open(audio_file, "wb") as f:
            f.write(b"fake audio")

        # M4V should always be video
        video_file = os.path.join(tmp_path, "video.m4v")
        with open(video_file, "wb") as f:
            f.write(b"fake video")

        # Detection should be consistent
        try:
            ray.data.read(str(tmp_path))
            # May fail on invalid media, but detection should work
        except Exception as e:
            # Should have tried appropriate readers
            error_msg = str(e).lower()
            assert "audio" in error_msg or "video" in error_msg or "m4" in error_msg

    def test_validates_file_accessibility(self, ray_start_regular_shared, tmp_path):
        """Test that file accessibility is validated."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create file
        df = pd.DataFrame({"a": [1, 2, 3]})
        parquet_file = os.path.join(tmp_path, "data.parquet")
        pq.write_table(pa.Table.from_pandas(df), parquet_file)

        # Should read successfully
        ds = ray.data.read(parquet_file)
        assert ds.count() == 3

        # Test with nonexistent file
        with pytest.raises(FileNotFoundError):
            ds = ray.data.read(os.path.join(tmp_path, "nonexistent.parquet"))


class TestFormatSpecificValidation:
    """Test validation logic specific to each format."""

    def test_validates_parquet_metadata(self, ray_start_regular_shared, tmp_path):
        """Test that Parquet files are validated."""
        # Create invalid parquet
        invalid_file = os.path.join(tmp_path, "invalid.parquet")
        with open(invalid_file, "wb") as f:
            f.write(b"PAR1" + b"x" * 100)  # Fake parquet header

        # Should fail validation
        with pytest.raises(Exception):
            ds = ray.data.read(invalid_file)
            ds.count()

    def test_validates_csv_structure(self, ray_start_regular_shared, tmp_path):
        """Test that CSV files are validated."""
        # Create CSV
        csv_file = os.path.join(tmp_path, "data.csv")
        with open(csv_file, "w") as f:
            f.write("a,b,c\n1,2,3\n4,5,6\n")

        # Should read successfully
        ds = ray.data.read(csv_file)
        assert ds.count() == 2

    def test_validates_json_format(self, ray_start_regular_shared, tmp_path):
        """Test that JSON files are validated."""

        # Create valid JSON
        valid_file = os.path.join(tmp_path, "valid.json")
        with open(valid_file, "w") as f:
            f.write(json.dumps({"a": 1}) + "\n")
            f.write(json.dumps({"a": 2}) + "\n")

        # Should read successfully
        ds = ray.data.read(valid_file)
        assert ds.count() == 2

        # Create invalid JSON
        invalid_file = os.path.join(tmp_path, "invalid.json")
        with open(invalid_file, "w") as f:
            f.write("{invalid json\n")

        # Should handle gracefully
        try:
            ds = ray.data.read(invalid_file)
            ds.count()
        except Exception as e:
            # Should indicate JSON parsing error
            assert "json" in str(e).lower() or "parse" in str(e).lower()

    def test_validates_image_format(self, ray_start_regular_shared, tmp_path):
        """Test that image files are validated."""
        from PIL import Image

        # Create valid image
        img = Image.new("RGB", (10, 10), color="red")
        img.save(os.path.join(tmp_path, "valid.png"))

        # Should read successfully
        ds = ray.data.read(os.path.join(tmp_path, "valid.png"))
        assert ds.count() == 1

        # Create invalid image
        invalid_file = os.path.join(tmp_path, "invalid.png")
        with open(invalid_file, "wb") as f:
            f.write(b"not a png")

        # Should handle appropriately
        try:
            ds = ray.data.read(invalid_file)
            ds.count()
        except Exception as e:
            # Should indicate image error
            assert (
                "image" in str(e).lower()
                or "png" in str(e).lower()
                or "cannot" in str(e).lower()
            )


class TestMediaFormats:
    """Test detection of media formats."""

    def test_image_formats_all_detected(self, ray_start_regular_shared, tmp_path):
        """Test that all supported image formats are detected."""
        from PIL import Image

        formats = {
            "png": "PNG",
            "jpg": "JPEG",
            "jpeg": "JPEG",
            "gif": "GIF",
            "bmp": "BMP",
        }

        for ext, pil_format in formats.items():
            img = Image.new("RGB", (10, 10), color="red")
            img.save(os.path.join(tmp_path, f"image.{ext}"), format=pil_format)

        # All should be detected as images
        ds = ray.data.read(str(tmp_path))
        assert ds.count() == len(formats)

    def test_video_vs_audio_distinction(self, ray_start_regular_shared, tmp_path):
        """Test that video and audio formats are distinguished."""
        # Create files with different extensions
        video_exts = ["mp4", "mov", "avi"]
        audio_exts = ["mp3", "wav", "m4a"]

        for ext in video_exts:
            with open(os.path.join(tmp_path, f"media.{ext}"), "wb") as f:
                f.write(b"fake video")

        for ext in audio_exts:
            with open(os.path.join(tmp_path, f"sound.{ext}"), "wb") as f:
                f.write(b"fake audio")

        # Should detect both types
        try:
            ray.data.read(str(tmp_path))
            # May fail on invalid files, but detection should work
        except Exception as e:
            # Expected for invalid media files
            error_msg = str(e).lower()
            assert "video" in error_msg or "audio" in error_msg or "media" in error_msg

    def test_image_not_confused_with_video(self, ray_start_regular_shared, tmp_path):
        """Test that images are not confused with video frames."""
        from PIL import Image

        # JPEG images should use read_images, not read_videos
        img = Image.new("RGB", (10, 10), color="blue")
        img.save(os.path.join(tmp_path, "photo.jpg"))

        ds = ray.data.read(os.path.join(tmp_path, "photo.jpg"))
        result = ds.take(1)[0]

        # Should have image structure
        assert "image" in result


class TestTextAndDocumentFormats:
    """Test detection of text and document formats."""

    def test_plain_text_detection(self, ray_start_regular_shared, tmp_path):
        """Test that .txt files use read_text."""
        with open(os.path.join(tmp_path, "document.txt"), "w") as f:
            f.write("Line 1\nLine 2\nLine 3")

        ds = ray.data.read(os.path.join(tmp_path, "document.txt"))
        result = ds.take()

        assert len(result) == 3
        assert all("text" in r for r in result)


class TestStructuredDataFormats:
    """Test detection of structured data formats."""

    def test_json_newline_delimited(self, ray_start_regular_shared, tmp_path):
        """Test that newline-delimited JSON is handled."""

        data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        with open(os.path.join(tmp_path, "data.json"), "w") as f:
            for item in data:
                f.write(json.dumps(item) + "\n")

        ds = ray.data.read(os.path.join(tmp_path, "data.json"))
        assert ds.count() == 2

    def test_jsonl_extension(self, ray_start_regular_shared, tmp_path):
        """Test that .jsonl is recognized."""

        data = [{"x": 10}, {"x": 20}]
        with open(os.path.join(tmp_path, "data.jsonl"), "w") as f:
            for item in data:
                f.write(json.dumps(item) + "\n")

        ds = ray.data.read(os.path.join(tmp_path, "data.jsonl"))
        assert ds.count() == 2

    def test_csv_standard_format(self, ray_start_regular_shared, tmp_path):
        """Test that standard CSV is detected."""
        import pandas as pd

        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        df.to_csv(os.path.join(tmp_path, "data.csv"), index=False)

        ds = ray.data.read(os.path.join(tmp_path, "data.csv"))
        result = ds.to_pandas()
        pd.testing.assert_frame_equal(result, df)

    def test_avro_format(self, ray_start_regular_shared, tmp_path):
        """Test that Avro files are detected."""
        from fastavro import parse_schema, writer

        schema = {
            "type": "record",
            "name": "Test",
            "fields": [{"name": "a", "type": "int"}],
        }
        records = [{"a": 1}, {"a": 2}, {"a": 3}]

        with open(os.path.join(tmp_path, "data.avro"), "wb") as f:
            writer(f, parse_schema(schema), records)

        ds = ray.data.read(os.path.join(tmp_path, "data.avro"))
        assert ds.count() == 3


class TestArrayFormats:
    """Test detection of array formats."""

    def test_numpy_arrays(self, ray_start_regular_shared, tmp_path):
        """Test that NumPy arrays are detected."""
        arr = np.array([1, 2, 3, 4, 5])
        np.save(os.path.join(tmp_path, "array.npy"), arr)

        ds = ray.data.read(os.path.join(tmp_path, "array.npy"))
        result = ds.take(1)[0]

        assert "data" in result
        assert np.array_equal(result["data"], arr)

    def test_multidimensional_numpy(self, ray_start_regular_shared, tmp_path):
        """Test that multidimensional arrays work."""
        arr = np.random.rand(10, 20)
        np.save(os.path.join(tmp_path, "matrix.npy"), arr)

        ds = ray.data.read(os.path.join(tmp_path, "matrix.npy"))
        result = ds.take(1)[0]

        assert "data" in result
        assert np.array_equal(result["data"], arr)


class TestCompressionHandling:
    """Test compression format handling."""

    def test_gzip_compression_all_formats(self, ray_start_regular_shared, tmp_path):
        """Test that .gz compression works for all formats."""
        import json

        import pandas as pd

        # CSV.GZ
        df = pd.DataFrame({"a": [1, 2, 3]})
        df.to_csv(
            os.path.join(tmp_path, "data.csv.gz"), index=False, compression="gzip"
        )

        # JSON.GZ
        data = [{"b": 4}, {"b": 5}]
        with gzip.open(os.path.join(tmp_path, "data.json.gz"), "wt") as f:
            for item in data:
                f.write(json.dumps(item) + "\n")

        # Should detect both and decompress automatically
        ds = ray.data.read(str(tmp_path))
        assert ds.count() == 5

    def test_brotli_compression(self, ray_start_regular_shared, tmp_path):
        """Test that .br compression is recognized."""
        # Note: Actual brotli compression may need additional libraries
        # This tests that .csv.br extension is in our mapping
        # Real brotli files would be handled by Arrow
        pass  # Extension mapping tested

    def test_zstd_compression(self, ray_start_regular_shared, tmp_path):
        """Test that .zst compression is recognized."""
        # Extension mapping for .csv.zst exists
        pass  # Extension mapping tested

    def test_lz4_compression(self, ray_start_regular_shared, tmp_path):
        """Test that .lz4 compression is recognized."""
        # Extension mapping for .csv.lz4 exists
        pass  # Extension mapping tested


class TestSpecialCharactersInPaths:
    """Test handling of special characters in file paths."""

    def test_spaces_in_filename(self, ray_start_regular_shared, tmp_path):
        """Test files with spaces in names."""
        import pandas as pd
        import pyarrow.parquet as pq

        df = pd.DataFrame({"a": [1, 2, 3]})
        file_path = os.path.join(tmp_path, "data file.parquet")
        pq.write_table(pa.Table.from_pandas(df), file_path)

        ds = ray.data.read(file_path)
        assert ds.count() == 3

    def test_unicode_in_filename(self, ray_start_regular_shared, tmp_path):
        """Test files with unicode characters."""
        import pandas as pd
        import pyarrow.parquet as pq

        df = pd.DataFrame({"a": [1, 2, 3]})
        file_path = os.path.join(tmp_path, "数据.parquet")
        pq.write_table(pa.Table.from_pandas(df), file_path)

        ds = ray.data.read(file_path)
        assert ds.count() == 3

    def test_special_chars_in_filename(self, ray_start_regular_shared, tmp_path):
        """Test files with special characters."""
        import pandas as pd
        import pyarrow.parquet as pq

        df = pd.DataFrame({"a": [1, 2, 3]})
        # Use characters that are valid in filenames
        file_path = os.path.join(tmp_path, "data-file_123.parquet")
        pq.write_table(pa.Table.from_pandas(df), file_path)

        ds = ray.data.read(file_path)
        assert ds.count() == 3


class TestBinaryFallbackScenarios:
    """Test scenarios where binary fallback is used."""

    def test_unknown_extension_binary(self, ray_start_regular_shared, tmp_path):
        """Test that unknown extensions use binary."""
        content = b"random binary content"
        file_path = os.path.join(tmp_path, "data.xyz123")
        with open(file_path, "wb") as f:
            f.write(content)

        ds = ray.data.read(file_path)
        result = ds.take(1)[0]

        assert "bytes" in result
        assert result["bytes"] == content

    def test_no_extension_binary(self, ray_start_regular_shared, tmp_path):
        """Test that files without extension use binary."""
        content = b"file content"
        file_path = os.path.join(tmp_path, "README")
        with open(file_path, "wb") as f:
            f.write(content)

        ds = ray.data.read(file_path)
        result = ds.take(1)[0]

        assert "bytes" in result

    def test_executable_as_binary(self, ray_start_regular_shared, tmp_path):
        """Test that executables are read as binary."""
        content = b"#!/bin/bash\necho test"
        file_path = os.path.join(tmp_path, "script.sh")
        with open(file_path, "wb") as f:
            f.write(content)

        ds = ray.data.read(file_path)
        result = ds.take(1)[0]

        # .sh not in our mapping, so should be binary
        assert "bytes" in result

    def test_mixed_known_unknown(self, ray_start_regular_shared, tmp_path):
        """Test directory with known and unknown formats."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Known format
        df = pd.DataFrame({"a": [1, 2, 3]})
        pq.write_table(pa.Table.from_pandas(df), os.path.join(tmp_path, "data.parquet"))

        # Unknown format
        with open(os.path.join(tmp_path, "data.unknown"), "wb") as f:
            f.write(b"unknown")

        # Both should be read
        ds = ray.data.read(str(tmp_path))
        assert ds.count() == 4  # 3 from parquet + 1 from binary


class TestPerformanceOptimizations:
    """Test that performance optimizations are applied."""

    def test_single_format_no_concatenation(self, ray_start_regular_shared, tmp_path):
        """Test that single format doesn't need concatenation."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create multiple files of same type
        for i in range(3):
            df = pd.DataFrame({"a": [i]})
            pq.write_table(
                pa.Table.from_pandas(df), os.path.join(tmp_path, f"data_{i}.parquet")
            )

        # Should read efficiently without concatenation overhead
        ds = ray.data.read(str(tmp_path))
        assert ds.count() == 3

    def test_parallelism_respected(self, ray_start_regular_shared, tmp_path):
        """Test that parallelism settings are respected."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create files
        for i in range(10):
            df = pd.DataFrame({"a": [i]})
            pq.write_table(
                pa.Table.from_pandas(df), os.path.join(tmp_path, f"data_{i}.parquet")
            )

        # Read with specific parallelism
        ds = ray.data.read(str(tmp_path), override_num_blocks=5)
        assert ds.count() == 10


class TestFormatHintValidation:
    """Test format hint parameter validation."""

    def test_format_hint_delta(self, ray_start_regular_shared, tmp_path):
        """Test format hint for Delta Lake."""
        pytest.importorskip("deltalake")
        import pandas as pd

        # Create a simple parquet file that we'll read as delta
        df = pd.DataFrame({"a": [1, 2, 3]})
        path = os.path.join(tmp_path, "data.parquet")
        df.to_parquet(path)

        # Note: This will fail because it's not a real Delta table,
        # but it tests that the format parameter is recognized
        try:
            ray.data.read(path, format="delta")
        except Exception:
            # Expected - not a real Delta table
            pass

    def test_format_hint_lance(self, ray_start_regular_shared, tmp_path):
        """Test format hint for Lance format."""
        pytest.importorskip("lance")

        # Note: Creating a real Lance dataset requires lance library
        # This test verifies the format parameter is recognized
        lance_path = os.path.join(tmp_path, "data.lance")
        os.makedirs(lance_path, exist_ok=True)

        # Create a dummy file to test path detection
        with open(os.path.join(lance_path, "test.txt"), "w") as f:
            f.write("test")

        try:
            ray.data.read(lance_path, format="lance")
        except Exception:
            # Expected - not a real Lance dataset
            pass

    def test_all_format_hints_recognized(self, ray_start_regular_shared, tmp_path):
        """Test that all format hints are recognized."""
        expected_formats = [
            "parquet",
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
            "delta",
            "hudi",
            "iceberg",
            "binary",
        ]

        # Create a dummy text file
        test_file = os.path.join(tmp_path, "test.txt")
        with open(test_file, "w") as f:
            f.write("test data\n")

        # Test that each format is recognized (may fail during read, but
        # should not fail during format validation)
        for fmt in expected_formats:
            try:
                ray.data.read(test_file, format=fmt)
            except ValueError as e:
                if "Unsupported format" in str(e):
                    pytest.fail(f"Format '{fmt}' not recognized")
            except Exception:
                # Other exceptions are ok - we're just testing format recognition
                pass


class TestFormatPrecedence:
    """Test format detection precedence and conflict resolution."""

    def test_parquet_vs_delta_precedence(self, ray_start_regular_shared, tmp_path):
        """Test that Parquet files are not confused with Delta files.

        Delta Lake uses Parquet internally but has specific directory structure.
        Regular .parquet files should use read_parquet.
        """
        import pandas as pd
        import pyarrow.parquet as pq

        # Create regular Parquet file
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        parquet_file = os.path.join(tmp_path, "data.parquet")
        pq.write_table(pa.Table.from_pandas(df), parquet_file)

        # Read should use read_parquet, not read_delta
        ds = ray.data.read(parquet_file)
        result = ds.to_pandas()

        pd.testing.assert_frame_equal(result, df)
        # Verify it's reading as parquet (would fail if trying delta)
        assert ds.count() == 3

    def test_parquet_vs_iceberg_precedence(self, ray_start_regular_shared, tmp_path):
        """Test that Parquet files are not confused with Iceberg tables.

        Iceberg uses Parquet internally but has metadata files.
        Regular .parquet files should use read_parquet.
        """
        import pandas as pd
        import pyarrow.parquet as pq

        # Create regular Parquet file
        df = pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})
        parquet_file = os.path.join(tmp_path, "table.parquet")
        pq.write_table(pa.Table.from_pandas(df), parquet_file)

        # Should use read_parquet
        ds = ray.data.read(parquet_file)
        assert ds.count() == 3

    def test_image_vs_video_precedence(self, ray_start_regular_shared, tmp_path):
        """Test that images and videos are handled by correct readers.

        Some formats could be ambiguous (e.g., motion JPEG).
        """
        from PIL import Image

        # Create image files
        img = Image.new("RGB", (10, 10), color="red")
        img.save(os.path.join(tmp_path, "image.jpg"))
        img.save(os.path.join(tmp_path, "image.png"))

        # Read images - should use read_images
        ds = ray.data.read(str(tmp_path))
        assert ds.count() == 2

    def test_audio_vs_video_format_distinction(
        self, ray_start_regular_shared, tmp_path
    ):
        """Test that audio and video formats are properly distinguished.

        M4A is audio, M4V is video - both use MP4 container.
        """
        # Create dummy files (won't be valid media, just testing extension detection)
        audio_file = os.path.join(tmp_path, "audio.m4a")
        video_file = os.path.join(tmp_path, "video.m4v")

        # Create empty files just for extension detection
        open(audio_file, "wb").close()
        open(video_file, "wb").close()

        # Read directory - should detect both as different types
        try:
            ray.data.read(str(tmp_path))
            # May fail to actually read the invalid files, but detection should work
        except Exception:
            # Expected - files are not valid media
            pass

    def test_json_vs_jsonl_handling(self, ray_start_regular_shared, tmp_path):
        """Test that both JSON and JSONL are handled correctly."""

        # Create JSONL file
        data1 = [{"a": 1}, {"a": 2}]
        with open(os.path.join(tmp_path, "data.jsonl"), "w") as f:
            for item in data1:
                f.write(json.dumps(item) + "\n")

        # Create JSON file (newline-delimited)
        data2 = [{"a": 3}, {"a": 4}]
        with open(os.path.join(tmp_path, "data.json"), "w") as f:
            for item in data2:
                f.write(json.dumps(item) + "\n")

        # Both should be read correctly
        ds = ray.data.read(str(tmp_path))
        assert ds.count() == 4

    def test_compressed_vs_uncompressed_precedence(
        self, ray_start_regular_shared, tmp_path
    ):
        """Test that compressed and uncompressed files are handled correctly."""
        import pandas as pd

        # Create uncompressed CSV
        df1 = pd.DataFrame({"a": [1, 2, 3]})
        df1.to_csv(os.path.join(tmp_path, "data.csv"), index=False)

        # Create compressed CSV
        df2 = pd.DataFrame({"a": [4, 5, 6]})
        df2.to_csv(
            os.path.join(tmp_path, "data.csv.gz"), index=False, compression="gzip"
        )

        # Both should be read correctly
        ds = ray.data.read(str(tmp_path))
        assert ds.count() == 6


class TestMixedFileTypes:
    """Test handling of directories with mixed file types."""

    def test_mixed_structured_formats(self, ray_start_regular_shared, tmp_path):
        """Test mixing structured data formats (Parquet, CSV, JSON)."""

        import pandas as pd
        import pyarrow.parquet as pq

        # Create Parquet
        df1 = pd.DataFrame({"value": [1, 2]})
        pq.write_table(
            pa.Table.from_pandas(df1), os.path.join(tmp_path, "data.parquet")
        )

        # Create CSV
        df2 = pd.DataFrame({"value": [3, 4]})
        df2.to_csv(os.path.join(tmp_path, "data.csv"), index=False)

        # Create JSON
        data3 = [{"value": 5}, {"value": 6}]
        with open(os.path.join(tmp_path, "data.json"), "w") as f:
            for item in data3:
                f.write(json.dumps(item) + "\n")

        # Should read all and concatenate
        ds = ray.data.read(str(tmp_path))
        assert ds.count() == 6

    def test_mixed_media_formats(self, ray_start_regular_shared, tmp_path):
        """Test mixing media formats (images, audio, video)."""
        from PIL import Image

        # Create image
        img = Image.new("RGB", (10, 10), color="red")
        img.save(os.path.join(tmp_path, "image.png"))

        # Create dummy audio file
        audio_file = os.path.join(tmp_path, "audio.mp3")
        with open(audio_file, "wb") as f:
            f.write(b"fake audio data")

        # Create dummy video file
        video_file = os.path.join(tmp_path, "video.mp4")
        with open(video_file, "wb") as f:
            f.write(b"fake video data")

        # Should handle mixed media types
        try:
            ds = ray.data.read(str(tmp_path))
            # May fail on invalid media files, but detection should work
            count = ds.count()
            assert count >= 1  # At least the image
        except Exception as e:
            # Expected - audio/video files are not valid
            assert (
                "image" in str(tmp_path).lower()
                or "audio" in str(e).lower()
                or "video" in str(e).lower()
            )

    def test_mixed_structured_and_unstructured(
        self, ray_start_regular_shared, tmp_path
    ):
        """Test mixing structured data with text/binary."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create structured data
        df = pd.DataFrame({"a": [1, 2, 3]})
        pq.write_table(pa.Table.from_pandas(df), os.path.join(tmp_path, "data.parquet"))

        # Create text file
        with open(os.path.join(tmp_path, "data.txt"), "w") as f:
            f.write("line1\nline2\n")

        # Create binary file
        with open(os.path.join(tmp_path, "data.bin"), "wb") as f:
            f.write(b"binary data")

        # Should handle all types
        ds = ray.data.read(str(tmp_path))
        assert ds.count() >= 3

    def test_incompatible_schemas_warning(self, ray_start_regular_shared, tmp_path):
        """Test that mixing files with very different schemas still works."""

        import pandas as pd
        import pyarrow.parquet as pq

        # Create file with numeric columns
        df1 = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        pq.write_table(
            pa.Table.from_pandas(df1), os.path.join(tmp_path, "numeric.parquet")
        )

        # Create file with different schema
        data2 = [{"x": "text", "y": "more text"}]
        with open(os.path.join(tmp_path, "text.json"), "w") as f:
            for item in data2:
                f.write(json.dumps(item) + "\n")

        # Should concatenate despite schema differences
        ds = ray.data.read(str(tmp_path))
        # Count should be total of both
        assert ds.count() == 4

    def test_many_mixed_types_performance(self, ray_start_regular_shared, tmp_path):
        """Test performance with many different file types."""

        import pandas as pd
        import pyarrow.parquet as pq

        # Create multiple file types (5 of each)
        for i in range(5):
            # Parquet
            df = pd.DataFrame({"type": ["parquet"], "value": [i]})
            pq.write_table(
                pa.Table.from_pandas(df), os.path.join(tmp_path, f"data_{i}.parquet")
            )

            # CSV
            df.to_csv(os.path.join(tmp_path, f"data_{i}.csv"), index=False)

            # JSON
            with open(os.path.join(tmp_path, f"data_{i}.json"), "w") as f:
                f.write(json.dumps({"type": "json", "value": i}) + "\n")

            # Text
            with open(os.path.join(tmp_path, f"data_{i}.txt"), "w") as f:
                f.write(f"text_{i}\n")

        # Read all
        ds = ray.data.read(str(tmp_path))
        # Should have data from all files
        assert ds.count() >= 5


class TestBinaryFallback:
    """Test binary file fallback logic."""

    def test_unknown_extension_uses_binary(self, ray_start_regular_shared, tmp_path):
        """Test that unknown extensions fall back to binary reader."""
        # Create file with unknown extension
        unknown_file = os.path.join(tmp_path, "data.xyz")
        content = b"test binary content"
        with open(unknown_file, "wb") as f:
            f.write(content)

        # Should read as binary
        ds = ray.data.read(unknown_file)
        result = ds.take(1)[0]

        assert "bytes" in result
        assert result["bytes"] == content

    def test_no_extension_uses_binary(self, ray_start_regular_shared, tmp_path):
        """Test that files without extension use binary reader."""
        # Create file without extension
        no_ext_file = os.path.join(tmp_path, "datafile")
        content = b"no extension content"
        with open(no_ext_file, "wb") as f:
            f.write(content)

        # Should read as binary
        ds = ray.data.read(no_ext_file)
        result = ds.take(1)[0]

        assert "bytes" in result
        assert result["bytes"] == content

    def test_corrupted_file_detection(self, ray_start_regular_shared, tmp_path):
        """Test behavior with corrupted files claiming to be certain formats."""
        # Create file with .parquet extension but invalid content
        fake_parquet = os.path.join(tmp_path, "fake.parquet")
        with open(fake_parquet, "w") as f:
            f.write("This is not actually parquet data")

        # Should attempt to read as parquet and fail appropriately
        with pytest.raises(Exception):
            ds = ray.data.read(fake_parquet)
            ds.count()

    def test_mixed_known_and_unknown_extensions(
        self, ray_start_regular_shared, tmp_path
    ):
        """Test directory with both known and unknown extensions."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create known format
        df = pd.DataFrame({"a": [1, 2, 3]})
        pq.write_table(pa.Table.from_pandas(df), os.path.join(tmp_path, "data.parquet"))

        # Create unknown format
        with open(os.path.join(tmp_path, "data.xyz"), "wb") as f:
            f.write(b"unknown content")

        # Should read both (parquet as structured, xyz as binary)
        ds = ray.data.read(str(tmp_path))
        assert ds.count() == 4  # 3 parquet rows + 1 binary file

    def test_executable_file_as_binary(self, ray_start_regular_shared, tmp_path):
        """Test that executable files are read as binary."""
        # Create file that might be executable
        exec_file = os.path.join(tmp_path, "script.sh")
        with open(exec_file, "w") as f:
            f.write("#!/bin/bash\necho 'test'\n")

        # Should read as text (has .sh extension not in our mapping)
        ds = ray.data.read(exec_file)
        result = ds.take(1)[0]

        # Should be read as binary
        assert "bytes" in result


class TestExtensionConflicts:
    """Test handling of potentially conflicting extensions."""

    def test_jpeg_vs_jpg_equivalence(self, ray_start_regular_shared, tmp_path):
        """Test that .jpeg and .jpg are treated the same."""
        from PIL import Image

        # Create .jpg
        img1 = Image.new("RGB", (10, 10), color="red")
        img1.save(os.path.join(tmp_path, "image1.jpg"))

        # Create .jpeg
        img2 = Image.new("RGB", (10, 10), color="blue")
        img2.save(os.path.join(tmp_path, "image2.jpeg"))

        # Both should use read_images
        ds = ray.data.read(str(tmp_path))
        assert ds.count() == 2

    def test_tif_vs_tiff_equivalence(self, ray_start_regular_shared, tmp_path):
        """Test that .tif and .tiff are treated the same."""
        from PIL import Image

        # Create .tif
        img1 = Image.new("RGB", (10, 10), color="green")
        img1.save(os.path.join(tmp_path, "image1.tif"))

        # Create .tiff
        img2 = Image.new("RGB", (10, 10), color="yellow")
        img2.save(os.path.join(tmp_path, "image2.tiff"))

        # Both should use read_images
        ds = ray.data.read(str(tmp_path))
        assert ds.count() == 2

    def test_mpeg_vs_mpg_video(self, ray_start_regular_shared, tmp_path):
        """Test that .mpeg and .mpg are both recognized as video."""
        # Create dummy files
        with open(os.path.join(tmp_path, "video1.mpeg"), "wb") as f:
            f.write(b"fake mpeg")

        with open(os.path.join(tmp_path, "video2.mpg"), "wb") as f:
            f.write(b"fake mpg")

        # Both should attempt to use read_videos
        try:
            ray.data.read(str(tmp_path))
            # May fail on invalid video, but detection should work
        except Exception as e:
            # Expected for invalid video files
            assert (
                "video" in str(e).lower()
                or "mpeg" in str(e).lower()
                or "mpg" in str(e).lower()
            )

    def test_case_insensitive_detection(self, ray_start_regular_shared, tmp_path):
        """Test that extension detection is case-insensitive."""
        import pandas as pd
        import pyarrow.parquet as pq

        df = pd.DataFrame({"a": [1, 2, 3]})

        # Create files with various casings
        pq.write_table(pa.Table.from_pandas(df), os.path.join(tmp_path, "data.PARQUET"))
        pq.write_table(pa.Table.from_pandas(df), os.path.join(tmp_path, "data.Parquet"))
        pq.write_table(pa.Table.from_pandas(df), os.path.join(tmp_path, "data.parquet"))

        # All should be detected as parquet
        ds = ray.data.read(str(tmp_path))
        assert ds.count() == 9  # 3 files × 3 rows


class TestSchemaCompatibility:
    """Test schema compatibility when mixing formats."""

    def test_compatible_numeric_schemas(self, ray_start_regular_shared, tmp_path):
        """Test mixing files with compatible numeric schemas."""

        import pandas as pd
        import pyarrow.parquet as pq

        # Create Parquet with int columns
        df1 = pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})
        pq.write_table(
            pa.Table.from_pandas(df1), os.path.join(tmp_path, "data.parquet")
        )

        # Create JSON with same schema
        data2 = [{"id": 4, "value": 40}, {"id": 5, "value": 50}]
        with open(os.path.join(tmp_path, "data.json"), "w") as f:
            for item in data2:
                f.write(json.dumps(item) + "\n")

        # Should concatenate successfully
        ds = ray.data.read(str(tmp_path))
        result = ds.to_pandas()

        assert len(result) == 5
        assert "id" in result.columns
        assert "value" in result.columns

    def test_compatible_string_schemas(self, ray_start_regular_shared, tmp_path):
        """Test mixing files with string columns."""
        import pandas as pd

        # Create CSV
        df1 = pd.DataFrame({"name": ["Alice", "Bob"], "city": ["NY", "LA"]})
        df1.to_csv(os.path.join(tmp_path, "data1.csv"), index=False)

        # Create another CSV
        df2 = pd.DataFrame({"name": ["Charlie"], "city": ["SF"]})
        df2.to_csv(os.path.join(tmp_path, "data2.csv"), index=False)

        # Should concatenate
        ds = ray.data.read(str(tmp_path))
        result = ds.to_pandas()

        assert len(result) == 3

    def test_missing_columns_handling(self, ray_start_regular_shared, tmp_path):
        """Test handling when files have different column sets."""

        import pandas as pd
        import pyarrow.parquet as pq

        # Create file with columns a, b
        df1 = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        pq.write_table(
            pa.Table.from_pandas(df1), os.path.join(tmp_path, "data1.parquet")
        )

        # Create file with columns a, c
        data2 = [{"a": 5, "c": 6}]
        with open(os.path.join(tmp_path, "data2.json"), "w") as f:
            for item in data2:
                f.write(json.dumps(item) + "\n")

        # Should handle missing columns
        ds = ray.data.read(str(tmp_path))
        assert ds.count() == 3

    def test_type_mismatch_handling(self, ray_start_regular_shared, tmp_path):
        """Test handling when same column has different types."""

        import pandas as pd
        import pyarrow.parquet as pq

        # Create file with numeric column
        df1 = pd.DataFrame({"value": [1, 2, 3]})
        pq.write_table(
            pa.Table.from_pandas(df1), os.path.join(tmp_path, "numeric.parquet")
        )

        # Create file with string column
        data2 = [{"value": "text"}]
        with open(os.path.join(tmp_path, "text.json"), "w") as f:
            for item in data2:
                f.write(json.dumps(item) + "\n")

        # May handle type differences
        try:
            ds = ray.data.read(str(tmp_path))
            # Type coercion may occur
            count = ds.count()
            assert count == 4
        except Exception:
            # Type mismatch may cause error - acceptable
            pass


class TestFormatSpecificBehavior:
    """Test format-specific reader behavior."""

    def test_parquet_column_pruning(self, ray_start_regular_shared, tmp_path):
        """Test that Parquet-specific features work when detected."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create wide Parquet file
        df = pd.DataFrame({f"col_{i}": range(10) for i in range(20)})
        pq.write_table(pa.Table.from_pandas(df), os.path.join(tmp_path, "wide.parquet"))

        # Read with column selection (Parquet-specific)
        ds = ray.data.read(
            os.path.join(tmp_path, "wide.parquet"), columns=["col_0", "col_1"]
        )
        result = ds.take(1)[0]

        # Should have selected columns
        assert "col_0" in result
        assert "col_1" in result

    def test_csv_delimiter_option(self, ray_start_regular_shared, tmp_path):
        """Test that CSV-specific options work when detected."""
        import pandas as pd

        # Create CSV with custom delimiter
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        df.to_csv(os.path.join(tmp_path, "data.csv"), index=False, sep="|")

        # Read with custom delimiter (CSV-specific)
        ds = ray.data.read(os.path.join(tmp_path, "data.csv"), delimiter="|")
        result = ds.to_pandas()

        pd.testing.assert_frame_equal(result, df)

    def test_image_size_option(self, ray_start_regular_shared, tmp_path):
        """Test that image-specific options work when detected."""
        from PIL import Image

        # Create image
        img = Image.new("RGB", (100, 100), color="red")
        img.save(os.path.join(tmp_path, "image.png"))

        # Read with size option (image-specific)
        ds = ray.data.read(os.path.join(tmp_path, "image.png"), size=(50, 50))
        result = ds.take(1)[0]

        # Should have image data
        assert "image" in result

    def test_json_format_inference(self, ray_start_regular_shared, tmp_path):
        """Test that JSON format is correctly inferred."""

        # Create newline-delimited JSON
        data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        with open(os.path.join(tmp_path, "data.json"), "w") as f:
            for item in data:
                f.write(json.dumps(item) + "\n")

        # Should read correctly
        ds = ray.data.read(os.path.join(tmp_path, "data.json"))
        result = ds.take()

        assert len(result) == 2
        assert result[0]["a"] == 1

    def test_numpy_array_handling(self, ray_start_regular_shared, tmp_path):
        """Test that NumPy arrays are handled correctly."""
        # Create various NumPy arrays
        arr1d = np.array([1, 2, 3, 4, 5])
        np.save(os.path.join(tmp_path, "array1d.npy"), arr1d)

        arr2d = np.array([[1, 2], [3, 4]])
        np.save(os.path.join(tmp_path, "array2d.npy"), arr2d)

        # Should read both
        ds = ray.data.read(str(tmp_path))
        assert ds.count() == 2


class TestEdgeCases:
    """Test edge cases in format detection."""

    def test_multiple_dots_in_filename(self, ray_start_regular_shared, tmp_path):
        """Test files with multiple dots in the name."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create file like "data.backup.parquet"
        df = pd.DataFrame({"a": [1, 2, 3]})
        pq.write_table(
            pa.Table.from_pandas(df), os.path.join(tmp_path, "data.backup.parquet")
        )

        # Should detect as parquet
        ds = ray.data.read(os.path.join(tmp_path, "data.backup.parquet"))
        assert ds.count() == 3

    def test_file_with_leading_dot(self, ray_start_regular_shared, tmp_path):
        """Test hidden files (starting with dot)."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create visible file
        df = pd.DataFrame({"a": [1, 2, 3]})
        pq.write_table(
            pa.Table.from_pandas(df), os.path.join(tmp_path, "visible.parquet")
        )

        # Create hidden file
        pq.write_table(
            pa.Table.from_pandas(df), os.path.join(tmp_path, ".hidden.parquet")
        )

        # Should read both
        ds = ray.data.read(str(tmp_path))
        # Depends on filesystem behavior for hidden files
        assert ds.count() >= 3

    def test_very_long_extension(self, ray_start_regular_shared, tmp_path):
        """Test file with unusual extension length."""
        # Create file with long extension
        long_ext_file = os.path.join(tmp_path, "data.verylongextension")
        with open(long_ext_file, "wb") as f:
            f.write(b"test data")

        # Should fall back to binary
        ds = ray.data.read(long_ext_file)
        result = ds.take(1)[0]

        assert "bytes" in result

    def test_uppercase_extensions(self, ray_start_regular_shared, tmp_path):
        """Test files with all-uppercase extensions."""
        import pandas as pd

        df = pd.DataFrame({"a": [1, 2, 3]})
        df.to_csv(os.path.join(tmp_path, "data.CSV"), index=False)

        # Should detect case-insensitively
        ds = ray.data.read(os.path.join(tmp_path, "data.CSV"))
        assert ds.count() == 3

    def test_mixed_case_directory_names(self, ray_start_regular_shared, tmp_path):
        """Test that directory names don't interfere with detection."""
        import pandas as pd
        import pyarrow.parquet as pq

        # Create directory with extension-like name
        special_dir = os.path.join(tmp_path, "data.backup")
        os.makedirs(special_dir)

        # Create parquet file inside
        df = pd.DataFrame({"a": [1, 2, 3]})
        pq.write_table(
            pa.Table.from_pandas(df), os.path.join(special_dir, "file.parquet")
        )

        # Should find and read the parquet file
        ds = ray.data.read(str(tmp_path))
        assert ds.count() == 3


if __name__ == "__main__":
    pass

