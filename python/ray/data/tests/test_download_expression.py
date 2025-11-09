import io

import pandas as pd
import pyarrow as pa
import pytest
from PIL import Image

import ray
from ray.data.expressions import DownloadExpr, col, download


class TestDownloadExpressionStructure:
    """Test DownloadExpr structural equality and basic properties."""

    def test_download_expression_creation(self):
        """Test that download() creates a DownloadExpr with correct properties."""
        expr = download("uri_column")

        assert isinstance(expr, DownloadExpr)
        assert expr.uri_column_name == "uri_column"

    def test_download_expression_structural_equality(self):
        """Test structural equality comparison for download expressions."""
        # Same expressions should be equal
        expr1 = download("uri")
        expr2 = download("uri")
        assert expr1.structurally_equals(expr2)
        assert expr2.structurally_equals(expr1)

        # Different URI column names should not be equal
        expr3 = download("different_uri")
        assert not expr1.structurally_equals(expr3)
        assert not expr3.structurally_equals(expr1)

        # Compare with non-DownloadExpr
        non_download_expr = col("uri")
        assert not expr1.structurally_equals(non_download_expr)
        assert not non_download_expr.structurally_equals(expr1)


class TestDownloadExpressionFunctionality:
    """Test actual download functionality with real and mocked data."""

    def test_download_expression_with_local_files(self, tmp_path):
        """Test basic download expression functionality with local files."""
        # Create sample files with different content types
        sample_data = [
            b"This is test file 1 content",
            b"Different content for file 2",
            b"File 3 has some binary data: \x00\x01\x02\x03",
        ]

        file_paths = []
        for i, data in enumerate(sample_data):
            file_path = tmp_path / f"test_file_{i}.txt"
            file_path.write_bytes(data)
            file_paths.append(str(file_path))

        # Create dataset with file URIs and metadata
        table = pa.Table.from_arrays(
            [
                pa.array([f"local://{path}" for path in file_paths]),
                pa.array([f"id_{i}" for i in range(len(file_paths))]),
                pa.array([f"metadata_{i}" for i in range(len(file_paths))]),
                pa.array(range(len(file_paths))),
            ],
            names=["file_uri", "file_id", "metadata", "index"],
        )

        ds = ray.data.from_arrow(table)

        # Add download column using expression
        ds_with_downloads = ds.with_column("file_bytes", download("file_uri"))

        # Verify results
        results = ds_with_downloads.take_all()
        assert len(results) == len(sample_data)

        for i, result in enumerate(results):
            # Download column should be added correctly
            assert "file_bytes" in result
            assert result["file_bytes"] == sample_data[i]

            # All original columns should be preserved
            assert result["file_id"] == f"id_{i}"
            assert result["metadata"] == f"metadata_{i}"
            assert result["index"] == i
            assert result["file_uri"] == f"local://{file_paths[i]}"

    def test_download_expression_empty_dataset(self):
        """Test download expression with empty dataset."""
        # Create empty dataset with correct schema
        table = pa.Table.from_arrays(
            [
                pa.array([], type=pa.string()),
            ],
            names=["uri"],
        )

        ds = ray.data.from_arrow(table)
        ds_with_downloads = ds.with_column("bytes", download("uri"))

        results = ds_with_downloads.take_all()
        assert len(results) == 0

    def test_download_expression_with_different_file_types(self, tmp_path):
        """Test download expression with various file types including actual images."""
        # Create a small 8x8 RGB image
        small_image = Image.new("RGB", (8, 8), color=(255, 0, 0))  # Red 8x8 image
        image_buffer = io.BytesIO()
        small_image.save(image_buffer, format="PNG")
        image_bytes = image_buffer.getvalue()

        # Create files with different types of content
        test_files = [
            ("text_file.txt", b"Simple text content"),
            ("binary_file.dat", b"\x00\x01\x02\x03\x04\x05"),
            ("json_file.json", b'{"key": "value", "number": 123}'),
            ("small_image.png", image_bytes),  # Actual PNG image (primary use case)
            ("empty_file.txt", b""),  # Empty file edge case
        ]

        file_paths = []
        expected_data = []
        for filename, content in test_files:
            file_path = tmp_path / filename
            file_path.write_bytes(content)
            file_paths.append(str(file_path))
            expected_data.append(content)

        # Create dataset
        table = pa.Table.from_arrays(
            [
                pa.array([f"local://{path}" for path in file_paths]),
                pa.array(
                    [f.split(".")[0] for f, _ in test_files]
                ),  # filename without extension
            ],
            names=["file_uri", "file_type"],
        )

        ds = ray.data.from_arrow(table)
        ds_with_downloads = ds.with_column("content", download("file_uri"))

        results = ds_with_downloads.take_all()
        assert len(results) == len(test_files)

        for i, result in enumerate(results):
            assert result["content"] == expected_data[i]
            assert result["file_type"] == test_files[i][0].split(".")[0]

            # Special verification for image file - ensure it can be loaded as an image
            if test_files[i][0].endswith(".png"):
                downloaded_image = Image.open(io.BytesIO(result["content"]))
                assert downloaded_image.size == (8, 8)
                assert downloaded_image.mode == "RGB"

    def test_chained_download_expressions(self, tmp_path):
        """Test chained download expressions functionality."""
        # Create sample files with different content
        sample_data = [
            b"Content for file 1",
            b"Content for file 2",
            b"Content for file 3",
        ]

        file_paths = []
        for i, data in enumerate(sample_data):
            file_path = tmp_path / f"test_file_{i}.txt"
            file_path.write_bytes(data)
            file_paths.append(str(file_path))

        # Create dataset with file URIs
        table = pa.Table.from_arrays(
            [
                pa.array([f"local://{path}" for path in file_paths]),
                pa.array([f"id_{i}" for i in range(len(file_paths))]),
            ],
            names=["file_uri", "file_id"],
        )

        ds = ray.data.from_arrow(table)

        # Chain multiple download expressions from the same URI column
        ds_with_chained_downloads = (
            ds.with_column("file_bytes_1", download("file_uri"))
            .with_column("file_bytes_2", download("file_uri"))
            .with_column("file_bytes_3", download("file_uri"))
        )

        # Verify results
        results = ds_with_chained_downloads.take_all()
        assert len(results) == len(sample_data)

        for i, result in enumerate(results):
            # All download columns should have the same content
            assert "file_bytes_1" in result
            assert "file_bytes_2" in result
            assert "file_bytes_3" in result
            assert result["file_bytes_1"] == sample_data[i]
            assert result["file_bytes_2"] == sample_data[i]
            assert result["file_bytes_3"] == sample_data[i]

            # Original columns should be preserved
            assert result["file_id"] == f"id_{i}"
            assert result["file_uri"] == f"local://{file_paths[i]}"

    def test_download_expression_with_pandas_blocks(self, tmp_path):
        """Test download with pandas blocks to ensure arrow conversion works.

        This tests the code path in PartitionActor.__call__ where non-arrow
        blocks are converted to arrow format before processing.
        """
        ctx = ray.data.context.DataContext.get_current()
        old_enable_pandas_block = ctx.enable_pandas_block
        ctx.enable_pandas_block = True
        try:
            # Create test files
            sample_data = [
                b"Pandas block test content 1",
                b"Pandas block test content 2",
            ]

            file_paths = []
            for i, data in enumerate(sample_data):
                file_path = tmp_path / f"pandas_test_{i}.txt"
                file_path.write_bytes(data)
                file_paths.append(str(file_path))

            # Create dataset with pandas blocks (not arrow)
            df = pd.DataFrame(
                {
                    "file_uri": [f"local://{path}" for path in file_paths],
                    "file_id": [f"id_{i}" for i in range(len(file_paths))],
                }
            )
            ds = ray.data.from_pandas(df)

            # Apply download - this should trigger arrow conversion in PartitionActor
            ds_with_downloads = ds.with_column("content", download("file_uri"))

            # Verify results
            results = ds_with_downloads.take_all()
            assert len(results) == len(sample_data)

            for i, result in enumerate(results):
                assert result["content"] == sample_data[i]
                assert result["file_id"] == f"id_{i}"
                assert result["file_uri"] == f"local://{file_paths[i]}"
        finally:
            ctx.enable_pandas_block = old_enable_pandas_block


class TestDownloadExpressionErrors:
    """Test error conditions and edge cases for download expressions."""

    def test_download_expression_invalid_uri_column(self):
        """Test download expression with non-existent URI column."""
        table = pa.Table.from_arrays(
            [
                pa.array(["local://test.txt"]),
            ],
            names=["existing_column"],
        )

        ds = ray.data.from_arrow(table)
        ds_with_downloads = ds.with_column("bytes", download("non_existent_column"))

        # Should raise error when trying to execute
        with pytest.raises(ValueError):
            ds_with_downloads.take_all()

    def test_download_expression_with_null_uris(self):
        """Test download expression handling of null/empty URIs."""
        table = pa.Table.from_arrays(
            [
                pa.array(["local://test.txt", None, ""]),
            ],
            names=["uri"],
        )

        ds = ray.data.from_arrow(table)
        ds_with_downloads = ds.with_column("bytes", download("uri"))

        # Should handle nulls gracefully (exact behavior may vary)
        # This test mainly ensures no crash occurs
        try:
            results = ds_with_downloads.take_all()
            # If it succeeds, verify structure is reasonable
            assert len(results) == 3
            for result in results:
                assert "bytes" in result
        except Exception as e:
            # If it fails, should be a reasonable error (not a crash)
            assert isinstance(e, (ValueError, KeyError, RuntimeError))


class TestDownloadExpressionIntegration:
    """Integration tests combining download expressions with other Ray Data operations."""

    def test_download_expression_with_map_batches(self, tmpdir):
        """Test download expression followed by map_batches processing."""
        # Create a test file
        test_file = tmpdir.join("test.txt")
        test_content = b"Hello, World!"
        test_file.write_binary(test_content)

        # Create dataset
        table = pa.Table.from_arrays(
            [
                pa.array([f"local://{test_file}"]),
            ],
            names=["uri"],
        )

        ds = ray.data.from_arrow(table)

        # Download then process
        ds_with_content = ds.with_column("raw_bytes", download("uri"))

        def decode_bytes(batch):
            # Access the specific column containing the bytes data
            batch["decoded_text"] = [
                data.decode("utf-8") for data in batch["raw_bytes"]
            ]
            return batch

        ds_decoded = ds_with_content.map_batches(decode_bytes)
        results = ds_decoded.take_all()

        assert len(results) == 1
        assert results[0]["decoded_text"] == "Hello, World!"
        assert results[0]["raw_bytes"] == test_content


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
