import io
import os
import tempfile

import pyarrow as pa
import pytest
from PIL import Image

import ray
from ray.data._internal.compute import ActorPoolStrategy, TaskPoolStrategy
from ray.data.expressions import MultiStageUDFExpr, UrlExpr, col


class TestUrlDownloadExpressionStructure:
    """Test URL download expression structure and properties."""

    def test_url_expr_creation(self):
        """Test that .url property creates UrlExpr correctly."""
        col_expr = col("uri")
        url_expr = col_expr.url

        assert isinstance(url_expr, UrlExpr)
        assert url_expr.base == col_expr

    def test_url_download_creates_multi_stage_expr(self):
        """Test that .url.download() creates a MultiStageUDFExpr."""
        expr = col("uri").url.download()

        assert isinstance(expr, MultiStageUDFExpr)
        assert len(expr.stages) == 2

    def test_url_download_stages_have_correct_compute_strategies(self):
        """Test that download expression stages use correct compute strategies."""
        expr = col("uri").url.download()

        # Stage 1: Partitioning should use ActorPoolStrategy
        assert isinstance(expr.stages[0].compute_strategy, ActorPoolStrategy)
        assert expr.stages[0].compute_strategy.min_size == 1
        assert expr.stages[0].compute_strategy.max_size == 1

        # Stage 2: Downloading should use TaskPoolStrategy
        assert isinstance(expr.stages[1].compute_strategy, TaskPoolStrategy)

    def test_url_download_structural_equality(self):
        """Test structural equality for URL download expressions."""
        expr1 = col("uri").url.download()
        expr2 = col("uri").url.download()
        expr3 = col("different_uri").url.download()

        # Same column should be equal
        assert expr1.structurally_equals(expr2)
        assert expr2.structurally_equals(expr1)

        # Different columns should not be equal
        assert not expr1.structurally_equals(expr3)
        assert not expr3.structurally_equals(expr1)


class TestUrlDownloadExpressionFunctionality:
    """Test URL download expression functionality with real files."""

    def test_url_download_with_local_files(self, tmp_path):
        """Test basic URL download expression functionality with local files."""
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
                pa.array([f"file://{path}" for path in file_paths]),
                pa.array([f"id_{i}" for i in range(len(file_paths))]),
                pa.array([f"metadata_{i}" for i in range(len(file_paths))]),
                pa.array(range(len(file_paths))),
            ],
            names=["file_uri", "file_id", "metadata", "index"],
        )

        ds = ray.data.from_arrow(table)

        # Add download column using new URL expression API
        ds_with_downloads = ds.with_column("file_bytes", col("file_uri").url.download())

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
            assert result["file_uri"] == f"file://{file_paths[i]}"

    def test_url_download_empty_dataset(self):
        """Test URL download expression with empty dataset."""
        # Create empty dataset with correct schema
        table = pa.Table.from_arrays(
            [
                pa.array([], type=pa.string()),
            ],
            names=["uri"],
        )

        ds = ray.data.from_arrow(table)
        ds_with_downloads = ds.with_column("bytes", col("uri").url.download())

        results = ds_with_downloads.take_all()
        assert len(results) == 0

    def test_url_download_with_different_file_types(self, tmp_path):
        """Test URL download expression with various file types including actual images."""
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
                pa.array([f"file://{path}" for path in file_paths]),
                pa.array(
                    [f.split(".")[0] for f, _ in test_files]
                ),  # filename without extension
            ],
            names=["file_uri", "file_type"],
        )

        ds = ray.data.from_arrow(table)
        ds_with_downloads = ds.with_column("content", col("file_uri").url.download())

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

    def test_url_download_with_different_file_types_comprehensive(self):
        """Test URL download with various file formats using the new API."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create various file types
            test_files = [
                ("text.txt", b"Plain text content"),
                ("binary.dat", b"\x00\x01\x02\x03\x04\x05"),
                ("json.json", b'{"test": true, "value": 42}'),
            ]

            file_uris = []
            expected_contents = []

            for filename, content in test_files:
                file_path = os.path.join(tmpdir, filename)
                with open(file_path, "wb") as f:
                    f.write(content)
                file_uris.append(f"file://{file_path}")
                expected_contents.append(content)

            # Create dataset
            ds = ray.data.from_items(
                [
                    {"uri": uri, "filename": filename}
                    for uri, (filename, _) in zip(file_uris, test_files)
                ]
            )

            # Download using URL expression
            result_ds = ds.with_column("content", col("uri").url.download())
            results = result_ds.take()

            assert len(results) == len(test_files)

            for i, result in enumerate(results):
                assert result["content"] == expected_contents[i]
                assert result["filename"] == test_files[i][0]


class TestUrlDownloadExpressionErrors:
    """Test error conditions and edge cases for URL download expressions."""

    def test_url_download_invalid_uri_column(self):
        """Test URL download expression with non-existent URI column."""
        table = pa.Table.from_arrays(
            [
                pa.array(["file://test.txt"]),
            ],
            names=["existing_column"],
        )

        ds = ray.data.from_arrow(table)
        ds_with_downloads = ds.with_column(
            "bytes", col("non_existent_column").url.download()
        )

        # Should raise error when trying to execute
        with pytest.raises(Exception):  # Could be KeyError or similar
            ds_with_downloads.take_all()

    def test_url_download_with_null_uris(self):
        """Test URL download expression handling of null/empty URIs."""
        table = pa.Table.from_arrays(
            [
                pa.array(["file://test.txt", None, ""]),
            ],
            names=["uri"],
        )

        ds = ray.data.from_arrow(table)
        ds_with_downloads = ds.with_column("bytes", col("uri").url.download())

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

    def test_url_download_with_missing_files(self):
        """Test URL download with non-existent files."""
        # Create dataset with non-existent file paths
        ds = ray.data.from_items(
            [
                {"uri": "file:///non/existent/file1.txt"},
                {"uri": "file:///another/missing/file2.txt"},
            ]
        )

        result_ds = ds.with_column("content", col("uri").url.download())

        # This should raise an error when executed
        with pytest.raises(Exception):
            result_ds.take()


class TestUrlDownloadExpressionIntegration:
    """Integration tests combining URL download expressions with other Ray Data operations."""

    def test_url_download_with_map_batches(self, tmpdir):
        """Test URL download expression followed by map_batches processing."""
        # Create a test file
        test_file = tmpdir.join("test.txt")
        test_content = b"Hello, World!"
        test_file.write_binary(test_content)

        # Create dataset
        table = pa.Table.from_arrays(
            [
                pa.array([f"file://{test_file}"]),
            ],
            names=["uri"],
        )

        ds = ray.data.from_arrow(table)

        # Download then process using new API
        ds_with_content = ds.with_column("raw_bytes", col("uri").url.download())

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

    def test_url_download_integration_with_additional_processing(self):
        """Test URL download followed by additional processing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a test file
            file_path = os.path.join(tmpdir, "test.txt")
            test_content = b"Hello, World!"
            with open(file_path, "wb") as f:
                f.write(test_content)

            # Create dataset
            ds = ray.data.from_items([{"uri": f"file://{file_path}"}])

            # Download and then process
            ds_with_content = ds.with_column("raw_bytes", col("uri").url.download())

            def process_bytes(batch):
                # Decode bytes to text
                batch["text"] = [data.decode("utf-8") for data in batch["raw_bytes"]]
                batch["length"] = [len(data) for data in batch["raw_bytes"]]
                return batch

            ds_processed = ds_with_content.map_batches(process_bytes)
            results = ds_processed.take()

            assert len(results) == 1
            assert results[0]["text"] == "Hello, World!"
            assert results[0]["length"] == len(test_content)
            assert results[0]["raw_bytes"] == test_content

    def test_url_download_multi_stage_pipeline_verification(self):
        """Test that URL download creates the expected multi-stage pipeline."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test files with content
            test_files = []
            file_contents = [b"Hello, World!", b"This is file 2", b"File number three"]

            for i, content in enumerate(file_contents):
                file_path = os.path.join(tmpdir, f"test_file_{i}.txt")
                with open(file_path, "wb") as f:
                    f.write(content)
                test_files.append(f"file://{file_path}")

            # Create dataset with file URIs
            ds = ray.data.from_items(
                [
                    {"uri": test_files[0], "id": 1},
                    {"uri": test_files[1], "id": 2},
                    {"uri": test_files[2], "id": 3},
                ]
            )

            # Test the download expression
            result_ds = ds.with_column("downloaded_bytes", col("uri").url.download())

            # Verify the results
            results = result_ds.take()

            # Check that we got the right number of results
            assert len(results) == 3

            # Check that each result has the expected structure
            for i, row in enumerate(results):
                assert "uri" in row
                assert "id" in row
                assert "downloaded_bytes" in row
                assert row["id"] == i + 1
                assert row["downloaded_bytes"] == file_contents[i]
                assert row["uri"] == test_files[i]

    @pytest.mark.parametrize("ray_init_local", [True, False])
    def test_url_download_with_different_ray_contexts(self, ray_init_local):
        """Test URL download works with different Ray initialization modes."""
        # Skip if Ray is already initialized (from other tests)
        if ray.is_initialized():
            ray.shutdown()

        if ray_init_local:
            ray.init(local_mode=True)
        else:
            ray.init()

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                # Create a test file
                file_path = os.path.join(tmpdir, "test.txt")
                test_content = b"Test content"
                with open(file_path, "wb") as f:
                    f.write(test_content)

                # Create dataset and test download
                ds = ray.data.from_items([{"uri": f"file://{file_path}"}])
                result_ds = ds.with_column("content", col("uri").url.download())
                results = result_ds.take()

                assert len(results) == 1
                assert results[0]["content"] == test_content
        finally:
            ray.shutdown()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
