import io
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow as pa
import pytest
from PIL import Image

import ray
from ray.data._internal.planner.plan_download_op import PartitionActor
from ray.data._internal.util import KiB, MiB
from ray.data.expressions import DownloadExpr, col, download

_TARGET = 128 * MiB


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

    def test_download_expression_with_custom_filesystem(self, tmp_path):
        import pyarrow.fs as pafs

        # 1. Setup paths
        subdir = tmp_path / "data"
        subdir.mkdir()

        file_name = "test_file.txt"
        file_path = subdir / file_name
        sample_content = b"File content with custom fs"
        file_path.write_bytes(sample_content)

        # 2. Setup SubTreeFileSystem
        # This treats 'subdir' as the root '/'
        base_fs = pafs.LocalFileSystem()
        custom_fs = pafs.SubTreeFileSystem(str(subdir), base_fs)

        # 3. Create Dataset
        # Note: We use the relative 'file_name' because the FS is rooted at 'subdir'
        ds = ray.data.from_items([{"file_uri": file_name, "file_id": 0}])

        # 4. Execute Download
        ds_with_downloads = ds.with_column(
            "content", download("file_uri", filesystem=custom_fs)
        )

        # 5. Assertions
        results = ds_with_downloads.take_all()

        assert len(results) == 1
        assert results[0]["content"] == sample_content
        assert results[0]["file_id"] == 0


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

    def test_download_expression_with_malformed_uris(self, tmp_path):
        """Test download expression with malformed URIs.

        This tests that various malformed URIs are caught and return None
        instead of crashing.

        All of the URIs should be malformed in order to test the ZeroDivisionError
        described in https://github.com/ray-project/ray/issues/58462.
        """
        malformed_uris = [
            f"local://{tmp_path}/nonexistent.txt",  # File doesn't exist
            "local:///this/path/does/not/exist/file.txt",  # Invalid path
            "",  # Empty URI
            "foobar",  # Random string
            # TODO(xyuzh): Currently, using the below URIs raises an exception
            # in _resolve_paths_and_filesystem. We need to fix that issue and
            # add the tests in.
            # "file:///\x00/null/byte",  # Null byte
            # "http://host/path\n\r",  # Line breaks
            # "foo://bar",  # Invalid scheme
            # "://no-scheme",  # Missing scheme
            # "http://host/path?query=<script>",  # Injection attempts
        ]

        ds = ray.data.from_items([{"uri": uri} for uri in malformed_uris])
        ds_with_downloads = ds.with_column("bytes", download("uri"))
        results = ds_with_downloads.take_all()

        # All malformed URIs should return None
        assert len(results) == len(malformed_uris)
        for result in results:
            assert result["bytes"] is None

    def test_download_expression_mixed_valid_and_invalid_uris(self, tmp_path):
        """Test download expression when some but not all of the URIs are invalid."""
        # Create one valid file
        valid_file = tmp_path / "valid.txt"
        valid_file.write_bytes(b"valid content")

        # Create URIs: one valid and one non-existent file.
        ds = ray.data.from_items(
            [
                {"uri": str(valid_file), "id": 0},
                {"uri": str(tmp_path / "nonexistent.txt"), "id": 1},
            ]
        )
        ds_with_downloads = ds.with_column("bytes", download("uri"))

        # Should not crash - failed downloads return None
        results = sorted(ds_with_downloads.take_all(), key=lambda row: row["id"])
        assert len(results) == 2

        # First URI should succeed
        assert results[0]["bytes"] == b"valid content"

        # Second URI should fail gracefully (return None)
        assert results[1]["bytes"] is None


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


def _make_partition_actor(uri_columns=None):
    """Helper to create a PartitionActor with a mock DataContext."""
    ctx = MagicMock()
    ctx.target_max_block_size = _TARGET
    ctx.retried_io_errors = ()
    return PartitionActor(uri_columns or ["uri"], ctx)


# Simulated block row count used when calling _estimate_nrows directly
# (no real block involved). Large enough that it's never the binding
# constraint — the estimator decides.
_BLOCK_ROWS = 10_000


def _feed_sizes(actor, sizes):
    """Feed file-size observations directly into the actor's estimator."""
    actor._estimator.update_batch(sizes)
    actor._blocks_seen = max(actor._blocks_seen, 1)


class TestPartitionActorUnevenDistributions:
    """Test that PartitionActor produces safe partition sizes under
    various uneven file-size distributions.
    """

    @pytest.mark.parametrize(
        "description, sizes, max_expected_nrows",
        [
            # All large files → very few rows per partition.
            (
                "uniform-large-50MB",
                [50 * MiB] * 30,
                3,
            ),
            # All small files → many rows per partition (up to block size).
            (
                "uniform-small-1KiB",
                [1 * KiB] * 30,
                _BLOCK_ROWS,
            ),
            # Small files first, then large → should adapt to large files.
            (
                "shift-small-to-large",
                [1 * KiB] * 20 + [50 * MiB] * 20,
                10,
            ),
            # Large files first, then small → should loosen to more rows.
            (
                "shift-large-to-small",
                [50 * MiB] * 20 + [1 * KiB] * 20,
                _BLOCK_ROWS,
            ),
            # Bimodal: mix of 1KB and 100MB → conservative (few rows).
            (
                "bimodal-1KB-and-100MB",
                [1 * KiB] * 15 + [100 * MiB] * 15,
                20,
            ),
            # Long tail: mostly small, a few very large outliers.
            (
                "long-tail-mostly-small",
                [10 * KiB] * 25 + [500 * MiB] * 5,
                50,
            ),
        ],
        ids=lambda x: x if isinstance(x, str) else "",
    )
    def test_nrows_bounded(self, description, sizes, max_expected_nrows):
        """Partition size stays within a safe upper bound for the
        given distribution.
        """
        actor = _make_partition_actor()
        _feed_sizes(actor, sizes)
        nrows = actor._estimate_nrows(_BLOCK_ROWS)

        assert PartitionActor._MIN_ROWS <= nrows <= max_expected_nrows, (
            f"[{description}] nrows={nrows}, "
            f"expected [{PartitionActor._MIN_ROWS}, {max_expected_nrows}]"
        )

    def test_no_observations_returns_block_size(self):
        """Before any file sizes are observed, use the block's row count."""
        actor = _make_partition_actor()
        assert actor._estimate_nrows(500) == 500
        assert actor._estimate_nrows(10_000) == 10_000

    @pytest.mark.parametrize(
        "description, phase1, phase2",
        [
            (
                "small-to-large",
                [1 * KiB] * 25,
                [50 * MiB] * 25,
            ),
            (
                "large-to-small",
                [50 * MiB] * 25,
                [1 * KiB] * 25,
            ),
        ],
        ids=["small-to-large", "large-to-small"],
    )
    def test_adapts_across_phases(self, description, phase1, phase2):
        """Partition size changes direction after a distribution shift."""
        actor = _make_partition_actor()

        _feed_sizes(actor, phase1)
        nrows_phase1 = actor._estimate_nrows(_BLOCK_ROWS)

        _feed_sizes(actor, phase2)
        nrows_phase2 = actor._estimate_nrows(_BLOCK_ROWS)

        if phase1[0] < phase2[0]:
            # Shifted to larger files → partitions should shrink.
            assert (
                nrows_phase2 < nrows_phase1
            ), f"[{description}] Expected shrink: {nrows_phase1} -> {nrows_phase2}"
        else:
            # Shifted to smaller files → partitions should grow.
            assert (
                nrows_phase2 > nrows_phase1
            ), f"[{description}] Expected growth: {nrows_phase1} -> {nrows_phase2}"

    def test_sample_and_update_with_mock(self):
        """_sample_and_update feeds sampled sizes into the estimator."""
        actor = _make_partition_actor()
        block = pa.table({"uri": [f"file_{i}.bin" for i in range(20)]})

        mock_sizes = [10 * MiB] * 20
        with patch.object(actor, "_sample_sizes", return_value=mock_sizes):
            actor._sample_and_update(block)

        assert actor._estimator.has_observations
        nrows = actor._estimate_nrows(_BLOCK_ROWS)
        # 128MB target / ~10MB per file → ~12 rows
        assert 5 <= nrows <= 20, f"Expected ~12 rows, got {nrows}"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
