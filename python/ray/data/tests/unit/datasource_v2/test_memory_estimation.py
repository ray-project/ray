"""Unit tests for memory estimation features in Parquet Datasource V2.

Tests cover:
- FileManifest with estimated in-memory sizes
- BlockMetadata with estimated sizes
- MapOperator's memory estimation logic
"""

import numpy as np
import pyarrow as pa
import pytest

import ray
from ray.data._internal.datasource_v2.listing.file_manifest import (
    ESTIMATED_IN_MEMORY_SIZE_COLUMN_NAME,
    FILE_SIZE_COLUMN_NAME,
    PATH_COLUMN_NAME,
    FileManifest,
)
from ray.data._internal.datasource_v2.readers.read_files_task_memory import (
    MAP_TASK_KWARG_MERGE_READ_TASK_MEMORY,
    READ_FILES_TASK_MEMORY_EPS_BYTES,
    enrich_file_manifest_block_metadata_if_applicable,
    estimate_read_files_task_memory_bytes,
)
from ray.data.block import BlockAccessor, BlockMetadata, BlockMetadataWithSchema


def _minimal_map_transformer():
    """Minimal MapTransformer for constructing map operators in unit tests."""
    from ray.data._internal.execution.operators.map_transformer import (
        BlockMapTransformFn,
        MapTransformer,
    )

    def _noop(blocks, ctx):
        return blocks

    return MapTransformer([BlockMapTransformFn(_noop, disable_block_shaping=True)])


class TestFileManifestWithEstimatedSizes:
    """Test FileManifest with estimated in-memory sizes."""

    def test_construct_manifest_with_explicit_estimates(self):
        """Test creating manifest with explicit estimated sizes."""
        paths = ["/path/to/file1.parquet", "/path/to/file2.parquet"]
        sizes = [1000, 2000]
        estimated_sizes = [5000, 10000]

        manifest = FileManifest.construct_manifest(
            paths=paths, sizes=sizes, estimated_in_memory_sizes=estimated_sizes
        )

        assert len(manifest) == 2
        np.testing.assert_array_equal(manifest.paths, np.array(paths, dtype=object))
        np.testing.assert_array_equal(manifest.file_sizes, np.array(sizes))
        np.testing.assert_array_equal(
            manifest.estimated_in_memory_sizes, np.array(estimated_sizes)
        )

    def test_construct_manifest_without_explicit_estimates(self):
        """Test creating manifest without explicit estimates falls back to 5x ratio."""
        paths = ["/path/to/file1.parquet", "/path/to/file2.parquet"]
        sizes = [1000, 2000]

        manifest = FileManifest.construct_manifest(paths=paths, sizes=sizes)

        assert len(manifest) == 2
        # Should use 5x compression ratio as fallback
        expected_estimates = np.array([5000, 10000])
        np.testing.assert_array_equal(
            manifest.estimated_in_memory_sizes, expected_estimates
        )

    def test_total_estimated_in_memory_size_with_explicit_estimates(self):
        """Test total estimated memory calculation with explicit sizes."""
        manifest = FileManifest.construct_manifest(
            paths=["/file1", "/file2", "/file3"],
            sizes=[1000, 2000, 3000],
            estimated_in_memory_sizes=[5000, 8000, 12000],
        )

        total = manifest.total_estimated_in_memory_size
        assert total == 5000 + 8000 + 12000

    def test_total_estimated_in_memory_size_without_explicit_estimates(self):
        """Test total estimated memory uses 5x ratio when not provided."""
        manifest = FileManifest.construct_manifest(
            paths=["/file1", "/file2"],
            sizes=[1000, 2000],
        )

        # Total should be (1000 * 5) + (2000 * 5) = 15000
        total = manifest.total_estimated_in_memory_size
        assert total == 15000

    def test_manifest_concat_preserves_max_uncompressed_row_group_sizes(self):
        manifest1 = FileManifest.construct_manifest(
            paths=["/f1"],
            sizes=[100],
            max_uncompressed_row_group_sizes=[1_000_000],
        )
        manifest2 = FileManifest.construct_manifest(
            paths=["/f2"],
            sizes=[200],
            max_uncompressed_row_group_sizes=[3_000_000],
        )
        concatenated = FileManifest.concat([manifest1, manifest2])
        np.testing.assert_array_equal(
            concatenated.max_uncompressed_row_group_sizes,
            np.array([1_000_000, 3_000_000]),
        )

    def test_manifest_concat_preserves_estimated_sizes(self):
        """Test concatenating manifests preserves estimated size info."""
        manifest1 = FileManifest.construct_manifest(
            paths=["/file1", "/file2"],
            sizes=[1000, 2000],
            estimated_in_memory_sizes=[4000, 8000],
        )

        manifest2 = FileManifest.construct_manifest(
            paths=["/file3", "/file4"],
            sizes=[3000, 4000],
            estimated_in_memory_sizes=[12000, 16000],
        )

        concatenated = FileManifest.concat([manifest1, manifest2])

        assert len(concatenated) == 4
        expected_paths = np.array(
            ["/file1", "/file2", "/file3", "/file4"], dtype=object
        )
        np.testing.assert_array_equal(concatenated.paths, expected_paths)

        expected_estimates = np.array([4000, 8000, 12000, 16000])
        np.testing.assert_array_equal(
            concatenated.estimated_in_memory_sizes, expected_estimates
        )

    def test_manifest_shuffle_preserves_estimated_sizes(self):
        """Test shuffling manifest preserves estimated size alignment."""
        paths = [f"/file{i}" for i in range(10)]
        sizes = [1000 * (i + 1) for i in range(10)]
        estimates = [5000 * (i + 1) for i in range(10)]

        manifest = FileManifest.construct_manifest(
            paths=paths, sizes=sizes, estimated_in_memory_sizes=estimates
        )

        shuffled = manifest.shuffle(seed=42)

        # Row count should remain same
        assert len(shuffled) == len(manifest)

        # Total estimated size should remain same
        assert (
            shuffled.total_estimated_in_memory_size
            == manifest.total_estimated_in_memory_size
        )

        # Path-size alignment should be maintained
        for i in range(len(shuffled)):
            path = shuffled.paths[i]
            size = shuffled.file_sizes[i]
            estimate = shuffled.estimated_in_memory_sizes[i]

            # Find original index
            orig_idx = paths.index(path)
            assert size == sizes[orig_idx]
            assert estimate == estimates[orig_idx]

    def test_shuffle_with_seed_is_deterministic(self):
        """Test that shuffle with same seed produces same order."""
        manifest = FileManifest.construct_manifest(
            paths=[f"/file{i}" for i in range(5)],
            sizes=[1000 * i for i in range(1, 6)],
            estimated_in_memory_sizes=[5000 * i for i in range(1, 6)],
        )

        shuffled1 = manifest.shuffle(seed=42)
        shuffled2 = manifest.shuffle(seed=42)

        np.testing.assert_array_equal(shuffled1.paths, shuffled2.paths)
        np.testing.assert_array_equal(shuffled1.file_sizes, shuffled2.file_sizes)
        np.testing.assert_array_equal(
            shuffled1.estimated_in_memory_sizes, shuffled2.estimated_in_memory_sizes
        )

    def test_manifest_from_arrow_table_with_estimates(self):
        """Test creating FileManifest from PyArrow table with estimated sizes."""
        table = pa.table(
            {
                PATH_COLUMN_NAME: ["/file1", "/file2", "/file3"],
                FILE_SIZE_COLUMN_NAME: [1000, 2000, 3000],
                ESTIMATED_IN_MEMORY_SIZE_COLUMN_NAME: [6000, 12000, 18000],
            }
        )

        manifest = FileManifest(table)

        assert len(manifest) == 3
        assert manifest.total_estimated_in_memory_size == 6000 + 12000 + 18000

    def test_manifest_from_arrow_table_without_estimates(self):
        """Test creating FileManifest from PyArrow table without estimated sizes."""
        table = pa.table(
            {
                PATH_COLUMN_NAME: ["/file1", "/file2"],
                FILE_SIZE_COLUMN_NAME: [1000, 2000],
            }
        )

        manifest = FileManifest(table)

        assert len(manifest) == 2
        # Should fallback to 5x ratio: (1000 + 2000) * 5 = 15000
        assert manifest.total_estimated_in_memory_size == 15000

    def test_empty_manifest(self):
        """Test empty manifest behavior."""
        manifest = FileManifest.construct_manifest(paths=[], sizes=[])

        assert len(manifest) == 0
        assert manifest.total_estimated_in_memory_size == 0

    def test_single_file_manifest(self):
        """Test manifest with single file."""
        manifest = FileManifest.construct_manifest(
            paths=["/single.parquet"],
            sizes=[5000],
            estimated_in_memory_sizes=[25000],
        )

        assert len(manifest) == 1
        assert manifest.total_estimated_in_memory_size == 25000

    def test_large_estimated_sizes(self):
        """Test handling of large estimated sizes (GB scale)."""
        # 1 GB in bytes
        gb = 1024 * 1024 * 1024
        manifest = FileManifest.construct_manifest(
            paths=["/large_file1.parquet", "/large_file2.parquet"],
            sizes=[gb, 2 * gb],
            estimated_in_memory_sizes=[5 * gb, 10 * gb],
        )

        assert len(manifest) == 2
        assert manifest.total_estimated_in_memory_size == 15 * gb


class TestBlockMetadataEstimatedSizes:
    """Test BlockMetadata with estimated size fields."""

    def test_block_metadata_with_estimated_size_bytes(self):
        """Test BlockMetadata has estimated_size_bytes field."""
        metadata = BlockMetadata(
            num_rows=1000,
            size_bytes=5000,
            # schema=pa.schema([("col", pa.int64())]),
            estimated_size_bytes=25000,
            exec_stats=None,
            task_exec_stats=None,
        )

        assert metadata.estimated_size_bytes == 25000
        assert metadata.size_bytes == 5000

    def test_block_metadata_with_input_files_estimated_bytes(self):
        """Test BlockMetadata has input_files_estimated_bytes field."""
        metadata = BlockMetadata(
            num_rows=1000,
            size_bytes=5000,
            # schema=pa.schema([("col", pa.int64())]),
            input_files_estimated_bytes=25000,
            exec_stats=None,
            task_exec_stats=None,
        )

        assert metadata.input_files_estimated_bytes == 25000
        assert metadata.size_bytes == 5000

    def test_block_metadata_both_estimate_fields(self):
        """Test BlockMetadata with both estimated fields."""
        metadata = BlockMetadata(
            num_rows=1000,
            size_bytes=5000,
            # schema=pa.schema([("col", pa.int64())]),
            estimated_size_bytes=25000,
            input_files_estimated_bytes=20000,
            exec_stats=None,
            task_exec_stats=None,
        )

        assert metadata.estimated_size_bytes == 25000
        assert metadata.input_files_estimated_bytes == 20000
        assert metadata.size_bytes == 5000

    def test_block_metadata_estimated_fields_optional(self):
        """Test BlockMetadata estimated fields default to None."""
        metadata = BlockMetadata(
            num_rows=1000,
            size_bytes=5000,
            # schema=pa.schema([("col", pa.int64())]),
            exec_stats=None,
            task_exec_stats=None,
        )

        assert metadata.estimated_size_bytes is None
        assert metadata.input_files_estimated_bytes is None

    def test_block_metadata_with_schema(self):
        """Test BlockMetadataWithSchema preserves estimates."""
        schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
        metadata = BlockMetadata(
            num_rows=500,
            size_bytes=2000,
            # schema=schema,
            estimated_size_bytes=10000,
            input_files_estimated_bytes=8000,
            exec_stats=None,
            task_exec_stats=None,
        )

        metadata_with_schema = BlockMetadataWithSchema.from_metadata(
            metadata, schema=schema
        )

        assert metadata_with_schema.estimated_size_bytes == 10000
        assert metadata_with_schema.input_files_estimated_bytes == 8000


class TestMapOperatorMemoryEstimation:
    """Test MapOperator's memory estimation logic."""

    def test_estimate_bundle_size_uses_input_files_estimated_bytes(self):
        """Test _estimate_bundle_size prioritizes input_files_estimated_bytes."""
        from ray.data._internal.execution.interfaces import RefBundle
        from ray.data._internal.execution.operators.input_data_buffer import (
            InputDataBuffer,
        )
        from ray.data._internal.execution.operators.task_pool_map_operator import (
            TaskPoolMapOperator,
        )
        from ray.data.context import DataContext

        # Create metadata with input_files_estimated_bytes
        metadata = BlockMetadata(
            num_rows=1000,
            size_bytes=5000,
            # schema=pa.schema([("col", pa.int64())]),
            input_files_estimated_bytes=25000,
            exec_stats=None,
            task_exec_stats=None,
        )

        block_ref = ray.ObjectRef(b"x" * 28)
        bundle = RefBundle(
            blocks=[(block_ref, metadata)],
            schema=None,
            owns_blocks=False,
        )

        map_transformer = _minimal_map_transformer()
        data_context = DataContext.get_current()
        input_op = InputDataBuffer(data_context, input_data=[])

        map_op = TaskPoolMapOperator(
            map_transformer=map_transformer,
            input_op=input_op,
            data_context=data_context,
            name="test_map",
            target_max_block_size_override=None,
            min_rows_per_bundle=None,
            ref_bundler=None,
            max_concurrency=1,
            supports_fusion=True,
            map_task_kwargs=None,
        )

        size = map_op._estimate_bundle_size(bundle)

        # Should use input_files_estimated_bytes (25000) instead of size_bytes (5000)
        assert size == 25000

    def test_estimate_bundle_size_fallback_to_estimated_size_bytes(self):
        """Test _estimate_bundle_size falls back to estimated_size_bytes."""
        from ray.data._internal.execution.interfaces import RefBundle
        from ray.data._internal.execution.operators.input_data_buffer import (
            InputDataBuffer,
        )
        from ray.data._internal.execution.operators.task_pool_map_operator import (
            TaskPoolMapOperator,
        )
        from ray.data.context import DataContext

        metadata = BlockMetadata(
            num_rows=1000,
            size_bytes=5000,
            # schema=pa.schema([("col", pa.int64())]),
            estimated_size_bytes=15000,
            exec_stats=None,
            task_exec_stats=None,
        )

        block_ref = ray.ObjectRef(b"x" * 28)
        bundle = RefBundle(
            blocks=[(block_ref, metadata)],
            schema=None,
            owns_blocks=False,
        )

        map_transformer = _minimal_map_transformer()
        data_context = DataContext.get_current()
        input_op = InputDataBuffer(data_context, input_data=[])

        map_op = TaskPoolMapOperator(
            map_transformer=map_transformer,
            input_op=input_op,
            data_context=data_context,
            name="test_map",
            target_max_block_size_override=None,
            min_rows_per_bundle=None,
            ref_bundler=None,
            max_concurrency=1,
            supports_fusion=True,
            map_task_kwargs=None,
        )

        size = map_op._estimate_bundle_size(bundle)

        # Should use estimated_size_bytes (15000) instead of size_bytes (5000)
        assert size == 15000

    def test_estimate_bundle_size_fallback_to_actual_size_bytes(self):
        """Test _estimate_bundle_size falls back to actual size_bytes."""
        from ray.data._internal.execution.interfaces import RefBundle
        from ray.data._internal.execution.operators.input_data_buffer import (
            InputDataBuffer,
        )
        from ray.data._internal.execution.operators.task_pool_map_operator import (
            TaskPoolMapOperator,
        )
        from ray.data.context import DataContext

        metadata = BlockMetadata(
            num_rows=1000,
            size_bytes=5000,
            # schema=pa.schema([("col", pa.int64())]),
            exec_stats=None,
            task_exec_stats=None,
        )

        block_ref = ray.ObjectRef(b"x" * 28)
        bundle = RefBundle(
            blocks=[(block_ref, metadata)],
            schema=None,
            owns_blocks=False,
        )

        map_transformer = _minimal_map_transformer()
        data_context = DataContext.get_current()
        input_op = InputDataBuffer(data_context, input_data=[])

        map_op = TaskPoolMapOperator(
            map_transformer=map_transformer,
            input_op=input_op,
            data_context=data_context,
            name="test_map",
            target_max_block_size_override=None,
            min_rows_per_bundle=None,
            ref_bundler=None,
            max_concurrency=1,
            supports_fusion=True,
            map_task_kwargs=None,
        )

        size = map_op._estimate_bundle_size(bundle)

        # Should fall back to actual size_bytes when no estimates are present
        assert size == 5000

    def test_estimate_bundle_size_multiple_blocks(self):
        """Test _estimate_bundle_size sums multiple metadata blocks."""
        from ray.data._internal.execution.interfaces import RefBundle
        from ray.data._internal.execution.operators.input_data_buffer import (
            InputDataBuffer,
        )
        from ray.data._internal.execution.operators.task_pool_map_operator import (
            TaskPoolMapOperator,
        )
        from ray.data.context import DataContext

        metadata1 = BlockMetadata(
            num_rows=1000,
            size_bytes=5000,
            # schema=pa.schema([("col", pa.int64())]),
            input_files_estimated_bytes=25000,
            exec_stats=None,
            task_exec_stats=None,
        )
        metadata2 = BlockMetadata(
            num_rows=2000,
            size_bytes=10000,
            # schema=pa.schema([("col", pa.int64())]),
            input_files_estimated_bytes=50000,
            exec_stats=None,
            task_exec_stats=None,
        )

        ref1 = ray.ObjectRef(b"1" * 28)
        ref2 = ray.ObjectRef(b"2" * 28)
        bundle = RefBundle(
            blocks=[(ref1, metadata1), (ref2, metadata2)],
            schema=None,
            owns_blocks=False,
        )

        map_transformer = _minimal_map_transformer()
        data_context = DataContext.get_current()
        input_op = InputDataBuffer(data_context, input_data=[])

        map_op = TaskPoolMapOperator(
            map_transformer=map_transformer,
            input_op=input_op,
            data_context=data_context,
            name="test_map",
            target_max_block_size_override=None,
            min_rows_per_bundle=None,
            ref_bundler=None,
            max_concurrency=1,
            supports_fusion=True,
            map_task_kwargs=None,
        )

        size = map_op._estimate_bundle_size(bundle)

        # Should sum both estimated sizes
        assert size == 25000 + 50000

    def test_estimate_bundle_size_mixed_metadata(self):
        """Test _estimate_bundle_size with mixed metadata having/not having estimates."""
        from ray.data._internal.execution.interfaces import RefBundle
        from ray.data._internal.execution.operators.input_data_buffer import (
            InputDataBuffer,
        )
        from ray.data._internal.execution.operators.task_pool_map_operator import (
            TaskPoolMapOperator,
        )
        from ray.data.context import DataContext

        # One with estimate, one without
        metadata1 = BlockMetadata(
            num_rows=1000,
            size_bytes=5000,
            # schema=pa.schema([("col", pa.int64())]),
            input_files_estimated_bytes=25000,
            exec_stats=None,
            task_exec_stats=None,
        )
        metadata2 = BlockMetadata(
            num_rows=2000,
            size_bytes=10000,
            # schema=pa.schema([("col", pa.int64())]),
            exec_stats=None,
            task_exec_stats=None,
        )

        ref1 = ray.ObjectRef(b"1" * 28)
        ref2 = ray.ObjectRef(b"2" * 28)
        bundle = RefBundle(
            blocks=[(ref1, metadata1), (ref2, metadata2)],
            schema=None,
            owns_blocks=False,
        )

        map_transformer = _minimal_map_transformer()
        data_context = DataContext.get_current()
        input_op = InputDataBuffer(data_context, input_data=[])

        map_op = TaskPoolMapOperator(
            map_transformer=map_transformer,
            input_op=input_op,
            data_context=data_context,
            name="test_map",
            target_max_block_size_override=None,
            min_rows_per_bundle=None,
            ref_bundler=None,
            max_concurrency=1,
            supports_fusion=True,
            map_task_kwargs=None,
        )

        size = map_op._estimate_bundle_size(bundle)

        # Should sum 25000 (from metadata1) and fall back to 10000 (from metadata2)
        assert size == 25000 + 10000

    def test_estimate_bundle_size_none_bundle(self):
        """Test _estimate_bundle_size with None bundle returns None."""
        from ray.data._internal.execution.operators.input_data_buffer import (
            InputDataBuffer,
        )
        from ray.data._internal.execution.operators.task_pool_map_operator import (
            TaskPoolMapOperator,
        )
        from ray.data.context import DataContext

        map_transformer = _minimal_map_transformer()
        data_context = DataContext.get_current()
        input_op = InputDataBuffer(data_context, input_data=[])

        map_op = TaskPoolMapOperator(
            map_transformer=map_transformer,
            input_op=input_op,
            data_context=data_context,
            name="test_map",
            target_max_block_size_override=None,
            min_rows_per_bundle=None,
            ref_bundler=None,
            max_concurrency=1,
            supports_fusion=True,
            map_task_kwargs=None,
        )

        size = map_op._estimate_bundle_size(None)

        # Should return None for None bundle
        assert size is None


class TestReadFilesTaskMemoryEstimate:
    """``max(uncompressed_rg, batch, block) + eps`` for ReadFiles scheduling."""

    def test_estimate_prefers_row_group_over_target_block(self):
        manifest = FileManifest.construct_manifest(
            paths=["/a.parquet"],
            sizes=[1000],
            estimated_in_memory_sizes=[5000],
            max_uncompressed_row_group_sizes=[50_000_000],
        )
        got = estimate_read_files_task_memory_bytes(
            manifest, target_max_block_size=8_000_000
        )
        assert got == 50_000_000 + READ_FILES_TASK_MEMORY_EPS_BYTES

    def test_estimate_prefers_target_block_when_larger_than_row_group(self):
        manifest = FileManifest.construct_manifest(
            paths=["/a.parquet"],
            sizes=[1000],
            max_uncompressed_row_group_sizes=[1_000_000],
        )
        got = estimate_read_files_task_memory_bytes(
            manifest, target_max_block_size=8_000_000
        )
        assert got == 8_000_000 + READ_FILES_TASK_MEMORY_EPS_BYTES

    def test_enrich_manifest_metadata_sets_task_memory_and_input_estimates(self):
        manifest = FileManifest.construct_manifest(
            paths=["/f"],
            sizes=[100],
            estimated_in_memory_sizes=[400],
            max_uncompressed_row_group_sizes=[2_000_000],
        )
        block = manifest.as_block()
        base = BlockAccessor.for_block(block).get_metadata()
        enriched = enrich_file_manifest_block_metadata_if_applicable(
            block, base, target_max_block_size=1_000_000
        )
        assert enriched.input_files_estimated_bytes == 400
        assert (
            enriched.task_memory_bytes == 2_000_000 + READ_FILES_TASK_MEMORY_EPS_BYTES
        )

    def test_non_manifest_block_not_enriched(self):
        block = pa.table({"x": [1, 2]})
        base = BlockAccessor.for_block(block).get_metadata()
        enriched = enrich_file_manifest_block_metadata_if_applicable(
            block, base, target_max_block_size=99
        )
        assert enriched is base


class TestMapOperatorTaskMemoryHint:
    def test_merge_task_memory_into_ray_remote_args(self):
        from ray.data._internal.execution.interfaces import RefBundle
        from ray.data._internal.execution.operators.input_data_buffer import (
            InputDataBuffer,
        )
        from ray.data._internal.execution.operators.task_pool_map_operator import (
            TaskPoolMapOperator,
        )
        from ray.data.context import DataContext

        metadata = BlockMetadata(
            num_rows=1,
            size_bytes=100,
            task_memory_bytes=500_000_000,
            exec_stats=None,
            task_exec_stats=None,
        )
        block_ref = ray.ObjectRef(b"m" * 28)
        bundle = RefBundle(
            blocks=[(block_ref, metadata)],
            schema=None,
            owns_blocks=False,
        )
        map_op = TaskPoolMapOperator(
            map_transformer=_minimal_map_transformer(),
            input_op=InputDataBuffer(DataContext.get_current(), input_data=[]),
            data_context=DataContext.get_current(),
            name="test_map",
            target_max_block_size_override=None,
            min_rows_per_bundle=None,
            ref_bundler=None,
            max_concurrency=1,
            supports_fusion=True,
            map_task_kwargs={MAP_TASK_KWARG_MERGE_READ_TASK_MEMORY: True},
            ray_remote_args={"memory": 1000},
        )
        remote_args = {"memory": 1000}
        map_op._merge_task_memory_into_ray_remote_args(bundle, remote_args)
        assert remote_args["memory"] == 500_000_000


class TestMemoryEstimationIntegration:
    """Integration tests for memory estimation across components."""

    def test_file_manifest_to_block_metadata_flow(self):
        """Test memory estimates flow from FileManifest through BlockMetadata."""
        # Create a manifest with explicit estimates
        manifest = FileManifest.construct_manifest(
            paths=["/file1.parquet", "/file2.parquet"],
            sizes=[1000, 2000],
            estimated_in_memory_sizes=[5000, 10000],
        )

        # Verify manifest estimates
        assert manifest.estimated_in_memory_sizes[0] == 5000
        assert manifest.estimated_in_memory_sizes[1] == 10000

        # Create BlockMetadata with these estimates
        metadata = BlockMetadata(
            num_rows=1000,
            size_bytes=3000,  # sum of file sizes
            # schema=pa.schema([("col", pa.int64())]),
            input_files_estimated_bytes=manifest.total_estimated_in_memory_size,
            exec_stats=None,
            task_exec_stats=None,
        )

        # Verify the flow
        assert metadata.input_files_estimated_bytes == 15000

    def test_concatenated_manifest_preserves_total_estimates(self):
        """Test total estimates preserved through concatenation."""
        manifest1 = FileManifest.construct_manifest(
            paths=["/file1.parquet"],
            sizes=[1000],
            estimated_in_memory_sizes=[5000],
        )

        manifest2 = FileManifest.construct_manifest(
            paths=["/file2.parquet"],
            sizes=[2000],
            estimated_in_memory_sizes=[10000],
        )

        concatenated = FileManifest.concat([manifest1, manifest2])

        # Total should be sum of both estimates
        assert concatenated.total_estimated_in_memory_size == 15000

        # Can create metadata from total
        metadata = BlockMetadata(
            num_rows=1500,
            size_bytes=3000,
            # schema=pa.schema([("col", pa.int64())]),
            input_files_estimated_bytes=concatenated.total_estimated_in_memory_size,
            exec_stats=None,
            task_exec_stats=None,
        )

        assert metadata.input_files_estimated_bytes == 15000


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-xvs", __file__]))
