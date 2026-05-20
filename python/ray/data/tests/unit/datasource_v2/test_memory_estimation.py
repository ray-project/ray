"""Unit tests for DataSource V2 ReadFiles memory-hint plumbing.

Covers the producer side:

- ``estimate_read_files_task_memory_bytes``: formula
  ``max(2 * target_max_block_size, max(file_sizes)) + eps``, including
  boundary conditions (``target=None``, ``eps=0``, empty manifest) and
  ``eps`` resolution from ``DataContext``.
- ``enrich_file_manifest_block_metadata_if_applicable``: idempotency,
  pass-through on non-manifest blocks, preservation of other metadata
  fields.
- ``enrich_manifest_block_metadata_for_map_task`` adapter: reads
  ``DataContext.read_files_task_memory_eps_bytes`` at call time.
"""

import pyarrow as pa
import pytest

from ray.data._internal.datasource_v2.listing.file_manifest import (
    FILE_SIZE_COLUMN_NAME,
    PATH_COLUMN_NAME,
    FileManifest,
)
from ray.data._internal.datasource_v2.readers.read_files_task_memory import (
    enrich_file_manifest_block_metadata_if_applicable,
    enrich_manifest_block_metadata_for_map_task,
    estimate_read_files_task_memory_bytes,
)
from ray.data.block import BlockAccessor, BlockMetadata
from ray.data.context import DataContext


@pytest.fixture
def reset_ctx_eps():
    """Restore the original eps value on the live ``DataContext`` after a test."""
    ctx = DataContext.get_current()
    original = ctx.read_files_task_memory_eps_bytes
    try:
        yield ctx
    finally:
        ctx.read_files_task_memory_eps_bytes = original


class TestReadFilesTaskMemoryEstimate:
    """Formula: ``max(2 * target_max_block_size, max(file_sizes)) + eps``."""

    def test_max_file_size_dominates_when_larger_than_two_target(self):
        manifest = FileManifest.construct_manifest(
            paths=["/a.parquet", "/b.parquet"],
            sizes=[50_000_000, 5_000_000],
        )
        # 2 * 8M = 16M, max(file_sizes) = 50M -> 50M dominates.
        got = estimate_read_files_task_memory_bytes(
            manifest, target_max_block_size=8_000_000, eps=64 * 1024 * 1024
        )
        assert got == 50_000_000 + 64 * 1024 * 1024

    def test_two_target_dominates_when_larger_than_max_file_size(self):
        manifest = FileManifest.construct_manifest(
            paths=["/a.parquet"], sizes=[1_000_000]
        )
        # 2 * 8M = 16M, max(file_sizes) = 1M -> 16M dominates.
        got = estimate_read_files_task_memory_bytes(
            manifest, target_max_block_size=8_000_000, eps=64 * 1024 * 1024
        )
        assert got == 16_000_000 + 64 * 1024 * 1024

    def test_target_none_coerced_to_zero(self):
        """``target_max_block_size=None`` -> 2*0=0; result is max_file + eps."""
        manifest = FileManifest.construct_manifest(
            paths=["/a.parquet"], sizes=[7_000_000]
        )
        got = estimate_read_files_task_memory_bytes(
            manifest, target_max_block_size=None, eps=64 * 1024 * 1024
        )
        assert got == 7_000_000 + 64 * 1024 * 1024

    def test_eps_zero_no_padding(self):
        manifest = FileManifest.construct_manifest(
            paths=["/a.parquet"], sizes=[3_000_000]
        )
        got = estimate_read_files_task_memory_bytes(
            manifest, target_max_block_size=1_000_000, eps=0
        )
        # max(2*1M, 3M) + 0 = 3M.
        assert got == 3_000_000

    def test_empty_manifest_returns_two_target_plus_eps(self):
        """Empty manifest -> max(file_sizes) treated as 0."""
        manifest = FileManifest.construct_manifest(paths=[], sizes=[])
        got = estimate_read_files_task_memory_bytes(
            manifest, target_max_block_size=4_000_000, eps=64 * 1024 * 1024
        )
        assert got == 8_000_000 + 64 * 1024 * 1024

    def test_eps_default_resolves_from_data_context(self, reset_ctx_eps):
        """``eps=None`` -> resolved from ``DataContext`` at call time."""
        reset_ctx_eps.read_files_task_memory_eps_bytes = 7 * 1024 * 1024  # 7 MiB
        manifest = FileManifest.construct_manifest(
            paths=["/a.parquet"], sizes=[5_000_000]
        )
        got = estimate_read_files_task_memory_bytes(
            manifest, target_max_block_size=1_000_000
        )
        # max(2M, 5M) + 7M = 5M + 7M.
        assert got == 5_000_000 + 7 * 1024 * 1024


class TestEnrichFileManifestBlockMetadata:
    """``enrich_file_manifest_block_metadata_if_applicable`` semantics."""

    def test_enrich_sets_task_memory_bytes_from_formula(self):
        """``enrich`` sets ``task_memory_bytes`` to the formula result."""
        manifest = FileManifest.construct_manifest(paths=["/f1"], sizes=[2_000_000])
        block = manifest.as_block()
        base = BlockMetadata(
            num_rows=42,
            size_bytes=999,
            input_files=("/f1",),
            exec_stats=None,
            task_exec_stats=None,
        )
        enriched = enrich_file_manifest_block_metadata_if_applicable(
            block, base, target_max_block_size=1_000_000, eps=64 * 1024 * 1024
        )
        # Other fields untouched.
        assert enriched.num_rows == 42
        assert enriched.size_bytes == 999
        assert enriched.input_files == ("/f1",)
        # max(2*1M, 2M) + 64M = 2M + 64M.
        assert enriched.task_memory_bytes == 2_000_000 + 64 * 1024 * 1024

    def test_enrich_idempotent(self):
        """Calling enrich twice yields the same hint (no compounding)."""
        manifest = FileManifest.construct_manifest(paths=["/f"], sizes=[2_000_000])
        block = manifest.as_block()
        base = BlockAccessor.for_block(block).get_metadata()

        once = enrich_file_manifest_block_metadata_if_applicable(
            block, base, target_max_block_size=1_000_000, eps=64 * 1024 * 1024
        )
        twice = enrich_file_manifest_block_metadata_if_applicable(
            block, once, target_max_block_size=1_000_000, eps=64 * 1024 * 1024
        )
        assert once.task_memory_bytes == twice.task_memory_bytes

    def test_enrich_non_manifest_block_is_pass_through(self):
        """Non-manifest block -> ``enrich`` returns the input meta object unchanged."""
        block = pa.table({"x": [1, 2]})
        base = BlockAccessor.for_block(block).get_metadata()
        enriched = enrich_file_manifest_block_metadata_if_applicable(
            block, base, target_max_block_size=99, eps=0
        )
        assert enriched is base


class TestEnrichManifestForMapTaskAdapter:
    """``enrich_manifest_block_metadata_for_map_task`` reads from ``DataContext``."""

    def test_adapter_reads_eps_from_data_context(self, reset_ctx_eps):
        """The adapter pulls ``eps`` from the live ``DataContext`` field."""
        reset_ctx_eps.read_files_task_memory_eps_bytes = 11 * 1024 * 1024
        reset_ctx_eps.target_max_block_size = 1_000_000

        manifest = FileManifest.construct_manifest(paths=["/f"], sizes=[5_000_000])
        block = manifest.as_block()
        base = BlockAccessor.for_block(block).get_metadata()

        enriched = enrich_manifest_block_metadata_for_map_task(
            block, base, reset_ctx_eps
        )
        # max(2*1M, 5M) + 11M = 5M + 11M.
        assert enriched.task_memory_bytes == 5_000_000 + 11 * 1024 * 1024


class TestFileManifestColumnInvariants:
    """Sanity invariants for the ``__file_size`` column the formula depends on."""

    def test_file_sizes_round_trip_through_arrow_table(self):
        table = pa.table(
            {
                PATH_COLUMN_NAME: ["/f1", "/f2", "/f3"],
                FILE_SIZE_COLUMN_NAME: [10, 20, 30],
            }
        )
        manifest = FileManifest(table)
        assert int(manifest.file_sizes.max()) == 30
        assert int(manifest.file_sizes.sum()) == 60


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-xvs", __file__]))
