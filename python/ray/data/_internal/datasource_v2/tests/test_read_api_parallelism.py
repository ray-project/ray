"""Unit tests for the parallelism (``num_buckets``) decision made by V2's
``read_api`` entry. The decision delegates to ``_autodetect_parallelism``
(also used by V1), so V2 picks up the same CPU-saturation floor.

These tests exercise the helper directly with ``mem_size=None`` —
matching how V2 calls it (no plan-time data-size estimate) — and lock
in three behaviors:

1. ``parallelism != -1`` is honored verbatim (user override).
2. ``parallelism == -1`` with a small cluster falls back to
   ``ctx.read_op_min_num_blocks``.
3. ``parallelism == -1`` with a large cluster honors the CPU floor
   (``2 * avail_cpus``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from ray.data._internal.util import _autodetect_parallelism
from ray.data.context import DataContext

if TYPE_CHECKING:
    from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest


@pytest.fixture
def ctx() -> DataContext:
    return DataContext.get_current()


def test_num_buckets_honors_user_override(ctx: DataContext):
    """``parallelism != -1`` is returned verbatim."""
    resolved, _reason, _mem_size = _autodetect_parallelism(
        parallelism=50,
        target_max_block_size=ctx.target_max_block_size,
        ctx=ctx,
        mem_size=None,
    )
    assert resolved == 50


def test_num_buckets_falls_back_to_read_op_min_num_blocks_on_small_cluster(
    ctx: DataContext,
):
    """Small cluster + no override → ``read_op_min_num_blocks`` floor wins."""
    resolved, _reason, _mem_size = _autodetect_parallelism(
        parallelism=-1,
        target_max_block_size=ctx.target_max_block_size,
        ctx=ctx,
        mem_size=None,
        avail_cpus=4,  # 2 * 4 = 8 < 200
    )
    assert resolved == ctx.read_op_min_num_blocks


def test_num_buckets_honors_cpu_floor_on_large_cluster(ctx: DataContext):
    """Large cluster + no override → CPU floor (2 * avail_cpus) wins.

    This is the V1 parity guarantee Option 1 introduces to V2:
    downstream operators with ``concurrency=N`` are never starved by a
    read stage that produced fewer than ~2N blocks.
    """
    resolved, _reason, _mem_size = _autodetect_parallelism(
        parallelism=-1,
        target_max_block_size=ctx.target_max_block_size,
        ctx=ctx,
        mem_size=None,
        avail_cpus=256,  # 2 * 256 = 512 > 200 default floor
    )
    assert resolved == 512


def test_num_buckets_size_floors_inert_when_mem_size_none(ctx: DataContext):
    """With ``mem_size=None`` (V2's call shape), the helper's
    ``min_safe_parallelism`` / ``max_reasonable_parallelism`` branches
    stay at identity defaults; result depends only on
    ``read_op_min_num_blocks`` and ``2 * avail_cpus``."""
    resolved, _reason, _mem_size = _autodetect_parallelism(
        parallelism=-1,
        target_max_block_size=ctx.target_max_block_size,
        ctx=ctx,
        mem_size=None,
        avail_cpus=100,  # 2 * 100 = 200, tied with default floor
    )
    assert resolved == max(ctx.read_op_min_num_blocks, 200)


# ---------------------------------------------------------------------------
# Adaptive ParquetFileChunker target (Fix 1)
# ---------------------------------------------------------------------------


def _make_sample_manifest(*sizes: int) -> "FileManifest":
    """Build a ``FileManifest`` with the given per-file sizes."""
    import pyarrow as pa

    from ray.data._internal.datasource_v2.listing.file_manifest import (
        FILE_CHUNK_METADATA_COLUMN_NAME,
        FILE_SIZE_COLUMN_NAME,
        PATH_COLUMN_NAME,
        FileManifest,
    )

    n = len(sizes)
    return FileManifest(
        pa.Table.from_pydict(
            {
                PATH_COLUMN_NAME: [f"/f{i}.parquet" for i in range(n)],
                FILE_SIZE_COLUMN_NAME: list(sizes),
                FILE_CHUNK_METADATA_COLUMN_NAME: [None] * n,
            }
        )
    )


MiB = 1024 * 1024
GiB = 1024 * MiB


@pytest.fixture
def reset_ctx_chunker_knob():
    """Clear ``parquet_chunker_target_chunk_size`` after the test."""
    ctx = DataContext.get_current()
    original = ctx.parquet_chunker_target_chunk_size
    ctx.parquet_chunker_target_chunk_size = None
    try:
        yield ctx
    finally:
        ctx.parquet_chunker_target_chunk_size = original


def test_adaptive_target_single_file_clamps_to_min_block_size(
    reset_ctx_chunker_knob: DataContext,
):
    """1 file × 225 MiB, num_buckets=560 → raw=411 KiB → clamps to
    ``target_min_block_size`` (1 MiB)."""
    from ray.data.read_api import _compute_adaptive_parquet_chunk_size

    ctx = reset_ctx_chunker_knob
    sample = _make_sample_manifest(225 * MiB)
    target = _compute_adaptive_parquet_chunk_size(
        sample,
        num_buckets=560,
        ctx=ctx,
        explicit_path_count=1,
    )
    assert target == ctx.target_min_block_size


def test_adaptive_target_multi_file_in_range(
    reset_ctx_chunker_knob: DataContext,
):
    """7 × 500 MiB sample, num_buckets=560 → raw=6.4 MiB → no clamp."""
    from ray.data.read_api import _compute_adaptive_parquet_chunk_size

    ctx = reset_ctx_chunker_knob
    sample = _make_sample_manifest(*([500 * MiB] * 7))
    target = _compute_adaptive_parquet_chunk_size(
        sample,
        num_buckets=560,
        ctx=ctx,
        explicit_path_count=7,
    )
    # 7 × 500 MiB / 560 = 6.4 MiB. Between min (1 MiB) and max (128 MiB).
    expected = (7 * 500 * MiB) // 560
    assert target == expected


def test_adaptive_target_huge_dataset_clamps_to_max_block_size(
    reset_ctx_chunker_knob: DataContext,
):
    """16 × 4 GiB sample, num_buckets=200 → raw=327 MiB → clamps to 128 MiB."""
    from ray.data.read_api import _compute_adaptive_parquet_chunk_size

    ctx = reset_ctx_chunker_knob
    # 16 files exactly at sample cap → extrapolation kicks in.
    sample = _make_sample_manifest(*([4 * GiB] * 16))
    target = _compute_adaptive_parquet_chunk_size(
        sample,
        num_buckets=200,
        ctx=ctx,
        # explicit_path_count == sample size triggers extrapolation
        # path; the result still clamps to target_max_block_size.
        explicit_path_count=16,
    )
    assert target == ctx.target_max_block_size


def test_adaptive_target_empty_sample_returns_none(
    reset_ctx_chunker_knob: DataContext,
):
    """Empty sample → no signal, caller skips override."""
    from ray.data.read_api import _compute_adaptive_parquet_chunk_size

    ctx = reset_ctx_chunker_knob
    sample = _make_sample_manifest()
    target = _compute_adaptive_parquet_chunk_size(
        sample,
        num_buckets=200,
        ctx=ctx,
        explicit_path_count=0,
    )
    assert target is None


def test_adaptive_target_capped_sample_extrapolates_with_path_count(
    reset_ctx_chunker_knob: DataContext,
):
    """Sample capped at 16 with explicit_path_count=100 → extrapolates
    via ``avg × 100`` instead of using ``sample_bytes`` directly."""
    from ray.data.read_api import _compute_adaptive_parquet_chunk_size

    ctx = reset_ctx_chunker_knob
    # 16 × 10 MiB. With explicit_path_count=100 we want estimated total
    # = 10 MiB × 100 = 1 GiB; num_buckets=100 → raw=10 MiB (in range).
    sample = _make_sample_manifest(*([10 * MiB] * 16))
    target = _compute_adaptive_parquet_chunk_size(
        sample,
        num_buckets=100,
        ctx=ctx,
        explicit_path_count=100,
    )
    assert target == 10 * MiB


def test_adaptive_target_returns_none_when_user_set_ctx_knob(
    reset_ctx_chunker_knob: DataContext,
):
    """User-set ``ctx.parquet_chunker_target_chunk_size`` wins."""
    from ray.data.read_api import _compute_adaptive_parquet_chunk_size

    ctx = reset_ctx_chunker_knob
    ctx.parquet_chunker_target_chunk_size = 64 * MiB
    sample = _make_sample_manifest(225 * MiB)
    target = _compute_adaptive_parquet_chunk_size(
        sample,
        num_buckets=560,
        ctx=ctx,
        explicit_path_count=1,
    )
    assert target is None


def test_adaptive_target_zero_num_buckets_returns_none(
    reset_ctx_chunker_knob: DataContext,
):
    """``num_buckets <= 0`` → no division; return None."""
    from ray.data.read_api import _compute_adaptive_parquet_chunk_size

    ctx = reset_ctx_chunker_knob
    sample = _make_sample_manifest(100 * MiB)
    target = _compute_adaptive_parquet_chunk_size(
        sample,
        num_buckets=0,
        ctx=ctx,
        explicit_path_count=1,
    )
    assert target is None


# ---------------------------------------------------------------------------
# ParquetDatasourceV2._get_file_indexer(target_chunk_size_override) wiring (Fix 1)
# ---------------------------------------------------------------------------


def test_get_file_indexer_honors_chunk_size_override():
    """Override flows into a fresh ``ParquetFileChunker``."""
    from ray.data._internal.datasource_v2.chunkers.file_chunker import (
        ParquetFileChunker,
    )
    from ray.data._internal.datasource_v2.parquet_datasource_v2 import (
        ParquetDatasourceV2,
    )

    ds = ParquetDatasourceV2(paths=["/tmp/nonexistent"])
    indexer = ds._get_file_indexer(target_chunk_size_override=10 * MiB)
    chunker = indexer.file_chunker
    assert isinstance(chunker, ParquetFileChunker)
    assert chunker._target_chunk_size == 10 * MiB


def test_get_file_indexer_default_when_no_override():
    """Without override, the chunker keeps its default target (1 GiB)."""
    from ray.data._internal.datasource_v2.chunkers.file_chunker import (
        ParquetFileChunker,
    )
    from ray.data._internal.datasource_v2.parquet_datasource_v2 import (
        ParquetDatasourceV2,
    )

    ds = ParquetDatasourceV2(paths=["/tmp/nonexistent"])
    indexer = ds._get_file_indexer()
    chunker = indexer.file_chunker
    assert isinstance(chunker, ParquetFileChunker)
    assert chunker._target_chunk_size == ParquetFileChunker._DEFAULT_TARGET_CHUNK_SIZE


# ---------------------------------------------------------------------------
# ParquetFileReader batch-size knob (Fix 2)
# ---------------------------------------------------------------------------


@pytest.fixture
def reset_ctx_reader_batch_knob():
    """Clear ``parquet_reader_target_batch_size_bytes`` after the test."""
    ctx = DataContext.get_current()
    original = ctx.parquet_reader_target_batch_size_bytes
    ctx.parquet_reader_target_batch_size_bytes = None
    try:
        yield ctx
    finally:
        ctx.parquet_reader_target_batch_size_bytes = original


def test_resolve_batch_target_bytes_falls_back_to_target_min_block_size(
    reset_ctx_reader_batch_knob: DataContext,
):
    """When the new knob is unset, fall back to ``target_min_block_size``."""
    from ray.data._internal.datasource_v2.readers.parquet_file_reader import (
        ParquetFileReader,
    )

    ctx = reset_ctx_reader_batch_knob
    reader = ParquetFileReader(filesystem=None)
    assert reader._resolve_batch_target_bytes() == ctx.target_min_block_size


def test_resolve_batch_target_bytes_uses_knob_when_set(
    reset_ctx_reader_batch_knob: DataContext,
):
    """Knob value takes precedence over the ``target_min_block_size`` fallback."""
    from ray.data._internal.datasource_v2.readers.parquet_file_reader import (
        ParquetFileReader,
    )

    ctx = reset_ctx_reader_batch_knob
    ctx.parquet_reader_target_batch_size_bytes = 64 * 1024  # 64 KiB
    reader = ParquetFileReader(filesystem=None)
    assert reader._resolve_batch_target_bytes() == 64 * 1024


def test_resolve_batch_target_bytes_none_when_both_unset(
    reset_ctx_reader_batch_knob: DataContext,
):
    """Both knob and ``target_min_block_size`` unset → returns None."""
    from ray.data._internal.datasource_v2.readers.parquet_file_reader import (
        ParquetFileReader,
    )

    ctx = reset_ctx_reader_batch_knob
    original_min = ctx.target_min_block_size
    ctx.target_min_block_size = None
    try:
        reader = ParquetFileReader(filesystem=None)
        assert reader._resolve_batch_target_bytes() is None
    finally:
        ctx.target_min_block_size = original_min


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
