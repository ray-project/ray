"""Unit tests for footer-derived in-memory size estimation (v2a/v2b/v2d).

These exercise the type-aware per-chunk in-memory estimate the row-group-aware
``ParquetFileChunker`` stamps into chunk metadata, and the
``FooterDerivedInMemorySizeEstimator`` that consumes it (with fallback). Pure
unit tests: build small Parquet files, no Ray cluster.
"""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ray.data._internal.datasource_v2.chunkers.file_chunker import (
    ParquetFileChunker,
    estimate_chunk_in_memory_size,
)
from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.readers.in_memory_size_estimator import (
    PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT,
    FooterDerivedInMemorySizeEstimator,
)


def _chunks(path, **ctx_overrides):
    from ray.data.context import DataContext

    ctx = DataContext.get_current()
    saved = {k: getattr(ctx, k) for k in ctx_overrides}
    for k, v in ctx_overrides.items():
        setattr(ctx, k, v)
    try:
        chunker = ParquetFileChunker()
        return list(chunker.generate_chunk_metadatas(path, Path(path).stat().st_size))
    finally:
        for k, v in saved.items():
            setattr(ctx, k, v)


# ----------------------------------------------------------------------
# estimate_chunk_in_memory_size — the type-aware math.
# ----------------------------------------------------------------------


def test_fixed_width_is_exact_and_encoding_independent():
    schema = pa.schema(
        [
            pa.field("i64", pa.int64(), nullable=False),
            pa.field("f32", pa.float32(), nullable=False),
        ]
    )
    # 1000 rows: int64 = 8 bytes, float32 = 4 bytes -> 12_000 bytes, no validity.
    size = estimate_chunk_in_memory_size(
        schema, num_rows=1000, uncompressed_by_column={}, var_width_factor=2.0
    )
    assert size == 1000 * (8 + 4)
    # Uncompressed footer bytes are IGNORED for fixed-width columns, so the
    # estimate is independent of compression / encoding.
    size2 = estimate_chunk_in_memory_size(
        schema,
        num_rows=1000,
        uncompressed_by_column={"i64": 999999, "f32": 999999},
        var_width_factor=2.0,
    )
    assert size2 == size


def test_nullable_adds_validity_bitmap():
    schema = pa.schema([pa.field("i64", pa.int64(), nullable=True)])
    size = estimate_chunk_in_memory_size(
        schema, num_rows=800, uncompressed_by_column={}, var_width_factor=2.0
    )
    # 800*8 data + ceil(800/8)=100 validity.
    assert size == 800 * 8 + 100


def test_var_width_uses_factor_and_offsets():
    schema = pa.schema([pa.field("s", pa.string(), nullable=False)])
    size = estimate_chunk_in_memory_size(
        schema,
        num_rows=100,
        uncompressed_by_column={"s": 1000},
        var_width_factor=2.0,
    )
    # 1000 * 2.0 (data) + 100 * 4 (int32 offsets).
    assert size == int(1000 * 2.0) + 100 * 4


def test_projection_counts_only_selected_columns():
    schema = pa.schema(
        [
            pa.field("a", pa.int64(), nullable=False),
            pa.field("b", pa.int64(), nullable=False),
        ]
    )
    full = estimate_chunk_in_memory_size(
        schema, num_rows=10, uncompressed_by_column={}, var_width_factor=2.0
    )
    projected = estimate_chunk_in_memory_size(
        schema,
        num_rows=10,
        uncompressed_by_column={},
        var_width_factor=2.0,
        projected_columns={"a"},
    )
    assert full == 10 * 16
    assert projected == 10 * 8


def test_bool_is_bit_packed():
    schema = pa.schema([pa.field("b", pa.bool_(), nullable=False)])
    size = estimate_chunk_in_memory_size(
        schema, num_rows=80, uncompressed_by_column={}, var_width_factor=2.0
    )
    assert size == 80 // 8  # 1 bit per value


# ----------------------------------------------------------------------
# Chunker stamps a usable in_memory_size; it tracks logical (not on-disk) size.
# ----------------------------------------------------------------------


def test_chunker_stamps_in_memory_size(tmp_path):
    path = str(tmp_path / "t.parquet")
    table = pa.table({"i64": pa.array(list(range(1000)), pa.int64())})
    pq.write_table(table, path, row_group_size=1000)

    chunks = _chunks(path, parquet_chunker_target_chunk_size=1 << 30)
    assert len(chunks) == 1
    md, on_disk = chunks[0]
    # Fixed-width int64 × 1000 rows = 8000 bytes of data. ``pa.table`` makes
    # the field nullable by default, so the estimate also includes a validity
    # bitmap of ceil(1000/8) = 125 bytes. Either way it tracks the logical
    # (in-memory) size, not the much smaller on-disk size.
    expected = 1000 * 8 + (1000 + 7) // 8
    assert md["in_memory_size"] == expected
    assert on_disk < md["in_memory_size"]


def test_chunker_in_memory_size_compression_independent(tmp_path):
    """Two files with identical logical int64 data but different compression
    get the SAME in_memory_size (the whole point), despite different on-disk
    sizes."""
    data = pa.table({"i64": pa.array([i % 7 for i in range(5000)], pa.int64())})
    p_snappy = str(tmp_path / "snappy.parquet")
    p_none = str(tmp_path / "none.parquet")
    pq.write_table(data, p_snappy, row_group_size=5000, compression="snappy")
    pq.write_table(data, p_none, row_group_size=5000, compression="none")

    (md_s, disk_s) = _chunks(p_snappy, parquet_chunker_target_chunk_size=1 << 30)[0]
    (md_n, disk_n) = _chunks(p_none, parquet_chunker_target_chunk_size=1 << 30)[0]

    # The whole point: identical logical data -> identical in-memory estimate,
    # independent of codec. 5000 * 8 data bytes + ceil(5000/8) = 625 validity
    # bytes (nullable-by-default field) = 40625.
    expected = 5000 * 8 + (5000 + 7) // 8
    assert md_s["in_memory_size"] == md_n["in_memory_size"] == expected
    # On-disk sizes differ (compression), confirming the estimate ignores them.
    assert disk_s != disk_n


# ----------------------------------------------------------------------
# FooterDerivedInMemorySizeEstimator — reads the hint, falls back.
# ----------------------------------------------------------------------


def test_estimator_reads_hint():
    manifest = FileManifest.construct_manifest(
        ["a.parquet", "b.parquet"],
        [10, 20],  # on-disk sizes (ignored when a hint is present)
        [
            {"row_group_start": 0, "row_group_end": 1, "in_memory_size": 100},
            {"row_group_start": 0, "row_group_end": 1, "in_memory_size": 250},
        ],
    )
    est = FooterDerivedInMemorySizeEstimator()
    out = est.estimate_in_memory_sizes(manifest)
    assert list(out) == [100.0, 250.0]


def test_estimator_falls_back_when_no_hint():
    # Whole-file fallback chunks carry ``None`` metadata -> no hint -> use
    # on_disk × fallback_ratio (the previous behavior).
    manifest = FileManifest.construct_manifest(
        ["a.parquet"],
        [10],
        [None],
    )
    est = FooterDerivedInMemorySizeEstimator()
    out = est.estimate_in_memory_sizes(manifest)
    assert list(out) == [10.0 * PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
