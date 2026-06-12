"""Unit tests for :class:`ParquetDatasourceV2`.

These tests exercise schema inference, scanner/estimator creation, and
include-paths schema augmentation against a local tmpdir — they do not
spin up Ray.
"""

import os

import pyarrow as pa
import pyarrow.parquet as pq

from ray.data._internal.datasource_v2.chunkers.file_chunker import (
    ByteEstimateParquetFileChunker,
    ByteEstimateParquetFileChunkMetadata,
    ParquetFileChunker,
    ParquetFileChunkMetadata,
    WholeFileChunker,
    create_chunk_metadata,
)
from ray.data._internal.datasource_v2.chunkers.parquet_file_chunking_utils import (
    _calculate_row_group_range,
    _fragments_from_chunk_metadata,
)
from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.parquet_datasource_v2 import (
    ParquetDatasourceV2,
)
from ray.data._internal.datasource_v2.readers.in_memory_size_estimator import (
    ParquetInMemorySizeEstimator,
)
from ray.data._internal.datasource_v2.readers.parquet_file_reader import (
    ParquetFileReader,
)
from ray.data._internal.datasource_v2.scanners.parquet_scanner import (
    ParquetScanner,
)
from ray.data.datasource.partitioning import Partitioning, PartitionStyle


def _write_parquet(path: str, table: pa.Table) -> None:
    pq.write_table(table, path)


def _manifest_of(paths):
    sizes = [os.path.getsize(p) for p in paths]
    return FileManifest.construct_manifest(paths, sizes, [None] * len(paths))


def test_infer_schema_unpartitioned(tmp_path):
    file_path = tmp_path / "data.parquet"
    _write_parquet(str(file_path), pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]}))

    datasource = ParquetDatasourceV2([str(file_path)])
    schema = datasource.infer_schema(_manifest_of([str(file_path)]))

    assert schema.names == ["a", "b"]
    assert schema.field("a").type == pa.int64()
    assert schema.field("b").type == pa.string()


def test_infer_schema_hive_partitioned(tmp_path):
    for part in ["a", "b"]:
        d = tmp_path / f"color={part}"
        d.mkdir()
        _write_parquet(str(d / "data.parquet"), pa.table({"x": [1, 2]}))

    first_file = str(tmp_path / "color=a" / "data.parquet")
    datasource = ParquetDatasourceV2(
        [str(tmp_path)], partitioning=Partitioning(PartitionStyle.HIVE)
    )
    schema = datasource.infer_schema(_manifest_of([first_file]))

    assert "x" in schema.names
    assert "color" in schema.names
    assert schema.field("color").type == pa.string()


def test_infer_schema_with_include_paths(tmp_path):
    file_path = tmp_path / "data.parquet"
    _write_parquet(str(file_path), pa.table({"a": [1, 2]}))

    datasource = ParquetDatasourceV2([str(file_path)], include_paths=True)
    schema = datasource.infer_schema(_manifest_of([str(file_path)]))

    assert "path" in schema.names
    assert schema.field("path").type == pa.string()


def test_infer_schema_returns_empty_schema_on_empty_manifest(tmp_path):
    datasource = ParquetDatasourceV2([str(tmp_path)])
    empty = FileManifest.construct_manifest([], [], [])
    schema = datasource.infer_schema(empty)
    assert schema.names == []


def test_create_scanner_returns_parquet_scanner(tmp_path):
    file_path = tmp_path / "data.parquet"
    _write_parquet(str(file_path), pa.table({"a": [1]}))

    datasource = ParquetDatasourceV2([str(file_path)])
    schema = datasource.infer_schema(_manifest_of([str(file_path)]))
    scanner = datasource.create_scanner(schema)

    assert isinstance(scanner, ParquetScanner)
    assert scanner.schema == schema


def test_get_size_estimator_returns_parquet_estimator(tmp_path):
    datasource = ParquetDatasourceV2([str(tmp_path)])
    assert isinstance(datasource.get_size_estimator(), ParquetInMemorySizeEstimator)


def test_paths_and_filesystem_resolved(tmp_path):
    file_path = tmp_path / "data.parquet"
    _write_parquet(str(file_path), pa.table({"a": [1]}))

    datasource = ParquetDatasourceV2([str(file_path)])
    # _resolve_paths_and_filesystem produces a concrete filesystem even when
    # the caller passed None.
    assert datasource.filesystem is not None
    assert len(datasource.paths) == 1


def test_infer_schema_with_include_row_hash(tmp_path):
    file_path = tmp_path / "data.parquet"
    _write_parquet(str(file_path), pa.table({"a": [1, 2]}))

    datasource = ParquetDatasourceV2([str(file_path)], include_row_hash=True)
    schema = datasource.infer_schema(_manifest_of([str(file_path)]))

    assert "row_hash" in schema.names
    assert schema.field("row_hash").type == pa.uint64()


def test_infer_schema_with_include_row_hash_existing_column_promoted_to_uint64(
    tmp_path,
):
    file_path = tmp_path / "data.parquet"
    _write_parquet(str(file_path), pa.table({"val": [1, 2], "row_hash": [10, 20]}))

    datasource = ParquetDatasourceV2([str(file_path)], include_row_hash=True)
    schema = datasource.infer_schema(_manifest_of([str(file_path)]))

    assert schema.field("row_hash").type == pa.uint64()


def test_create_scanner_propagates_include_row_hash(tmp_path):
    file_path = tmp_path / "data.parquet"
    _write_parquet(str(file_path), pa.table({"a": [1]}))

    datasource = ParquetDatasourceV2([str(file_path)], include_row_hash=True)
    schema = datasource.infer_schema(_manifest_of([str(file_path)]))
    scanner = datasource.create_scanner(schema)

    assert scanner.include_row_hash is True


def test_nested_fallback_handles_schema_evolution(tmp_path, monkeypatch):
    """Regression: when the nested-type fallback fires on a fragment that
    lacks a filter-referenced column, the V2 reader must null-fill the
    missing column instead of letting pyarrow raise. Matches the
    scanner path, which null-fills via dataset-level schema pinning.
    """
    import pyarrow.dataset as pds

    from ray.data._internal.datasource import parquet_datasource
    from ray.data._internal.datasource_v2.readers.parquet_file_reader import (
        ParquetFileReader,
    )
    from ray.data.expressions import col

    _write_parquet(
        str(tmp_path / "with_b.parquet"),
        pa.table({"a": [1, 2, 3], "b": [10, 20, 30]}),
    )
    _write_parquet(
        str(tmp_path / "without_b.parquet"),
        pa.table({"a": [4, 5, 6]}),
    )

    unified_schema = pa.schema([("a", pa.int64()), ("b", pa.int64())])
    predicate = col("b") > 15

    # Force the fallback path; the source-module attribute is what V2's
    # function-local import resolves to on each call.
    monkeypatch.setattr(
        parquet_datasource, "_needs_nested_type_fallback", lambda *a, **kw: True
    )

    reader = ParquetFileReader(
        columns=["a"], predicate=predicate, schema=unified_schema
    )
    dataset = pds.dataset(str(tmp_path), format="parquet", schema=unified_schema)
    scanner_kwargs = {
        "columns": ["a"],
        "filter": predicate.to_pyarrow(),
        "batch_size": None,
    }

    rows_by_fragment = {}
    for fragment in dataset.get_fragments():
        tables = list(reader._iter_fragment_tables(fragment, scanner_kwargs))
        rows_by_fragment[os.path.basename(fragment.path)] = sum(
            t.num_rows for t in tables
        )

    # with_b: rows where b > 15 → 2 rows (b=20, b=30)
    # without_b: b is null-filled → null > 15 is null → 0 rows
    assert rows_by_fragment == {"with_b.parquet": 2, "without_b.parquet": 0}


def test_datasource_defaults_to_parquet_file_chunker(tmp_path):
    """``ParquetDatasourceV2`` plugs ``ParquetFileChunker`` into its indexer."""
    file_path = tmp_path / "data.parquet"
    _write_parquet(str(file_path), pa.table({"a": [1, 2, 3]}))

    datasource = ParquetDatasourceV2([str(file_path)])
    indexer = datasource._get_file_indexer()
    assert isinstance(indexer.file_chunker, ParquetFileChunker)


def test_datasource_accepts_custom_chunker(tmp_path):
    """An explicit ``file_chunker`` override propagates to the indexer."""
    file_path = tmp_path / "data.parquet"
    _write_parquet(str(file_path), pa.table({"a": [1, 2, 3]}))

    custom = WholeFileChunker()
    datasource = ParquetDatasourceV2([str(file_path)], file_chunker=custom)
    indexer = datasource._get_file_indexer()
    assert indexer.file_chunker is custom


def _chunker_for_flag(path, row_group_aware: bool):
    """Construct a ``ParquetDatasourceV2`` under the given flag and return its
    selected chunker, restoring the flag afterwards."""
    from ray.data.context import DataContext

    ctx = DataContext.get_current()
    original = ctx.parquet_chunker_row_group_aware
    ctx.parquet_chunker_row_group_aware = row_group_aware
    try:
        return ParquetDatasourceV2([str(path)])._file_chunker
    finally:
        ctx.parquet_chunker_row_group_aware = original


def test_datasource_selects_row_group_chunker_when_flag_on(tmp_path):
    file_path = tmp_path / "data.parquet"
    _write_parquet(str(file_path), pa.table({"a": [1, 2, 3]}))
    assert isinstance(_chunker_for_flag(file_path, True), ParquetFileChunker)


def test_datasource_selects_byte_estimate_chunker_when_flag_off(tmp_path):
    file_path = tmp_path / "data.parquet"
    _write_parquet(str(file_path), pa.table({"a": [1, 2, 3]}))
    chunker = _chunker_for_flag(file_path, False)
    assert isinstance(chunker, ByteEstimateParquetFileChunker)
    # ``isinstance`` against the row-group subclass relationship is not a thing
    # here — they're independent ``FileChunker`` subclasses.
    assert not isinstance(chunker, ParquetFileChunker)


def test_explicit_chunker_overrides_flag(tmp_path):
    """An injected ``file_chunker`` wins even when the flag would pick another."""
    from ray.data.context import DataContext

    file_path = tmp_path / "data.parquet"
    _write_parquet(str(file_path), pa.table({"a": [1, 2, 3]}))
    ctx = DataContext.get_current()
    original = ctx.parquet_chunker_row_group_aware
    ctx.parquet_chunker_row_group_aware = False
    try:
        custom = WholeFileChunker()
        ds = ParquetDatasourceV2([str(file_path)], file_chunker=custom)
        assert ds._file_chunker is custom
    finally:
        ctx.parquet_chunker_row_group_aware = original


def _write_multi_row_group_parquet(path, num_rows: int, row_group_size: int):
    table = pa.table({"id": list(range(num_rows))})
    pq.write_table(table, path, row_group_size=row_group_size)
    return table


def test_fragments_from_chunk_metadata_subsets_by_row_group(tmp_path):
    """``_fragments_from_chunk_metadata`` slices a fragment to the explicit range."""
    import pyarrow.dataset as pds

    file_path = str(tmp_path / "multi.parquet")
    # 1000 rows, 100 row groups (row_group_size=10).
    _write_multi_row_group_parquet(file_path, num_rows=1000, row_group_size=10)

    dataset = pds.dataset(file_path, format="parquet")
    (fragment,) = dataset.get_fragments()
    assert fragment.metadata.num_row_groups == 100

    # Explicit range [25, 50) → 25 row groups, starting row offset 250.
    chunk_md = create_chunk_metadata(
        ParquetFileChunkMetadata, row_group_start=25, row_group_end=50
    )
    sub_fragments = _fragments_from_chunk_metadata(fragment, chunk_md)
    assert len(sub_fragments) == 25
    expected_offset = 250  # 25 row groups × 10 rows each precede the range.
    for sub, offset in sub_fragments:
        assert len(sub.row_groups) == 1
        assert offset == expected_offset
        expected_offset += sub.metadata.row_group(sub.row_groups[0].id).num_rows


def test_fragments_from_chunk_metadata_clamps_range_beyond_row_groups(tmp_path):
    """A range beyond the file's actual row-group count is clamped (no crash)."""
    import pyarrow.dataset as pds

    file_path = str(tmp_path / "single.parquet")
    # 5 rows, single row group.
    _write_multi_row_group_parquet(file_path, num_rows=5, row_group_size=5)

    dataset = pds.dataset(file_path, format="parquet")
    (fragment,) = dataset.get_fragments()
    assert fragment.metadata.num_row_groups == 1

    # Fully out-of-range [5, 6) → clamped to [1, 1) → no sub-fragments.
    chunk_md = create_chunk_metadata(
        ParquetFileChunkMetadata, row_group_start=5, row_group_end=6
    )
    assert _fragments_from_chunk_metadata(fragment, chunk_md) == []

    # Partially out-of-range [0, 9) → clamped to [0, 1) → the one real row group.
    chunk_md = create_chunk_metadata(
        ParquetFileChunkMetadata, row_group_start=0, row_group_end=9
    )
    sub_fragments = _fragments_from_chunk_metadata(fragment, chunk_md)
    assert len(sub_fragments) == 1
    assert sub_fragments[0][1] == 0  # row offset


def test_calculate_row_group_range_distributes_evenly():
    # 10 row groups across 3 chunks -> [0,4), [4,7), [7,10) (extras go first).
    assert _calculate_row_group_range(0, 3, 10) == (0, 4)
    assert _calculate_row_group_range(1, 3, 10) == (4, 7)
    assert _calculate_row_group_range(2, 3, 10) == (7, 10)
    # Over-estimated chunk index (beyond the real row-group count) -> None.
    assert _calculate_row_group_range(5, 6, 3) is None


def test_fragments_from_chunk_metadata_reconciles_byte_estimate(tmp_path):
    """Byte-estimate metadata is reconciled to a real row-group range."""
    import pyarrow.dataset as pds

    file_path = str(tmp_path / "multi.parquet")
    # 100 rows, 10 row groups (row_group_size=10).
    _write_multi_row_group_parquet(file_path, num_rows=100, row_group_size=10)

    dataset = pds.dataset(file_path, format="parquet")
    (fragment,) = dataset.get_fragments()
    assert fragment.metadata.num_row_groups == 10

    # chunk 1 of 3 over 10 row groups -> range [4, 7) -> 3 sub-fragments,
    # starting at row offset 40.
    chunk_md = create_chunk_metadata(
        ByteEstimateParquetFileChunkMetadata, chunk_idx=1, total_num_chunks=3
    )
    sub_fragments = _fragments_from_chunk_metadata(fragment, chunk_md)
    assert len(sub_fragments) == 3
    assert sub_fragments[0][1] == 40


def test_fragments_from_chunk_metadata_byte_estimate_over_count_drops_chunk(tmp_path):
    """An over-estimated chunk (beyond real row groups) yields no sub-fragments."""
    import pyarrow.dataset as pds

    file_path = str(tmp_path / "single.parquet")
    _write_multi_row_group_parquet(file_path, num_rows=5, row_group_size=5)
    dataset = pds.dataset(file_path, format="parquet")
    (fragment,) = dataset.get_fragments()
    assert fragment.metadata.num_row_groups == 1

    # 5 estimated chunks but only 1 row group -> chunk_idx=3 is dropped.
    chunk_md = create_chunk_metadata(
        ByteEstimateParquetFileChunkMetadata, chunk_idx=3, total_num_chunks=5
    )
    assert _fragments_from_chunk_metadata(fragment, chunk_md) == []


def _read_via_reader(reader, manifest):
    return list(reader.read(manifest))


def test_parquet_file_reader_reads_chunked_manifest(tmp_path):
    """End-to-end: a manifest with per-chunk rows is read into the same rows
    as a single whole-file manifest."""
    file_path = str(tmp_path / "data.parquet")
    expected_rows = 200
    _write_multi_row_group_parquet(file_path, num_rows=expected_rows, row_group_size=20)
    file_size = os.path.getsize(file_path)

    reader_whole = ParquetFileReader()
    whole_manifest = FileManifest.construct_manifest([file_path], [file_size], [None])
    whole_tables = _read_via_reader(reader_whole, whole_manifest)
    whole_rows = pa.concat_tables(whole_tables).column("id").to_pylist()

    # target_chunk_size=1 forces one chunk per row group.
    chunker = ParquetFileChunker(target_chunk_size=1)
    chunks = list(chunker.generate_chunk_metadatas(file_path, file_size))
    assert len(chunks) > 1, "test setup expects ParquetFileChunker to chunk"

    paths = [file_path] * len(chunks)
    chunk_metadatas = [md for md, _ in chunks]
    chunk_sizes = [sz for _, sz in chunks]
    chunked_manifest = FileManifest.construct_manifest(
        paths, chunk_sizes, chunk_metadatas
    )

    reader_chunked = ParquetFileReader()
    chunked_tables = _read_via_reader(reader_chunked, chunked_manifest)
    chunked_rows = pa.concat_tables(chunked_tables).column("id").to_pylist()

    assert sorted(chunked_rows) == sorted(whole_rows) == list(range(expected_rows))


def test_parquet_file_reader_chunked_row_hashes_are_unique(tmp_path):
    """Row hashes must remain unique across chunked sub-fragments of the
    same file.

    Regression: ``_read_fragments_sequential`` previously reseeded
    ``offset=0`` for every fragment. Since chunked sub-fragments share
    ``fragment.path``, ``_compute_row_hashes(path, 0, n)`` collided across
    row groups of the same file.
    """
    file_path = str(tmp_path / "data.parquet")
    expected_rows = 200
    _write_multi_row_group_parquet(file_path, num_rows=expected_rows, row_group_size=20)
    file_size = os.path.getsize(file_path)

    chunker = ParquetFileChunker(target_chunk_size=1)
    chunks = list(chunker.generate_chunk_metadatas(file_path, file_size))
    assert len(chunks) > 1, "test setup expects ParquetFileChunker to chunk"

    paths = [file_path] * len(chunks)
    chunk_metadatas = [md for md, _ in chunks]
    chunk_sizes = [sz for _, sz in chunks]
    chunked_manifest = FileManifest.construct_manifest(
        paths, chunk_sizes, chunk_metadatas
    )

    reader = ParquetFileReader(include_row_hash=True)
    chunked_tables = list(reader.read(chunked_manifest))
    hashes = pa.concat_tables(chunked_tables).column("row_hash").to_pylist()
    assert len(hashes) == expected_rows
    assert (
        len(set(hashes)) == expected_rows
    ), "row_hash must be unique across chunked sub-fragments of one file"


def test_parquet_file_reader_handles_out_of_range_chunks(tmp_path):
    """Defensively clamped out-of-range chunk metadata yields no rows, no crash.

    The chunker never emits out-of-range ranges (they're computed from the
    same footer the reader sees), but a hand-constructed range beyond the
    file's row groups must be handled gracefully.
    """
    file_path = str(tmp_path / "tiny.parquet")
    # 5 rows, single row group.
    _write_multi_row_group_parquet(file_path, num_rows=5, row_group_size=5)
    file_size = os.path.getsize(file_path)

    # Explicit range entirely beyond the file's one row group.
    out_of_range = create_chunk_metadata(
        ParquetFileChunkMetadata, row_group_start=3, row_group_end=4
    )
    manifest = FileManifest.construct_manifest([file_path], [file_size], [out_of_range])

    reader = ParquetFileReader()
    tables = list(reader.read(manifest))
    assert sum(t.num_rows for t in tables) == 0
