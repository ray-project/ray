"""Unit tests for :class:`ParquetDatasourceV2`.

These tests exercise schema inference, scanner/estimator creation, and
include-paths schema augmentation against a local tmpdir — they do not
spin up Ray.
"""

import os

import pyarrow as pa
import pyarrow.parquet as pq

from ray.data._internal.datasource_v2.chunkers.file_chunker import (
    ParquetFileChunker,
    ParquetFileChunkMetadata,
    WholeFileChunker,
    create_chunk_metadata,
)
from ray.data._internal.datasource_v2.chunkers.parquet_file_chunking_utils import (
    fragments_to_read_for_manifest,
)
from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.parquet_datasource_v2 import (
    ParquetDatasourceV2,
)
from ray.data._internal.datasource_v2.readers.in_memory_size_estimator import (
    ParquetFooterDerivedInMemorySizeEstimator,
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


def test_get_size_estimator_returns_footer_derived(tmp_path):
    # V2 sizes partitions with the footer-derived, type-aware estimator: the
    # chunker stamps each chunk's decoded Arrow size onto its metadata at listing
    # time, and this estimator reads that hint (per-chunk ratio fallback).
    datasource = ParquetDatasourceV2([str(tmp_path)])
    assert isinstance(
        datasource.get_size_estimator(), ParquetFooterDerivedInMemorySizeEstimator
    )


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


def _write_multi_row_group_parquet(path, num_rows: int, row_group_size: int):
    table = pa.table({"id": list(range(num_rows))})
    pq.write_table(table, path, row_group_size=row_group_size)
    return table


def test_fragments_to_read_coalesces_sister_chunks(tmp_path):
    """Sister chunks of one file collapse into a single contiguous-run scan."""
    import pyarrow.dataset as pds

    file_path = str(tmp_path / "multi.parquet")
    _write_multi_row_group_parquet(file_path, num_rows=100, row_group_size=10)
    (fragment,) = pds.dataset(file_path, format="parquet").get_fragments()

    # Two adjacent row-group chunks of the same file: [0, 2) and [2, 4).
    chunk_a = create_chunk_metadata(
        ParquetFileChunkMetadata, row_group_start=0, row_group_end=2, in_memory_size=0
    )
    chunk_b = create_chunk_metadata(
        ParquetFileChunkMetadata, row_group_start=2, row_group_end=4, in_memory_size=0
    )
    frags = fragments_to_read_for_manifest(
        {fragment.path: fragment},
        [fragment.path, fragment.path],
        [chunk_a, chunk_b],
    )
    # One coalesced scan over [0, 4): a single open instead of one per chunk.
    assert len(frags) == 1
    sub, offset = frags[0]
    assert offset == 0
    assert len(sub.row_groups) == 4


def test_fragments_to_read_groups_by_file(tmp_path):
    """Chunks of different files map to one scan per file (no cross-file scan)."""
    import pyarrow.dataset as pds

    path_to_fragment, paths, metas = {}, [], []
    for name in ("a", "b"):
        p = str(tmp_path / f"{name}.parquet")
        _write_multi_row_group_parquet(p, num_rows=40, row_group_size=10)  # 4 rgs
        (fragment,) = pds.dataset(p, format="parquet").get_fragments()
        path_to_fragment[fragment.path] = fragment
        paths.append(fragment.path)
        metas.append(
            create_chunk_metadata(
                ParquetFileChunkMetadata,
                row_group_start=0,
                row_group_end=4,
                in_memory_size=0,
            )
        )
    frags = fragments_to_read_for_manifest(path_to_fragment, paths, metas)
    assert len(frags) == 2  # one sub-fragment per file
    assert {sub.path for sub, _ in frags} == set(paths)


def test_fragments_to_read_whole_file_chunk_passes_through(tmp_path):
    """A ``None`` (whole-file) chunk yields the full fragment at offset 0."""
    import pyarrow.dataset as pds

    file_path = str(tmp_path / "whole.parquet")
    _write_multi_row_group_parquet(file_path, num_rows=20, row_group_size=10)
    (fragment,) = pds.dataset(file_path, format="parquet").get_fragments()
    frags = fragments_to_read_for_manifest(
        {fragment.path: fragment}, [fragment.path], [None]
    )
    assert len(frags) == 1
    assert frags[0][1] == 0


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
        ParquetFileChunkMetadata, row_group_start=3, row_group_end=4, in_memory_size=0
    )
    manifest = FileManifest.construct_manifest([file_path], [file_size], [out_of_range])

    reader = ParquetFileReader()
    tables = list(reader.read(manifest))
    assert sum(t.num_rows for t in tables) == 0


class _StubFragment:
    def __init__(self, path: str):
        self.path = path


def test_resolve_num_read_workers_caps_at_distinct_files():
    """Per-task fragment-read concurrency keys on DISTINCT files, not fragment
    count: same-file sub-scans (file-affinity) stay sequential; cross-file
    partitions (round-robin) parallelize one worker per file, capped at the
    thread budget."""
    from ray.data._internal.datasource_v2.readers.file_reader import (
        _resolve_num_read_workers,
    )

    # File-affinity: many sub-fragments of ONE file -> sequential (1 worker).
    same_file = [(_StubFragment("a.parquet"), off) for off in (0, 2, 4, 6)]
    assert _resolve_num_read_workers(same_file, num_threads=4) == 1

    # Round-robin: 3 distinct files (one repeated) under a 4-thread budget -> 3.
    mixed = [
        (_StubFragment("a.parquet"), 0),
        (_StubFragment("b.parquet"), 0),
        (_StubFragment("c.parquet"), 0),
        (_StubFragment("a.parquet"), 5),
    ]
    assert _resolve_num_read_workers(mixed, num_threads=4) == 3

    # More distinct files than threads -> capped at the thread budget.
    many = [(_StubFragment(f"{c}.parquet"), 0) for c in "abcdef"]
    assert _resolve_num_read_workers(many, num_threads=4) == 4
