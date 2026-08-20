"""Unit tests for :class:`ParquetDatasourceV2`.

These tests exercise schema inference, scanner/estimator creation, and
include-paths schema augmentation against a local tmpdir — they do not
spin up Ray.
"""

import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ray.data._internal.datasource_v2.chunkers.file_chunker import (
    ParquetFileChunker,
    ParquetFileChunkMetadata,
    ParquetRowGroupChunkMetadata,
    WholeFileChunker,
    create_chunk_metadata,
)
from ray.data._internal.datasource_v2.chunkers.parquet_file_chunking_utils import (
    _calculate_row_group_range,
    _fragments_from_chunk_metadata,
)
from ray.data._internal.datasource_v2.chunkers.parquet_footer_types import (
    FileChunks,
    RowGroupInfo,
)
from ray.data._internal.datasource_v2.listing.file_indexer import (
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.listing.footer_file_indexer import (
    FooterFileIndexer,
)
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


@pytest.mark.parametrize(
    "total_row_groups,total_num_chunks,expected_ranges",
    [
        # Even distribution.
        (10, 2, [(0, 5), (5, 10)]),
        (12, 3, [(0, 4), (4, 8), (8, 12)]),
        (20, 4, [(0, 5), (5, 10), (10, 15), (15, 20)]),
        # Uneven distribution: earlier chunks get extra row groups.
        (10, 3, [(0, 4), (4, 7), (7, 10)]),
        (11, 3, [(0, 4), (4, 8), (8, 11)]),
        (13, 4, [(0, 4), (4, 7), (7, 10), (10, 13)]),
        # Edge cases — over-estimated chunk counts must produce ``None``.
        (1, 1, [(0, 1)]),
        (1, 2, [(0, 1), None]),
        (0, 1, [None]),
        (5, 10, [(0, 1), (1, 2), (2, 3), (3, 4), (4, 5)] + [None] * 5),
    ],
)
def test_calculate_row_group_range_distribution(
    total_row_groups, total_num_chunks, expected_ranges
):
    """Row-group distribution across chunks is even and covers everything."""
    for chunk_idx in range(total_num_chunks):
        result = _calculate_row_group_range(
            chunk_idx, total_num_chunks, total_row_groups
        )
        expected = (
            expected_ranges[chunk_idx] if chunk_idx < len(expected_ranges) else None
        )
        assert (
            result == expected
        ), f"Chunk {chunk_idx}: expected {expected}, got {result}"

    # No gaps, no overlaps, every row group covered exactly once.
    covered = set()
    for chunk_idx in range(total_num_chunks):
        result = _calculate_row_group_range(
            chunk_idx, total_num_chunks, total_row_groups
        )
        if result is not None:
            start, end = result
            chunk_rows = set(range(start, end))
            assert not (covered & chunk_rows)
            covered.update(chunk_rows)
    assert covered == set(range(total_row_groups))


def _write_multi_row_group_parquet(path, num_rows: int, row_group_size: int):
    table = pa.table({"id": list(range(num_rows))})
    pq.write_table(table, path, row_group_size=row_group_size)
    return table


def test_fragments_from_chunk_metadata_subsets_by_row_group(tmp_path):
    """``_fragments_from_chunk_metadata`` slices a file fragment per chunk."""
    import pyarrow.dataset as pds

    file_path = str(tmp_path / "multi.parquet")
    # 1000 rows, 100 row groups (row_group_size=10).
    _write_multi_row_group_parquet(file_path, num_rows=1000, row_group_size=10)

    dataset = pds.dataset(file_path, format="parquet")
    (fragment,) = dataset.get_fragments()
    assert fragment.metadata.num_row_groups == 100

    # 100 row groups split into 4 chunks -> 25 row groups each.
    chunk_md = create_chunk_metadata(
        ParquetFileChunkMetadata, chunk_idx=1, total_num_chunks=4
    )
    sub_fragments = _fragments_from_chunk_metadata(fragment, chunk_md)
    assert len(sub_fragments) == 25
    # chunk_idx=1, 25 rows/chunk * 10 rows/row_group = starting offset 250.
    expected_offset = 250
    for sub, offset in sub_fragments:
        assert len(sub.row_groups) == 1
        assert offset == expected_offset
        expected_offset += sub.metadata.row_group(sub.row_groups[0].id).num_rows


def test_fragments_from_chunk_metadata_returns_empty_for_out_of_range_chunk(
    tmp_path,
):
    """Over-estimated chunk indices fall off the end → no sub-fragments."""
    import pyarrow.dataset as pds

    file_path = str(tmp_path / "single.parquet")
    # 5 rows, single row group.
    _write_multi_row_group_parquet(file_path, num_rows=5, row_group_size=5)

    dataset = pds.dataset(file_path, format="parquet")
    (fragment,) = dataset.get_fragments()

    # chunk_idx=4 with 5 chunks but only 1 row group → no sub-fragments.
    chunk_md = create_chunk_metadata(
        ParquetFileChunkMetadata, chunk_idx=4, total_num_chunks=5
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

    chunker = ParquetFileChunker(target_chunk_size=1024)
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

    chunker = ParquetFileChunker(target_chunk_size=1024)
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
    """Out-of-range chunk metadata is silently dropped — no exception, no rows."""
    file_path = str(tmp_path / "tiny.parquet")
    _write_multi_row_group_parquet(file_path, num_rows=5, row_group_size=5)

    file_size = os.path.getsize(file_path)
    # ``ParquetFileChunker`` over-estimates here; keep only the over-estimated
    # tail (chunk_idx >= 1) to assert the reader yields no tables.
    chunker = ParquetFileChunker(target_chunk_size=128)
    chunks = list(chunker.generate_chunk_metadatas(file_path, file_size))
    assert len(chunks) > 1

    paths = [file_path] * (len(chunks) - 1)
    out_of_range_metadatas = [md for md, _ in chunks[1:]]
    sizes = [sz for _, sz in chunks[1:]]
    manifest = FileManifest.construct_manifest(paths, sizes, out_of_range_metadatas)

    reader = ParquetFileReader()
    tables = list(reader.read(manifest))
    # All sub-fragments are out-of-range -> 0 tables emitted.
    assert sum(t.num_rows for t in tables) == 0


def test_datasource_uses_footer_indexer_when_flag_is_set(tmp_path, monkeypatch):
    """The footer-based indexer is opt-in; the blind chunker stays the default."""
    file_path = tmp_path / "data.parquet"
    _write_parquet(str(file_path), pa.table({"a": [1, 2, 3]}))
    datasource = ParquetDatasourceV2([str(file_path)])

    assert isinstance(datasource._get_file_indexer(), NonSamplingFileIndexer)

    monkeypatch.setenv("RAY_DATA_PARQUET_ENABLE_FOOTER_INDEXER", "1")
    assert isinstance(datasource._get_file_indexer(), FooterFileIndexer)


def test_footer_indexer_skip_paths_excludes_listed_file(tmp_path, monkeypatch):
    """``skip_paths`` must reach ``FooterFileIndexer`` or excluded files are read."""
    monkeypatch.setenv("RAY_DATA_PARQUET_ENABLE_FOOTER_INDEXER", "1")
    keep = tmp_path / "keep.parquet"
    skip = tmp_path / "skip.parquet"
    _write_parquet(str(keep), pa.table({"a": [1]}))
    _write_parquet(str(skip), pa.table({"a": [2]}))

    datasource = ParquetDatasourceV2([str(keep), str(skip)], skip_paths=[str(skip)])
    indexer = datasource._get_file_indexer()
    assert isinstance(indexer, FooterFileIndexer)

    infos = list(
        indexer.list_file_infos(
            pa.array(datasource.paths), filesystem=datasource.filesystem
        )
    )
    assert [info.path for info in infos] == [datasource.paths[0]]


def test_footer_indexer_skip_paths_ignores_missing_named_path(tmp_path, monkeypatch):
    """A skip-only missing path must not fail listing on the footer indexer."""
    monkeypatch.setenv("RAY_DATA_PARQUET_ENABLE_FOOTER_INDEXER", "1")
    keep = tmp_path / "keep.parquet"
    missing = str(tmp_path / "gone.parquet")
    _write_parquet(str(keep), pa.table({"a": [1]}))

    datasource = ParquetDatasourceV2([str(keep), missing], skip_paths=[missing])
    indexer = datasource._get_file_indexer()
    assert isinstance(indexer, FooterFileIndexer)

    infos = list(
        indexer.list_file_infos(
            pa.array(datasource.paths), filesystem=datasource.filesystem
        )
    )
    assert [info.path for info in infos] == [datasource.paths[0]]


class _RecordingFooterActor:
    """Stands in for a ``FooterReaderActor`` handle, recording call kwargs.

    Lets the wiring be asserted without spinning up Ray: ``read_footers.remote``
    returns the batch's chunks inline, shaped like the streaming generator the
    driver expects (an iterable of refs, each resolving to a list of
    ``FileChunks``).
    """

    def __init__(self, calls):
        self._calls = calls
        self.read_footers = self

    def remote(self, batch, *, result_batch_size, preserve_order):
        self._calls.append(preserve_order)
        # Mimic the real ``read_footers``: honoring the flag yields in ``batch``
        # order, otherwise in completion order -- stubbed as reversed, standing in
        # for any IO-timing permutation.
        ordered = batch if preserve_order else list(reversed(batch))
        return [
            [
                FileChunks(
                    path=path,
                    size=size,
                    row_groups=(
                        RowGroupInfo(rg_idx=0, uncompressed_size=size, num_rows=1),
                    ),
                )
            ]
            for path, size in ordered
        ]


@pytest.fixture
def recorded_preserve_order_flags(monkeypatch):
    """Yield the ``preserve_order`` flags ``read_footers`` was invoked with."""
    import ray
    from ray.data._internal.datasource_v2.listing import footer_file_indexer

    calls = []

    class _FakeActorClass:
        @staticmethod
        def options(**_kwargs):
            class _Builder:
                @staticmethod
                def remote(*_args):
                    return _RecordingFooterActor(calls)

            return _Builder

    monkeypatch.setattr(footer_file_indexer, "FooterReaderActor", _FakeActorClass)
    # The driver resolves refs and tears the pool down; neither needs a real Ray.
    monkeypatch.setattr(ray, "get", lambda ref: ref)
    monkeypatch.setattr(ray, "kill", lambda _actor: None)
    return calls


@pytest.mark.parametrize("preserve_order", [True, False])
def test_footer_indexer_forwards_preserve_order_to_footer_reads(
    tmp_path, monkeypatch, recorded_preserve_order_flags, preserve_order
):
    """``preserve_order`` must reach ``read_footers``, not just path discovery.

    ``read_footers`` yields in completion order by default, so a dropped flag
    leaves manifest order -- and therefore bin packing and downstream read-task
    order -- dependent on footer IO timing.
    """
    monkeypatch.setenv("RAY_DATA_PARQUET_ENABLE_FOOTER_INDEXER", "1")
    monkeypatch.setenv("RAY_DATA_PARQUET_FOOTER_NUM_ACTORS", "2")
    # More files than one footer batch holds, so several dispatches are recorded.
    monkeypatch.setenv("RAY_DATA_PARQUET_FOOTER_BATCH_SIZE", "2")
    paths = []
    for i in range(5):
        path = tmp_path / f"f{i}.parquet"
        _write_parquet(str(path), pa.table({"a": [i]}))
        paths.append(str(path))

    datasource = ParquetDatasourceV2(paths)
    indexer = datasource._get_file_indexer()
    assert isinstance(indexer, FooterFileIndexer)

    manifests = list(
        indexer.list_files(
            pa.array(datasource.paths),
            filesystem=datasource.filesystem,
            preserve_order=preserve_order,
        )
    )

    assert manifests, "expected the footer path to emit manifests"
    # 5 files at a batch size of 2 -> 3 dispatched batches.
    assert recorded_preserve_order_flags == [preserve_order] * 3


def test_footer_indexer_preserves_listing_order_across_batches(
    tmp_path, monkeypatch, recorded_preserve_order_flags
):
    """Order must hold across footer batches, not only within one.

    ``read_footers`` orders a single batch; the driver's FIFO drain of dispatched
    batches is what makes the overall manifest order the listing order.
    """
    monkeypatch.setenv("RAY_DATA_PARQUET_ENABLE_FOOTER_INDEXER", "1")
    monkeypatch.setenv("RAY_DATA_PARQUET_FOOTER_NUM_ACTORS", "3")
    monkeypatch.setenv("RAY_DATA_PARQUET_FOOTER_BATCH_SIZE", "2")
    paths = []
    for i in range(6):
        path = tmp_path / f"f{i}.parquet"
        _write_parquet(str(path), pa.table({"a": [i]}))
        paths.append(str(path))

    datasource = ParquetDatasourceV2(paths)
    indexer = datasource._get_file_indexer()

    manifests = list(
        indexer.list_files(
            pa.array(datasource.paths),
            filesystem=datasource.filesystem,
            preserve_order=True,
        )
    )

    listed = [str(p) for m in manifests for p in m.paths]
    assert listed == datasource.paths


def _write_multi_row_group_parquet(path, num_rows: int, row_group_size: int):
    table = pa.table({"id": list(range(num_rows))})
    pq.write_table(table, path, row_group_size=row_group_size)
    return table


def _row_group_manifest(path, row_group_ids, num_rows):
    """A one-row footer-path manifest selecting explicit row groups of a file."""
    return FileManifest.construct_manifest(
        [path],
        [0],
        [
            create_chunk_metadata(
                ParquetRowGroupChunkMetadata,
                row_group_ids=tuple(row_group_ids),
                num_rows=num_rows,
                # Nominal projected uncompressed size (8-byte int64 ids); only
                # used for footer-free batch sizing, not row selection.
                uncompressed_size=num_rows * 8,
                fully_matched=True,
                rg_sizes=(),
                rg_rows=(),
            )
        ],
    )


def test_parquet_file_reader_reads_selected_row_groups(tmp_path):
    """The reader reads exactly the row groups named by the footer metadata."""
    file_path = str(tmp_path / "data.parquet")
    # 200 rows, 10 row groups of 20 rows each.
    _write_multi_row_group_parquet(file_path, num_rows=200, row_group_size=20)

    # Select row groups 0, 2, 5 -> rows [0,20) + [40,60) + [100,120).
    manifest = _row_group_manifest(file_path, row_group_ids=[0, 2, 5], num_rows=60)
    tables = list(ParquetFileReader().read(manifest))
    rows = sorted(pa.concat_tables(tables).column("id").to_pylist())
    expected = list(range(0, 20)) + list(range(40, 60)) + list(range(100, 120))
    assert rows == sorted(expected)


def test_parquet_file_reader_row_group_row_hashes_are_unique(tmp_path):
    """Row hashes stay unique across per-row-group sub-fragments of one file.

    With ``include_row_hash`` the footer path fans one sub-fragment per row
    group, each seeded with its cumulative file row offset, so hashes can't
    collide across row groups that share ``fragment.path``.
    """
    file_path = str(tmp_path / "data.parquet")
    expected_rows = 200
    _write_multi_row_group_parquet(file_path, num_rows=expected_rows, row_group_size=20)

    manifest = _row_group_manifest(
        file_path, row_group_ids=range(10), num_rows=expected_rows
    )
    reader = ParquetFileReader(include_row_hash=True)
    hashes = pa.concat_tables(reader.read(manifest)).column("row_hash").to_pylist()
    assert len(hashes) == expected_rows
    assert len(set(hashes)) == expected_rows
