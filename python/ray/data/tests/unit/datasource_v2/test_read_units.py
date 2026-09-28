"""Unit tests for read unit exclusion at listing time.

A resumed job hands the listing task the ids of the read units it already
finished (``TaskContext.kwargs[EXCLUDED_READ_UNIT_IDS_KWARG_NAME]``). The ids
are the ones the reader reports in ``ReadUnitPosition.unit`` (see
``test_synthesized_columns.py``): a file path for a whole file, ``<path>#rg<N>``
for one Parquet row group. Whole files are dropped before any footer read, row
groups inside the footer reader before predicate pruning and limit accounting,
so the partitioner only ever sees the remaining work.
"""
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyarrow.fs import LocalFileSystem

from ray.data._internal.datasource_v2.chunkers.file_chunker import (
    ParquetRowGroupChunkMetadata,
    create_chunk_metadata,
)
from ray.data._internal.datasource_v2.chunkers.parquet_file_chunking_utils import (
    _row_group_unit_id,
)
from ray.data._internal.datasource_v2.listing import footer_file_indexer
from ray.data._internal.datasource_v2.listing.file_indexer import (
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.listing.file_manifest import (
    PATH_COLUMN_NAME,
    FileManifest,
)
from ray.data._internal.datasource_v2.listing.footer_file_indexer import (
    FooterFileIndexer,
    _file_chunks_to_manifest,
)
from ray.data._internal.datasource_v2.listing.footer_reader import FooterReader
from ray.data._internal.datasource_v2.listing.listing_utils import (
    list_files_for_each_block,
    partition_files,
)
from ray.data._internal.datasource_v2.partitioners.online_bin_packer import (
    OnlineBinPacker,
)
from ray.data._internal.datasource_v2.read_units import (
    EXCLUDED_READ_UNIT_IDS_KWARG_NAME,
)
from ray.data._internal.datasource_v2.readers.synthesized_columns import (
    ReadUnitPosition,
    SynthesizedColumn,
)
from ray.data._internal.datasource_v2.scanners.parquet_scanner import ParquetScanner
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data.expressions import col

ROW_GROUP_SIZE = 25
NUM_ROW_GROUPS = 4
NUM_ROWS = ROW_GROUP_SIZE * NUM_ROW_GROUPS
SCHEMA = pa.schema([("id", pa.int64())])


@dataclass(frozen=True)
class UnitIdColumn(SynthesizedColumn):
    """Stand-in for a checkpoint ID column: the read unit id of every row."""

    name = "unit_id"
    type = pa.string()
    requires_read_unit_boundaries = True

    def compute(self, position: ReadUnitPosition, num_rows: int) -> pa.Array:
        return pa.repeat(pa.scalar(position.unit.id, type=pa.string()), num_rows)


def _write_file(path, num_rows=NUM_ROWS):
    pq.write_table(
        pa.table({"id": list(range(num_rows))}), path, row_group_size=ROW_GROUP_SIZE
    )
    return str(path), path.stat().st_size


def _row_group_chunk(row_group_ids):
    row_group_ids = tuple(row_group_ids)
    return create_chunk_metadata(
        ParquetRowGroupChunkMetadata,
        row_group_ids=row_group_ids,
        num_rows=ROW_GROUP_SIZE * len(row_group_ids),
        uncompressed_size=ROW_GROUP_SIZE * 8 * len(row_group_ids),
        fully_matched=True,
        rg_sizes=(),
        rg_rows=(),
    )


def _read(manifest):
    reader = ParquetScanner(
        schema=SCHEMA, synthesized_columns=(UnitIdColumn(),)
    ).create_reader()
    tables = list(reader.read(manifest))
    return pa.concat_tables(tables) if tables else SCHEMA.empty_table()


def _footer_reader(**kwargs):
    return FooterReader(filesystem=LocalFileSystem(), io_concurrency=2, **kwargs)


def _chunked_row_groups(chunks):
    """``(rg_idx, fully_matched)`` per row group of a ``FileChunks``."""
    return [(rg.rg_idx, rg.fully_matched) for rg in chunks.row_groups]


def _task_context(**kwargs):
    return TaskContext(task_idx=0, op_name="test", kwargs=kwargs)


def test_unit_ids_round_trip_through_the_footer_reader(tmp_path):
    """Ids the reader reports are the ids the footer reader leaves out."""
    path, size = _write_file(tmp_path / "data.parquet")
    manifest = FileManifest.construct_manifest(
        paths=[path], sizes=[size], chunk_metadatas=[_row_group_chunk(range(4))]
    )

    table = _read(manifest)
    unit_ids = table.column("unit_id").to_pylist()
    assert sorted(set(unit_ids)) == [_row_group_unit_id(path, rg) for rg in range(4)]
    assert all(unit_ids.count(unit_id) == ROW_GROUP_SIZE for unit_id in set(unit_ids))

    # Half finished: the listing names the rest, and reading it yields the rest.
    finished = {_row_group_unit_id(path, 0), _row_group_unit_id(path, 2)}
    chunks = _footer_reader(excluded_read_unit_ids=finished)._read_and_chunk(path, size)
    assert _chunked_row_groups(chunks) == [(1, True), (3, True)]
    table = _read(_file_chunks_to_manifest(chunks))
    assert set(table.column("unit_id").to_pylist()) == {
        _row_group_unit_id(path, 1),
        _row_group_unit_id(path, 3),
    }
    assert sorted(table.column("id").to_pylist()) == [
        *range(ROW_GROUP_SIZE, 2 * ROW_GROUP_SIZE),
        *range(3 * ROW_GROUP_SIZE, 4 * ROW_GROUP_SIZE),
    ]


def test_footer_reader_ignores_unknown_ids_and_drops_a_finished_file(tmp_path):
    path, size = _write_file(tmp_path / "data.parquet")

    unknown = {"/elsewhere.parquet", _row_group_unit_id(path, 7)}
    chunks = _footer_reader(excluded_read_unit_ids=unknown)._read_and_chunk(path, size)
    assert [rg.rg_idx for rg in chunks.row_groups] == [0, 1, 2, 3]

    # Every row group finished: the file contributes no chunk, and
    # ``read_footers`` yields nothing for it.
    finished = {_row_group_unit_id(path, rg) for rg in range(4)}
    reader = _footer_reader(excluded_read_unit_ids=finished)
    assert reader._read_and_chunk(path, size).row_groups == ()
    # ``@ray.method`` types the plain method as a remote stub.
    # pyrefly: ignore[not-callable]
    assert list(reader.read_footers([(path, size)])) == []


def test_footer_reader_excludes_before_predicate_and_limit_accounting(tmp_path):
    """A finished row group is gone before the predicate split, so it is neither
    listed as a survivor nor counted as fully matched toward a pushed-down limit."""
    path, size = _write_file(tmp_path / "data.parquet")
    # Row groups 0 and 1 (ids 0..49) match entirely, 2 (50..74) partially, 3 not.
    predicate = col("id") < 60

    chunks = _footer_reader(filter_expr=predicate)._read_and_chunk(path, size)
    assert _chunked_row_groups(chunks) == [(0, True), (1, True), (2, False)]

    finished = {_row_group_unit_id(path, 0)}
    chunks = _footer_reader(
        filter_expr=predicate, excluded_read_unit_ids=finished
    )._read_and_chunk(path, size)
    assert _chunked_row_groups(chunks) == [(1, True), (2, False)]


def test_plain_indexer_skips_whole_files_named_by_path(tmp_path):
    path_a, _ = _write_file(tmp_path / "a.parquet")
    path_b, _ = _write_file(tmp_path / "b.parquet")
    indexer = NonSamplingFileIndexer(ignore_missing_paths=False)

    def _listed(excluded):
        manifests = indexer.list_files(
            pa.array([str(tmp_path)]),
            filesystem=LocalFileSystem(),
            excluded_read_unit_ids=excluded,
        )
        return sorted(str(p) for m in manifests for p in m.paths)

    assert _listed(None) == [path_a, path_b]
    assert _listed({path_a}) == [path_b]
    # A row-group id names nothing in a whole-file listing.
    assert _listed({_row_group_unit_id(path_a, 0)}) == [path_a, path_b]
    assert _listed({path_a, path_b}) == []


def test_footer_indexer_skips_whole_files_before_footer_reads_and_forwards_ids(
    tmp_path, monkeypatch
):
    """The footer indexer drops a finished file before it reaches the footer
    reading actors and hands the remaining ids to those actors."""
    import ray

    path_a, size_a = _write_file(tmp_path / "a.parquet")
    path_b, size_b = _write_file(tmp_path / "b.parquet")
    excluded = {path_a, _row_group_unit_id(path_b, 0)}
    actor_kwargs = []
    footer_batches = []

    class _FakeActor:
        def __init__(self):
            self.read_footers = self

        def remote(self, batch, **_kwargs):
            footer_batches.append(batch)
            return []

    class _FakeActorClass:
        @staticmethod
        def options(**_kwargs):
            class _Builder:
                @staticmethod
                def remote(*_args, **kwargs):
                    actor_kwargs.append(kwargs)
                    return _FakeActor()

            return _Builder

    monkeypatch.setenv("RAY_DATA_PARQUET_FOOTER_NUM_ACTORS", "1")
    monkeypatch.setattr(footer_file_indexer, "FooterReaderActor", _FakeActorClass)
    monkeypatch.setattr(ray, "kill", lambda _actor: None)

    indexer = FooterFileIndexer(ignore_missing_paths=False)
    list(
        indexer.list_files(
            pa.array([str(tmp_path)]),
            filesystem=LocalFileSystem(),
            excluded_read_unit_ids=excluded,
        )
    )

    assert actor_kwargs == [{"excluded_read_unit_ids": excluded}]
    assert footer_batches == [[(path_b, size_b)]]


def test_list_task_forwards_excluded_ids_from_task_kwargs(tmp_path):
    path_a, _ = _write_file(tmp_path / "a.parquet")
    path_b, _ = _write_file(tmp_path / "b.parquet")

    def _listed(ctx):
        blocks = list_files_for_each_block(
            [pa.table({PATH_COLUMN_NAME: [str(tmp_path)]})],
            ctx,
            indexer=NonSamplingFileIndexer(ignore_missing_paths=False),
            filesystem=LocalFileSystem(),
        )
        return sorted(str(p) for block in blocks for p in FileManifest(block).paths)

    assert _listed(_task_context()) == [path_a, path_b]
    ctx = _task_context(**{EXCLUDED_READ_UNIT_IDS_KWARG_NAME: {path_a}})
    assert _listed(ctx) == [path_b]
    # Everything finished: the task yields no block at all.
    ctx = _task_context(**{EXCLUDED_READ_UNIT_IDS_KWARG_NAME: {path_a, path_b}})
    assert _listed(ctx) == []


def test_excluding_units_at_listing_time_packs_only_remaining_work(tmp_path):
    """Why exclusion happens in listing: the bin packer never sees the finished
    row groups, so a resumed job gets a few full read tasks instead of one
    near-empty task per touched file."""
    num_files = 20
    files = [_write_file(tmp_path / f"{i}.parquet") for i in range(num_files)]

    def _partitions(excluded):
        reader = _footer_reader(excluded_read_unit_ids=excluded)
        manifests = [
            _file_chunks_to_manifest(chunks).as_block()
            for path, size in files
            for chunks in [reader._read_and_chunk(path, size)]
            if chunks.row_groups
        ]
        return [
            FileManifest(block)
            for block in partition_files(manifests, _task_context(), partitioner)
        ]

    # A bin holds exactly one file's worth of row groups.
    one_file = _footer_reader()._read_and_chunk(*files[0])
    file_bytes = sum(rg.uncompressed_size for rg in one_file.row_groups)
    partitioner = OnlineBinPacker(max_bin_bytes=file_bytes)
    full = _partitions(None)
    assert len(full) == num_files

    # 90% finished: every row group except the last of the first eight files.
    remaining = {(path, 3) for path, _ in files[:8]}
    finished = {
        _row_group_unit_id(path, rg)
        for path, _ in files
        for rg in range(4)
        if (path, rg) not in remaining
    }
    partitioner = OnlineBinPacker(max_bin_bytes=file_bytes)
    resumed = _partitions(finished)

    listed = {
        (str(path), int(rg))
        for m in resumed
        for path, chunk in zip(m.paths, m.file_chunk_metadatas)
        for rg in chunk["row_group_ids"]
    }
    assert listed == remaining
    # Eight quarter-file row groups pack into two bins, not eight.
    assert len(resumed) == 2


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
