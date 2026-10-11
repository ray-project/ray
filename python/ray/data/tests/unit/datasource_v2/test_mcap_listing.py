"""Unit tests for listing MCAP files as chunk rows and packing them into read tasks.

The indexer and the bin packer are called directly, with no Ray cluster.
"""

import dataclasses
import importlib.util
import os
from typing import Any, Iterable, List, Tuple

import pyarrow as pa
import pytest

from ray.data._internal.datasource_v2.common.online_bin_packer import OnlineBinPacker
from ray.data._internal.datasource_v2.formats.mcap import mcap_datasource_v2
from ray.data._internal.datasource_v2.formats.mcap.mcap_datasource_v2 import (
    MCAPDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import (
    ROW_ID_COLUMN,
    TimeRange,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_summary import (
    _READ_PEAK_FACTOR,
    chunk_unit_id,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data.tests.unit.datasource_v2.mcap_testing import (
    BASE_TIME,
    CHUNKED_FILE_MESSAGES,
    STEP,
    list_manifests,
    pack,
    read_all,
    round_robin_messages,
    scanner_for,
    summary_of,
    without_summary_records,
    write_cut_short,
    write_mcap,
    write_mcap_without_summary_schemas,
    write_mixed_channels,
)

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("mcap") is None,
    reason="mcap module not available. Install with: pip install mcap",
)


def unit_ids(chunk_metadata: Any) -> List[int]:
    """The chunk offsets a listing row reads, from its ``unit_ids``."""
    return [int(i) for i in chunk_metadata["unit_ids"]]


def chunk_units(tasks: Iterable[FileManifest]) -> List[Tuple[str, int]]:
    """Every ``(path, chunk offset)`` the tasks read."""
    return [
        (path, i)
        for task in tasks
        for path, md in zip(task.paths, task.file_chunk_metadatas)
        for i in unit_ids(md)
    ]


def record_read_back(monkeypatch) -> List[int]:
    """Record the offset of every earlier chunk a reader reads back for records."""
    from ray.data._internal.datasource_v2.formats.mcap import mcap_chunks

    real_scan = mcap_chunks._scan_declarations
    offsets: List[int] = []

    def scan(f, chunk_index, declared):
        offsets.append(chunk_index.chunk_start_offset)
        real_scan(f, chunk_index, declared)

    monkeypatch.setattr(mcap_chunks, "_scan_declarations", scan)
    return offsets


def test_indexer_lists_one_row_per_chunk(chunked_file):
    """Each chunk is one listing row, carrying its offset, its message count and a
    size above its uncompressed bytes."""
    datasource = MCAPDatasourceV2([chunked_file])

    (manifest,) = list_manifests(datasource)
    summary = summary_of(chunked_file)

    assert len(manifest) == len(summary.chunk_indexes) == CHUNKED_FILE_MESSAGES
    offsets = [unit_ids(md)[0] for md in manifest.file_chunk_metadatas]
    assert offsets == [c.chunk_start_offset for c in summary.chunk_indexes]
    assert [int(md["num_rows"]) for md in manifest.file_chunk_metadatas] == [1] * 9
    assert all(
        int(md["size_bytes"]) > chunk.uncompressed_size
        for md, chunk in zip(manifest.file_chunk_metadatas, summary.chunk_indexes)
    )
    # Row counts are estimates: never trusted as exact survivor counts.
    assert not any(md["fully_matched"] for md in manifest.file_chunk_metadatas)
    assert all(path == chunked_file for path in manifest.paths)


def test_a_task_reading_part_of_a_file_reports_a_chunk_as_its_read_unit(
    chunked_file,
):
    """A task that reads some chunks of a file names its first chunk as the read
    unit, not the file. Handed back on resume, the unit skips that chunk only,
    never chunks another task still has to read."""
    from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
        ReadUnitPosition,
        SynthesizedColumn,
    )

    class UnitId(SynthesizedColumn):
        """The id of the read unit each row came from."""

        name = "unit_id"
        type = pa.string()

        def compute(self, position: ReadUnitPosition, num_rows: int) -> pa.Array:
            return pa.repeat(pa.scalar(position.unit.id, pa.string()), num_rows)

    datasource = MCAPDatasourceV2([chunked_file])
    scanner = scanner_for(datasource)
    scanner = dataclasses.replace(scanner, synthesized_columns=(UnitId(),))
    (manifest,) = list_manifests(datasource)
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    first_task = FileManifest(block.slice(0, 3))

    table = read_all(datasource, first_task, scanner=scanner)

    (unit,) = set(table.column("unit_id").to_pylist())
    assert unit == f"{chunked_file}#c={unit_ids(manifest.file_chunk_metadatas[0])[0]}"

    (resumed,) = list_manifests(datasource, excluded_read_unit_ids={unit})
    assert len(resumed) == len(manifest) - 1


def test_indexer_prunes_chunks_by_topic_and_time(chunked_file):
    """``topics`` and ``time_range`` drop the chunks they rule out, and a file with
    no matching chunk is not listed at all."""
    (by_topic,) = list_manifests(MCAPDatasourceV2([chunked_file], topics=["/a"]))
    assert len(by_topic) == 3  # messages 0, 3, 6

    window = TimeRange(start_time=BASE_TIME + 2 * STEP, end_time=BASE_TIME + 5 * STEP)
    (by_time,) = list_manifests(MCAPDatasourceV2([chunked_file], time_range=window))
    assert len(by_time) == 3  # messages 2, 3, 4

    assert list_manifests(MCAPDatasourceV2([chunked_file], topics=["/nope"])) == []
    assert (
        list_manifests(
            MCAPDatasourceV2(
                [chunked_file],
                time_range=TimeRange(start_time=1, end_time=2),
            )
        )
        == []
    )


def test_indexer_honors_excluded_read_unit_ids(tmp_path, chunked_file):
    """An excluded chunk and an excluded whole file are left out of the listing."""
    other = os.path.join(tmp_path, "other.mcap")
    write_mcap(other, round_robin_messages(2))
    datasource = MCAPDatasourceV2([chunked_file, other])
    summary = summary_of(chunked_file)
    excluded = {
        chunk_unit_id(chunked_file, summary.chunk_indexes[0].chunk_start_offset),
        other,  # a whole file a checkpoint finished
    }

    manifests = list_manifests(datasource, excluded_read_unit_ids=excluded)

    assert len(manifests) == 1
    assert len(manifests[0]) == CHUNKED_FILE_MESSAGES - 1
    assert set(manifests[0].paths) == {chunked_file}


def test_file_without_index_is_one_whole_file_row(tmp_path):
    """A file without a chunk index is listed as one whole-file row, and its row
    ids count messages from the start of the file."""
    path = os.path.join(tmp_path, "unindexed.mcap")
    write_mcap(path, round_robin_messages(3), index=False)
    datasource = MCAPDatasourceV2([path], include_row_id=True)

    (manifest,) = list_manifests(datasource)
    assert len(manifest) == 1
    assert manifest.file_chunk_metadatas[0] is None

    table = read_all(datasource, manifest)
    assert table.num_rows == 3
    assert table.column("data").to_pylist() == [{"seq": 0}, {"seq": 1}, {"seq": 2}]
    assert table.column(ROW_ID_COLUMN).to_pylist() == [
        f"{path}#m=0",
        f"{path}#m=1",
        f"{path}#m=2",
    ]


def test_summary_without_channels_is_one_whole_file_row(chunked_file, monkeypatch):
    """A file whose summary lists no channels is listed whole and still read."""
    without_summary_records(monkeypatch, "channels")
    datasource = MCAPDatasourceV2([chunked_file], topics=["/a"])

    (manifest,) = list_manifests(datasource)
    assert len(manifest) == 1 and manifest.file_chunk_metadatas[0] is None

    table = read_all(datasource, manifest)
    assert table.column("topic").to_pylist() == ["/a"] * 3


def test_message_types_apply_when_the_summary_lists_no_schemas(tmp_path):
    """``message_types`` applies at read time when the summary lists no schemas.

    The listing cannot filter by schema, so the reader uses the schema records
    inside the chunks.
    """
    path = os.path.join(tmp_path, "noschemas.mcap")
    write_mcap_without_summary_schemas(path)
    summary = summary_of(path)
    assert summary.channels and not summary.schemas
    datasource = MCAPDatasourceV2([path], message_types=["other_schema"])

    (manifest,) = list_manifests(datasource)
    table = read_all(datasource, manifest)

    assert table.column("topic").to_pylist() == ["/b"] * 3
    assert table.column("schema_name").to_pylist() == ["other_schema"] * 3


def test_schemas_declared_in_earlier_chunks_are_found(tmp_path):
    """A task that owns only later chunks finds schemas written in earlier ones.

    The schema columns are filled and ``message_types`` applies.
    """
    path = os.path.join(tmp_path, "noschemas_split.mcap")
    write_mcap_without_summary_schemas(path)
    datasource = MCAPDatasourceV2([path])
    (manifest,) = list_manifests(datasource)
    block = manifest.as_block()
    assert isinstance(block, pa.Table) and len(manifest) == 6
    later = FileManifest(block.slice(3))

    table = read_all(datasource, later)

    assert table.column("topic").to_pylist() == ["/b", "/a", "/b"]
    assert table.column("schema_name").to_pylist() == [
        "other_schema",
        "test_schema",
        "other_schema",
    ]
    assert all(table.column("schema_data").to_pylist())

    filtered = MCAPDatasourceV2([path], message_types=["other_schema"])
    (manifest,) = list_manifests(filtered)
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    table = read_all(filtered, FileManifest(block.slice(3)))
    assert table.column("topic").to_pylist() == ["/b", "/b"]


def test_channels_declared_only_in_earlier_chunks_are_found(chunked_file, monkeypatch):
    """The reader finds channel records that only earlier chunks hold.

    Each channel record sits in the first chunk that uses it, and the reader's
    summary lists none, so a task owning only later chunks reads back for them.
    """
    # Only the reader gets the stripped summary: the listing still has a row per chunk.
    datasource = MCAPDatasourceV2([chunked_file])
    (manifest,) = list_manifests(datasource)
    assert len(manifest) == CHUNKED_FILE_MESSAGES
    scanner = scanner_for(datasource)
    without_summary_records(monkeypatch, "channels", "schemas")

    table = read_all(datasource, manifest, scanner=scanner)

    assert table.num_rows == CHUNKED_FILE_MESSAGES
    assert table.column("topic").to_pylist() == ["/a", "/b", "/c"] * 3
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    read_back = record_read_back(monkeypatch)
    tail = read_all(datasource, FileManifest(block.slice(6)), scanner=scanner)
    assert tail.column("topic").to_pylist() == ["/a", "/b", "/c"]
    # Chunks 0, 1 and 2 declare /a, /b and /c.
    summary = summary_of(chunked_file)
    assert read_back == [c.chunk_start_offset for c in summary.chunk_indexes[:3]]


def test_unreadable_file_is_listed_as_one_row_naming_no_chunk(tmp_path, chunked_file):
    """A file whose summary cannot be read does not fail the listing. It is one
    row that names no chunk and weighs a whole read task; the other file keeps
    its chunk rows."""
    cut_short = write_cut_short(os.path.join(tmp_path, "cut_short.mcap"), chunked_file)
    datasource = MCAPDatasourceV2([chunked_file, cut_short])

    rows = [
        (path, len(md["unit_ids"]), int(md["size_bytes"]))
        for manifest in list_manifests(datasource)
        for path, md in zip(manifest.paths, manifest.file_chunk_metadatas)
    ]

    budget = mcap_datasource_v2._BIN_PACKING_BYTES
    assert [row for row in rows if row[0] == cut_short] == [(cut_short, 0, budget)]
    assert sum(path == chunked_file for path, _, _ in rows) == CHUNKED_FILE_MESSAGES


def test_unreadable_file_gets_a_read_task_of_its_own(tmp_path):
    """The packer puts no other file in the read task of an unreadable file, so
    reading it fails that task alone."""
    paths = []
    for i in range(3):
        path = os.path.join(tmp_path, f"f{i}.mcap")
        write_mcap(path, round_robin_messages(4))
        paths.append(path)
    # Listed between valid files, as a crashed run in a folder of recordings.
    cut_short = write_cut_short(os.path.join(tmp_path, "f1_cut.mcap"), paths[0])
    datasource = MCAPDatasourceV2(paths + [cut_short])

    tasks = pack(datasource, paths + [cut_short])

    files_per_task = [set(task.paths) for task in tasks]
    assert {cut_short} in files_per_task
    assert sum(cut_short in files for files in files_per_task) == 1
    # The valid files' chunks are read as if the folder held no bad file.
    valid_tasks = [task for task in tasks if cut_short not in task.paths]
    assert sorted(chunk_units(valid_tasks)) == sorted(
        chunk_units(pack(MCAPDatasourceV2(paths), paths))
    )


@pytest.mark.parametrize(
    "include_metadata, include_row_id, include_paths",
    [(False, False, False), (True, False, False), (True, True, True)],
)
def test_listed_sizes_bound_the_table_a_read_builds(
    tmp_path, include_metadata, include_row_id, include_paths
):
    """A chunk's listed size is ``_READ_PEAK_FACTOR`` times a bound on the bytes
    its rows take as a table, and the bound is tight: a read of the file builds
    a table between 80% and 100% of it, whichever columns the rows carry."""
    path = os.path.join(tmp_path, "mixed.mcap")
    write_mixed_channels(path, num_messages=400, payload_bytes=1000)
    datasource = MCAPDatasourceV2(
        [path],
        include_metadata=include_metadata,
        include_row_id=include_row_id,
        include_paths=include_paths,
    )
    (manifest,) = list_manifests(datasource)

    table = read_all(datasource, manifest)

    listed = sum(int(md["size_bytes"]) for md in manifest.file_chunk_metadatas)
    bound = listed / _READ_PEAK_FACTOR
    assert 0.8 * bound <= table.nbytes <= bound


def test_chunk_rows_fall_back_to_statistics_without_message_indexes(tmp_path):
    """A file with a chunk index but no message indexes splits the statistics'
    message count across its chunks by size, rounded per chunk."""
    from mcap.writer import IndexType, Writer

    path = os.path.join(tmp_path, "no_message_index.mcap")
    with open(path, "wb") as stream:
        writer = Writer(stream, chunk_size=1, index_types=IndexType.CHUNK)
        writer.start(profile="", library="ray-test")
        channel = writer.register_channel("/a", "cdr", 0)
        for i in range(6):
            writer.add_message(channel, log_time=i, data=b"x" * 10, publish_time=i)
        writer.finish()
    assert not any(c.message_index_offsets for c in summary_of(path).chunk_indexes)

    (manifest,) = list_manifests(MCAPDatasourceV2([path]))

    rows = [int(md["num_rows"]) for md in manifest.file_chunk_metadatas]
    assert min(rows) >= 1
    assert abs(sum(rows) - 6) <= len(rows) // 2


def test_bin_packer_groups_chunks_by_their_listed_size(tmp_path):
    """Bins of two chunks' worth of bytes put every chunk of three files in exactly
    one read task."""
    paths = []
    for i in range(3):
        path = os.path.join(tmp_path, f"f{i}.mcap")
        write_mcap(path, round_robin_messages(4))
        paths.append(path)
    datasource = MCAPDatasourceV2(paths)
    manifests = list_manifests(datasource)
    chunk_bytes = max(
        int(md["size_bytes"]) for m in manifests for md in m.file_chunk_metadatas
    )

    packer = OnlineBinPacker(max_bin_bytes=2 * chunk_bytes)
    for manifest in manifests:
        packer.add_input(manifest)
    packer.finalize()
    bins = []
    while packer.has_partition():
        bins.append(packer.next_partition())

    # Twelve chunks of about the same size, two per bin: about six tasks.
    assert 4 <= len(bins) <= 12
    seen = []
    for manifest in bins:
        for path, md in zip(manifest.paths, manifest.file_chunk_metadatas):
            seen.extend((path, i) for i in unit_ids(md))
    assert len(seen) == 12 and len(set(seen)) == 12


def test_packer_packs_per_listing_shard(tmp_path):
    """Packing per listing shard still puts every chunk in exactly one read task."""
    paths = []
    for i in range(4):
        path = os.path.join(tmp_path, f"f{i}.mcap")
        write_mcap(path, round_robin_messages(4))
        paths.append(path)
    datasource = MCAPDatasourceV2(paths)
    partitioner = datasource.get_file_partitioner()
    assert partitioner is not None and not partitioner.requires_global_input

    per_shard = pack(datasource, paths[:2]) + pack(datasource, paths[2:])
    whole = pack(datasource, paths)

    assert sorted(chunk_units(per_shard)) == sorted(chunk_units(whole))
    assert len(set(chunk_units(per_shard))) == len(chunk_units(per_shard)) == 16
    # Two shards may each leave one bin under-filled.
    assert len(per_shard) <= len(whole) + 1


def listed_rows(datasource: MCAPDatasourceV2) -> List[Tuple[str, List[int], int]]:
    """Every listing row in listing order: its path, chunk offsets and weight."""
    return [
        (str(path), unit_ids(md), int(md["size_bytes"]))
        for manifest in list_manifests(datasource)
        for path, md in zip(manifest.paths, manifest.file_chunk_metadatas)
    ]


def test_listing_order_does_not_depend_on_summary_concurrency(tmp_path, monkeypatch):
    """Listing 30 files with one summary read at a time or 32 at once gives the
    same rows in the same order, so the packer forms the same read tasks."""
    for i in range(30):
        write_mcap(os.path.join(tmp_path, f"f{i:02d}.mcap"), round_robin_messages(4))

    monkeypatch.setenv("RAY_DATA_MCAP_SUMMARY_IO_CONCURRENCY", "1")
    one_at_a_time = listed_rows(MCAPDatasourceV2([str(tmp_path)]))
    monkeypatch.setenv("RAY_DATA_MCAP_SUMMARY_IO_CONCURRENCY", "32")
    many_at_once = listed_rows(MCAPDatasourceV2([str(tmp_path)]))

    assert len(one_at_a_time) == 30 * 4
    assert many_at_once == one_at_a_time


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
