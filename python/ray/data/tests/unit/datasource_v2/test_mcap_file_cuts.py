"""Unit tests for cutting MCAP files into read tasks (``mcap_file_cuts``).

Most tests build listing rows directly: one row per chunk, carrying the chunk's
log times in its index record, as ``MCAPSummaryIndexer`` lists them. The rows go
through ``OrderedFileCutter`` in front of ``OnlineBinPacker``, and each test reads
back which chunks every task holds, in the order the tasks leave. The last tests
list and read real files. None needs a Ray cluster.
"""

import importlib.util
import os
from typing import List, Optional, Tuple

import pytest

from ray.data._internal.datasource_v2.common.online_bin_packer import OnlineBinPacker
from ray.data._internal.datasource_v2.formats.mcap import mcap_datasource_v2
from ray.data._internal.datasource_v2.formats.mcap.mcap_datasource_v2 import (
    MCAPDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_file_cuts import (
    OrderedFileCutter,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_io import (
    parse_summary_records,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_task_summary import (
    task_summaries,
    with_listing_records,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FileChunk,
    FileManifest,
)
from ray.data.context import DataContext
from ray.data.tests.unit.datasource_v2.mcap_testing import (
    BASE_TIME,
    CHUNKED_FILE_MESSAGES,
    STEP,
    list_manifests,
    pack,
    read_all,
    write_mcap,
)

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("mcap") is None,
    reason="mcap module not available. Install with: pip install mcap",
)

BUDGET = 100  # bytes per read task in the row-level tests
WEIGHT = 40  # bytes per chunk in those tests: two chunks fit a task, three do not
FILE_SIZE = 1000  # on-disk size of every file in the row-level tests

# A task as the tests read it: the chunk offsets of each file it holds, or
# ``None`` for a file read whole.
Task = List[Tuple[str, Optional[List[int]]]]


def chunk_index_record(start: int, end: int, offset: int) -> bytes:
    """A ``ChunkIndex`` record for a chunk at ``offset`` logged from ``start``
    to ``end``, as the listing carries it."""
    from mcap.data_stream import RecordBuilder
    from mcap.records import ChunkIndex

    builder = RecordBuilder()
    ChunkIndex(
        message_start_time=start,
        message_end_time=end,
        chunk_start_offset=offset,
        chunk_length=1,
        message_index_offsets={},
        message_index_length=0,
        compression="zstd",
        compressed_size=1,
        uncompressed_size=1,
    ).write(builder)
    return builder.end()


def listed_file(path: str, spans: List[Tuple[int, int]], weight=WEIGHT) -> FileManifest:
    """The listing rows of a file whose chunk at offset ``i`` logs ``spans[i]``."""
    runs = [
        FileChunk(unit_ids=(i,), num_rows=1, size_bytes=weight, fully_matched=False)
        for i in range(len(spans))
    ]
    manifest = FileManifest.construct_manifest(
        paths=[path] * len(runs),
        sizes=[FILE_SIZE] * len(runs),
        chunk_metadatas=[run.to_metadata() for run in runs],
    )
    records = [chunk_index_record(s, e, i) for i, (s, e) in enumerate(spans)]
    return with_listing_records(manifest, b"", records)


def one_writer(num_chunks: int) -> List[Tuple[int, int]]:
    """Spans of chunks written one after another: each ends as the next starts."""
    return [(10 * i, 10 * (i + 1)) for i in range(num_chunks)]


def as_tasks(manifests: List[FileManifest]) -> List[Task]:
    """Each task as its ``(path, sorted chunk offsets)`` rows."""
    return [
        [
            (str(path), None if md is None else sorted(int(u) for u in md["unit_ids"]))
            for path, md in zip(m.paths, m.file_chunk_metadatas)
        ]
        for m in manifests
    ]


def cut(
    files: List[FileManifest],
    budget=BUDGET,
    log_time_order=True,
    preserve_order=True,
) -> List[Task]:
    """Feed ``files`` to a cutter one by one, as a listing task does, and return
    every task in the order it leaves. ``preserve_order`` is the read's setting."""
    packer = OnlineBinPacker(budget, requires_global_input=False)
    cutter = OrderedFileCutter(packer, budget=budget, log_time_order=log_time_order)
    options = DataContext.get_current().execution_options
    saved, options.preserve_order = options.preserve_order, preserve_order
    out = []
    try:
        for manifest in files:
            cutter.add_input(manifest)
            while cutter.has_partition():
                out.append(cutter.next_partition())
        cutter.finalize()
        while cutter.has_partition():
            out.append(cutter.next_partition())
    finally:
        options.preserve_order = saved
    return as_tasks(out)


def test_light_file_stays_in_one_task():
    """A file that fits one task is never split, even where today's packer would
    put its first chunk in the room another file left."""
    # One 60-byte chunk leaves 40 bytes in its task: room for one of b's chunks.
    files = [listed_file("a", [(0, 9)], weight=60), listed_file("b", one_writer(2))]

    tasks = cut(files, preserve_order=False)

    assert tasks == [[("a", [0])], [("b", [0, 1])]]


def test_chunks_written_out_of_time_order_are_cut_by_log_time():
    """In a read that does not keep order, chunks are taken by first log time,
    not file offset, so each task spans one stretch of time."""
    #   offset   0      1      2      3      4
    #   logs     20-29  0-9    40-49  10-19  30-39
    spans = [(20, 29), (0, 9), (40, 49), (10, 19), (30, 39)]

    tasks = cut([listed_file("f", spans)], preserve_order=False)

    assert tasks == [[("f", [1, 3])], [("f", [0, 4])], [("f", [2])]]


# A file whose chunks each hold one topic: chunk 5 logs robot state from 10 to 49
# while chunks 1-4 log camera frames in that time. Like a writer that flushes a
# chunk when it fills, the file holds the state chunk after them.
#
#   offset   0     1      2      3      4      5      6      7
#   logs     0-9   10-19  20-29  30-39  40-49  10-49  50-59  60-69
#            cam   cam    cam    cam    cam    state  cam    cam
#
# By first log time the order is 0, 1, 5, 2, 3, 4, 6, 7.
ONE_TOPIC_PER_CHUNK = [
    (0, 9),
    (10, 19),
    (20, 29),
    (30, 39),
    (40, 49),
    (10, 49),
    (50, 59),
    (60, 69),
]


@pytest.mark.parametrize(
    "log_time_order, preserve_order, tasks",
    [
        # Taken by first log time: 0, 1 | 5, 2 | 3, 4 | 6, 7.
        (True, False, [[0, 1], [2, 5], [3, 4], [6, 7]]),
        # Taken in file order.
        (False, True, [[0, 1], [2, 3], [4, 5], [6, 7]]),
    ],
    ids=["unordered_read", "file_order"],
)
def test_overlapping_file_is_cut_at_every_boundary(
    log_time_order, preserve_order, tasks
):
    """A read that does not keep its blocks in order gets them in any order
    anyway, and without ``log_time_order`` the reader yields chunks in file order.
    Either way a file whose chunks overlap in time is cut wherever the next chunk
    would overflow."""
    got = cut(
        [listed_file("f", ONE_TOPIC_PER_CHUNK)],
        log_time_order=log_time_order,
        preserve_order=preserve_order,
    )

    assert got == [[("f", chunks)] for chunks in tasks]


def listed_rows(path, run, record, size=FILE_SIZE, file_records=b""):
    """One listing row of ``path``: chunk metadata ``run`` (``None`` for a file
    read whole) and the chunk index ``record`` it carries."""
    manifest = FileManifest.construct_manifest(
        paths=[path],
        sizes=[size],
        chunk_metadatas=[None if run is None else run.to_metadata()],
    )
    return with_listing_records(manifest, file_records, [record])


def unplaceable_files() -> List[FileManifest]:
    """The listing's files whose rows carry no ``ChunkIndex`` record.

    ::

        whole  30 bytes   listed whole: no chunk index
        bad   100 bytes   unreadable: no chunk and no records
    """
    unreadable = FileChunk(unit_ids=(), num_rows=0, size_bytes=BUDGET)
    return [
        listed_rows("whole", None, None, size=30),
        listed_rows("bad", unreadable, None, file_records=None),
    ]


def test_rows_it_cannot_place_in_time_go_to_the_packer_unchanged():
    """In a read that does not keep order, a file the cutter cannot place in
    time reaches the packer as listed, so it gets the tasks the packer alone
    gives."""
    files = unplaceable_files()
    packer = OnlineBinPacker(BUDGET, requires_global_input=False)
    for manifest in files:
        packer.add_input(manifest)
    packer.finalize()
    alone = []
    while packer.has_partition():
        alone.append(packer.next_partition())

    tasks = cut(files, preserve_order=False)

    assert tasks == as_tasks(alone)
    assert sorted(row for task in tasks for row in task) == [
        ("bad", []),
        ("whole", None),
    ]


def test_rows_it_cannot_place_in_time_keep_listing_order_in_an_ordered_read():
    """In a read that keeps order, those files are packed whole in listing
    order. The two would pass the budget together, so each gets a task of its
    own."""
    tasks = cut(unplaceable_files())

    assert tasks == [[("whole", None)], [("bad", [])]]


def test_ordered_read_keeps_files_whole_in_listing_order():
    """In a read that keeps log-time order, a heavy file is one task that leaves
    after the light files listed before it, and light files share a task only
    with their neighbours."""
    files = [
        listed_file("a", one_writer(1)),
        listed_file("b", one_writer(1)),
        listed_file("heavy", one_writer(3)),
        listed_file("c", one_writer(1)),
    ]

    tasks = cut(files)

    assert tasks == [
        [("a", [0]), ("b", [0])],
        [("heavy", [0, 1, 2])],
        [("c", [0])],
    ]


# --- real files ------------------------------------------------------------------


def chunk_weight(datasource) -> int:
    """The heaviest listed chunk of ``datasource``'s files."""
    return max(
        int(md["size_bytes"])
        for manifest in list_manifests(datasource)
        for md in manifest.file_chunk_metadatas
    )


def ready_tasks(partitioner) -> List[FileManifest]:
    """The tasks ``partitioner`` has ready, in the order they leave."""
    tasks = []
    while partitioner.has_partition():
        tasks.append(partitioner.next_partition())
    return tasks


@pytest.mark.parametrize(
    "chunks_per_task",
    [
        # The default budget holds the whole file.
        None,
        # Room for two chunks and a half, so the file is cut.
        2.5,
    ],
    ids=["light_file", "cut_file"],
)
def test_task_summaries_hold_each_tasks_chunks(
    chunked_file, monkeypatch, chunks_per_task
):
    """Each task's summary records index exactly the chunks it names, whether
    the file leaves as one row or is cut. The partitioner holds a file until its
    last task has left."""
    datasource = MCAPDatasourceV2([chunked_file])
    is_cut = chunks_per_task is not None
    if is_cut:
        budget = int(chunks_per_task * chunk_weight(datasource))
        monkeypatch.setattr(mcap_datasource_v2, "_BIN_PACKING_BYTES", budget)
    partitioner = datasource.get_file_partitioner()
    (manifest,) = list_manifests(datasource)

    partitioner.add_input(manifest)
    tasks = ready_tasks(partitioner)
    held_while_open = len(partitioner._held)
    partitioner.finalize()
    tasks += ready_tasks(partitioner)

    # A light file waits in the packer's open bin until the input ends, while
    # a cut file's tasks leave at once.
    assert held_while_open == (0 if is_cut else 1)
    assert partitioner._held == {}
    assert (len(tasks) > 1) == is_cut
    for task in tasks:
        (records,) = task_summaries(task)
        assert records is not None
        indexed = parse_summary_records(records, chunked_file).chunk_indexes
        (metadata,) = task.file_chunk_metadatas
        assert [c.chunk_start_offset for c in indexed] == sorted(
            int(u) for u in metadata["unit_ids"]
        )
    assert sum(len(t.file_chunk_metadatas[0]["unit_ids"]) for t in tasks) == (
        CHUNKED_FILE_MESSAGES
    )


def write_overlapping(path: str) -> None:
    """Eight messages on ``/a``, written out of log-time order, a chunk closing
    every message or two, so that every chunk overlaps another in time.

        written   35, 10, 30, 20, 50, 40, 60, 15   (log times in STEP)
    """
    times = [35, 10, 30, 20, 50, 40, 60, 15]
    write_mcap(
        path,
        [
            {"topic": "/a", "data": {"t": t}, "log_time": BASE_TIME + t * STEP}
            for t in times
        ],
        chunk_size=60,
    )


def test_overlapping_file_is_read_whole_in_log_time_order(tmp_path, monkeypatch):
    """With ``preserve_order``, a file whose chunks all overlap in time is one
    task even with a budget below one chunk, and it reads in log-time order."""
    path = os.path.join(tmp_path, "overlapping.mcap")
    write_overlapping(path)
    monkeypatch.setattr(mcap_datasource_v2, "_BIN_PACKING_BYTES", 1)
    monkeypatch.setattr(
        DataContext.get_current().execution_options, "preserve_order", True
    )
    datasource = MCAPDatasourceV2([path])

    tasks = pack(datasource)
    table = read_all(datasource, *tasks)

    times = table.column("log_time").to_pylist()
    assert len(tasks) == 1
    assert times == sorted(times) and len(times) == 8


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
