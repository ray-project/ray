"""Unit tests for keeping each MCAP file in one read task (``mcap_file_cuts``).

The tests build listing rows directly, one row per chunk as
``MCAPSummaryIndexer`` lists them, and feed them through ``OrderedFileGrouper``
in front of ``OnlineBinPacker``. Each test reads back which chunks every task
holds, in the order the tasks leave. None needs a Ray cluster.
"""

from typing import List, Optional, Tuple

import pytest

from ray.data._internal.datasource_v2.common.online_bin_packer import OnlineBinPacker
from ray.data._internal.datasource_v2.formats.mcap.mcap_file_cuts import (
    OrderedFileGrouper,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FileChunk,
    FileManifest,
)
from ray.data.context import DataContext

BUDGET = 100  # bytes per read task
WEIGHT = 40  # bytes per chunk: two chunks fit a task, three do not
FILE_SIZE = 1000  # on-disk size of every file

# A task as the tests read it: the chunk offsets of each file it holds, or
# ``None`` for a file read whole.
Task = List[Tuple[str, Optional[List[int]]]]


@pytest.fixture
def preserve_order(monkeypatch):
    """Run the test as a read that keeps its blocks in order."""
    options = DataContext.get_current().execution_options
    monkeypatch.setattr(options, "preserve_order", True)


def listed_file(path: str, num_chunks: int) -> FileManifest:
    """The listing rows of a file with chunks at offsets ``0..num_chunks-1``."""
    runs = [
        FileChunk(unit_ids=(i,), num_rows=1, size_bytes=WEIGHT, fully_matched=False)
        for i in range(num_chunks)
    ]
    return FileManifest.construct_manifest(
        paths=[path] * num_chunks,
        sizes=[FILE_SIZE] * num_chunks,
        chunk_metadatas=[run.to_metadata() for run in runs],
    )


def listed_whole(path: str) -> FileManifest:
    """The one row of a file listed whole, such as one without a chunk index."""
    return FileManifest.construct_manifest(
        paths=[path], sizes=[FILE_SIZE], chunk_metadatas=[None]
    )


def group(files: List[FileManifest]) -> List[Task]:
    """Feed ``files`` to a grouper one by one, as a listing task does, and
    return every task in the order it leaves."""
    grouper = OrderedFileGrouper(
        OnlineBinPacker(BUDGET, requires_global_input=False), budget=BUDGET
    )
    out = []
    for manifest in files:
        grouper.add_input(manifest)
        while grouper.has_partition():
            out.append(grouper.next_partition())
    grouper.finalize()
    while grouper.has_partition():
        out.append(grouper.next_partition())
    return [
        [
            (str(path), None if md is None else sorted(int(u) for u in md["unit_ids"]))
            for path, md in zip(m.paths, m.file_chunk_metadatas)
        ]
        for m in out
    ]


def test_an_unordered_read_splits_a_file_across_tasks():
    """Without ``preserve_order``, the rows go to the packer as listed, so a
    file heavier than a task is split by chunk."""
    tasks = group([listed_file("a", 3)])

    # Two chunks fill a task, so the third starts another.
    assert [chunks for task in tasks for _, chunks in task] == [[0, 1], [2]]


def test_an_ordered_read_keeps_a_heavy_file_in_a_task_of_its_own(preserve_order):
    """With ``preserve_order``, a file heavier than a task is read by one task
    that holds no other file."""
    tasks = group([listed_file("a", 3), listed_file("b", 1)])

    assert [("a", [0, 1, 2])] in tasks
    assert all(len(task) == 1 for task in tasks if ("a", [0, 1, 2]) in task)


def test_an_ordered_read_never_splits_a_light_file(preserve_order):
    """With ``preserve_order``, a light file stays whole even when a shared
    task has room for only part of it."""
    # "a" leaves 60 bytes in its task: room for one chunk of "b", not both.
    tasks = group([listed_file("a", 1), listed_file("b", 2)])

    assert sorted(file for task in tasks for file in task) == [
        ("a", [0]),
        ("b", [0, 1]),
    ]


def test_an_ordered_read_keeps_files_in_listing_order(preserve_order):
    """With ``preserve_order``, a heavy file between light ones leaves after the
    files listed before it, and light files share a task only with their
    neighbours."""
    tasks = group(
        [
            listed_file("a", 1),
            listed_file("b", 1),
            listed_file("heavy", 3),
            listed_file("c", 1),
        ]
    )

    assert tasks == [
        [("a", [0]), ("b", [0])],
        [("heavy", [0, 1, 2])],
        [("c", [0])],
    ]


def test_an_ordered_read_passes_a_whole_file_row_through(preserve_order):
    """A row that names no chunk already covers its file, so it goes into a
    task as listed."""
    tasks = group([listed_whole("w")])

    assert tasks == [[("w", None)]]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
