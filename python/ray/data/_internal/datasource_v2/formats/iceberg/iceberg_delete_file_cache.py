"""Fetches a delete file shared by several data files once per read unit.

The reader calls ``ArrowScan`` once per data file, and each call reads that
file's delete files again, so N data files sharing one position delete file
fetch it N times. On object storage that fetch dominates the read (#66148
measured 8 data files sharing one delete file at 74-96 s of a 94-113 s read).

``with_shared_delete_files_cached`` wraps the table's ``FileIO`` for one read
unit. ``ArrowScan`` opens every file through ``FileIO.new_input(path).open()``,
so the wrapper hands back the bytes it already fetched for any delete file
that two or more of the unit's data files name, and passes every other path
through unchanged. Only public ``FileIO`` and ``InputFile`` methods are used.

The cache keeps compressed bytes, not decoded deletes, so each data file still
decodes its delete file; the fetch is what costs. It is bounded by
``_DELETE_FILE_CACHE_BYTES`` over the delete files' recorded sizes, and lives
only as long as the read unit. This module imports PyIceberg, so the reader
imports it lazily.
"""

from __future__ import annotations

import threading
from collections import Counter
from typing import TYPE_CHECKING, Dict, Sequence, Set, Union

import pyarrow as pa
from pyiceberg.io import FileIO, InputFile, InputStream, OutputFile

if TYPE_CHECKING:
    from pyiceberg.table import FileScanTask

# The most delete-file bytes one read unit keeps.
_DELETE_FILE_CACHE_BYTES = 64 * 1024 * 1024


def with_shared_delete_files_cached(
    io: FileIO, tasks: Sequence["FileScanTask"]
) -> FileIO:
    """``io``, wrapped to fetch the delete files ``tasks`` share only once.

    Args:
        io: The table's ``FileIO``.
        tasks: The read unit's scan tasks.

    Returns:
        ``io`` itself when no delete file is shared, else a caching wrapper.
    """
    references = Counter(
        delete_file.file_path for task in tasks for delete_file in task.delete_files
    )
    sizes = {
        delete_file.file_path: delete_file.file_size_in_bytes
        for task in tasks
        for delete_file in task.delete_files
    }
    shared: Set[str] = set()
    budget = _DELETE_FILE_CACHE_BYTES
    for path, count in references.items():
        if count > 1 and 0 < sizes[path] <= budget:
            shared.add(path)
            budget -= sizes[path]
    if not shared:
        return io
    return _SharedDeleteFileIO(io, shared_paths=shared)


class _SharedDeleteFileIO(FileIO):
    """Serves ``shared_paths`` from memory after the first fetch."""

    def __init__(self, io: FileIO, *, shared_paths: Set[str]):
        super().__init__(properties=io.properties)
        self._io = io
        self._shared_paths = shared_paths
        self._contents: Dict[str, bytes] = {}
        # ``ArrowScan`` reads a task's delete files on a thread pool, so each
        # path gets its own lock and different files still load in parallel.
        self._path_locks: Dict[str, threading.Lock] = {}
        self._lock = threading.Lock()

    def new_input(self, location: str) -> InputFile:
        if location in self._shared_paths:
            return _CachedInputFile(location, cache=self)
        return self._io.new_input(location)

    def new_output(self, location: str) -> OutputFile:
        return self._io.new_output(location)

    def delete(self, location: Union[str, InputFile, OutputFile]) -> None:
        self._io.delete(location)

    def contents(self, location: str) -> bytes:
        with self._lock:
            path_lock = self._path_locks.setdefault(location, threading.Lock())
        with path_lock:
            if location not in self._contents:
                with self._io.new_input(location).open() as stream:
                    self._contents[location] = stream.read()
            return self._contents[location]


class _CachedInputFile(InputFile):
    """An ``InputFile`` whose bytes come from ``_SharedDeleteFileIO``."""

    def __init__(self, location: str, *, cache: _SharedDeleteFileIO):
        super().__init__(location)
        self._cache = cache

    def __len__(self) -> int:
        return len(self._cache.contents(self.location))

    def exists(self) -> bool:
        return True

    def open(self, seekable: bool = True) -> InputStream:
        return pa.BufferReader(self._cache.contents(self.location))
