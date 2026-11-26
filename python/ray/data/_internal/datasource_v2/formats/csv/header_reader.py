"""Reads each split CSV file's header line, to name its chunks' columns.

Mirrors ``parquet/footer_reader.py``: a plain class, run as a pool of Ray
actors (see :data:`HeaderReaderActor`) so header IO spreads across the
cluster instead of bottlenecking the listing task.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Iterable, List

import pyarrow as pa
import pyarrow.dataset as pds

import ray
from ray.data._internal.datasource_v2.common.line_chunking import read_line_aligned

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem

    from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo


logger = logging.getLogger(__name__)


class HeaderReader:
    """Reads one CSV file's header line and returns its column names.

    Each :meth:`read_headers` actor call reads a batch of files' headers
    concurrently. A ``RetryingPyFileSystem`` supplies the transient-error
    retries; Ray restarts the actor and re-runs its batch on death or OOM, up
    to the indexer's ``max_restarts``/``max_task_retries``.
    """

    def __init__(
        self,
        *,
        filesystem: "FileSystem",
        file_format: pds.CsvFileFormat,
        io_concurrency: int,
    ):
        """Initialize.

        Args:
            filesystem: Filesystem to read headers through, typically a
                ``RetryingPyFileSystem`` wrapping the resolved one.
            file_format: Format built with default convert options, so
                ``include_columns`` can't drop names from a header.
            io_concurrency: Header reads to run at once per batch.
        """
        self._filesystem = filesystem
        self._file_format = file_format
        self._io_concurrency = io_concurrency

    @ray.method()
    def read_headers(self, files: "Iterable[FileInfo]") -> List[List[str]]:
        """Return each file's header column names, in input order."""
        files = list(files)
        if not files:
            return []
        with ThreadPoolExecutor(
            max_workers=min(self._io_concurrency, len(files))
        ) as pool:
            return list(pool.map(self.read_header, files))

    def read_header(self, file_info: "FileInfo") -> List[str]:
        """Read one file's header line and return its column names."""
        assert file_info.size is not None
        with self._filesystem.open_input_file(file_info.path) as file:
            header = read_line_aligned(file, start=0, end=1, file_size=file_info.size)
        return self._file_format.inspect(pa.py_buffer(header)).names


HeaderReaderActor = ray.remote(HeaderReader)
