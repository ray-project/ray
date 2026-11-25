"""Reads each split CSV file's header line, to name its chunks' columns.

Mirrors ``parquet/footer_reader.py``: a plain class, run as a pool of Ray
actors (see :data:`HeaderReaderActor`) so header IO spreads across the
cluster instead of bottlenecking the listing task.
"""

from __future__ import annotations

import io
import logging
from codecs import BOM_UTF8
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, Iterable, List, Optional

import pyarrow as pa
import pyarrow.dataset as pds

import ray
from ray.data._internal.util import KiB

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem

    from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo


logger = logging.getLogger(__name__)


def _read_header_for_first_chunk(
    file: io.BufferedReader[Any], chunk_size: int, ignore_empty_lines: bool
) -> Optional[bytes]:
    """Read the first chunk's header, or defer to the whole-file parser."""
    if not ignore_empty_lines:
        # An empty first record can name a single column.
        return file.readline()

    # Leave a BOM on a nonempty header for Arrow. It belongs to the skipped
    # prefix only if it is immediately followed by a line ending.
    prefix = file.read(len(BOM_UTF8) + 1)
    offset = len(prefix) if prefix in (BOM_UTF8 + b"\r", BOM_UTF8 + b"\n") else 0
    file.seek(offset)

    while offset < chunk_size:
        block = file.peek(1)[: chunk_size - offset]
        if not block:
            break
        # Skip runs of blank lines without a read per line. Spaces and tabs
        # can be column names and must reach Arrow.
        skipped = len(block) - len(block.lstrip(b"\r\n"))
        file.read(skipped)
        offset += skipped
        if skipped < len(block):
            header = file.readline()
            # Keep a BOM in a later column name when Arrow inspects it in isolation.
            if offset and header.startswith(BOM_UTF8):
                return b"\n" + header
            return header
    return None


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
        chunk_size: int,
        io_concurrency: int,
    ):
        """Initialize.

        Args:
            filesystem: Filesystem to read headers through, typically a
                ``RetryingPyFileSystem`` wrapping the resolved one.
            file_format: Format built with default convert options, so
                ``include_columns`` can't drop names from a header.
            chunk_size: First chunk's exclusive end offset. Only that chunk
                can consume the file's header.
            io_concurrency: Header reads to run at once per batch.
        """
        self._filesystem = filesystem
        self._file_format = file_format
        self._chunk_size = chunk_size
        self._io_concurrency = io_concurrency

    @ray.method()
    def read_headers(self, files: "Iterable[FileInfo]") -> List[Optional[List[str]]]:
        """Return column names or a whole-file fallback for each file, in order."""
        files = list(files)
        if not files:
            return []
        with ThreadPoolExecutor(
            max_workers=min(self._io_concurrency, len(files))
        ) as pool:
            return list(pool.map(self.read_header, files))

    def read_header(self, file_info: "FileInfo") -> Optional[List[str]]:
        """Return the header's names, or ``None`` to read this file whole."""
        with io.BufferedReader(
            self._filesystem.open_input_file(file_info.path), buffer_size=64 * KiB
        ) as file:
            header = _read_header_for_first_chunk(
                file,
                self._chunk_size,
                self._file_format.parse_options.ignore_empty_lines,
            )
        if header is None:
            return None
        return self._file_format.inspect(pa.py_buffer(header)).names


HeaderReaderActor = ray.remote(HeaderReader)
