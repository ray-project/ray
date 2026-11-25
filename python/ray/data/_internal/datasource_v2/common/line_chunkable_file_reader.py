"""Reader for the CSV and JSONL listing rows that ``LineChunkFileIndexer`` makes.

Purpose: read one task's manifest, where each row is a whole file or a
``FileChunk`` naming byte-range chunks of a file. Yield Arrow tables with
projection, limit, partition columns and synthesized columns applied
exactly as ``FileReader`` applies them to whole files.

How it works: ``FileReader`` scans pyarrow fragments, and pyarrow can't scan
a byte range of a text file. For each chunk the reader:

1. Creates one read unit while planning the task, without IO
   (``_get_chunk_fragments``; unsplit files take the base class's
   ``_get_whole_file_fragment``).
2. In the scan threads, fetches the chunk's whole lines with
   ``read_line_aligned`` and wraps those bytes as an in-memory pyarrow
   fragment (``_iter_unit_tables``).
3. Scans that fragment through ``_iter_fragment_tables``, with the schema
   ``_get_scan_schema`` picks.

A whole-file row of an uncompressed file of known size, at most ``chunk_size``
bytes, is read with one request and scanned like a chunk. Larger, compressed
and unknown-size files are streamed by ``_iter_streamed_unit_tables``, which by
default is the plain ``FileReader`` scan that lets pyarrow decompress them by
extension.

Design decisions:

- The download happens in ``_iter_unit_tables``, the hook ``FileReader``
  calls inside its retry loop and thread pool. Chunk reads therefore run in
  parallel, and a failure retries only that chunk. Downloading while
  planning units would fetch every chunk serially on one thread, with no
  retry.
- A read task holds one chunk's raw text, up to ``chunk_size`` bytes, while
  pyarrow parses it. We don't expect this to matter: the text is bounded by
  ``chunk_size``, and V1's ``ArrowJSONDatasource`` already reads whole files
  into memory. If it does matter, override ``_iter_unit_tables`` in the CSV
  subclass to stream the chunk instead:

  1. Find the aligned start and end with small reads near each cut, by the
     same rule as ``read_line_aligned``.
  2. Parse ``[start, end)`` with ``pyarrow.csv.open_csv`` over a stream
     limited to that range.
  3. Apply the projection from ``scanner_kwargs`` to each batch, since the
     scan no longer goes through a pyarrow fragment.

  JSONL can't stream this way before pyarrow 20, which added
  ``pyarrow.json.open_json``.
- Column types are inferred per fragment, as V1 infers them per file, not
  pinned to the plan-time schema. Two chunks of one file can therefore come
  back with different types, which downstream schema unification promotes.
  A plan-time column a fragment lacks is null-filled. Each fragment parses
  its bytes as one block, so its types come from all of its rows.
- CSV chunks after the first have no header. The CSV subclass names their
  columns from the file's own header line, which its indexer reads once
  while listing and stores in each chunk's metadata.
- Parsing uses pyarrow (``pds.CsvFileFormat`` and ``pds.JsonFileFormat``). A
  subclass may parse the lines ``_read_chunk_lines`` returns with another
  library, as ``read_json(lines=True)`` does with pandas. JSON input must be
  JSON Lines; a top-level array can't be split by lines.
- ``row_hash`` needs each chunk's first row number, but line numbers are
  unknown. A chunk numbers its rows from its starting byte instead, which is
  unique because a chunk has fewer rows than bytes. The numbers depend on
  ``chunk_size``, so they differ from an unsplit read's and V1's. Only
  ``read_parquet`` exposes ``include_row_hash`` today.
- Format specifics live in one hook, ``_make_chunk_fragment``, implemented
  once per format in a subclass. CSV's header handling stays inside its
  override, so JSONL carries nothing for it.
"""

from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Set

import pyarrow as pa
import pyarrow.dataset as pds
from typing_extensions import override

from ray.data._internal.datasource_v2.common.file_reader import FileFormat, FileReader
from ray.data._internal.datasource_v2.common.line_chunk_file_indexer import (
    _is_compressed,
)
from ray.data._internal.datasource_v2.common.line_chunking import (
    line_chunk_byte_range,
    read_line_aligned,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FileChunk,
    FileManifest,
)
from ray.data._internal.datasource_v2.interfaces.read_units import (
    ReadUnit,
    ReadUnitFragment,
)
from ray.data._internal.object_extensions.arrow import raise_on_pickle_object_columns
from ray.data._internal.util import NumBytes, RetryingPyFileSystem
from ray.data.datasource.file_based_datasource import _decompress_snappy

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem


def read_snappy_buffer(path: str, filesystem: "FileSystem") -> pa.Buffer:
    """Return a whole Snappy file decompressed, since pyarrow can't read it."""
    if isinstance(filesystem, RetryingPyFileSystem):
        # Unwrap the retry wrapper, as V1 does, so ``_decompress_snappy``
        # sees the ``HadoopFileSystem`` underneath and applies Hadoop framing.
        filesystem = filesystem.unwrap()
    with filesystem.open_input_stream(path, compression=None) as file:
        return pa.py_buffer(_decompress_snappy(file, filesystem).getbuffer())


# pyarrow's ``ReadOptions.block_size`` is a 32-bit int.
_MAX_BLOCK_SIZE = 2**31 - 1


def one_block_size(lines: memoryview) -> int:
    """Return a ``ReadOptions.block_size`` that parses ``lines`` as one block.

    Args:
        lines: The bytes to parse.

    Returns:
        ``len(lines)``, within pyarrow's bounds of 1 and 2**31 - 1.
    """
    return max(1, min(len(lines), _MAX_BLOCK_SIZE))


class LineChunkableFileReader(FileReader):
    """Reads newline-delimited files (CSV, JSONL) one byte-range chunk at a time.

    Subclass it once per format and implement :meth:`_make_chunk_fragment`.
    The chunk-reading steps below are shared.
    """

    def __init__(
        self,
        format: FileFormat,
        *,
        chunk_size: NumBytes,
        **file_reader_kwargs: Any,
    ):
        """Initialize the reader.

        Args:
            format: ``FileFormat.CSV`` or ``FileFormat.JSON``.
            chunk_size: Bytes per chunk. Must equal the indexer's
                ``chunk_size`` so a unit id maps to the same byte range.
            **file_reader_kwargs: Passed to ``FileReader``.
        """
        super().__init__(format=format, **file_reader_kwargs)
        # TODO: Carry each chunk's byte range in the manifest instead, once
        # bin sizes can vary per file.
        self._chunk_size = chunk_size
        self._file_sizes: Dict[str, Optional[int]] = {}

    @override
    def _get_fragments_to_read(
        self,
        dataset: pds.Dataset,
        manifest: FileManifest,
    ) -> List[ReadUnitFragment]:
        self._file_sizes = dict(zip(manifest.paths, manifest.known_file_sizes))
        return super()._get_fragments_to_read(dataset, manifest)

    @override
    def _get_chunk_fragments(
        self,
        fragment: pds.Fragment,
        chunk_metadata: Dict[str, Any],
        file_size: Optional[int],
    ) -> List[ReadUnitFragment]:
        """Return one :class:`ReadUnitFragment` per chunk of a split file.

        Reads no bytes: this runs once on the task's main thread, where IO
        would be neither parallel nor retried. Each chunk unit carries its
        file's whole-file fragment; :meth:`_iter_unit_tables` replaces it
        with the chunk's lines. An unsplit file takes the base class's
        whole-file unit.

        ``unit_start_row`` is the chunk's first byte. The line number of the
        chunk's first row is unknown, but a chunk holds fewer rows than
        bytes, so ``unit_start_row + rows_before`` stays unique within the
        file for ``row_hash``.

        Args:
            fragment: The file's whole-file fragment.
            chunk_metadata: The row's :class:`FileChunk` metadata.
            file_size: Unused; the chunk's range is computed at read time.

        Returns:
            The file's chunk units, in order.
        """
        return [
            ReadUnitFragment(
                fragment,
                ReadUnit(
                    id=f"{fragment.path}#chunk{index}",
                    source=fragment.path,
                    index=index,
                ),
                unit_start_row=index * self._chunk_size,
            )
            for index in FileChunk.from_metadata(chunk_metadata).unit_ids
        ]

    @override
    def _iter_unit_tables(
        self,
        fragment_to_read: ReadUnitFragment,
        scanner_kwargs: dict,
    ) -> Iterator[pa.Table]:
        """Read one chunk's whole lines and scan them as if they were a file.

        Runs inside the retry loop and the scan threads, so the range read is
        retried and chunks download in parallel. An uncompressed whole file that
        fits in one chunk is read the same way, in one request; other whole
        files are streamed by ``_iter_streamed_unit_tables``.

        Args:
            fragment_to_read: The unit to read and its whole-file fragment.
            scanner_kwargs: Projection, filter and sizing for the pyarrow scan.

        Yields:
            pa.Table: The unit's tables.
        """
        unit = fragment_to_read.unit
        if unit.id == unit.source:
            data = self._read_whole_file(fragment_to_read)
            if data is None:
                yield from self._iter_streamed_unit_tables(
                    fragment_to_read, scanner_kwargs
                )
                return
            fragment = self._make_chunk_fragment(
                data, starts_at_file_start=True, path=unit.source
            )
        else:
            fragment = self._read_chunk_fragment(fragment_to_read)
        if fragment is not None:
            yield from self._iter_fragment_tables(
                fragment=fragment, scanner_kwargs=scanner_kwargs
            )

    def _iter_streamed_unit_tables(
        self,
        fragment_to_read: ReadUnitFragment,
        scanner_kwargs: dict,
    ) -> Iterator[pa.Table]:
        """Stream a whole-file unit that can't be read into one chunk.

        That is a compressed file, a file of unknown size, and a file larger than
        a chunk that can't be split. Override this when pyarrow's dataset scan of
        the stream would read ahead of the consumer without bound. CSV does;
        JSON handles its whole-file units in its own ``_iter_unit_tables``,
        because a small ``.json`` file needs the ``json.load`` fallback that
        ``_make_chunk_fragment`` lacks.
        """
        yield from super()._iter_unit_tables(fragment_to_read, scanner_kwargs)

    def _read_whole_file(
        self, fragment_to_read: ReadUnitFragment
    ) -> Optional[memoryview]:
        """Read a whole-file unit's file into memory if it fits in one chunk.

        Runs once per whole-file unit, so a task with several files reads each
        file in its own scan thread. A compressed file, a file larger than
        ``chunk_size`` and a file of unknown size return ``None`` and are
        streamed by ``_iter_streamed_unit_tables`` instead.

        The size comes from the listing, so a file of unknown size, such as an
        HTTP file without ``Content-Length``, is never opened for its size,
        which a streaming file can't report.
        """
        path = fragment_to_read.unit.source
        size = self._file_sizes[path]
        if _is_compressed(path) or size is None or size > self._chunk_size:
            return None
        with fragment_to_read.fragment.open() as file:
            return memoryview(file.read_buffer())

    def _read_chunk_lines(
        self, fragment_to_read: ReadUnitFragment
    ) -> Optional[memoryview]:
        """Fetch a chunk unit's whole lines; ``None`` when it holds no rows."""
        unit = fragment_to_read.unit
        with fragment_to_read.fragment.open() as file:
            file_size = file.size()
            start, end = line_chunk_byte_range(
                index=unit.index, chunk_size=self._chunk_size, file_size=file_size
            )
            lines = read_line_aligned(file, start=start, end=end, file_size=file_size)
        return lines or None

    def _read_chunk_fragment(
        self, fragment_to_read: ReadUnitFragment
    ) -> Optional[pds.Fragment]:
        """Fetch a chunk unit's whole lines as a fragment; ``None`` when it holds no rows."""
        lines = self._read_chunk_lines(fragment_to_read)
        if lines is None:
            return None
        unit = fragment_to_read.unit
        return self._make_chunk_fragment(
            lines, starts_at_file_start=unit.index == 0, path=unit.source
        )

    @override
    def _iter_fragment_tables(
        self, fragment: pds.Fragment, scanner_kwargs: dict
    ) -> Iterator[pa.Table]:
        columns = scanner_kwargs.get("columns")
        if columns is not None:
            scanner_kwargs = {
                **scanner_kwargs,
                "columns": self._with_file_partition_keys(
                    columns, fragment.physical_schema.names
                ),
            }
        yield from super()._iter_fragment_tables(fragment, scanner_kwargs)

    def _with_file_partition_keys(
        self, columns: List[str], file_names: List[str]
    ) -> List[str]:
        """Add the file columns named like a requested partition key to ``columns``.

        ``columns`` leaves out partition keys, so a file column named like one is
        read for ``_apply_partition_values`` to check against the path value.
        """
        file_names_set = set(file_names)
        columns_set = set(columns)
        return columns + [
            name
            for name in self._columns or []
            if name in self._partition_keys
            and name in file_names_set
            and name not in columns_set
        ]

    @override
    def _on_disk_column_names(self, dataset: pds.Dataset) -> Set[str]:
        """Count a requested column the plan schema lacks as read from the file.

        The plan schema is a sample of the files, so a column absent from it may
        still be in a file. It is read and null-filled where a file lacks it, by
        ``_get_scan_schema``, instead of being treated as synthesized and dropped.
        """
        schema = self._schema
        planned = set(schema.names) if schema is not None else set()
        synthesized = {column.name for column in self._synthesized_columns}
        unsampled = set(self._columns or ()) - planned - synthesized
        return super()._on_disk_column_names(dataset) | unsampled

    @override
    def _get_scan_schema(
        self, fragment: pds.Fragment, scanner_kwargs: dict
    ) -> pa.Schema:
        """Scan ``fragment`` with the types pyarrow infers for it.

        The plan-time schema is only a sample, so its types aren't pinned.
        Columns the scan reads that ``fragment`` lacks are added as ``null``
        columns, so they null-fill instead of failing: the projected ones, or
        without a projection, the plan-time file columns.
        """
        physical_schema = fragment.physical_schema
        columns = scanner_kwargs.get("columns")
        if columns is None:
            columns = (self._file_dataset_schema or pa.schema([])).names
        missing_fields = [
            pa.field(name, pa.null())
            for name in columns
            if physical_schema.get_field_index(name) == -1
        ]
        return pa.schema(list(physical_schema) + missing_fields)

    @override
    def _on_batch_read(self, table: pa.Table) -> None:
        super()._on_batch_read(table)
        raise_on_pickle_object_columns(table)

    def _make_chunk_fragment(
        self, lines: memoryview, *, starts_at_file_start: bool, path: str
    ) -> Optional[pds.Fragment]:
        """Return a pyarrow fragment that parses ``lines`` as one block.

        Each format's subclass implements it with its format's read options,
        setting ``block_size`` to :func:`one_block_size`. The fragment's
        schema must come from every line: the scan pins the fragment's
        ``physical_schema``, which pyarrow infers from the first block only.
        With the default 1 MiB block, a later value of another type fails to
        convert, and a JSON key first seen after the first block is dropped.
        CSV also names the columns of a chunk after the first, which has no
        header.

        TODO: Move this to an ``ArrowFileReader`` when one exists, so this
        class doesn't assume pyarrow formats.

        Args:
            lines: Whole lines: a chunk's from ``read_line_aligned``, or a
                whole file's.
            starts_at_file_start: Whether ``lines`` start at byte 0, as a
                whole file's do.
            path: The file's path, for error messages.

        Returns:
            A fragment over ``lines``, or ``None`` when they hold no rows, such
            as a chunk of only blank lines, which a format may not parse.
        """
        raise NotImplementedError
