"""Reader for the CSV and JSONL listing rows that ``LineChunkFileIndexer`` makes.

Status: skeleton. The class and its hooks exist with the planned steps in
their docstrings; the bodies raise ``NotImplementedError``.

Purpose: read one task's manifest, where each row is a whole file or a
``FileChunk`` naming byte-range chunks of a file. Yield Arrow tables with
projection, filter, limit, partition columns and synthesized columns applied
exactly as ``FileReader`` applies them to whole files.

How it works: ``FileReader`` scans pyarrow fragments, and pyarrow can't scan
a byte range of a text file. For each chunk the reader:

1. Creates one read unit while planning the task, without IO
   (``_get_fragments_to_read``).
2. In the scan threads, fetches the chunk's whole lines with
   ``read_line_aligned`` and wraps those bytes as an in-memory pyarrow
   fragment (``_iter_unit_tables``).
3. Scans that fragment through the shared ``_iter_fragment_tables``.

Whole-file rows skip all of this and take the plain ``FileReader`` path.

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
  3. Apply the projection and filter from ``scanner_kwargs`` to each batch,
     since the scan no longer goes through a pyarrow fragment.

  JSONL can't stream this way before pyarrow 20, which added
  ``pyarrow.json.open_json``.
- CSV chunks after the first have no header. They are parsed with the
  plan-time schema's column names, by position, so every file must list its
  columns in the same order. Each file's first chunk compares its header
  with the schema and raises on a mismatch.
- Parsing uses pyarrow only (``pds.CsvFileFormat`` and
  ``pds.JsonFileFormat``), no pandas. JSON input must be JSON Lines; a
  top-level array can't be split by lines.
- ``row_hash`` needs each chunk's first row number, but line numbers are
  unknown. A chunk numbers its rows from its starting byte instead, which is
  unique because a chunk has fewer rows than bytes.
- Format specifics live in one hook, ``_make_chunk_fragment``, implemented
  once per format in a subclass. CSV's header handling stays inside its
  override, so JSONL carries nothing for it.
"""

from typing import Any, Iterator, List

import pyarrow as pa
import pyarrow.dataset as pds
from typing_extensions import override

from ray.data._internal.datasource_v2.common.file_reader import FileFormat, FileReader
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.read_units import ReadUnitFragment
from ray.data._internal.util import NumBytes


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

    @override
    def _get_fragments_to_read(
        self,
        dataset: pds.Dataset,
        manifest: FileManifest,
    ) -> List[ReadUnitFragment]:
        """Return one :class:`ReadUnitFragment` per chunk and per unchunked file.

        Not implemented yet. The implementation should:

            1. Look up each path's whole-file fragment in ``dataset``.
            2. For a row with no chunk metadata, emit that fragment as one
               whole-file unit, as ``FileReader`` does.
            3. For a ``FileChunk`` row, emit one unit per id ``i`` in
               ``unit_ids``: ``ReadUnit(id=f"{path}#chunk{i}", source=path,
               index=i)`` with the same whole-file fragment. Read no bytes
               here; this runs once on the task's main thread, so IO here is
               neither parallel nor retried.
            4. Set ``unit_start_row`` to ``i * chunk_size``, the chunk's first
               byte. The line number of the chunk's first row is unknown, but a
               chunk holds fewer rows than bytes, so ``unit_start_row +
               rows_before`` stays unique within the file for ``row_hash``.

        Args:
            dataset: The ``pds.Dataset`` over the manifest's paths.
            manifest: The read task's manifest.

        Returns:
            The units to scan, in manifest order.
        """
        raise NotImplementedError

    @override
    def _iter_unit_tables(
        self,
        fragment_to_read: ReadUnitFragment,
        scanner_kwargs: dict,
    ) -> Iterator[pa.Table]:
        """Read one chunk's whole lines and scan them as if they were a file.

        Runs inside the retry loop and the scan threads, so the range read is
        retried and chunks download in parallel.

        Not implemented yet. The implementation should:

            1. Defer a whole-file unit to ``super()._iter_unit_tables``.
            2. Open ``unit.source`` with ``fragment.filesystem.open_input_file``
               and compute the range with ``line_chunk_byte_range(
               index=unit.index, chunk_size=self._chunk_size,
               file_size=file.size())``.
            3. Fetch the chunk's lines with ``read_line_aligned``. Return
               without yielding when they are empty.
            4. Build ``chunk_fragment`` with ``self._make_chunk_fragment(
               lines=lines, starts_at_file_start=start == 0,
               path=unit.source)``.
            5. Yield from ``self._iter_fragment_tables(
               fragment=chunk_fragment, scanner_kwargs=scanner_kwargs)``: the
               scan whole files get, so the pinned schema gives every chunk of
               a file the same column types.

        Args:
            fragment_to_read: The unit to read and its whole-file fragment.
            scanner_kwargs: Projection, filter and sizing for the pyarrow scan.

        Returns:
            Iterator over the chunk's tables.
        """
        raise NotImplementedError

    def _make_chunk_fragment(
        self, lines: memoryview, *, starts_at_file_start: bool, path: str
    ) -> pds.Fragment:
        """Return a pyarrow fragment that parses one chunk's lines.

        TODO: Move this to an ``ArrowFileReader`` when one exists, so this
        class doesn't assume pyarrow formats.

        Not implemented yet. Each format's subclass does:

            1. JSONL: return ``self._make_format().make_fragment(
               pa.py_buffer(lines))``, the format whole files use.
            2. CSV at byte 0: build the fragment the same way, then raise
               ``ValueError`` naming ``path`` and both column orders when its
               ``physical_schema.names`` differ from the file columns of the
               plan-time schema.
            3. CSV, any other chunk: use that format with
               ``ReadOptions(column_names=...)`` set to the plan-time schema's
               file columns, because these bytes have no header.

        Args:
            lines: The chunk's whole lines, from ``read_line_aligned``.
            starts_at_file_start: Whether the chunk starts at byte 0.
            path: The file's path, for error messages.

        Returns:
            A fragment over ``lines``.
        """
        raise NotImplementedError
