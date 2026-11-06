"""Reader for the CSV and JSONL listing rows that ``LineChunkFileIndexer`` makes.

Purpose: read one task's manifest, where each row is a whole file or a
``FileChunk`` naming byte-range chunks of a file. Yield Arrow tables with
projection, limit, partition columns and synthesized columns applied
exactly as ``FileReader`` applies them to whole files.

How it works: ``FileReader`` scans pyarrow fragments, and pyarrow can't scan
a byte range of a text file. For each chunk the reader:

1. Creates one read unit while planning the task, without IO
   (``_get_fragments_to_read``).
2. In the scan threads, fetches the chunk's whole lines with
   ``read_line_aligned`` and wraps those bytes as an in-memory pyarrow
   fragment (``_iter_unit_tables``).
3. Scans that fragment through ``_iter_fragment_tables``.

Whole-file rows skip steps 1 and 2 and take the plain ``FileReader`` path.

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
  A plan-time column a fragment lacks is null-filled.
- CSV chunks after the first have no header. They are parsed with the
  plan-time schema's column names, by position, so every split file must
  list its columns in the same order. Each file's first chunk compares its
  header with the schema and raises on a mismatch.
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

from typing import Any, Iterator, List, Optional

import pyarrow as pa
import pyarrow.dataset as pds
from typing_extensions import override

from ray.data._internal.datasource_v2.common.file_reader import FileFormat, FileReader
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

        Reads no bytes: this runs once on the task's main thread, where IO
        would be neither parallel nor retried. A chunk unit carries its
        file's whole-file fragment; :meth:`_iter_unit_tables` replaces it
        with the chunk's lines.

        ``unit_start_row`` is the chunk's first byte. The line number of the
        chunk's first row is unknown, but a chunk holds fewer rows than
        bytes, so ``unit_start_row + rows_before`` stays unique within the
        file for ``row_hash``.

        Args:
            dataset: The ``pds.Dataset`` over the manifest's paths.
            manifest: The read task's manifest.

        Returns:
            The units to scan, in manifest order.
        """
        path_to_fragment = {
            fragment.path: fragment for fragment in dataset.get_fragments()
        }
        fragments: List[ReadUnitFragment] = []
        for path, chunk_metadata in zip(manifest.paths, manifest.file_chunk_metadatas):
            fragment = path_to_fragment[path]
            if chunk_metadata is None:
                fragments.append(
                    ReadUnitFragment(fragment, ReadUnit(id=path, source=path, count=1))
                )
                continue
            for index in FileChunk.from_metadata(chunk_metadata).unit_ids:
                fragments.append(
                    ReadUnitFragment(
                        fragment,
                        ReadUnit(id=f"{path}#chunk{index}", source=path, index=index),
                        unit_start_row=index * self._chunk_size,
                    )
                )
        return fragments

    @override
    def _iter_unit_tables(
        self,
        fragment_to_read: ReadUnitFragment,
        scanner_kwargs: dict,
    ) -> Iterator[pa.Table]:
        """Read one chunk's whole lines and scan them as if they were a file.

        Runs inside the retry loop and the scan threads, so the range read is
        retried and chunks download in parallel. A whole-file unit takes the
        plain ``FileReader`` path.

        Args:
            fragment_to_read: The unit to read and its whole-file fragment.
            scanner_kwargs: Projection, filter and sizing for the pyarrow scan.

        Yields:
            pa.Table: The unit's tables.
        """
        unit = fragment_to_read.unit
        if unit.id == unit.source:
            yield from super()._iter_unit_tables(fragment_to_read, scanner_kwargs)
        else:
            chunk_fragment = self._read_chunk_fragment(fragment_to_read)
            if chunk_fragment is not None:
                yield from self._iter_fragment_tables(
                    fragment=chunk_fragment, scanner_kwargs=scanner_kwargs
                )

    def _read_chunk_fragment(
        self, fragment_to_read: ReadUnitFragment
    ) -> Optional[pds.Fragment]:
        """Fetch a chunk unit's whole lines; ``None`` when no line starts in it."""
        unit = fragment_to_read.unit
        filesystem = fragment_to_read.fragment.filesystem
        with filesystem.open_input_file(unit.source) as file:
            file_size = file.size()
            start, end = line_chunk_byte_range(
                index=unit.index, chunk_size=self._chunk_size, file_size=file_size
            )
            lines = read_line_aligned(file, start=start, end=end, file_size=file_size)
        if not lines:
            return None
        return self._make_chunk_fragment(
            lines, starts_at_file_start=start == 0, path=unit.source
        )

    @override
    def _iter_fragment_tables(
        self,
        fragment: pds.Fragment,
        scanner_kwargs: dict,
    ) -> Iterator[pa.Table]:
        """Scan ``fragment`` with the types pyarrow infers for it.

        The plan-time schema is only a sample, so its types aren't pinned.
        Its file columns that ``fragment`` lacks are added as ``null``
        columns, so a projection naming them null-fills instead of failing.
        """
        physical_schema = fragment.physical_schema
        missing_fields = [
            pa.field(name, pa.null())
            for name in (self._file_dataset_schema or pa.schema([])).names
            if physical_schema.get_field_index(name) == -1
        ]
        scanner = fragment.scanner(
            **scanner_kwargs, schema=pa.schema(list(physical_schema) + missing_fields)
        )
        for tagged in scanner.scan_batches():
            yield pa.Table.from_batches(batches=[tagged.record_batch])

    @override
    def _on_batch_read(self, table: pa.Table) -> None:
        super()._on_batch_read(table)
        raise_on_pickle_object_columns(table)

    def _make_chunk_fragment(
        self, lines: memoryview, *, starts_at_file_start: bool, path: str
    ) -> pds.Fragment:
        """Return a pyarrow fragment that parses one chunk's lines.

        Each format's subclass implements it, typically as
        ``self._make_format().make_fragment(pa.py_buffer(lines))``. CSV also
        supplies column names to chunks after the first, which have no
        header, and checks the header of the chunk at byte 0.

        TODO: Move this to an ``ArrowFileReader`` when one exists, so this
        class doesn't assume pyarrow formats.

        Args:
            lines: The chunk's whole lines, from ``read_line_aligned``.
            starts_at_file_start: Whether the chunk starts at byte 0.
            path: The file's path, for error messages.

        Returns:
            A fragment over ``lines``.
        """
        raise NotImplementedError
