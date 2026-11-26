"""``FileReader`` for CSV on DataSource V2, splitting large files at newlines."""

import copy
from typing import Any, Dict, Iterator, List, Optional

import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.dataset as pds
from typing_extensions import override

from ray.data._internal.datasource_v2.common.file_reader import FileFormat
from ray.data._internal.datasource_v2.common.line_chunkable_file_reader import (
    LineChunkableFileReader,
    one_block_size,
    read_snappy_buffer,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.read_units import ReadUnitFragment
from ray.data._internal.util import KiB, infer_compression


def csv_file_format(
    read_options: pcsv.ReadOptions,
    parse_options: pcsv.ParseOptions,
    convert_options: pcsv.ConvertOptions,
) -> pds.CsvFileFormat:
    return pds.CsvFileFormat(
        read_options=read_options,
        parse_options=parse_options,
        convert_options=convert_options,
    )


# Chunk metadata key for a split file's column names, read from its header.
COLUMN_NAMES_KEY = "column_names"

_QUOTE_SCAN_BUFFER_BYTES = 64 * KiB
_MULTILINE_CSV_HINT = (
    "Pass parse_options=pyarrow.csv.ParseOptions(newlines_in_values=True) "
    "to read the file without byte-range splitting."
)


def has_header(read_options: pcsv.ReadOptions) -> bool:
    """Whether a CSV file's first line names its columns."""
    return not (read_options.column_names or read_options.autogenerate_column_names)


def _chunk_ends_in_quoted_value(
    lines: memoryview, parse_options: pcsv.ParseOptions
) -> bool:
    """Return whether a newline-aligned CSV chunk ends inside a quoted field.

    Scan backwards in bounded buffers. An even run of doubled quotes preserves
    quote state. An odd run toggles it at a field's start, or leaves it outside
    otherwise. The latter determines the answer without scanning earlier bytes.

    Files with an escape character are read whole and bypass this check.

    Args:
        lines: The chunk's newline-aligned bytes.
        parse_options: The CSV delimiter, quote character and doubled-quote settings.

    Returns:
        Whether the chunk ends before its quoted field closes.
    """
    if not parse_options.quote_char:
        return False
    quote = parse_options.quote_char.encode("ascii")
    quote_byte = ord(quote)
    separators = (ord(parse_options.delimiter), ord("\r"), ord("\n"))
    double_quote = parse_options.double_quote
    toggled = False
    end = len(lines)
    while end:
        start = max(0, end - _QUOTE_SCAN_BUFFER_BYTES)
        block = lines[start:end].tobytes()
        if double_quote and quote not in block.replace(quote * 2, b""):
            # Every quote run is even, so this buffer preserves quote state.
            end = start
            continue
        position = len(block)
        while position:
            last = block.rfind(quote, 0, position)
            if last < 0:
                break
            first = last
            while first and block[first - 1] == quote_byte:
                first -= 1
            run_length = last - first + 1
            crossed_buffer = False
            # A quote run can cross the scan-buffer boundary. Read backwards
            # until its start is known, without copying the whole chunk.
            while first == 0 and start > 0 and lines[start - 1] == quote_byte:
                end = start
                start = max(0, end - _QUOTE_SCAN_BUFFER_BYTES)
                block = lines[start:end].tobytes()
                prefix = block.rstrip(quote)
                run_length += len(block) - len(prefix)
                first = len(prefix)
                crossed_buffer = True
            position = first
            if double_quote and run_length % 2 == 0:
                if crossed_buffer and quote not in block[:position].replace(
                    quote * 2, b""
                ):
                    break
                continue
            if (not double_quote and run_length > 1) or (
                start + first > 0 and lines[start + first - 1] not in separators
            ):
                # A non-opening quote leaves the parser outside a quoted field,
                # whether it closes one or is literal data such as 5" pipe.
                return toggled
            toggled = not toggled
        end = start
    return toggled


def invalid_csv_error(path: str, error: Exception) -> ValueError:
    """Return V1 ``read_csv``'s error for a file pyarrow can't parse.

    Args:
        path: The file that failed.
        error: The error pyarrow raised, shown in the message.

    Returns:
        The error to raise.
    """
    return ValueError(
        f"Failed to read CSV file: {path}. "
        "Please check the CSV file has correct format, or filter out non-CSV "
        "file with 'partition_filter' field. See read_csv() documentation for "
        f"more details. Original error: {error}"
    )


def invalid_csv_chunk_error(path: str, error: Exception) -> ValueError:
    """Return the error for a piece of a split file that pyarrow can't parse.

    Args:
        path: The split file.
        error: The error pyarrow raised, shown in the message.

    Returns:
        The error to raise.
    """
    message = f"Failed to read a chunk of CSV file: {path}. Original error: {error}. "
    if "Expected" in str(error) and "columns" in str(error):
        message += (
            "The file is split at newline bytes, so a newline inside a quoted or "
            "escaped value that the piece can't see can cause a row with a "
            f"different number of columns. {_MULTILINE_CSV_HINT} "
        )
    return ValueError(message + "See read_csv() documentation for more details.")


class CsvFileReader(LineChunkableFileReader):
    """Reads CSV files whole or in byte-range chunks.

    A chunk after the first has no header. It is parsed with its file's own
    column names, which ``CsvFileIndexer`` read while listing and stored in
    the chunk's metadata. Chunks ending inside a quoted value are rejected
    before parsing, since pyarrow can otherwise accept the incomplete row.
    """

    def __init__(
        self,
        *,
        read_options: pcsv.ReadOptions,
        parse_options: pcsv.ParseOptions,
        convert_options: pcsv.ConvertOptions,
        **line_chunkable_kwargs: Any,
    ):
        super().__init__(FileFormat.CSV, **line_chunkable_kwargs)
        # Re-init invalid row handler: https://issues.apache.org/jira/browse/ARROW-17641
        if hasattr(parse_options, "invalid_row_handler"):
            parse_options.invalid_row_handler = parse_options.invalid_row_handler
        self._read_options = read_options
        self._parse_options = parse_options
        # pyarrow's dataset scan appends the projection to ``include_columns``,
        # which duplicates a column named in both, so ``include_columns`` is
        # applied as the projection instead.
        self._include_columns = list(convert_options.include_columns)
        self._convert_options = copy.copy(convert_options)
        self._convert_options.include_columns = []
        self._column_names_by_path: Dict[str, List[str]] = {}

    @override
    def _make_format(self) -> pds.CsvFileFormat:
        return csv_file_format(
            self._read_options, self._parse_options, self._convert_options
        )

    @override
    def _get_fragments_to_read(
        self,
        dataset: pds.Dataset,
        manifest: FileManifest,
    ) -> List[ReadUnitFragment]:
        self._column_names_by_path = {
            path: [str(name) for name in metadata[COLUMN_NAMES_KEY]]
            for path, metadata in zip(manifest.paths, manifest.file_chunk_metadatas)
            if metadata is not None and COLUMN_NAMES_KEY in metadata
        }
        return super()._get_fragments_to_read(dataset, manifest)

    @override
    def _iter_unit_tables(
        self,
        fragment_to_read: ReadUnitFragment,
        scanner_kwargs: dict,
    ) -> Iterator[pa.Table]:
        unit = fragment_to_read.unit
        if self._include_columns and scanner_kwargs.get("columns") is None:
            scanner_kwargs = {**scanner_kwargs, "columns": self._include_columns}
        try:
            if unit.id == unit.source and infer_compression(unit.source) == "snappy":
                buffer = read_snappy_buffer(unit.source, self._filesystem)
                yield from self._iter_fragment_tables(
                    self._make_format().make_fragment(buffer), scanner_kwargs
                )
            else:
                yield from super()._iter_unit_tables(fragment_to_read, scanner_kwargs)
        except pa.ArrowInvalid as e:
            if unit.id == unit.source:
                raise invalid_csv_error(unit.source, e) from e
            raise invalid_csv_chunk_error(unit.source, e) from e

    @override
    def _read_chunk_lines(
        self, fragment_to_read: ReadUnitFragment
    ) -> Optional[memoryview]:
        lines = super()._read_chunk_lines(fragment_to_read)
        if lines is not None and _chunk_ends_in_quoted_value(
            lines, self._parse_options
        ):
            raise ValueError(
                f"A chunk of CSV file {fragment_to_read.unit.source!r} ends inside "
                f"a quoted value. {_MULTILINE_CSV_HINT}"
            )
        return lines

    @override
    def _make_chunk_fragment(
        self, lines: memoryview, *, starts_at_file_start: bool, path: str
    ) -> pds.Fragment:
        read_options = copy.copy(self._read_options)
        read_options.block_size = one_block_size(lines)
        if not starts_at_file_start and has_header(read_options):
            read_options.column_names = self._column_names_by_path[path]
        return csv_file_format(
            read_options, self._parse_options, self._convert_options
        ).make_fragment(pa.py_buffer(lines))
