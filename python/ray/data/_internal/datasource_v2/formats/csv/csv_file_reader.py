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
from ray.data._internal.util import infer_compression


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


def has_header(read_options: pcsv.ReadOptions) -> bool:
    """Whether a CSV file's first line names its columns."""
    return not (read_options.column_names or read_options.autogenerate_column_names)


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
            "different number of columns: pass "
            "parse_options=pyarrow.csv.ParseOptions(newlines_in_values=True) to "
            "read each file whole. "
        )
    return ValueError(message + "See read_csv() documentation for more details.")


class CsvFileReader(LineChunkableFileReader):
    """Reads CSV files whole or in byte-range chunks.

    A chunk after the first has no header. It is parsed with its file's own
    column names, which ``CsvFileIndexer`` read while listing and stored in
    the chunk's metadata.
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
        self._streamed_convert_options = convert_options
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
    def _iter_streamed_unit_tables(
        self,
        fragment_to_read: ReadUnitFragment,
        scanner_kwargs: dict,
    ) -> Iterator[pa.Table]:
        """Stream the file with ``pcsv.open_csv``, as V1 does.

        pyarrow's dataset scan of a CSV stream reads ahead of its consumer
        without bound, so a file larger than memory would fill it. ``open_csv``
        parses a block only when asked for the next one. The projection, null
        fill and filter that the scan would apply are applied to each block.
        """
        path = fragment_to_read.unit.source
        assert self._filesystem is not None
        with self._filesystem.open_input_stream(path) as file:
            reader = pcsv.open_csv(
                file,
                read_options=self._read_options,
                parse_options=self._parse_options,
                convert_options=self._streamed_convert_options,
            )
            columns = scanner_kwargs.get("columns")
            if columns is not None:
                columns = self._with_file_partition_keys(columns, reader.schema.names)
            for batch in reader:
                yield self._scan_block(
                    pa.Table.from_batches([batch]),
                    columns,
                    scanner_kwargs.get("filter"),
                )

    def _scan_block(
        self, table: pa.Table, columns: Optional[List[str]], predicate: Any
    ) -> pa.Table:
        """Return ``table`` as the dataset scan would: null-filled, filtered, projected.

        A column the plan schema or the projection names but the file lacks is
        added as ``null``, so it null-fills instead of failing.
        """
        planned = (self._file_dataset_schema or pa.schema([])).names
        for name in planned + list(columns or []):
            if name not in table.column_names:
                table = table.append_column(name, pa.nulls(len(table)))
        if predicate is not None:
            table = table.filter(predicate)
        return table if columns is None else table.select(columns)

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
