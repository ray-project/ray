"""``FileReader`` for CSV on DataSource V2, splitting large files at newlines."""

import copy
from typing import TYPE_CHECKING, Any, Iterator, List

import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.dataset as pds
from typing_extensions import override

from ray.data._internal.datasource_v2.common.file_reader import FileFormat
from ray.data._internal.datasource_v2.common.line_chunkable_file_reader import (
    LineChunkableFileReader,
)
from ray.data._internal.datasource_v2.interfaces.read_units import ReadUnitFragment
from ray.data._internal.util import infer_compression
from ray.data.datasource.file_based_datasource import _decompress_snappy

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem


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


def invalid_csv_error(path: str) -> ValueError:
    """Return V1 ``read_csv``'s error for a file pyarrow can't parse."""
    return ValueError(
        f"Failed to read CSV file: {path}. "
        "Please check the CSV file has correct format, or filter out non-CSV "
        "file with 'partition_filter' field. See read_csv() documentation for "
        "more details."
    )


def read_snappy_buffer(path: str, filesystem: "FileSystem") -> pa.Buffer:
    """Return a whole Snappy file decompressed, since pyarrow can't read it."""
    with filesystem.open_input_stream(path, compression=None) as file:
        return pa.py_buffer(_decompress_snappy(file, filesystem).getbuffer())


class CsvFileReader(LineChunkableFileReader):
    """Reads CSV files whole or in byte-range chunks.

    A chunk after the first has no header, so it is parsed with the
    plan-time schema's column names. The chunk at byte 0 checks that its
    header matches those names, in order.
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
        self._convert_options = convert_options

    @override
    def _make_format(self) -> pds.CsvFileFormat:
        return csv_file_format(
            self._read_options, self._parse_options, self._convert_options
        )

    @override
    def _iter_unit_tables(
        self,
        fragment_to_read: ReadUnitFragment,
        scanner_kwargs: dict,
    ) -> Iterator[pa.Table]:
        unit = fragment_to_read.unit
        try:
            if unit.id == unit.source and infer_compression(unit.source) == "snappy":
                buffer = read_snappy_buffer(
                    unit.source, fragment_to_read.fragment.filesystem
                )
                yield from self._iter_fragment_tables(
                    self._make_format().make_fragment(buffer), scanner_kwargs
                )
            else:
                yield from super()._iter_unit_tables(fragment_to_read, scanner_kwargs)
        except pa.ArrowInvalid as e:
            raise invalid_csv_error(unit.source) from e

    @override
    def _make_chunk_fragment(
        self, lines: memoryview, *, starts_at_file_start: bool, path: str
    ) -> pds.Fragment:
        has_header = not (
            self._read_options.column_names
            or self._read_options.autogenerate_column_names
        )
        if starts_at_file_start or not has_header:
            fragment = self._make_format().make_fragment(pa.py_buffer(lines))
            if has_header:
                self._check_header(fragment.physical_schema.names, path)
            return fragment

        headerless_options = copy.deepcopy(self._read_options)
        headerless_options.column_names = self._file_column_names()
        headerless_options.skip_rows = 0
        headerless_options.skip_rows_after_names = 0
        return csv_file_format(
            headerless_options, self._parse_options, self._convert_options
        ).make_fragment(pa.py_buffer(lines))

    def _file_column_names(self) -> List[str]:
        schema = self._file_dataset_schema
        assert schema is not None
        return schema.names

    def _check_header(self, header: List[str], path: str) -> None:
        expected = self._file_column_names()
        if header != expected:
            raise ValueError(
                f"CSV file {path} has columns {header}, but the files sampled "
                f"for the schema have {expected}. `read_csv` splits files larger "
                f"than {self._chunk_size} bytes and names the columns of each "
                "later piece from the sampled schema, so every such file must "
                "list the same columns in the same order. Set "
                "RAY_DATA_LINE_CHUNK_BYTES above the file size to read it "
                "whole, or set "
                "`ray.data.DataContext.get_current().use_datasource_v2 = False`."
            )
