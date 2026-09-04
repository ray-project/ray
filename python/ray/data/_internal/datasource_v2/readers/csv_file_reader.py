from __future__ import annotations

import copy
import io
from functools import partial
from typing import Any, Dict, Iterator, Optional

import pyarrow as pa
from pyarrow import csv
from pyarrow.fs import FileSystem, LocalFileSystem

from ray.data._internal.datasource_v2.chunkers.file_chunker import (
    LineDelimitedFileChunkMetadata,
)
from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.readers.base_reader import Reader
from ray.data._internal.util import infer_compression, iterate_with_retry
from ray.data.context import DataContext
from ray.data.datasource.file_based_datasource import _add_partitions_to_table
from ray.data.datasource.partitioning import Partitioning, PathPartitionParser
from ray.util.annotations import DeveloperAPI

_BOUNDARY_SCAN_SIZE = 64 * 1024


class _BoundedInputStream(io.RawIOBase):
    """Sequential view over a byte range of a random-access Arrow file."""

    def __init__(self, file: pa.NativeFile, start: int, end: int):
        super().__init__()
        self._file = file
        self._start = start
        self._length = end - start
        self._position = 0

    def readable(self) -> bool:
        return True

    def readinto(self, buffer) -> int:
        remaining = self._length - self._position
        if remaining <= 0:
            return 0
        size = min(len(buffer), remaining)
        data = self._file.read_at(size, self._start + self._position)
        bytes_read = len(data)
        buffer[:bytes_read] = data
        self._position += bytes_read
        return bytes_read


def _is_record_boundary(file: pa.NativeFile, offset: int, file_size: int) -> bool:
    if offset <= 0 or offset >= file_size:
        return True

    previous = file.read_at(1, offset - 1)
    if previous == b"\n":
        return True
    if previous == b"\r":
        # ``offset`` points between CR and LF in a CRLF terminator.
        return file.read_at(1, offset) != b"\n"
    return False


def _find_record_boundary(file: pa.NativeFile, offset: int, file_size: int) -> int:
    """Return the first byte after a record terminator at/after ``offset``."""
    position = offset
    while position < file_size:
        data = file.read_at(min(_BOUNDARY_SCAN_SIZE, file_size - position), position)
        for index, value in enumerate(data):
            absolute = position + index
            if value == ord("\n"):
                return absolute + 1
            if value == ord("\r"):
                if absolute + 1 < file_size and file.read_at(1, absolute + 1) == b"\n":
                    return absolute + 2
                return absolute + 1
        if not data:
            break
        position += len(data)
    return file_size


def _align_chunk_range(
    file: pa.NativeFile,
    metadata: LineDelimitedFileChunkMetadata,
    file_size: int,
) -> tuple[int, int]:
    """Align an estimated byte range to complete CSV record boundaries."""
    start = min(metadata["chunk_byte_start_idx"], file_size)
    end = min(metadata["chunk_byte_end_idx"], file_size)

    if not _is_record_boundary(file, start, file_size):
        start = _find_record_boundary(file, start, file_size)
    if not _is_record_boundary(file, end, file_size):
        end = _find_record_boundary(file, end, file_size)

    return start, max(start, end)


@DeveloperAPI
class CSVFileReader(Reader[FileManifest]):
    """Streaming CSV reader for whole-file and line-delimited manifests."""

    def __init__(
        self,
        *,
        schema: pa.Schema,
        physical_schema: Optional[pa.Schema] = None,
        filesystem: Optional[FileSystem] = None,
        partitioning: Optional[Partitioning] = None,
        include_paths: bool = False,
        read_options: Optional[csv.ReadOptions] = None,
        parse_options: Optional[csv.ParseOptions] = None,
        arrow_csv_args: Optional[Dict[str, Any]] = None,
        open_stream_args: Optional[Dict[str, Any]] = None,
    ):
        self._schema = schema
        self._physical_schema = (
            physical_schema if physical_schema is not None else schema
        )
        self._filesystem = filesystem or LocalFileSystem()
        self._partition_parser = (
            PathPartitionParser(partitioning) if partitioning is not None else None
        )
        self._include_paths = include_paths
        self._read_options = read_options or csv.ReadOptions(use_threads=False)
        self._parse_options = parse_options or csv.ParseOptions()
        self._arrow_csv_args = dict(arrow_csv_args or {})
        self._open_stream_args = dict(open_stream_args or {})
        self._file_schemas: Dict[str, pa.Schema] = {}

    def read(self, input_split: FileManifest) -> Iterator[pa.Table]:
        if len(input_split) == 0:
            return

        context = DataContext.get_current()
        for path, file_size, chunk_metadata in zip(
            input_split.paths,
            input_split.file_sizes,
            input_split.file_chunk_metadatas,
        ):
            iterator = partial(
                self._read_path,
                str(path),
                int(file_size),
                chunk_metadata,
            )
            for table in iterate_with_retry(
                iterator,
                f"read CSV file {path}",
                match=context.retried_io_errors,
            ):
                yield self._finalize_table(table, str(path))

    def inspect_schema(self, path: str) -> pa.Schema:
        """Infer the schema using the same streaming options as execution."""
        if path not in self._file_schemas:
            try:
                with self._open_input_stream(path) as stream:
                    reader = self._open_csv(stream)
                    self._file_schemas[path] = reader.schema
            except pa.ArrowInvalid as error:
                raise self._invalid_csv_error(path) from error
        return self._file_schemas[path]

    def _read_path(
        self,
        path: str,
        file_size: int,
        chunk_metadata: Optional[LineDelimitedFileChunkMetadata],
    ) -> Iterator[pa.Table]:
        try:
            if chunk_metadata is None:
                with self._open_input_stream(path) as stream:
                    yield from self._read_stream(stream)
            else:
                yield from self._read_chunk(path, file_size, chunk_metadata)
        except pa.ArrowInvalid as error:
            raise self._invalid_csv_error(path) from error

    def _read_chunk(
        self,
        path: str,
        file_size: int,
        metadata: LineDelimitedFileChunkMetadata,
    ) -> Iterator[pa.Table]:
        with self._filesystem.open_input_file(path) as file:
            # Manifest sizes describe the chunk, not necessarily the whole file.
            file_size = file.size()
            start, end = _align_chunk_range(file, metadata, file_size)
            if start >= end:
                return

            read_options = self._read_options
            arrow_csv_args = self._arrow_csv_args
            if start > 0:
                read_options = copy.deepcopy(self._read_options)
                read_options.column_names = self._physical_schema.names
                read_options.autogenerate_column_names = False
                read_options.skip_rows = 0
                read_options.skip_rows_after_names = 0

                arrow_csv_args = copy.deepcopy(self._arrow_csv_args)
                configured_convert_options = arrow_csv_args.get("convert_options")
                convert_options = (
                    csv.ConvertOptions()
                    if configured_convert_options is None
                    else copy.deepcopy(configured_convert_options)
                )
                column_types = dict(convert_options.column_types)
                column_types.update(
                    {field.name: field.type for field in self._physical_schema}
                )
                convert_options.column_types = column_types
                arrow_csv_args["convert_options"] = convert_options

            bounded = _BoundedInputStream(file, start, end)
            with pa.PythonFile(bounded, mode="r") as stream:
                yield from self._read_stream(
                    stream,
                    read_options=read_options,
                    arrow_csv_args=arrow_csv_args,
                )

    def _read_stream(
        self,
        stream: pa.NativeFile,
        *,
        read_options: Optional[csv.ReadOptions] = None,
        arrow_csv_args: Optional[Dict[str, Any]] = None,
    ) -> Iterator[pa.Table]:
        reader = self._open_csv(
            stream,
            read_options=read_options,
            arrow_csv_args=arrow_csv_args,
        )
        schema = None
        while True:
            try:
                batch = reader.read_next_batch()
            except StopIteration:
                return
            table = pa.Table.from_batches([batch], schema=schema)
            if schema is None:
                schema = table.schema
            yield table

    def _open_csv(
        self,
        stream: pa.NativeFile,
        *,
        read_options: Optional[csv.ReadOptions] = None,
        arrow_csv_args: Optional[Dict[str, Any]] = None,
    ):
        # Reinitialize the handler after serialization (ARROW-17641).
        parse_options = copy.deepcopy(self._parse_options)
        if hasattr(parse_options, "invalid_row_handler"):
            parse_options.invalid_row_handler = parse_options.invalid_row_handler
        return csv.open_csv(
            stream,
            read_options=read_options or self._read_options,
            parse_options=parse_options,
            **(arrow_csv_args or self._arrow_csv_args),
        )

    def _open_input_stream(self, path: str) -> pa.NativeFile:
        open_args = dict(self._open_stream_args)
        compression = open_args.get("compression")
        if compression is None:
            compression = infer_compression(path)
        open_args["compression"] = compression
        buffer_size = open_args.pop("buffer_size", None)
        if buffer_size is None:
            buffer_size = DataContext.get_current().streaming_read_buffer_size
        return self._filesystem.open_input_stream(
            path, buffer_size=buffer_size, **open_args
        )

    def _finalize_table(self, table: pa.Table, path: str) -> pa.Table:
        if self._partition_parser is not None:
            partitions = self._partition_parser(path)
            if partitions:
                table = _add_partitions_to_table(table, partitions)

        if self._include_paths:
            if "path" in table.column_names:
                table = table.drop(["path"])
            table = table.append_column(
                "path", pa.repeat(pa.scalar(path, type=pa.string()), table.num_rows)
            )

        unknown_columns = [
            name
            for name in table.column_names
            if self._schema.get_field_index(name) == -1
        ]
        if unknown_columns:
            raise ValueError(
                f"CSV file {path!r} contains columns that were not present in "
                f"the sampled schema: {unknown_columns}. Expected columns: "
                f"{self._schema.names}. Ray Data requires a consistent CSV "
                "schema across files."
            )

        arrays = []
        output_fields = []
        for field in self._schema:
            field_index = table.schema.get_field_index(field.name)
            if field_index == -1:
                arrays.append(pa.nulls(table.num_rows, type=field.type))
                output_fields.append(field)
                continue
            column = table.column(field_index)
            if column.type != field.type:
                column = column.cast(field.type)
            arrays.append(column)
            output_fields.append(field)

        output_schema = pa.schema(output_fields, metadata=self._schema.metadata)
        return pa.Table.from_arrays(arrays, schema=output_schema)

    @staticmethod
    def _invalid_csv_error(path: str) -> ValueError:
        return ValueError(
            f"Failed to read CSV file: {path}. "
            "Please check the CSV file has correct format, or filter out non-CSV "
            "file with 'partition_filter' field. See read_csv() documentation for "
            "more details."
        )
