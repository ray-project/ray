from __future__ import annotations

import copy
import io
from collections import Counter
from functools import partial
from typing import Any, Dict, Iterator, Optional, Tuple

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
from ray.data.datasource.file_based_datasource import (
    _add_partitions_to_table,
    _file_to_snappy_stream,
)
from ray.data.datasource.partitioning import Partitioning, PathPartitionParser
from ray.util.annotations import DeveloperAPI

_BOUNDARY_SCAN_SIZE = 64 * 1024


class _BoundedInputStream(io.RawIOBase):
    """Sequential view over a byte range of a random-access Arrow file.

    Bytes are fetched in ``read_ahead_size`` slabs. Arrow's CSV reader asks for
    one ``ReadOptions.block_size`` (1 MiB by default) at a time; forwarding each
    of those requests to ``read_at`` would turn a 256 MiB chunk into hundreds of
    range requests against an object store. Reading ahead in
    ``DataContext.streaming_read_buffer_size`` slabs keeps the request pattern
    of a chunked read in line with V1's buffered ``open_input_stream``.
    """

    def __init__(
        self,
        file: pa.NativeFile,
        start: int,
        end: int,
        *,
        read_ahead_size: int,
    ):
        super().__init__()
        self._file = file
        self._start = start
        self._length = max(0, end - start)
        self._position = 0
        self._read_ahead_size = max(1, read_ahead_size)
        self._buffered = memoryview(b"")
        # Arrow strips one UTF-8 BOM from every new CSV stream. For a noninitial
        # chunk, give it a synthetic BOM so a real U+FEFF in the first field is
        # preserved. This also works for headerless input and custom dialects.
        self._prefix = b"\xef\xbb\xbf" if start > 0 else b""

    def readable(self) -> bool:
        return True

    def readinto(self, buffer) -> int:
        written = 0
        if self._prefix:
            size = min(len(buffer), len(self._prefix))
            buffer[:size] = self._prefix[:size]
            self._prefix = self._prefix[size:]
            written = size
        while written < len(buffer):
            if not self._buffered and not self._read_ahead():
                break
            size = min(len(buffer) - written, len(self._buffered))
            buffer[written : written + size] = self._buffered[:size]
            self._buffered = self._buffered[size:]
            written += size
        return written

    def _read_ahead(self) -> bool:
        remaining = self._length - self._position
        if remaining <= 0:
            return False
        data = self._file.read_at(
            min(self._read_ahead_size, remaining), self._start + self._position
        )
        if not data:
            # The file is shorter than the manifest claimed; treat it as EOF.
            self._position = self._length
            return False
        self._position += len(data)
        self._buffered = memoryview(data)
        return True


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
) -> Tuple[int, int]:
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
    """Streaming CSV reader for whole-file and line-delimited manifests.

    Whole-file manifest rows stream through ``open_input_stream`` exactly like
    the V1 datasource. Byte-range rows open the file for random access, align
    the range to record boundaries, and parse it with the column names and
    types taken from the file's own header block. Every file is therefore
    self-describing: no schema travels in the manifest, and a file whose
    columns or inferred types differ from the driver's bounded sample is still
    read in parallel with its own types, matching V1's per-file behavior.
    """

    def __init__(
        self,
        *,
        filesystem: Optional[FileSystem] = None,
        partitioning: Optional[Partitioning] = None,
        include_paths: bool = False,
        read_options: Optional[csv.ReadOptions] = None,
        parse_options: Optional[csv.ParseOptions] = None,
        arrow_csv_args: Optional[Dict[str, Any]] = None,
        open_stream_args: Optional[Dict[str, Any]] = None,
    ):
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
        # Manifest sizes only weight partitioning; the reader sizes chunks from
        # the file itself, so an unknown or stale size can't affect the read.
        for path, chunk_metadata in zip(
            input_split.paths, input_split.file_chunk_metadatas
        ):
            path = str(path)
            iterator = partial(self._read_path, path, chunk_metadata)
            for table in iterate_with_retry(
                iterator,
                f"read CSV file {path}",
                match=context.retried_io_errors,
            ):
                yield self._finalize_table(table, path)

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
        chunk_metadata: Optional[LineDelimitedFileChunkMetadata],
    ) -> Iterator[pa.Table]:
        try:
            if chunk_metadata is None:
                with self._open_input_stream(path) as stream:
                    yield from self._read_stream(stream)
            else:
                yield from self._read_chunk(path, chunk_metadata)
        except pa.ArrowInvalid as error:
            raise self._invalid_csv_error(path) from error

    def _read_chunk(
        self,
        path: str,
        metadata: LineDelimitedFileChunkMetadata,
    ) -> Iterator[pa.Table]:
        with self._filesystem.open_input_file(path) as file:
            file_size = file.size()
            start, end = _align_chunk_range(file, metadata, file_size)
            if start >= end:
                return

            read_options = self._read_options
            arrow_csv_args = self._arrow_csv_args
            if start > 0:
                file_schema = self._inspect_chunked_file_schema(path, file, file_size)
                read_options, arrow_csv_args = self._headerless_chunk_options(
                    file_schema
                )

            bounded = _BoundedInputStream(
                file,
                start,
                end,
                read_ahead_size=DataContext.get_current().streaming_read_buffer_size,
            )
            with pa.PythonFile(bounded, mode="r") as stream:
                yield from self._read_stream(
                    stream,
                    read_options=read_options,
                    arrow_csv_args=arrow_csv_args,
                )

    def _inspect_chunked_file_schema(
        self, path: str, file: pa.NativeFile, file_size: int
    ) -> pa.Schema:
        """Return the schema Arrow infers from the file's own first block.

        A noninitial byte-range chunk has no header, so it needs the file's
        physical column names, and it must use the types a whole-file
        streaming read would have used: those inferred from the first block.
        Reading that block here costs one ``ReadOptions.block_size`` range
        request per read task, and keeps chunked reads independent of the
        driver's bounded schema sample.
        """
        if path not in self._file_schemas:
            prefix = _BoundedInputStream(
                file, 0, file_size, read_ahead_size=self._read_options.block_size
            )
            with pa.PythonFile(prefix, mode="r") as stream:
                self._file_schemas[path] = self._open_csv(stream).schema
        return self._file_schemas[path]

    def _headerless_chunk_options(
        self, file_schema: pa.Schema
    ) -> Tuple[csv.ReadOptions, Dict[str, Any]]:
        read_options = copy.deepcopy(self._read_options)
        read_options.column_names = file_schema.names
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
        # Pin every type the first block could infer. A column that was
        # entirely null there stays unpinned so a chunk that does have values
        # for it converts instead of failing; downstream schema unification
        # promotes the null-typed blocks.
        column_types.update(
            {
                field.name: field.type
                for field in file_schema
                if not pa.types.is_null(field.type)
            }
        )
        convert_options.column_types = column_types
        arrow_csv_args["convert_options"] = convert_options
        return read_options, arrow_csv_args

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
        if compression == "snappy":
            open_args["compression"] = None
            file = self._filesystem.open_input_stream(
                path, buffer_size=buffer_size, **open_args
            )
            return _file_to_snappy_stream(file, self._filesystem)
        return self._filesystem.open_input_stream(
            path, buffer_size=buffer_size, **open_args
        )

    def _finalize_table(self, table: pa.Table, path: str) -> pa.Table:
        duplicate_columns = [
            name for name, count in Counter(table.column_names).items() if count > 1
        ]
        if duplicate_columns:
            raise ValueError(
                f"CSV file {path!r} contains duplicate column names: "
                f"{duplicate_columns}. Duplicate CSV headers can't be aligned "
                "unambiguously across files."
            )

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

        # Preserve the concrete per-file schema. In particular, don't turn an
        # absent column into a present null-valued column, and don't cast a
        # file's inferred scalar types to the bounded sampled schema. V1 emits
        # the native table for each file and downstream schema discovery unifies
        # block schemas only when needed.
        return table

    @staticmethod
    def _invalid_csv_error(path: str) -> ValueError:
        return ValueError(
            f"Failed to read CSV file: {path}. "
            "Please check the CSV file has correct format, or filter out non-CSV "
            "file with 'partition_filter' field. See read_csv() documentation for "
            "more details."
        )
