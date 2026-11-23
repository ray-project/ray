"""``FileReader`` for JSON on DataSource V2, splitting JSON Lines at newlines."""

import copy
import io
import logging
from functools import cached_property
from typing import TYPE_CHECKING, Any, Iterator, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as pds
import pyarrow.json as pajson
from typing_extensions import override

from ray.data._internal.arrow_block import _BATCH_SIZE_PRESERVING_STUB_COL_NAME
from ray.data._internal.datasource.json_datasource import (
    iter_json_lines_frames,
    read_with_python_json,
)
from ray.data._internal.datasource_v2.common.file_reader import FileFormat
from ray.data._internal.datasource_v2.common.line_chunkable_file_reader import (
    _MAX_BLOCK_SIZE,
    LineChunkableFileReader,
    one_block_size,
    read_snappy_buffer,
)
from ray.data._internal.datasource_v2.common.line_chunking import iter_line_pieces
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.read_units import ReadUnitFragment
from ray.data._internal.util import infer_compression
from ray.data.context import DataContext
from ray.data.datasource.file_based_datasource import _add_partitions_to_dataframe

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem

logger = logging.getLogger(__name__)

_STRADDLING_ERROR = "straddling object straddles two block boundaries"


def infer_json_compression(path: str) -> Optional[str]:
    """Return the codec named by ``path``'s extension, ignoring case.

    ``infer_compression`` misses upper-case extensions and ``.br``.
    """
    lowered = path.lower()
    if lowered.endswith(".br"):
        return "brotli"
    return infer_compression(lowered)


def _is_json_lines_path(path: str) -> bool:
    """Whether ``path`` names a ``.jsonl`` file, compressed or not."""
    lowered = path.lower()
    if infer_json_compression(lowered) is not None:
        lowered = lowered.rsplit(".", 1)[0]
    return lowered.endswith(".jsonl")


def open_json_stream(path: str, filesystem: "FileSystem") -> pa.NativeFile:
    """Open a file for reading its decompressed bytes.

    A Snappy file is decompressed whole into memory, since pyarrow can't
    stream it.
    """
    compression = infer_json_compression(path)
    if compression == "snappy":
        return pa.BufferReader(read_snappy_buffer(path, filesystem))
    return filesystem.open_input_stream(path, compression=compression)


def read_whole_json_file(path: str, filesystem: "FileSystem") -> pa.Buffer:
    """Read a file into memory, decompressing by its extension."""
    with open_json_stream(path, filesystem) as file:
        return file.read_buffer()


def parse_json(
    buffer: pa.Buffer,
    path: str,
    *,
    read_options: pajson.ReadOptions,
    parse_options: pajson.ParseOptions,
    python_fallback: bool,
) -> Optional[pa.Table]:
    """Parse JSON with ``pyarrow.json.read_json``, as V1 ``read_json`` does.

    A line longer than ``read_options.block_size`` is retried with one block
    the size of ``buffer``. Other parse errors fall back to ``json.load``
    when ``python_fallback`` is set (a whole ``.json`` file, which may be a
    top-level array), and otherwise raise with ``path``.

    Args:
        buffer: The JSON text.
        path: The file it came from, for error messages.
        read_options: ``pyarrow.json`` read options.
        parse_options: ``pyarrow.json`` parse options.
        python_fallback: Whether to retry a parse error with ``json.load``.

    Returns:
        The parsed table, or ``None`` when ``buffer`` holds no rows.
    """
    if buffer.size == 0:
        return None
    try:
        try:
            # ``io.BytesIO`` rather than ``pa.BufferReader``: V1 parses a
            # Python file, where ``block_size=-1`` reads everything.
            return pajson.read_json(
                io.BytesIO(buffer),
                read_options=read_options,
                parse_options=parse_options,
            )
        except pa.ArrowInvalid as e:
            if _STRADDLING_ERROR not in str(e):
                raise
        one_block = copy.deepcopy(read_options)
        one_block.block_size = one_block_size(buffer)
        return pajson.read_json(
            io.BytesIO(buffer), read_options=one_block, parse_options=parse_options
        )
    except pa.ArrowInvalid as e:
        if _STRADDLING_ERROR in str(e):
            raise ValueError(
                f"Failed to parse JSON from file: {path}. A line is longer than "
                f"the largest block pyarrow reads, {_MAX_BLOCK_SIZE} bytes. "
                f"Original error: {e}"
            ) from e
        if not python_fallback:
            raise ValueError(
                f"Failed to parse line-delimited JSON from file: {path}. "
                f"Original error: {e}"
            ) from e
        logger.warning(
            f"Error reading {path} with pyarrow.json.read_json(). "
            f"Falling back to native json.load(), which may be slower. "
            f"PyArrow error was:\n{e}"
        )
        return read_with_python_json(buffer, path)


class JsonFileReader(LineChunkableFileReader):
    """Reads JSON files whole and ``.jsonl`` files in byte-range chunks.

    Each unit is parsed into a table with ``pyarrow.json.read_json`` and
    scanned as an in-memory fragment. A ``pyarrow.dataset`` JSON scan would
    infer the schema from its first block and drop keys that first appear
    later.
    """

    def __init__(
        self,
        *,
        read_options: pajson.ReadOptions,
        parse_options: pajson.ParseOptions,
        **line_chunkable_kwargs: Any,
    ):
        super().__init__(FileFormat.JSON, **line_chunkable_kwargs)
        self._read_options = read_options
        self._parse_options = parse_options

    @cached_property
    def _file_dataset_schema(self) -> Optional[pa.Schema]:
        # An explicit schema keeps ``pds.dataset`` from inspecting a file,
        # which fails for a top-level JSON array.
        return super()._file_dataset_schema or pa.schema([])

    @override
    def _make_format(self) -> pds.JsonFileFormat:
        return pds.JsonFileFormat(
            read_options=self._read_options, parse_options=self._parse_options
        )

    @override
    def _iter_unit_tables(
        self,
        fragment_to_read: ReadUnitFragment,
        scanner_kwargs: dict,
    ) -> Iterator[pa.Table]:
        unit = fragment_to_read.unit
        if unit.id != unit.source:
            yield from super()._iter_unit_tables(fragment_to_read, scanner_kwargs)
            return
        if self._streams_in_pieces(unit.source):
            with open_json_stream(unit.source, self._filesystem) as file:
                for piece in iter_line_pieces(file, self._chunk_size):
                    fragment = self._make_chunk_fragment(
                        memoryview(piece), starts_at_file_start=False, path=unit.source
                    )
                    if fragment is not None:
                        yield from self._iter_fragment_tables(fragment, scanner_kwargs)
            return
        table = parse_json(
            read_whole_json_file(unit.source, self._filesystem),
            unit.source,
            read_options=self._read_options,
            parse_options=self._parse_options,
            python_fallback=True,
        )
        if table is not None:
            # An in-memory dataset has one fragment per record batch.
            for fragment in pds.dataset(table).get_fragments():
                yield from self._iter_fragment_tables(fragment, scanner_kwargs)

    def _streams_in_pieces(self, path: str) -> bool:
        """Whether a whole-file unit is read as pieces of lines, not parsed whole.

        Parsing a file whole holds several times its size in memory. A JSON Lines
        file that is compressed, of unknown size or larger than a chunk but can't
        be split is read in pieces instead, each parsed like a split chunk. A
        ``.json`` file may be one document, so it is parsed whole.
        """
        size = self._file_sizes[path]
        return (
            not self._parse_options.newlines_in_values
            and _is_json_lines_path(path)
            and (
                size is None
                or infer_json_compression(path) is not None
                or size > self._chunk_size
            )
        )

    @override
    def _make_chunk_fragment(
        self, lines: memoryview, *, starts_at_file_start: bool, path: str
    ) -> Optional[pds.Fragment]:
        # One block parses the chunk into one record batch, hence one
        # fragment, and no line can straddle two blocks.
        read_options = copy.deepcopy(self._read_options)
        read_options.block_size = one_block_size(lines)
        table = parse_json(
            pa.py_buffer(lines),
            path,
            read_options=read_options,
            parse_options=self._parse_options,
            python_fallback=False,
        )
        assert table is not None
        # A no-op unless the chunk exceeded the largest ``block_size``.
        table = table.combine_chunks()
        # A chunk of only blank lines parses to no columns, hence no fragment.
        return next(iter(pds.dataset(table).get_fragments()), None)


class PandasJsonLinesFileReader(LineChunkableFileReader):
    """Reads JSON Lines with ``pandas.read_json``, as V1 ``read_json(lines=True)``.

    Byte-range chunks, whole files and streamed (compressed or unknown-size)
    files are parsed into pandas DataFrames, so a column pyarrow can't type,
    such as mixed types or nested values of changing shape, stays a column of
    Python objects.

    This reader never calls ``raise_on_pickle_object_columns``: the check guards
    ``pa.Table`` columns of pickled objects, and a DataFrame here holds values
    ``json`` decoded, so nothing is unpickled. The frames must stay pandas, since
    converting a mixed-type column to Arrow would pickle it.
    """

    def __init__(self, **line_chunkable_kwargs: Any):
        super().__init__(FileFormat.JSON, **line_chunkable_kwargs)

    @cached_property
    def _file_dataset_schema(self) -> Optional[pa.Schema]:
        # An explicit schema keeps ``pds.dataset`` from inspecting a file.
        return super()._file_dataset_schema or pa.schema([])

    @override
    def read(self, input_split: FileManifest) -> Iterator[pd.DataFrame]:
        if len(input_split) == 0:
            return
        dataset = self._make_dataset(input_split)
        requested = None if self._columns is None else set(self._columns)

        rows_read = 0
        for df, position in self._read_fragment_batches(dataset, {}, input_split):
            # ``read_json`` numbers rows across chunks; column assignment
            # aligns on the index.
            df = df.reset_index(drop=True)
            if self._limit is not None:
                if rows_read >= self._limit:
                    break
                df = df.iloc[: self._limit - rows_read]

            if self._partition_parser is not None:
                partitions = self._get_partition_values(position.unit.source)
                if requested is not None:
                    partitions = {k: v for k, v in partitions.items() if k in requested}
                df = _add_partitions_to_dataframe(df, partitions)

            for column in self._synthesized_columns:
                if requested is None or column.name in requested:
                    df[column.name] = column.compute(position, len(df)).to_numpy(
                        zero_copy_only=False
                    )

            if self._columns is not None:
                for column in self._columns:
                    if column not in df.columns:
                        df[column] = None
                df = df[self._columns]
            if df.shape[1] == 0 and len(df) > 0:
                # Keeps the row count of an empty projection, as ``FileReader``.
                df[_BATCH_SIZE_PRESERVING_STUB_COL_NAME] = None

            rows_read += len(df)
            yield df

    @override
    def _iter_unit_tables(
        self,
        fragment_to_read: ReadUnitFragment,
        scanner_kwargs: dict,
    ) -> Iterator[pd.DataFrame]:
        unit = fragment_to_read.unit
        target = DataContext.get_current().target_max_block_size
        if unit.id != unit.source:
            lines = self._read_chunk_lines(fragment_to_read)
            if lines is not None:
                with pa.BufferReader(pa.py_buffer(lines)) as file:
                    yield from iter_json_lines_frames(file, unit.source, target)
            return

        data = self._read_whole_file(fragment_to_read)
        if data is not None:
            if len(data):
                with pa.BufferReader(pa.py_buffer(data)) as file:
                    yield from iter_json_lines_frames(file, unit.source, target)
            return

        with open_json_stream(unit.source, self._filesystem) as file:
            yield from iter_json_lines_frames(file, unit.source, target)
