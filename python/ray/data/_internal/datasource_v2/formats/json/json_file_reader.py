"""``FileReader`` for JSON on DataSource V2, splitting JSON Lines at newlines."""

import copy
import io
import logging
from functools import cached_property
from typing import TYPE_CHECKING, Any, Iterator, Optional

import pyarrow as pa
import pyarrow.dataset as pds
import pyarrow.json as pajson
from typing_extensions import override

from ray.data._internal.datasource.json_datasource import read_with_python_json
from ray.data._internal.datasource_v2.common.file_reader import FileFormat
from ray.data._internal.datasource_v2.common.line_chunkable_file_reader import (
    LineChunkableFileReader,
    read_snappy_buffer,
)
from ray.data._internal.datasource_v2.interfaces.read_units import ReadUnitFragment
from ray.data._internal.util import infer_compression

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem

logger = logging.getLogger(__name__)

_STRADDLING_ERROR = "straddling object straddles two block boundaries"

# ``ReadOptions.block_size`` is a 32-bit int.
_MAX_BLOCK_SIZE = 2**31 - 1


def read_whole_json_file(path: str, filesystem: "FileSystem") -> pa.Buffer:
    """Read a file into memory, decompressing by its extension as V1 does."""
    compression = infer_compression(path)
    if compression == "snappy":
        return read_snappy_buffer(path, filesystem)
    with filesystem.open_input_stream(path, compression=compression) as file:
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
        one_block.block_size = min(buffer.size, _MAX_BLOCK_SIZE)
        return pajson.read_json(
            io.BytesIO(buffer), read_options=one_block, parse_options=parse_options
        )
    except pa.ArrowInvalid as e:
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
        return next(read_with_python_json(buffer, path), None)


class JsonFileReader(LineChunkableFileReader):
    """Reads JSON files whole and JSON Lines files in byte-range chunks.

    Each unit is parsed into a table with ``pyarrow.json.read_json`` and
    scanned as an in-memory fragment. A ``pyarrow.dataset`` JSON scan would
    infer the schema from its first block and drop keys that first appear
    later.
    """

    def __init__(
        self,
        *,
        lines: bool,
        read_options: pajson.ReadOptions,
        parse_options: pajson.ParseOptions,
        **line_chunkable_kwargs: Any,
    ):
        super().__init__(FileFormat.JSON, **line_chunkable_kwargs)
        self._lines = lines
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
        else:
            buffer = read_whole_json_file(
                unit.source, fragment_to_read.fragment.filesystem
            )
            table = parse_json(
                buffer,
                unit.source,
                read_options=self._read_options,
                parse_options=self._parse_options,
                python_fallback=not self._lines,
            )
            if table is not None:
                # An in-memory dataset has one fragment per record batch.
                for fragment in pds.dataset(table).get_fragments():
                    yield from self._iter_fragment_tables(fragment, scanner_kwargs)

    @override
    def _make_chunk_fragment(
        self, lines: memoryview, *, starts_at_file_start: bool, path: str
    ) -> pds.Fragment:
        # One block parses the chunk into one record batch, hence one
        # fragment, and no line can straddle two blocks.
        read_options = copy.deepcopy(self._read_options)
        read_options.block_size = min(
            max(read_options.block_size, len(lines)), _MAX_BLOCK_SIZE
        )
        table = parse_json(
            pa.py_buffer(lines),
            path,
            read_options=read_options,
            parse_options=self._parse_options,
            python_fallback=False,
        )
        assert table is not None
        # A no-op unless the chunk exceeded ``_MAX_BLOCK_SIZE``.
        table = table.combine_chunks()
        return next(iter(pds.dataset(table).get_fragments()))
