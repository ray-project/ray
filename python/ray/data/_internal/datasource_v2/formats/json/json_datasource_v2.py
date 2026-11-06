"""Concrete ``DataSourceV2`` for JSON and JSON Lines files."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.json as pajson
from typing_extensions import override

from ray.data._internal.datasource_v2.common.arrow_file_scanner import (
    ArrowFileScanner,
)
from ray.data._internal.datasource_v2.common.file_reader import (
    _ARROW_DEFAULT_BATCH_SIZE,
)
from ray.data._internal.datasource_v2.common.line_chunk_file_indexer import (
    LineChunkFileIndexer,
)
from ray.data._internal.datasource_v2.common.line_chunkable_datasource_v2 import (
    LineChunkableDatasourceV2,
)
from ray.data._internal.datasource_v2.common.line_chunkable_file_reader import (
    read_snappy_buffer,
)
from ray.data._internal.datasource_v2.formats.json.json_file_reader import (
    JsonFileReader,
    parse_json,
    read_whole_json_file,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import (
    FileIndexer,
    FileInfo,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import ChunkMetadata
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    SynthesizedColumn,
)
from ray.data._internal.util import MiB, infer_compression
from ray.data.expressions import Expr
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem

# ``read_json`` keyword arguments this datasource takes; any other goes to V1.
JSON_V2_ARGS = frozenset({"read_options", "parse_options"})

# Bytes of each sampled file the plan-time schema is inferred from.
_SCHEMA_SAMPLE_BYTES = 1 * MiB


class _JsonLinesFileIndexer(LineChunkFileIndexer):
    """Splits only ``.jsonl`` files: a ``.json`` file may be one document."""

    @override
    def _chunk_metadatas_of_file(
        self, file_info: FileInfo
    ) -> List[Optional[ChunkMetadata]]:
        if not file_info.path.lower().endswith(".jsonl"):
            return [None]
        return super()._chunk_metadatas_of_file(file_info)


def _read_json_sample(path: str, filesystem: "FileSystem") -> Tuple[pa.Buffer, bool]:
    """Return the file's first whole lines up to ``_SCHEMA_SAMPLE_BYTES``.

    Args:
        path: The file to sample.
        filesystem: The filesystem holding ``path``.

    Returns:
        The sample, and whether it is the whole file.
    """
    if infer_compression(path) == "snappy":
        return read_snappy_buffer(path, filesystem), True
    with filesystem.open_input_stream(
        path, compression=infer_compression(path)
    ) as file:
        data = file.read(_SCHEMA_SAMPLE_BYTES + 1)
    if len(data) <= _SCHEMA_SAMPLE_BYTES:
        return pa.py_buffer(data), True
    last_newline = data.rfind(b"\n", 0, _SCHEMA_SAMPLE_BYTES)
    if last_newline == -1:
        return read_whole_json_file(path, filesystem), True
    return pa.py_buffer(data[: last_newline + 1]), False


@DeveloperAPI
@dataclass(frozen=True)
class JsonScanner(ArrowFileScanner):
    """Configure JSON reads through :class:`JsonFileReader`."""

    synthesized_columns: Tuple[SynthesizedColumn, ...] = ()
    chunk_size: int = 0
    lines: bool = False
    read_options: Optional[pajson.ReadOptions] = None
    parse_options: Optional[pajson.ParseOptions] = None

    @override
    def push_filters(
        self, predicate: Expr
    ) -> Tuple["ArrowFileScanner", Optional[Expr]]:
        """Keep filters above the read, as V1 ``read_json`` does.

        Column types are inferred per chunk, so a filter typed against the
        sampled schema may not bind to every chunk.
        """
        return self, predicate

    @override
    def create_reader(self) -> JsonFileReader:
        return JsonFileReader(
            lines=self.lines,
            read_options=self.read_options,
            parse_options=self.parse_options,
            chunk_size=self.chunk_size,
            batch_size=self.batch_size or _ARROW_DEFAULT_BATCH_SIZE,
            columns=list(self.columns) if self.columns is not None else None,
            predicate=self.predicate,
            limit=self.limit,
            filesystem=self.filesystem,
            partitioning=self.partitioning,
            ignore_prefixes=self.ignore_prefixes,
            synthesized_columns=self.synthesized_columns,
            schema=self.schema,
        )


@DeveloperAPI
class JsonDatasourceV2(LineChunkableDatasourceV2):
    """V2 datasource that lists JSON files and reads them with ``JsonScanner``.

    Splits large uncompressed JSON Lines files into byte-range chunks: every
    such file when ``lines=True``, else only ``.jsonl`` files. Files stay
    whole when ``parse_options.newlines_in_values`` lets an object span
    lines. With ``lines=False`` a whole file that isn't JSON Lines, such as
    a top-level array, falls back to ``json.load`` as in V1.
    """

    _FORMAT_NAME = "JSON"

    def __init__(
        self,
        paths: List[str],
        *,
        lines: bool = False,
        arrow_json_args: Optional[Dict[str, Any]] = None,
        **line_chunkable_kwargs: Any,
    ):
        super().__init__(paths, name="JsonV2", **line_chunkable_kwargs)
        arrow_json_args = dict(arrow_json_args or {})
        assert set(arrow_json_args) <= JSON_V2_ARGS, sorted(arrow_json_args)
        self._lines = lines
        self._read_options: pajson.ReadOptions = arrow_json_args.get(
            "read_options"
        ) or pajson.ReadOptions(use_threads=False)
        self._parse_options: pajson.ParseOptions = (
            arrow_json_args.get("parse_options") or pajson.ParseOptions()
        )

    @override
    def _can_split_files(self) -> bool:
        return not self._parse_options.newlines_in_values

    @override
    def _get_file_indexer(self) -> FileIndexer:
        if self._can_split_files() and not self._lines:
            return _JsonLinesFileIndexer(
                chunk_size=self._chunk_size,
                ignore_missing_paths=self._ignore_missing_paths,
            )
        return super()._get_file_indexer()

    @override
    def _read_file_schema(self, path: str) -> pa.Schema:
        sample, is_whole_file = _read_json_sample(path, self._filesystem)
        if not is_whole_file:
            try:
                return self._schema_of(sample, path, python_fallback=False)
            except ValueError:
                if self._lines:
                    raise
            # A ``.json`` file may be one document, such as a top-level array.
            sample = read_whole_json_file(path, self._filesystem)
        return self._schema_of(sample, path, python_fallback=not self._lines)

    def _schema_of(
        self, buffer: pa.Buffer, path: str, *, python_fallback: bool
    ) -> pa.Schema:
        table = parse_json(
            buffer,
            path,
            read_options=self._read_options,
            parse_options=self._parse_options,
            python_fallback=python_fallback,
        )
        return table.schema if table is not None else pa.schema([])

    @override
    def create_scanner(
        self,
        schema: pa.Schema,
        filesystem: Optional["FileSystem"] = None,
        **options,
    ) -> JsonScanner:
        return JsonScanner(
            schema=schema,
            filesystem=filesystem or self._filesystem,
            partitioning=options.get("partitioning", self._partitioning),
            synthesized_columns=self._synthesized_columns,
            chunk_size=self._chunk_size,
            lines=self._lines,
            read_options=self._read_options,
            parse_options=self._parse_options,
        )
