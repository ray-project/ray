"""Concrete ``DataSourceV2`` for CSV files."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Tuple, Union

import pyarrow as pa
import pyarrow.csv as pcsv
from typing_extensions import override

import ray
from ray._common.utils import env_integer
from ray.data._internal.datasource_v2.common.actor_pool import (
    LazyActorPool,
    dispatch_batches,
    reader_actor_pool,
)
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
from ray.data._internal.datasource_v2.common.listing_utils import batch_file_infos
from ray.data._internal.datasource_v2.formats.csv.csv_file_reader import (
    COLUMN_NAMES_KEY,
    CsvFileReader,
    csv_file_format,
    has_header,
    invalid_csv_error,
)
from ray.data._internal.datasource_v2.formats.csv.header_reader import (
    HeaderReader,
    HeaderReaderActor,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import (
    FileIndexer,
    FileInfo,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    ChunkMetadata,
    FileManifest,
)
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    SynthesizedColumn,
)
from ray.data._internal.util import infer_compression
from ray.data.expressions import Expr
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem

logger = logging.getLogger(__name__)

# ``read_csv`` keyword arguments this datasource takes; any other goes to V1.
CSV_V2_ARGS = frozenset({"read_options", "parse_options", "convert_options"})

_DEFAULT_NUM_ACTORS = env_integer("RAY_DATA_CSV_HEADER_NUM_ACTORS", 8)
_DEFAULT_IO_CONCURRENCY = env_integer("RAY_DATA_CSV_HEADER_IO_CONCURRENCY", 32)
_DEFAULT_HEADER_BATCH_SIZE = env_integer("RAY_DATA_CSV_HEADER_BATCH_SIZE", 32)
# Actor restarts and batch re-runs after a death or OOM, as the footer
# indexer restores actors.
_MAX_ACTOR_RETRIES = 10


class CsvFileIndexer(LineChunkFileIndexer):
    """Lists CSV files in chunks, storing each split file's column names.

    A chunk after the first has no header, so the indexer reads the split
    file's header line once and puts its column names in every chunk's
    metadata. The first ``RAY_DATA_CSV_HEADER_BATCH_SIZE`` split files read
    their headers inline in the listing task, so a few large files pay no
    actor startup. Further headers read in a spread actor pool, as Parquet
    indexers read footers, made when the first of them is listed;
    ``RAY_DATA_CSV_HEADER_NUM_ACTORS=0`` reads every header inline.
    """

    def __init__(
        self,
        *,
        filesystem: "FileSystem",
        read_options: pcsv.ReadOptions,
        parse_options: pcsv.ParseOptions,
        **line_chunk_kwargs: Any,
    ):
        super().__init__(**line_chunk_kwargs)
        self._filesystem = filesystem
        # Knobs are re-read here, not at import time, so ``monkeypatch.setenv``
        # and release-test env overrides work after import.
        self._num_actors = env_integer(
            "RAY_DATA_CSV_HEADER_NUM_ACTORS", _DEFAULT_NUM_ACTORS
        )
        self._io_concurrency = env_integer(
            "RAY_DATA_CSV_HEADER_IO_CONCURRENCY", _DEFAULT_IO_CONCURRENCY
        )
        self._header_batch_size = env_integer(
            "RAY_DATA_CSV_HEADER_BATCH_SIZE", _DEFAULT_HEADER_BATCH_SIZE
        )
        # Default convert options, so ``include_columns`` can't drop names.
        self._header_format = csv_file_format(
            read_options, parse_options, pcsv.ConvertOptions()
        )
        self._reader = HeaderReader(
            filesystem=filesystem,
            file_format=self._header_format,
            io_concurrency=self._io_concurrency,
        )
        self._header_names: Dict[str, List[str]] = {}
        self._inline_headers_read = 0

    @override
    def _process_file_infos_to_manifests(
        self, file_infos: Iterable[FileInfo]
    ) -> Iterable[FileManifest]:
        if self._num_actors == 0:
            yield from super()._process_file_infos_to_manifests(file_infos)
            return
        try:
            self._header_names = {}
            self._inline_headers_read = 0
            with reader_actor_pool(
                HeaderReaderActor,
                num_actors=self._num_actors,
                options={
                    # Restart an actor that dies or OOMs and re-run its batch.
                    "max_restarts": _MAX_ACTOR_RETRIES,
                    "max_task_retries": _MAX_ACTOR_RETRIES,
                },
                filesystem=self._filesystem,
                file_format=self._header_format,
                io_concurrency=self._io_concurrency,
            ) as pool:
                yield from super()._process_file_infos_to_manifests(
                    self._with_prefetched_headers(file_infos, pool)
                )
        finally:
            self._header_names = {}
            self._inline_headers_read = 0

    def _with_prefetched_headers(
        self,
        file_infos: Iterable[FileInfo],
        pool: LazyActorPool,
    ) -> Iterable[FileInfo]:
        """Yield files in order, their headers read ahead by the actor pool.

        The pool is made when the first file past the inline headers is
        listed, so a listing of a few split files starts no actors.
        """

        def submit(
            pool: LazyActorPool, batch: List[FileInfo]
        ) -> Optional[Tuple[List[FileInfo], Union[List[List[str]], "ray.ObjectRef"]]]:
            with_headers = [info for info in batch if self._is_chunked(info)]
            if not with_headers:
                return None
            if self._inline_headers_read < self._header_batch_size:
                self._inline_headers_read += len(with_headers)
                # ``@ray.method`` types the plain call as an actor method.
                # pyrefly: ignore[not-callable]
                names = self._reader.read_headers(with_headers)
                return with_headers, names
            # ``ActorProxy`` stubs type ``.remote`` narrower than it is.
            ref = pool.next_actor().read_headers.remote(
                with_headers
            )  # pyrefly: ignore[missing-attribute]
            return with_headers, ref

        batches = batch_file_infos(file_infos, self._header_batch_size)
        for batch, headers in dispatch_batches(pool, batches, submit):
            if headers is not None:
                with_headers, names_or_ref = headers
                names_per_file = (
                    ray.get(names_or_ref)
                    if isinstance(names_or_ref, ray.ObjectRef)
                    else names_or_ref
                )
                for info, names in zip(with_headers, names_per_file):
                    self._header_names[info.path] = names
            yield from batch

    @override
    def _chunk_metadatas_of_file(
        self, file_info: FileInfo
    ) -> List[Optional[ChunkMetadata]]:
        metadatas = super()._chunk_metadatas_of_file(file_info)
        if metadatas == [None]:
            return metadatas
        names = self._header_names.pop(file_info.path, None)
        if names is None:
            # The inline path (``RAY_DATA_CSV_HEADER_NUM_ACTORS=0``) and
            # direct off-listing calls read the header right here.
            names = self._reader.read_header(file_info)
        return [
            None if metadata is None else {**metadata, COLUMN_NAMES_KEY: names}
            for metadata in metadatas
        ]


@DeveloperAPI
@dataclass(frozen=True)
class CsvScanner(ArrowFileScanner):
    """Configure CSV reads through :class:`CsvFileReader`."""

    synthesized_columns: Tuple[SynthesizedColumn, ...] = ()
    chunk_size: int = 0
    read_options: Optional[pcsv.ReadOptions] = None
    parse_options: Optional[pcsv.ParseOptions] = None
    convert_options: Optional[pcsv.ConvertOptions] = None

    @override
    def push_filters(
        self, predicate: Expr
    ) -> Tuple["ArrowFileScanner", Optional[Expr]]:
        """Keep filters above the read, as V1 ``read_csv`` does.

        Column types are inferred per chunk, so a filter typed against the
        sampled schema may not bind to every chunk.
        """
        return self, predicate

    @override
    def create_reader(self) -> CsvFileReader:
        return CsvFileReader(
            read_options=self.read_options,
            parse_options=self.parse_options,
            convert_options=self.convert_options,
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
class CsvDatasourceV2(LineChunkableDatasourceV2):
    """V2 datasource that lists CSV files and reads them with ``CsvScanner``.

    Splits a large uncompressed file into byte-range chunks unless the parse
    options let a row span lines (``newlines_in_values`` or
    ``escape_char``), skip rows at the start of each file, or use a non-UTF-8
    encoding.
    """

    _FORMAT_NAME = "CSV"

    def __init__(
        self,
        paths: List[str],
        *,
        arrow_csv_args: Optional[Dict[str, Any]] = None,
        **line_chunkable_kwargs: Any,
    ):
        super().__init__(paths, name="CsvV2", **line_chunkable_kwargs)
        arrow_csv_args = dict(arrow_csv_args or {})
        assert set(arrow_csv_args) <= CSV_V2_ARGS, sorted(arrow_csv_args)
        self._read_options: pcsv.ReadOptions = arrow_csv_args.get(
            "read_options"
        ) or pcsv.ReadOptions(use_threads=False)
        self._parse_options: pcsv.ParseOptions = (
            arrow_csv_args.get("parse_options") or pcsv.ParseOptions()
        )
        self._convert_options: pcsv.ConvertOptions = (
            arrow_csv_args.get("convert_options") or pcsv.ConvertOptions()
        )

    @override
    def _can_split_files(self) -> bool:
        # TODO: Support quoted newlines (``newlines_in_values``) in split reads.
        encoding = self._read_options.encoding.lower().replace("-", "")
        return (
            not self._parse_options.newlines_in_values
            # An escaped newline breaks the same every-newline-ends-a-row
            # assumption that ``newlines_in_values`` breaks. Unset is falsy
            # (``None`` or pyarrow's ``False``).
            and not self._parse_options.escape_char
            and self._read_options.skip_rows == 0
            and self._read_options.skip_rows_after_names == 0
            and encoding == "utf8"
        )

    @override
    def _get_file_indexer(self) -> FileIndexer:
        if self._can_split_files() and has_header(self._read_options):
            return CsvFileIndexer(
                chunk_size=self._chunk_size,
                ignore_missing_paths=self._ignore_missing_paths,
                filesystem=self._filesystem,
                read_options=self._read_options,
                parse_options=self._parse_options,
            )
        return super()._get_file_indexer()

    @override
    def _read_file_schema(self, path: str) -> pa.Schema:
        file_format = csv_file_format(
            self._read_options, self._parse_options, self._convert_options
        )
        try:
            if infer_compression(path) == "snappy":
                return file_format.inspect(read_snappy_buffer(path, self._filesystem))
            return file_format.inspect(path, filesystem=self._filesystem)
        except pa.ArrowInvalid as e:
            raise invalid_csv_error(path, e) from e

    @override
    def create_scanner(
        self,
        schema: pa.Schema,
        filesystem: Optional["FileSystem"] = None,
        **options,
    ) -> CsvScanner:
        return CsvScanner(
            schema=schema,
            filesystem=filesystem or self._filesystem,
            partitioning=options.get("partitioning", self._partitioning),
            synthesized_columns=self._synthesized_columns,
            chunk_size=self._chunk_size,
            read_options=self._read_options,
            parse_options=self._parse_options,
            convert_options=self._convert_options,
        )
