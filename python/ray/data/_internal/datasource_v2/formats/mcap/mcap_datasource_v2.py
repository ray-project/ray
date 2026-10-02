"""Concrete ``DataSourceV2`` for MCAP files.

Constructed from ``read_api.read_mcap`` when ``DataContext.use_datasource_v2``
is set. Listing reads each file's summary and emits one row per chunk
(``MCAPSummaryIndexer``), ``OnlineBinPacker`` groups chunks into tasks of about
``RAY_DATA_MCAP_BIN_PACKING_BYTES`` uncompressed bytes (128 MiB by default),
and ``MCAPReader`` seeks to a task's chunks. Compared with the legacy
``MCAPDatasource``, a recording is read by as many tasks as its size calls
for rather than one, files that cannot match the selection are never opened,
and every row can carry a deterministic ``row_id``.

Format specification: https://mcap.dev/spec
"""

from __future__ import annotations

import copy
import logging
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Literal, Optional, Union

import pyarrow as pa
from typing_extensions import override

from ray._common.utils import env_integer
from ray.data._internal.datasource_v2.common.synthesized_columns import PathColumn
from ray.data._internal.datasource_v2.formats.mcap.mcap_file_indexer import (
    MCAPSummaryIndexer,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import (
    MCAPSelection,
    TimeRange,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_reader import (
    decode_payload,
    message_schema,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_scanner import MCAPScanner
from ray.data._internal.datasource_v2.formats.mcap.mcap_summary import read_summary
from ray.data._internal.datasource_v2.interfaces.datasource_v2 import (
    DatasourceCategory,
    FileDataSourceV2,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileIndexer
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    SynthesizedColumn,
)
from ray.data._internal.tensor_extensions.arrow import convert_to_pyarrow_array
from ray.data._internal.util import MiB, _check_import, _is_local_scheme
from ray.data.context import DataContext
from ray.data.datasource.partitioning import (
    Partitioning,
    PathPartitionParser,
    _partition_field_types_to_pa_schema,
)
from ray.data.datasource.path_util import _resolve_paths_and_filesystem
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem

    from ray.data.datasource.file_based_datasource import FileShuffleConfig

logger = logging.getLogger(__name__)

# Files opened at planning time to settle the ``data`` column: whether every
# selected channel is JSON-encoded, and if so the type of one decoded message.
_SCHEMA_SAMPLE_FILES = 4


@DeveloperAPI
class MCAPDatasourceV2(FileDataSourceV2):
    """V2 MCAP datasource: summary-driven listing, chunk-level read tasks."""

    def __init__(
        self,
        paths: List[str],
        *,
        topics: Optional[Iterable[str]] = None,
        time_range: Optional[TimeRange] = None,
        message_types: Optional[Iterable[str]] = None,
        include_metadata: bool = True,
        log_time_order: bool = True,
        include_row_id: bool = False,
        include_paths: bool = False,
        filesystem: Optional["FileSystem"] = None,
        partitioning: Optional[Partitioning] = None,
        file_extensions: Optional[Union[List[str], tuple[str, ...]]] = ("mcap",),
        ignore_missing_paths: bool = False,
        shuffle: Optional[Union[Literal["files"], "FileShuffleConfig"]] = None,
    ):
        super().__init__(name="MCAP", category=DatasourceCategory.FILE_BASED)
        _check_import(self, module="mcap", package="mcap")

        # Captured against the original paths: resolution below strips the
        # ``local://`` scheme (see ``ParquetDatasourceV2``).
        self._supports_distributed_reads = not _is_local_scheme(paths)
        resolved_paths, resolved_filesystem = _resolve_paths_and_filesystem(
            paths, filesystem
        )
        self._paths: List[str] = resolved_paths
        self._filesystem = resolved_filesystem
        self._selection = MCAPSelection.create(topics, time_range, message_types)
        self._include_metadata = include_metadata
        self._log_time_order = log_time_order
        self._include_row_id = include_row_id
        self._partitioning = partitioning
        self._file_extensions = (
            list(file_extensions) if file_extensions is not None else None
        )
        self._ignore_missing_paths = ignore_missing_paths
        self._shuffle = shuffle
        synthesized: List[SynthesizedColumn] = []
        if include_paths:
            synthesized.append(PathColumn())
        self._synthesized_columns = tuple(synthesized)

    @property
    def paths(self) -> List[str]:
        return self._paths

    @property
    def filesystem(self) -> "FileSystem":
        return self._filesystem

    @property
    def file_extensions(self) -> Optional[List[str]]:
        return self._file_extensions

    @property
    def shuffle(self) -> Optional[Union[Literal["files"], "FileShuffleConfig"]]:
        return self._shuffle

    @property
    def selection(self) -> MCAPSelection:
        return self._selection

    def _get_file_indexer(self) -> FileIndexer:
        return MCAPSummaryIndexer(
            selection=self._selection,
            ignore_missing_paths=self._ignore_missing_paths,
        )

    def get_file_partitioner(self, **kwargs):
        # Listing rows are chunks with exact uncompressed sizes, so pack them
        # into read tasks by bytes instead of estimating whole files.
        from ray.data._internal.datasource_v2.common.online_bin_packer import (
            OnlineBinPacker,
        )

        max_bin_bytes = env_integer("RAY_DATA_MCAP_BIN_PACKING_BYTES", 128 * MiB)
        max_shared_open_bins = env_integer(
            "RAY_DATA_MCAP_BIN_PACKING_MAX_SHARED_OPEN_BINS", 16
        )
        return OnlineBinPacker(
            max_bin_bytes=max_bin_bytes, max_shared_open_bins=max_shared_open_bins
        )

    @property
    @override
    def schema_needs_file_sample(self) -> bool:
        return True

    @override
    def resolve_partitioning(
        self, sample: Optional[FileManifest]
    ) -> Optional[Partitioning]:
        """``self._partitioning`` with field names discovered from a sample path."""
        if self._partitioning is None or sample is None or len(sample) == 0:
            return copy.deepcopy(self._partitioning)
        if self._partitioning.field_names:
            return copy.deepcopy(self._partitioning)
        partition_kv = PathPartitionParser(self._partitioning)(sample.paths.tolist()[0])
        if not partition_kv:
            return copy.deepcopy(self._partitioning)
        return Partitioning(
            style=self._partitioning.style,
            base_dir=self._partitioning.base_dir,
            field_names=list(partition_kv.keys()),
            field_types=self._partitioning.field_types,
            filesystem=self._partitioning.filesystem,
        )

    def infer_schema(self, sample: Optional[FileManifest]) -> pa.Schema:
        """The schema of message rows, plus partition and synthesized columns.

        Every column but ``data`` has a fixed type. ``data`` holds decoded JSON
        values when every selected channel of the sampled files is
        JSON-encoded (the type is inferred from one decoded message, as the
        legacy datasource's first block would show it), and the raw payload
        bytes (``binary``) otherwise. The reader follows this one decision for
        every file, so a selection mixing encodings keeps every payload as bytes
        rather than mixing values and bytes in one column.
        """
        assert sample is not None, "MCAP always receives a sample"
        data_type = (
            self._infer_data_type(sample.paths.tolist()[:_SCHEMA_SAMPLE_FILES])
            if len(sample) > 0
            else None
        )
        schema = message_schema(
            include_metadata=self._include_metadata,
            include_row_id=self._include_row_id,
            data_type=data_type,
        )
        partitioning = self.resolve_partitioning(sample)
        if partitioning is not None and len(sample) > 0:
            partition_kv = PathPartitionParser(partitioning)(sample.paths.tolist()[0])
            partition_schema = _partition_field_types_to_pa_schema(
                field_names=list(partition_kv.keys()),
                field_types=partitioning.field_types or {},
            )
            for name in partition_kv:
                if schema.get_field_index(name) == -1:
                    schema = schema.append(partition_schema.field(name))
        for column in self._synthesized_columns:
            idx = schema.get_field_index(column.name)
            if idx == -1:
                schema = schema.append(pa.field(column.name, column.type))
            elif schema.field(idx).type != column.type:
                schema = schema.set(idx, pa.field(column.name, column.type))
        return schema

    def _infer_data_type(self, paths: List[str]) -> Optional[pa.DataType]:
        """Type of ``data`` when it is decoded JSON; ``None`` when it is ``binary``.

        Arrow has no column type for a mix of bytes and decoded values, so the
        decision is made once, here, and every reader follows it: ``data`` is
        decoded only when every selected channel of the sampled files is
        JSON-encoded. The value type is that of the first JSON message found;
        a recording whose JSON messages differ in shape gets the type of that
        one message, and blocks whose structs differ are unified downstream.

        An indexed file settles its channels from the summary and reads at
        most one chunk; a file without an index is scanned for its channel
        records, stopping at the first selected channel that is not JSON.
        """
        from mcap.data_stream import ReadDataStream
        from mcap.records import Channel, Chunk, Message, Schema
        from mcap.stream_reader import StreamReader, breakup_chunk

        value_type: Optional[pa.DataType] = None
        for path in paths:
            summary = read_summary(self._filesystem, path)
            where = f"{path} (sampled for the schema)"
            with self._filesystem.open_input_file(path) as f:
                if summary is None or not summary.chunk_indexes:
                    schemas: Dict[int, Schema] = {}
                    channels: Dict[int, Channel] = {}
                    f.seek(0)
                    for record in StreamReader(f).records:
                        if isinstance(record, Schema):
                            schemas[record.id] = record
                        elif isinstance(record, Channel):
                            channels[record.id] = record
                            schema = (
                                schemas.get(record.schema_id)
                                if record.schema_id
                                else None
                            )
                            if (
                                self._selection.accepts_channel(record, schema)
                                and record.message_encoding != "json"
                            ):
                                return None
                        elif isinstance(record, Message) and value_type is None:
                            channel = channels.get(record.channel_id)
                            if channel is None:
                                continue
                            schema = (
                                schemas.get(channel.schema_id)
                                if channel.schema_id
                                else None
                            )
                            if self._selection.accepts_channel(channel, schema):
                                value_type = convert_to_pyarrow_array(
                                    [decode_payload(channel, record.data, where)],
                                    "data",
                                ).type
                    continue
                selected = self._selection.selected_channel_ids(
                    summary.channels, summary.schemas
                )
                if any(
                    summary.channels[cid].message_encoding != "json" for cid in selected
                ):
                    return None
                if value_type is not None or not selected:
                    continue
                for chunk_index in summary.chunk_indexes:
                    if chunk_index.message_index_offsets and not (
                        selected & set(chunk_index.message_index_offsets)
                    ):
                        continue
                    f.seek(chunk_index.chunk_start_offset + 1 + 8)
                    for record in breakup_chunk(Chunk.read(ReadDataStream(f))):
                        if (
                            isinstance(record, Message)
                            and record.channel_id in selected
                        ):
                            channel = summary.channels[record.channel_id]
                            value_type = convert_to_pyarrow_array(
                                [decode_payload(channel, record.data, where)], "data"
                            ).type
                            break
                    if value_type is not None:
                        break
        return value_type

    def create_scanner(
        self,
        schema: pa.Schema,
        filesystem: Optional["FileSystem"] = None,
        **options: Any,
    ) -> MCAPScanner:
        return MCAPScanner(
            schema=schema,
            selection=self._selection,
            include_metadata=self._include_metadata,
            include_row_id=self._include_row_id,
            log_time_order=self._log_time_order,
            filesystem=filesystem or self._filesystem,
            partitioning=options.get("partitioning", self._partitioning),
            synthesized_columns=self._synthesized_columns,
            shuffle=self._shuffle,
            target_block_size=DataContext.get_current().target_max_block_size,
        )
