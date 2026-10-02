"""Concrete ``DataSourceV2`` for MCAP files.

Constructed from ``read_api.read_mcap`` when ``DataContext.use_datasource_v2``
is set. Listing reads each file's summary and emits one row per chunk
(``MCAPSummaryIndexer``), ``OnlineBinPacker`` groups chunks into tasks of about
``RAY_DATA_MCAP_BIN_PACKING_BYTES`` uncompressed bytes (128 MiB by default),
and ``MCAPReader`` seeks to a task's chunks. Compared with the legacy
``MCAPDatasource``, a recording is read by as many tasks as its size calls
for rather than one, files that cannot match the selection are never opened,
and every row can carry a deterministic ``row_id``.

``read_granularity`` picks what one row is. ``message`` is today's row.
``window``, ``topic`` and ``file`` pack the messages of a time window, a topic
or a whole file into one row of parallel lists, each row decodable and
checkpointable on its own; see ``mcap_windows`` for the layouts.

Format specification: https://mcap.dev/spec
"""

from __future__ import annotations

import copy
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    Union,
)

import pyarrow as pa
from typing_extensions import override

from ray._common.utils import env_integer
from ray.data._internal.datasource_v2.common.synthesized_columns import PathColumn
from ray.data._internal.datasource_v2.formats.mcap.mcap_decode import decode_one
from ray.data._internal.datasource_v2.formats.mcap.mcap_file_indexer import (
    MCAPSummaryIndexer,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import (
    FILE_GRANULARITY,
    GRANULARITIES,
    MESSAGE_GRANULARITY,
    METADATA_GRANULARITY,
    RECORD_GRANULARITIES,
    TOPIC_GRANULARITY,
    WINDOW_GRANULARITY,
    MCAPSelection,
    TimeRange,
    VideoOptions,
    WindowSpec,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_reader import (
    DEFAULT_MAX_ROW_BYTES,
    decode_payload,
    is_video_channel,
    message_schema,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_records import record_schema
from ray.data._internal.datasource_v2.formats.mcap.mcap_scanner import MCAPScanner
from ray.data._internal.datasource_v2.formats.mcap.mcap_summary import read_summary
from ray.data._internal.datasource_v2.formats.mcap.mcap_video import (
    VideoCodec,
    detect_codec,
    is_keyframe,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_windows import (
    coarse_row_schema,
)
from ray.data._internal.datasource_v2.interfaces.datasource_v2 import (
    DatasourceCategory,
    FileDataSourceV2,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileIndexer
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    SynthesizedColumn,
)
from ray.data._internal.tensor_extensions.arrow import (
    ArrowTensorTypeV2,
    ArrowVariableShapedTensorType,
    convert_to_pyarrow_array,
)
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
    from mcap.records import Channel, Message, Schema
    from pyarrow.fs import FileSystem

    from ray.data.datasource.file_based_datasource import FileShuffleConfig

logger = logging.getLogger(__name__)

# Files opened at planning time to settle the ``data`` column (whether every
# selected channel is JSON-encoded, and if so the type of one decoded message)
# and to check that video topics can be decoded. Only files whose summary
# selects a channel of interest are read past the summary.
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
        read_granularity: str = MESSAGE_GRANULARITY,
        window: Optional[WindowSpec] = None,
        video: Optional[VideoOptions] = None,
        filesystem: Optional["FileSystem"] = None,
        partitioning: Optional[Partitioning] = None,
        file_extensions: Optional[Union[List[str], tuple[str, ...]]] = ("mcap",),
        ignore_missing_paths: bool = False,
        shuffle: Optional[Union[Literal["files"], "FileShuffleConfig"]] = None,
    ):
        super().__init__(name="MCAP", category=DatasourceCategory.FILE_BASED)
        _check_import(self, module="mcap", package="mcap")
        _validate_granularity(
            read_granularity,
            window,
            video,
            selects_channels=bool(topics) or bool(message_types),
            has_time_range=time_range is not None,
        )

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
        self._granularity = read_granularity
        self._window = window
        self._video = video
        self._partitioning = partitioning
        self._file_extensions = (
            list(file_extensions) if file_extensions is not None else None
        )
        self._ignore_missing_paths = ignore_missing_paths
        self._shuffle = shuffle
        synthesized: List[SynthesizedColumn] = []
        # Coarse rows carry ``path`` natively; only message rows synthesize it.
        if include_paths and read_granularity == MESSAGE_GRANULARITY:
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

    @property
    def granularity(self) -> str:
        return self._granularity

    def _get_file_indexer(self) -> FileIndexer:
        return MCAPSummaryIndexer(
            selection=self._selection,
            granularity=self._granularity,
            ignore_missing_paths=self._ignore_missing_paths,
        )

    def get_file_partitioner(self, **kwargs):
        if self._granularity in (TOPIC_GRANULARITY, FILE_GRANULARITY):
            # The indexer already emits one listing block per (file, topic) or
            # per file; each block is one read task, so there is nothing to
            # group.
            return None

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
        """The schema of the rows, plus partition and synthesized columns.

        At ``message`` granularity every column but ``data`` has a fixed type.
        ``data`` holds decoded JSON values when every selected channel of the
        sampled files is JSON-encoded (the type is inferred from one decoded
        message, as the legacy datasource's first block would show it), and the
        raw payload bytes (``binary``) otherwise. The reader follows this one
        decision for every file, so a selection mixing encodings keeps every
        payload as bytes rather than mixing values and bytes in one column.
        Coarse rows have a fixed schema; at ``window`` and ``topic``
        granularity the sample's video topics are checked here, so a topic
        whose keyframes cannot be detected fails the read before any task runs.
        """
        assert sample is not None, "MCAP always receives a sample"
        sample_paths = sample.paths.tolist()[:_SCHEMA_SAMPLE_FILES]
        if self._granularity == MESSAGE_GRANULARITY:
            decode = self._video is not None and self._video.decode
            schema = message_schema(
                include_metadata=self._include_metadata,
                include_row_id=self._include_row_id,
                data_type=(
                    self._infer_data_type(sample_paths)
                    if sample_paths and not decode
                    else None
                ),
                frame_type=self._infer_frame_type(sample_paths) if decode else None,
            )
        elif self._granularity in RECORD_GRANULARITIES:
            schema = record_schema(
                self._granularity, include_row_id=self._include_row_id
            )
        else:
            if self._granularity in (WINDOW_GRANULARITY, TOPIC_GRANULARITY):
                self._check_video_topics(sample_paths)
            schema = coarse_row_schema(
                self._granularity,
                include_metadata=self._include_metadata,
                include_row_id=self._include_row_id,
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

    def _first_messages(
        self, path: str, wanted: "Set[int] | None"
    ) -> Iterable[tuple["Channel", Optional["Schema"], "Message"]]:
        """Yield the first message of each selected channel of ``path``.

        ``wanted`` narrows the channels; ``None`` means every selected one. At
        most one chunk is read per channel found, and reading stops once every
        wanted channel has been seen.
        """
        from mcap.data_stream import ReadDataStream
        from mcap.records import Channel, Chunk, Message, Schema
        from mcap.stream_reader import StreamReader, breakup_chunk

        summary = read_summary(self._filesystem, path)
        seen: Set[int] = set()
        with self._filesystem.open_input_file(path) as f:
            if summary is None or not summary.chunk_indexes:
                schemas: dict = {}
                channels: dict = {}
                f.seek(0)
                for record in StreamReader(f).records:
                    if isinstance(record, Schema):
                        schemas[record.id] = record
                    elif isinstance(record, Channel):
                        channels[record.id] = record
                    elif isinstance(record, Message):
                        channel = channels.get(record.channel_id)
                        if channel is None or channel.id in seen:
                            continue
                        schema = (
                            schemas.get(channel.schema_id)
                            if channel.schema_id
                            else None
                        )
                        if not self._selection.accepts_channel(channel, schema):
                            continue
                        if wanted is not None and channel.id not in wanted:
                            continue
                        seen.add(channel.id)
                        yield channel, schema, record
                        if wanted is not None and seen >= wanted:
                            return
                return
            selected = self._selection.selected_channel_ids(
                summary.channels, summary.schemas
            )
            targets = selected if wanted is None else (selected & wanted)
            for chunk_index in summary.chunk_indexes:
                remaining = targets - seen
                if not remaining:
                    return
                if chunk_index.message_index_offsets and not (
                    remaining & set(chunk_index.message_index_offsets)
                ):
                    continue
                f.seek(chunk_index.chunk_start_offset + 1 + 8)
                for record in breakup_chunk(Chunk.read(ReadDataStream(f))):
                    if isinstance(record, Message) and record.channel_id in remaining:
                        channel = summary.channels[record.channel_id]
                        schema = (
                            summary.schemas.get(channel.schema_id)
                            if channel.schema_id
                            else None
                        )
                        seen.add(channel.id)
                        remaining.discard(channel.id)
                        yield channel, schema, record
                        if not remaining:
                            break

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

    def _check_video_topics(self, paths: List[str]) -> None:
        """Fail now if a video topic's keyframes cannot be detected.

        Only without a fixed ``lead_in_s``: that option exists for exactly the
        codecs this check cannot recognise. The first message of each selected
        video channel of each sample file is sniffed.
        """
        if self._video is not None and self._video.lead_in_ns is not None:
            return
        for path in paths:
            for channel, schema, message in self._first_messages(path, None):
                if not is_video_channel(channel, schema, self._video):
                    continue
                if detect_codec(message.data) is None:
                    raise ValueError(
                        f"Topic {channel.topic!r} in {path!r} is a video topic "
                        f"(schema {schema.name if schema else None!r}) but its "
                        "keyframes cannot be detected from the payload: only "
                        "JPEG, PNG and H.264/H.265 Annex-B are recognised. Pass "
                        "VideoOptions(lead_in_s=...) to read a fixed lead-in "
                        "before each window instead."
                    )

    def _infer_frame_type(self, paths: List[str]) -> pa.DataType:
        """The tensor type of decoded frames, from the sample files.

        Every selected channel of every sample file must be a video topic with
        a recognisable codec whose decoder is importable, or the read fails
        here naming the topic; only then is the shape settled. With ``resize``
        the shape is known; otherwise the first keyframe of the first video
        channel found is decoded for it. Falls back to a variable-shaped tensor
        when the sample holds no decodable keyframe.
        """
        assert self._video is not None
        sample: Optional[Tuple[str, int, VideoCodec]] = None
        for path in paths:
            for channel, schema, message in self._first_messages(path, None):
                if not is_video_channel(channel, schema, self._video):
                    raise ValueError(
                        f"Cannot decode topic {channel.topic!r} in {path!r}: it is "
                        f"not a video topic (schema {schema.name if schema else None!r}). "
                        "Pass topics=[...] to select only the video topics, or "
                        "VideoOptions(topics=[...]) to force one."
                    )
                codec = detect_codec(message.data)
                if codec is None:
                    raise ValueError(
                        f"Cannot decode topic {channel.topic!r} in {path!r}: its "
                        "payload is not JPEG, PNG or H.264/H.265 Annex-B."
                    )
                if codec.every_frame_is_a_keyframe:
                    _check_import(self, module="PIL", package="Pillow")
                else:
                    _check_import(self, module="av", package="av")
                if sample is None:
                    sample = (path, channel.id, codec)
        if self._video.resize is not None:
            height, width = self._video.resize
            return ArrowTensorTypeV2((height, width, 3), pa.uint8())
        if sample is not None:
            path, channel_id, codec = sample
            head = self._stream_head(path, channel_id, codec)
            frame = decode_one(head, codec, None) if head else None
            if frame is not None:
                return ArrowTensorTypeV2(tuple(frame.shape), pa.uint8())
        return ArrowVariableShapedTensorType(pa.uint8(), 3)

    # How far ``_stream_head`` looks: messages of the channel, and records of
    # any kind, so a sparse or absent channel in an unindexed file does not turn
    # planning into a scan of the whole file.
    _KEYFRAME_SCAN_MESSAGES = 1_000
    _KEYFRAME_SCAN_RECORDS = 50_000

    def _stream_head(
        self, path: str, channel_id: int, codec: VideoCodec
    ) -> List[bytes]:
        """The channel's payloads from its first message through its first keyframe.

        Empty when no keyframe is found within the scan budget. The messages
        before the keyframe matter: a recorder may write the parameter sets in
        a message of their own.
        """
        from mcap.data_stream import ReadDataStream
        from mcap.records import Chunk, Message
        from mcap.stream_reader import StreamReader, breakup_chunk

        summary = read_summary(self._filesystem, path)
        head: List[bytes] = []
        records_left = self._KEYFRAME_SCAN_RECORDS

        def consider(record: Any) -> Optional[bool]:
            """``True`` once the keyframe is found, ``False`` when the budget is spent."""
            nonlocal records_left
            records_left -= 1
            if isinstance(record, Message) and record.channel_id == channel_id:
                head.append(record.data)
                if is_keyframe(record.data, codec):
                    return True
                if len(head) >= self._KEYFRAME_SCAN_MESSAGES:
                    return False
            return False if records_left <= 0 else None

        with self._filesystem.open_input_file(path) as f:
            if summary is None or not summary.chunk_indexes:
                f.seek(0)
                records: Any = StreamReader(f).records
                for record in records:
                    verdict = consider(record)
                    if verdict is not None:
                        return head if verdict else []
                return []
            for chunk_index in summary.chunk_indexes:
                if chunk_index.message_index_offsets and channel_id not in (
                    chunk_index.message_index_offsets
                ):
                    continue
                f.seek(chunk_index.chunk_start_offset + 1 + 8)
                for record in breakup_chunk(Chunk.read(ReadDataStream(f))):
                    verdict = consider(record)
                    if verdict is not None:
                        return head if verdict else []
        return []

    def create_scanner(
        self,
        schema: pa.Schema,
        filesystem: Optional["FileSystem"] = None,
        **options: Any,
    ) -> MCAPScanner:
        return MCAPScanner(
            schema=schema,
            selection=self._selection,
            granularity=self._granularity,
            window=self._window,
            video=self._video,
            include_metadata=self._include_metadata,
            include_row_id=self._include_row_id,
            log_time_order=self._log_time_order,
            filesystem=filesystem or self._filesystem,
            partitioning=options.get("partitioning", self._partitioning),
            synthesized_columns=self._synthesized_columns,
            shuffle=self._shuffle,
            target_block_size=DataContext.get_current().target_max_block_size,
            max_row_bytes=env_integer(
                "RAY_DATA_MCAP_MAX_ROW_BYTES", DEFAULT_MAX_ROW_BYTES
            ),
        )


def _validate_granularity(
    granularity: str,
    window: Optional[WindowSpec],
    video: Optional[VideoOptions],
    *,
    selects_channels: bool = False,
    has_time_range: bool = False,
) -> None:
    """Reject option combinations that cannot mean anything."""
    if granularity not in GRANULARITIES:
        raise ValueError(
            f"read_granularity must be one of {list(GRANULARITIES)}, got "
            f"{granularity!r}"
        )
    if granularity in RECORD_GRANULARITIES and selects_channels:
        raise ValueError(
            "topics and message_types select messages; they do not apply to "
            f"read_granularity={granularity!r}"
        )
    if granularity == METADATA_GRANULARITY and has_time_range:
        raise ValueError(
            "Metadata records carry no timestamp; time_range does not apply to "
            "read_granularity='metadata'"
        )
    if granularity == WINDOW_GRANULARITY and window is None:
        raise ValueError(
            "read_granularity='window' needs a WindowSpec: pass "
            "window=WindowSpec(length_s=...)"
        )
    if granularity != WINDOW_GRANULARITY and window is not None:
        raise ValueError(
            f"window applies to read_granularity='window', not {granularity!r}"
        )
    if video is not None:
        if video.decode and granularity != MESSAGE_GRANULARITY:
            raise ValueError(
                "VideoOptions(decode=True) decodes one frame per row and applies to "
                f"read_granularity='message', not {granularity!r}; window rows keep "
                "their payloads encoded for decoding downstream"
            )
        if not video.decode and granularity not in (
            WINDOW_GRANULARITY,
            TOPIC_GRANULARITY,
        ):
            raise ValueError(
                "video applies to read_granularity='window' and 'topic' (or to "
                f"'message' with decode=True), not {granularity!r}"
            )
