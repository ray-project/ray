"""Concrete ``DataSourceV2`` for MCAP files.

``read_api.read_mcap`` constructs it when ``DataContext.use_datasource_v2`` is
set. Listing reads each file's summary, drops files that cannot match the
selection, and emits one row per chunk (``MCAPSummaryIndexer``).
``OnlineBinPacker`` groups the chunks into read tasks of about
``RAY_DATA_MCAP_BIN_PACKING_BYTES`` uncompressed bytes (128 MiB by default).
``MCAPScanner`` holds the optimizer's pushdowns and builds the ``MCAPReader``,
which seeks to each task's chunks. ``infer_schema`` calls ``infer_data_type``
(``mcap_data_column``) to decide whether ``data`` holds JSON values or bytes.
With ``video``, it asks a ``VideoPlanner`` (``mcap_video_planning``) for the
frame type, or at ``window`` granularity for the topics to decode.

``read_granularity`` picks what one row is. ``message`` gives one row per
message. ``window``, ``topic`` and ``file`` pack the messages of a time window,
a topic or a whole file into one row of parallel lists. Each such row decodes
and checkpoints on its own. ``mcap_coarse_layout`` defines the layouts.
``attachment`` and ``metadata`` give one row per record of that kind
(``mcap_records``).

Format specification: https://mcap.dev/spec
"""

from __future__ import annotations

import copy
from typing import (
    TYPE_CHECKING,
    Any,
    FrozenSet,
    Iterable,
    Iterator,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
)

import pyarrow as pa
from typing_extensions import override

from ray._common.utils import env_integer
from ray.data._internal.datasource_v2.common.synthesized_columns import PathColumn
from ray.data._internal.datasource_v2.formats.mcap.mcap_coarse_layout import (
    coarse_row_schema,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_data_column import (
    infer_data_type,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_file_indexer import (
    MCAPSummaryIndexer,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_message_rows import (
    message_schema,
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
    max_lead_in_ns,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_reader import (
    DEFAULT_MAX_ROW_BYTES,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_records import record_schema
from ray.data._internal.datasource_v2.formats.mcap.mcap_scanner import MCAPScanner
from ray.data._internal.datasource_v2.formats.mcap.mcap_video_planning import (
    VideoPlanner,
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
from ray.data._internal.util import MiB, _check_import, _is_local_scheme
from ray.data.context import DataContext
from ray.data.datasource.partitioning import (
    Partitioning,
    PathPartitionFilter,
    PathPartitionParser,
    _partition_field_types_to_pa_schema,
)
from ray.data.datasource.path_util import _resolve_paths_and_filesystem
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem

    from ray.data.datasource.file_based_datasource import FileShuffleConfig

# Chunks are packed into read tasks of about this many uncompressed bytes.
_BIN_PACKING_BYTES = env_integer("RAY_DATA_MCAP_BIN_PACKING_BYTES", 128 * MiB)
# How many partly filled read tasks the packer keeps open at once.
_BIN_PACKING_MAX_SHARED_OPEN_BINS = env_integer(
    "RAY_DATA_MCAP_BIN_PACKING_MAX_SHARED_OPEN_BINS", 16
)


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
        video_topics: Optional[Iterable[str]] = None,
        filesystem: Optional["FileSystem"] = None,
        partitioning: Optional[Partitioning] = None,
        partition_filter: Optional[PathPartitionFilter] = None,
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
        self._video_topics = _listed_video_topics(video_topics, self._selection)
        self._include_metadata = include_metadata
        self._log_time_order = log_time_order
        self._include_row_id = include_row_id
        self._granularity = read_granularity
        self._window = window
        self._video = video
        # With ``video`` at ``window`` granularity, the topics that get frame
        # columns. Settled by ``infer_schema``.
        self._decoded_topics: Tuple[str, ...] = ()
        self._partitioning = partitioning
        self._partition_filter = partition_filter
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
            # Each listing block is already one read task: one (file, topic)
            # or one file.
            return None

        # Each listing row is one chunk with its exact uncompressed size, so
        # chunks are packed into read tasks by bytes.
        #
        # Packing is per listing shard, not global, so listing can run as many
        # parallel tasks. Each chunk still lands in exactly one read task; the
        # cost is at most one under-filled task per shard.
        from ray.data._internal.datasource_v2.common.online_bin_packer import (
            OnlineBinPacker,
        )

        return OnlineBinPacker(
            max_bin_bytes=_BIN_PACKING_BYTES,
            max_shared_open_bins=_BIN_PACKING_MAX_SHARED_OPEN_BINS,
            requires_global_input=False,
        )

    @property
    @override
    def schema_needs_file_sample(self) -> bool:
        return True

    @override
    def resolve_partitioning(
        self, sample: Optional[FileManifest]
    ) -> Optional[Partitioning]:
        """``self._partitioning`` with field names discovered from a sample path.

        A partitioning is returned as a copy, so callers cannot change the
        datasource's own object.
        """
        if self._partitioning is None:
            return None
        if sample is None or len(sample) == 0 or self._partitioning.field_names:
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

        The sampled files settle the row columns that vary, and every read task
        follows that plan: the type of ``data`` in message rows, or with
        ``video`` the type of their ``frame`` column and which topics get frame
        columns in window rows. Every other row column has a fixed type.
        """
        assert sample is not None, "MCAP always receives a sample"
        if self._granularity == MESSAGE_GRANULARITY:
            schema = self._infer_message_schema(sample)
        elif self._granularity in RECORD_GRANULARITIES:
            schema = record_schema(
                self._granularity, include_row_id=self._include_row_id
            )
        else:
            schema = self._infer_coarse_schema(sample)
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

    def _infer_message_schema(self, sample: FileManifest) -> pa.Schema:
        """The schema of message rows.

        ``data`` holds decoded JSON values when every selected channel of the
        sampled files is JSON-encoded, and the raw payload bytes (``binary``)
        otherwise. The reader follows this one decision for every file, so a
        selection that mixes encodings keeps every payload as bytes. With
        ``video``, a ``frame`` column replaces ``data``.
        """
        data_type: Optional[pa.DataType] = None
        frame_type: Optional[pa.DataType] = None
        if self._video is not None:
            frame_type = self._video_planner().frame_type(sample.paths.tolist())
        elif len(sample) > 0:
            data_type = infer_data_type(
                self._selection,
                self._filesystem,
                sample.paths.tolist(),
                self._listed_paths,
            )
        return message_schema(
            include_metadata=self._include_metadata,
            include_row_id=self._include_row_id,
            data_type=data_type,
            frame_type=frame_type,
        )

    def _infer_coarse_schema(self, sample: FileManifest) -> pa.Schema:
        """The schema of window, topic and file rows."""
        decoded: Tuple[str, ...] = ()
        if self._video is not None:
            # Settled here, so every task decodes the same topics.
            self._decoded_topics = decoded = self._video_planner().decoded_topics(
                sample.paths.tolist()
            )
        return coarse_row_schema(
            self._granularity,
            include_metadata=self._include_metadata,
            include_row_id=self._include_row_id,
            decoded_topics=decoded,
        )

    def _video_planner(self) -> VideoPlanner:
        """The planner of the decoded columns, for a read with ``video``."""
        assert self._video is not None
        return VideoPlanner(
            self._selection,
            self._filesystem,
            self._video,
            self._video_topics,
            owner=self,
        )

    def _listed_paths(self) -> Iterator[str]:
        """The files the read lists, in listing order."""
        from ray.data._internal.datasource_v2.common.listing_utils import (
            _build_pruners,
        )

        for file_info in self._get_file_indexer().list_file_infos(
            pa.array(self._paths, pa.string()),
            filesystem=self._filesystem,
            # The filters the listing applies, so planning never opens a file
            # the read skips, such as a sidecar or a filtered-out partition.
            pruners=_build_pruners(self._file_extensions, self._partition_filter),
            preserve_order=True,
        ):
            yield file_info.path

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
            video_topics=self._video_topics,
            decoded_topics=self._decoded_topics,
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
            max_lead_in_ns=max_lead_in_ns(),
        )


def _listed_video_topics(
    video_topics: Optional[Iterable[str]], selection: MCAPSelection
) -> FrozenSet[str]:
    """``video_topics`` as a set, each entry checked to be a selected topic.

    An empty list means none. With ``topics``, every entry must be one of them.
    """
    listed = frozenset(video_topics or ())
    if selection.topics is not None:
        unselected = sorted(listed - selection.topics)
        if unselected:
            raise ValueError(
                f"video_topics lists topics that topics does not select: "
                f"{unselected}. Add them to topics, or drop them from video_topics."
            )
    return listed


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
    if video is not None and granularity not in (
        MESSAGE_GRANULARITY,
        WINDOW_GRANULARITY,
    ):
        raise ValueError(
            "video decodes the video topics into frames and applies to "
            "read_granularity='message' (one frame per row) and 'window' (a "
            f"window's frames per topic), not {granularity!r}: a decoded "
            f"{granularity} row would hold minutes to hours of frames in one "
            "value that cannot be cut into blocks"
        )
