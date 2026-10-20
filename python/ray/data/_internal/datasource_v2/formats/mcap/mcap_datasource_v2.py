"""Concrete ``DataSourceV2`` for MCAP files.

``read_api.read_mcap`` constructs it when ``DataContext.use_datasource_v2`` is
set. Listing reads each file's summary, drops files that cannot match the
selection, and emits one row per chunk (``MCAPSummaryIndexer``), weighted by
the memory reading it is estimated to need. Read tasks hold about
``RAY_DATA_MCAP_BIN_PACKING_BYTES`` of that estimate (128 MiB by default).
``OrderedFileCutter`` cuts a large file into tasks that leave in order, and
``OnlineBinPacker`` packs small files whole into shared tasks
(``mcap_file_cuts``). ``TaskSummaryPartitioner`` adds to each task the summary
records it needs.
``MCAPScanner`` holds the optimizer's pushdowns and builds the ``MCAPReader``,
which seeks to each task's chunks. ``infer_schema`` calls ``infer_data_type``
(``mcap_data_column``) to decide whether ``data`` holds JSON values or bytes.
With ``video``, it asks a ``VideoPlanner`` (``mcap_video_planning``) for the type
of the ``frame`` column that replaces ``data``.

``read_granularity`` picks what one row of a read of paths is: one message, or
one Attachment or Metadata record (``mcap_records``). Records are listed and
packed into read tasks like chunks, each weighted by its size.

A sample read (``read_mcap(samples=...)``) lists nothing: the samples dataset
names the files, and ``read_api`` feeds it to the read tasks as their manifest.
Its rows hold each topic's steps around a sample's anchor (``mcap_samples``).
With ``video``, a ``VideoPlanner`` (``mcap_video_planning``) settles which of its
topics are decoded.

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
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import pyarrow as pa
from typing_extensions import override

from ray._common.utils import env_integer
from ray.data._internal.datasource_v2.common.synthesized_columns import PathColumn
from ray.data._internal.datasource_v2.formats.mcap.mcap_data_column import (
    infer_data_type,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_file_cuts import (
    OrderedFileCutter,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_file_indexer import (
    MCAPSummaryIndexer,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_message_rows import (
    MessageRowLayout,
    message_schema,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import (
    GRANULARITIES,
    MESSAGE_GRANULARITY,
    METADATA_GRANULARITY,
    RECORD_GRANULARITIES,
    MCAPSelection,
    TimeRange,
    VideoOptions,
    max_lead_in_ns,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_records import record_schema
from ray.data._internal.datasource_v2.formats.mcap.mcap_sample_rows import (
    DEFAULT_MAX_ROW_BYTES,
    SamplePaths,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_samples import (
    TopicPlan,
    sample_row_schema,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_scanner import MCAPScanner
from ray.data._internal.datasource_v2.formats.mcap.mcap_task_summary import (
    TaskSummaryPartitioner,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_topic_spec import TopicSpec
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

# Chunks are packed into read tasks of about this many bytes of estimated read
# memory (``mcap_summary.chunk_run``).
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
        topics: Optional[Union[Iterable[str], Mapping[str, TopicSpec]]] = None,
        time_range: Optional[TimeRange] = None,
        message_types: Optional[Iterable[str]] = None,
        include_metadata: bool = True,
        log_time_order: bool = True,
        include_row_id: bool = False,
        include_paths: bool = False,
        filesystem: Optional["FileSystem"] = None,
        partitioning: Optional[Partitioning] = None,
        partition_filter: Optional[PathPartitionFilter] = None,
        file_extensions: Optional[Union[List[str], tuple[str, ...]]] = ("mcap",),
        ignore_missing_paths: bool = False,
        shuffle: Optional[Union[Literal["files"], "FileShuffleConfig"]] = None,
        read_task_memory: Optional[float] = None,
        read_granularity: str = MESSAGE_GRANULARITY,
        samples_schema: Optional[pa.Schema] = None,
        local_sample_files: bool = False,
        video: Optional[VideoOptions] = None,
        video_topics: Optional[Iterable[str]] = None,
        sample_files: Sequence[str] = (),
    ):
        """Set up a read of ``paths``, or, with ``samples_schema``, a sample read.

        A sample read takes ``topics`` as a mapping from each topic to its
        ``TopicSpec``, and ``samples_schema`` is the samples dataset's schema.
        Its files come with the samples, so ``paths`` is empty and the read
        tasks resolve each file against ``filesystem``, or its URI. With
        ``local_sample_files``, the files use the ``local://`` scheme, so the
        read tasks run on the driver's node. With ``video``, planning opens
        ``sample_files``, some of the samples' files, to settle which topics
        are decoded.
        """
        super().__init__(name="MCAP", category=DatasourceCategory.FILE_BASED)
        _check_import(self, module="mcap", package="mcap")
        self._samples_schema = samples_schema
        self._sample_files = tuple(sample_files)
        self._topic_plans: Tuple[TopicPlan, ...] = ()
        if samples_schema is not None:
            assert isinstance(topics, Mapping), "a sample read maps topics to specs"
            self._topic_plans = tuple(
                TopicPlan.from_spec(topic, spec) for topic, spec in topics.items()
            )
            self._supports_distributed_reads = not local_sample_files
            self._paths: List[str] = []
            self._filesystem = filesystem
        else:
            _validate_granularity(
                read_granularity,
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
            self._paths = resolved_paths
            self._filesystem = resolved_filesystem
        self._selection = MCAPSelection.create(topics, time_range, message_types)
        self._video = video
        self._video_topics = _listed_video_topics(video_topics, self._selection)
        # With ``video``, the topics that get frame columns, settled by
        # ``infer_schema``.
        self._decoded_topics: Tuple[str, ...] = ()
        self._granularity = read_granularity
        self._include_metadata = include_metadata
        self._log_time_order = log_time_order
        self._include_row_id = include_row_id
        self._partitioning = partitioning
        self._partition_filter = partition_filter
        self._file_extensions = (
            list(file_extensions) if file_extensions is not None else None
        )
        self._ignore_missing_paths = ignore_missing_paths
        self._shuffle = shuffle
        # What each read task reserves (``read_mcap(memory=...)``); the cutter's
        # memory warning stays quiet when it covers the estimate.
        self._read_task_memory = read_task_memory
        self._include_paths = include_paths
        synthesized: List[SynthesizedColumn] = []
        # Record rows carry ``path`` themselves; only message rows synthesize it.
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
    def topic_plans(self) -> Tuple[TopicPlan, ...]:
        """For a sample read, each topic's steps; empty otherwise."""
        return self._topic_plans

    def _get_file_indexer(self) -> FileIndexer:
        return MCAPSummaryIndexer(
            selection=self._selection,
            ignore_missing_paths=self._ignore_missing_paths,
            read_task_bytes=_BIN_PACKING_BYTES,
            row_layout=MessageRowLayout(
                include_metadata=self._include_metadata,
                include_row_id=self._include_row_id,
                include_paths=self._include_paths,
            ),
            granularity=self._granularity,
        )

    def get_file_partitioner(self, **kwargs):
        # Each listing row is one chunk weighted by its estimated read memory,
        # so read tasks are sized by that estimate. A large file is cut into
        # tasks that leave in order, and small files are packed whole. When the
        # read keeps order, files also keep their listing order
        # (``mcap_file_cuts`` says where a file may be cut).
        #
        # Packing is per listing shard, not global, so listing can run as many
        # parallel tasks. Each chunk still lands in exactly one read task; the
        # cost is that bins may not fill as well, which may mean more read tasks.
        from ray.data._internal.datasource_v2.common.online_bin_packer import (
            OnlineBinPacker,
        )

        packer = OnlineBinPacker(
            max_bin_bytes=_BIN_PACKING_BYTES,
            max_shared_open_bins=_BIN_PACKING_MAX_SHARED_OPEN_BINS,
            requires_global_input=False,
        )
        cutter = OrderedFileCutter(
            packer,
            budget=_BIN_PACKING_BYTES,
            log_time_order=self._log_time_order,
            reserved_memory=self._read_task_memory,
        )
        return TaskSummaryPartitioner(cutter)

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

        Every column but ``data`` has a fixed type. ``data`` holds decoded JSON
        values when every selected channel of the sampled files is JSON-encoded,
        and the raw payload bytes (``binary``) otherwise. The reader follows this
        one decision for every file, so a selection that mixes encodings keeps
        every payload as bytes. Record rows have a fixed schema
        (``record_schema``). Sample rows hold the samples' own columns, then
        each topic's (``sample_row_schema``).
        """
        if self._samples_schema is not None:
            resize = None
            if self._video is not None:
                resize = self._video.resize
                self._decoded_topics = self._sample_decoded_topics()
                _refuse_slots_of_decoded_topics(self._topic_plans, self._decoded_topics)
            return sample_row_schema(
                self._samples_schema, self._topic_plans, self._decoded_topics, resize
            )
        assert sample is not None, "MCAP always receives a sample"
        if self._granularity in RECORD_GRANULARITIES:
            schema = record_schema(
                self._granularity, include_row_id=self._include_row_id
            )
        else:
            schema = self._infer_message_schema(sample)
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
            topic_plans=self._topic_plans,
            video=self._video,
            video_topics=self._video_topics,
            decoded_topics=self._decoded_topics,
            max_lead_in_ns=max_lead_in_ns(),
        )

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
            planner = VideoPlanner(
                self._selection, self._filesystem, self._video_topics, owner=self
            )
            frame_type = planner.frame_type(sample.paths.tolist(), self._video.resize)
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

    def _sample_decoded_topics(self) -> Tuple[str, ...]:
        """The topics a sample read decodes: the listed video topics, and the
        video topics of the sample files on the first one's filesystem."""
        # One resolver, so files on the same filesystem share its object.
        sample_paths = SamplePaths(self._filesystem)
        resolved = [sample_paths.resolve(p) for p in self._sample_files]
        if not resolved:
            return tuple(sorted(self._video_topics))
        filesystem = resolved[0][1]
        paths = [path for path, fs in resolved if fs is filesystem]
        planner = VideoPlanner(
            self._selection, filesystem, self._video_topics, owner=self
        )
        return planner.decoded_topics(paths)


def _refuse_slots_of_decoded_topics(
    plans: Sequence[TopicPlan], decoded: Sequence[str]
) -> None:
    """Fail on a decoded topic read with ``match.all_in_step``: a step holds one
    decoded frame, not a list of them."""
    slotted = sorted(p.topic for p in plans if p.all_in_step and p.topic in decoded)
    if slotted:
        raise ValueError(
            f"match.all_in_step() takes a list of messages per step, so it cannot "
            f"be decoded to frames: {slotted}. Use match.nearest() or "
            "match.backward() for a decoded camera topic."
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
    video: Optional[VideoOptions],
    *,
    selects_channels: bool,
    has_time_range: bool,
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
    if video is not None and granularity != MESSAGE_GRANULARITY:
        raise ValueError(
            "video decodes the video topics of message rows into frames; it does "
            f"not apply to read_granularity={granularity!r}"
        )
