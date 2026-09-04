"""MCAP (Message Capture) datasource for Ray Data.

MCAP is a standardized format for storing timestamped messages from robotics and
autonomous systems, commonly used for sensor data, control commands, and other
time-series data.

Format specification: https://mcap.dev/spec

Two properties of the format shape this module:

- A file's summary section, which carries the message counts and the channel
  index, sits at the *end* of the file and is addressed by an offset in the
  footer. A seekable reader reaches it in two seeks; a non-seekable one has to
  scan every record to reconstruct it, and can only do so once.
- Schemas are stored once per file, not once per message. The row
  representation repeats them, so a file's in-memory size does not follow from
  its size on disk.
"""

import json
import logging
import math
import time
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.file_based_datasource import FileBasedDatasource
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import pyarrow
    from mcap.reader import Channel, Message, Schema, Summary

logger = logging.getLogger(__name__)

# The default multiplier applied to on-disk size when sampling is disabled or
# unavailable. MCAP chunks are typically compressed (zstd or lz4), and this
# datasource additionally materializes per-message columns -- topic, timestamps
# and, when ``include_metadata`` is set, the schema -- that have no per-message
# on-disk equivalent. The in-memory representation is therefore substantially
# larger than the file. Matches the Parquet datasource's default.
MCAP_ENCODING_RATIO_ESTIMATE_DEFAULT = 5

# The lower bound for the estimated MCAP encoding ratio.
MCAP_ENCODING_RATIO_ESTIMATE_LOWER_BOUND = 1

# The fraction of files sampled to estimate the encoding ratio, clamped to
# [MIN_NUM_SAMPLES, MAX_NUM_SAMPLES] so that sampling cost stays bounded.
MCAP_ENCODING_RATIO_ESTIMATE_SAMPLING_RATIO = 0.01
MCAP_ENCODING_RATIO_ESTIMATE_MIN_NUM_SAMPLES = 2
MCAP_ENCODING_RATIO_ESTIMATE_MAX_NUM_SAMPLES = 10

# The number of messages read from each sampled file to measure the in-memory
# size of a row. Kept low to avoid reading much data during planning.
MCAP_ENCODING_RATIO_ESTIMATE_NUM_MESSAGES = 100


@dataclass
class TimeRange:
    """Time range for filtering MCAP messages.

    Attributes:
        start_time: Start time in nanoseconds (inclusive).
        end_time: End time in nanoseconds (exclusive).
    """

    start_time: int
    end_time: int

    def __post_init__(self):
        """Validate time range after initialization."""
        if self.start_time >= self.end_time:
            raise ValueError(
                f"start_time ({self.start_time}) must be less than "
                f"end_time ({self.end_time})"
            )
        if self.start_time < 0 or self.end_time < 0:
            raise ValueError(
                f"time values must be non-negative, got start_time={self.start_time}, "
                f"end_time={self.end_time}"
            )


@DeveloperAPI
class MCAPDatasource(FileBasedDatasource):
    """MCAP (Message Capture) datasource for Ray Data.

    This datasource provides reading of MCAP files with predicate pushdown
    optimization for filtering by topics, time ranges, and message types.

    MCAP is a standardized format for storing timestamped messages from robotics and
    autonomous systems, commonly used for sensor data, control commands, and other
    time-series data.

    Examples:
        Basic usage:

        >>> import ray  # doctest: +SKIP
        >>> ds = ray.data.read_mcap("/path/to/data.mcap")  # doctest: +SKIP

        With topic filtering and time range:

        >>> from ray.data.datasource import TimeRange  # doctest: +SKIP
        >>> ds = ray.data.read_mcap(  # doctest: +SKIP
        ...     "/path/to/data.mcap",
        ...     topics={"/camera/image_raw", "/lidar/points"},
        ...     time_range=TimeRange(start_time=1000000000, end_time=2000000000)
        ... )  # doctest: +SKIP

        With multiple files and metadata:

        >>> ds = ray.data.read_mcap(  # doctest: +SKIP
        ...     ["file1.mcap", "file2.mcap"],
        ...     topics={"/camera/image_raw", "/lidar/points"},
        ...     message_types={"sensor_msgs/Image", "sensor_msgs/PointCloud2"},
        ...     include_metadata=True
        ... )  # doctest: +SKIP
    """

    _FILE_EXTENSIONS = ["mcap"]

    def __init__(
        self,
        paths: Union[str, List[str]],
        topics: Optional[Union[List[str], Set[str]]] = None,
        time_range: Optional[TimeRange] = None,
        message_types: Optional[Union[List[str], Set[str]]] = None,
        include_metadata: bool = True,
        **file_based_datasource_kwargs,
    ):
        """Initialize MCAP datasource.

        Args:
            paths: Path or list of paths to MCAP files.
            topics: Optional list/set of topic names to include. If specified,
                only messages from these topics will be read.
            time_range: Optional TimeRange for filtering messages by timestamp.
                TimeRange contains start_time and end_time in nanoseconds, where
                both values must be non-negative and start_time < end_time.
            message_types: Optional list/set of message type names (schema names)
                to include. Only messages with matching schema names will be read.
            include_metadata: Whether to include MCAP metadata fields in the output.
                Defaults to True. When True, includes schema, channel, and message
                metadata.
            **file_based_datasource_kwargs: Additional arguments for FileBasedDatasource.
        """
        super().__init__(paths, **file_based_datasource_kwargs)

        _check_import(self, module="mcap", package="mcap")

        # Convert to sets for faster lookup
        self._topics = set(topics) if topics else None
        self._message_types = set(message_types) if message_types else None
        self._time_range = time_range
        self._include_metadata = include_metadata
        # Computed lazily by `estimate_inmemory_data_size` and cached, so that
        # files are sampled at most once per datasource.
        self._encoding_ratio: Optional[float] = None

    def _read_stream(self, f: "pyarrow.NativeFile", path: str) -> Iterator[Block]:
        """Read MCAP file and yield blocks of message data.

        This method implements efficient MCAP reading with predicate pushdown.
        It uses MCAP's built-in filtering capabilities for optimal performance
        and applies additional filters when needed.

        Args:
            f: File-like object to read from. Must be seekable for MCAP reading.
            path: Path to the MCAP file being processed.

        Yields:
            Block: Blocks of MCAP message data as pyarrow Tables.

        Raises:
            ValueError: If the MCAP file cannot be read or has invalid format.
        """
        from mcap.reader import make_reader

        reader = make_reader(f)
        # Note: MCAP summaries are optional and iter_messages works without them
        # We don't need to validate the summary since it's not required

        # Use MCAP's built-in filtering for topics and time range
        messages = reader.iter_messages(
            topics=list(self._topics) if self._topics else None,
            start_time=self._time_range.start_time if self._time_range else None,
            end_time=self._time_range.end_time if self._time_range else None,
            log_time_order=True,
            reverse=False,
        )

        builder = DelegatingBlockBuilder()

        for schema, channel, message in messages:
            # Apply filters that couldn't be pushed down to MCAP level
            if not self._should_include_message(schema, channel, message):
                continue

            # Convert message to dictionary format
            message_data = self._message_to_dict(schema, channel, message, path)
            builder.add(message_data)

        # Yield the block if we have any messages
        if builder.num_rows() > 0:
            yield builder.build()

    def _should_include_message(
        self, schema: "Schema", channel: "Channel", message: "Message"
    ) -> bool:
        """Check if a message should be included based on filters.

        This method applies Python-level filtering that cannot be pushed down
        to the MCAP library level. Topic filters are already handled by the
        MCAP reader, so only message_types filtering is needed here.

        Args:
            schema: MCAP schema object containing message type information.
            channel: MCAP channel object containing topic and metadata.
            message: MCAP message object containing the actual data.

        Returns:
            True if the message should be included, False otherwise.
        """
        # Message type filter (cannot be pushed down to MCAP reader)
        if self._message_types and schema and schema.name not in self._message_types:
            return False

        return True

    def _message_to_dict(
        self, schema: "Schema", channel: "Channel", message: "Message", path: str
    ) -> Dict[str, Any]:
        """Convert MCAP message to dictionary format.

        This method converts MCAP message objects into a standardized dictionary
        format suitable for Ray Data processing.

        Args:
            schema: MCAP schema object containing message type and encoding info.
            channel: MCAP channel object containing topic and channel metadata.
            message: MCAP message object containing the actual message data.
            path: Path to the source file (for include_paths functionality).

        Returns:
            Dictionary containing message data in Ray Data format.
        """
        # Decode message data based on encoding
        decoded_data = message.data
        if channel.message_encoding == "json" and isinstance(message.data, bytes):
            try:
                decoded_data = json.loads(message.data.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                # Keep raw bytes if decoding fails
                decoded_data = message.data

        # Core message data
        message_data = {
            "data": decoded_data,
            "topic": channel.topic,
            "log_time": message.log_time,
            "publish_time": message.publish_time,
            "sequence": message.sequence,
        }

        # Add metadata if requested
        if self._include_metadata:
            message_data.update(
                {
                    "channel_id": message.channel_id,
                    "message_encoding": channel.message_encoding,
                    "schema_name": schema.name if schema else None,
                    "schema_encoding": schema.encoding if schema else None,
                    "schema_data": schema.data if schema else None,
                }
            )

        # Add file path if include_paths is enabled (from FileBasedDatasource)
        if getattr(self, "include_paths", False):
            message_data["path"] = path

        return message_data

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Return an estimate of the in-memory size of this datasource's output.

        ``FileBasedDatasource`` estimates in-memory size as the sum of the
        on-disk file sizes. For MCAP that is a large underestimate: chunks are
        usually compressed, and this datasource materializes per-message
        columns -- topic, timestamps and, when ``include_metadata`` is set, the
        schema -- that have no per-message on-disk equivalent.

        Ray Data derives both the read parallelism and the memory it provisions
        for the read from this number, so an underestimate produces too few,
        oversized blocks. To correct it, sample a bounded number of files,
        measure the in-memory size of a real block built from each, and scale
        the total on-disk size by the resulting ratio.
        """
        on_disk_size = super().estimate_inmemory_data_size()
        if not on_disk_size:
            return on_disk_size

        return int(on_disk_size * self._get_encoding_ratio())

    def _get_encoding_ratio(self) -> float:
        """Return the in-memory to on-disk size ratio, computing it once."""
        if self._encoding_ratio is None:
            self._encoding_ratio = self._estimate_files_encoding_ratio()
        return self._encoding_ratio

    def _estimate_files_encoding_ratio(self) -> float:
        """Estimate the in-memory to on-disk size ratio by sampling files.

        Overestimating is safer than underestimating here, so every failure
        path falls back to ``MCAP_ENCODING_RATIO_ESTIMATE_DEFAULT`` rather than
        to the uncorrected on-disk size.
        """
        if not self._data_context.decoding_size_estimation:
            return MCAP_ENCODING_RATIO_ESTIMATE_DEFAULT

        start_time = time.perf_counter()

        # Skip empty files and files of unknown size: they carry no usable
        # signal and would only add noise to the ratio.
        candidates = [
            (path, file_size)
            for path, file_size in zip(self._paths(), self._file_sizes())
            if file_size
        ]
        if not candidates:
            return MCAP_ENCODING_RATIO_ESTIMATE_DEFAULT

        ratios = []
        for path, file_size in _sample_files(candidates):
            in_memory_size = self._estimate_file_inmemory_size(path)
            # Zero is a measurement, not a failure: it means the filter selects
            # nothing from this file. Only `None` signals that the file could
            # not be sampled.
            if in_memory_size is not None:
                ratios.append(in_memory_size / file_size)

        sampling_duration = time.perf_counter() - start_time
        if sampling_duration > 5:
            logger.warning(
                "MCAP input size estimation took "
                f"{round(sampling_duration, 2)} seconds."
            )

        if not ratios:
            return MCAP_ENCODING_RATIO_ESTIMATE_DEFAULT

        ratio = sum(ratios) / len(ratios)
        logger.debug(f"Estimated MCAP encoding ratio from sampling is {ratio}.")
        return max(ratio, MCAP_ENCODING_RATIO_ESTIMATE_LOWER_BOUND)

    def _estimate_file_inmemory_size(self, path: str) -> Optional[int]:
        """Estimate the in-memory size of the rows one MCAP file produces.

        Reads at most ``MCAP_ENCODING_RATIO_ESTIMATE_NUM_MESSAGES`` messages,
        builds a block from them through the same conversion the read path
        uses, and scales the measured bytes per row by the file's message
        count. If the iteration finishes before reaching that cap it has seen
        the whole selection, and the measured size is returned unscaled.

        The sampled messages are the first ones the reader yields rather than a
        spread across the file, so a recording whose message size changes
        markedly from start to end is estimated from its opening. Bounding the
        read this way keeps planning cheap; the sample across *files* is spread
        instead (see ``_sample_files``).

        Returns ``None`` if the file carries no summary to read the message
        count from, or if it can't be sampled.
        """
        from mcap.reader import make_reader

        try:
            # Open for random access so that `make_reader` returns a
            # `SeekingReader`, which reads the summary from the file's index
            # rather than scanning the whole file to reconstruct it.
            with self._filesystem.open_input_file(path) as f:
                reader = make_reader(f)
                summary = reader.get_summary()
                if summary is None or summary.statistics is None:
                    return None

                num_messages = self._count_selected_messages(summary)
                if not num_messages:
                    return 0

                builder = DelegatingBlockBuilder()
                # Whether the iteration saw every message a read would select
                # rather than stopping at the sample cap.
                read_whole_selection = True
                for schema, channel, message in reader.iter_messages(
                    topics=list(self._topics) if self._topics else None,
                    start_time=(
                        self._time_range.start_time if self._time_range else None
                    ),
                    end_time=self._time_range.end_time if self._time_range else None,
                ):
                    if not self._should_include_message(schema, channel, message):
                        continue
                    builder.add(self._message_to_dict(schema, channel, message, path))
                    if builder.num_rows() >= MCAP_ENCODING_RATIO_ESTIMATE_NUM_MESSAGES:
                        read_whole_selection = False
                        break

                if builder.num_rows() == 0:
                    return 0

                block = builder.build()
                if self._include_paths:
                    # `FileBasedDatasource` appends this column downstream of
                    # `_read_stream`, so add it to keep the sample faithful.
                    block = BlockAccessor.for_block(block).fill_column("path", path)

                accessor = BlockAccessor.for_block(block)
                if read_whole_selection:
                    # Every message a read selects was materialized, so this is
                    # the size itself rather than a sample of it. This is the
                    # usual case for a narrow ``time_range`` or
                    # ``message_types`` filter, neither of which
                    # ``_count_selected_messages`` can resolve from the summary.
                    return accessor.size_bytes()

                bytes_per_row = accessor.size_bytes() / accessor.num_rows()
                return int(bytes_per_row * num_messages)
        except Exception:
            # Warn rather than log at debug: falling back silently restores the
            # fixed-ratio behaviour this method exists to replace, and a
            # debug-level message would not be seen.
            logger.warning(
                f"Failed to sample MCAP file '{path}' while estimating its "
                "in-memory size. Falling back to a default encoding ratio of "
                f"{MCAP_ENCODING_RATIO_ESTIMATE_DEFAULT}.",
                exc_info=True,
            )
            return None

    def _count_selected_messages(self, summary: "Summary") -> int:
        """Return an upper bound on how many messages of a file a read selects.

        Only the topic filter is resolvable from the summary. ``time_range``
        and ``message_types`` are not represented there, so a read using them
        selects at most this many messages.

        This bound is consulted only when sampling stopped at the message cap.
        A filter narrow enough to select fewer messages than the cap lets the
        caller measure the whole selection directly, so the bound is unused
        exactly where it would be loosest.
        """
        statistics = summary.statistics
        if not self._topics:
            return statistics.message_count

        return sum(
            count
            for channel_id, count in statistics.channel_message_counts.items()
            if channel_id in summary.channels
            and summary.channels[channel_id].topic in self._topics
        )

    def get_name(self) -> str:
        """Return a human-readable name for this datasource."""
        return "MCAP"

    @property
    def supports_distributed_reads(self) -> bool:
        """Whether this datasource supports distributed reads.

        MCAP files can be read in parallel across multiple files.
        """
        return True


def _sample_files(
    candidates: List[Tuple[str, int]],
) -> List[Tuple[str, int]]:
    """Pick evenly spaced files to sample.

    Even spacing rather than a prefix avoids a biased estimate when the input
    is ordered by size or by recording session.
    """
    num_samples = math.ceil(
        len(candidates) * MCAP_ENCODING_RATIO_ESTIMATE_SAMPLING_RATIO
    )
    num_samples = max(
        min(num_samples, MCAP_ENCODING_RATIO_ESTIMATE_MAX_NUM_SAMPLES),
        MCAP_ENCODING_RATIO_ESTIMATE_MIN_NUM_SAMPLES,
    )
    num_samples = min(num_samples, len(candidates))

    if num_samples <= 1:
        return candidates[:1]

    step = (len(candidates) - 1) / (num_samples - 1)
    return [candidates[round(i * step)] for i in range(num_samples)]
