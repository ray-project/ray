"""MCAP (Message Capture) datasource for Ray Data.

MCAP is a container format for timestamped, heterogeneous message streams,
standard in robotics and autonomous systems: one file holds every sensor and
control topic of a recording session, interleaved in log-time order.

Specification: https://mcap.dev/spec

What a file contains::

    Header
    -- data section ------------------------------------------
    Schema        one per message type, e.g. sensor_msgs/msg/Imu
    Channel       one per topic, e.g. /livox/imu -> schema 3
    Chunk         a compressed run of Message records
    MessageIndex
    ...
    Data End
    -- summary section (optional) ----------------------------
    Schema, Channel     repeated here for lookup
    ChunkIndex
    Statistics          message_count, channel_message_counts,
                        message_start_time, message_end_time
    -- footer ------------------------------------------------
    Footer        summary_start -> byte offset of the summary

A *message* is one published value: a payload blob plus a log time, a publish
time, a sequence number, and the channel it arrived on. A *channel* is a topic.
A *schema* describes a message type and is shared by every channel that uses it.

Three properties of the format shape this module:

1. The summary sits at the *end* of the file, addressed by an offset in the
   footer. A seekable reader reaches it in two seeks; a non-seekable one has to
   scan every record to reconstruct it, and can only do so once. ``Statistics``
   is where exact per-channel message counts come from without reading any
   payload.

2. Schemas are stored once per file, not once per message, while the row
   representation below repeats one on every row. Hence
   ``_dictionary_encode_schema_data``.

3. Chunks are the unit of compression, so reading a single message costs a whole
   chunk decompression, and a file's in-memory size does not follow from its
   size on disk.

Each row carries ``data``, ``topic``, ``log_time``, ``publish_time`` and
``sequence``. With ``include_metadata`` (the default) it also carries
``channel_id``, ``message_encoding``, ``schema_name``, ``schema_encoding`` and
``schema_data``.
"""

import json
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Set, Union

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.util import _check_import
from ray.data.block import Block
from ray.data.datasource.file_based_datasource import FileBasedDatasource
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import pyarrow
    from mcap.reader import Channel, Message, Schema

logger = logging.getLogger(__name__)


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
            yield _dictionary_encode_schema_data(builder.build())

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

    def get_name(self) -> str:
        """Return a human-readable name for this datasource."""
        return "MCAP"

    @property
    def supports_distributed_reads(self) -> bool:
        """Whether this datasource supports distributed reads.

        MCAP files can be read in parallel across multiple files.
        """
        return True


SCHEMA_DATA_COLUMN = "schema_data"


def _dictionary_encode_schema_data(block: Block) -> Block:
    """Store each distinct schema definition once per block, not once per row.

    A schema is a per-channel attribute, but the row representation repeats its
    whole definition on every message. Real ROS 2 definitions run 1.5-2.6 KB, so
    on a high-rate topic with a small payload the column dominates the block:
    86% of an IMU-only read of a 105-second FAST-LIVO recording, against 6% of
    the same recording read whole, where camera and lidar payloads outweigh it.

    Dictionary encoding replaces the repeated values with int32 indices into a
    dictionary holding one copy of each. Values read back are byte-identical.

    Only ``schema_data`` is encoded. ``topic`` and ``schema_name`` repeat too,
    but they are short enough that the saving is marginal, and callers sort and
    group by them -- which Arrow cannot do on a dictionary-encoded column.
    """
    import pyarrow as pa

    if not isinstance(block, pa.Table):
        # A pandas block, which `DelegatingBlockBuilder` produces for row
        # values Arrow cannot hold. Nothing to do.
        return block

    index = block.schema.get_field_index(SCHEMA_DATA_COLUMN)
    if index < 0:
        # `include_metadata=False`: the column was never added.
        return block

    column = block.column(index)
    if pa.types.is_dictionary(column.type) or pa.types.is_null(column.type):
        # Already encoded, or every schema was absent so there is nothing to
        # deduplicate.
        return block

    try:
        encoded = column.dictionary_encode().combine_chunks()
    except pa.ArrowNotImplementedError:
        # Arrow cannot dictionary-encode every type it can store. Leaving the
        # column as-is costs memory but never correctness.
        logger.debug(
            f"Could not dictionary-encode '{SCHEMA_DATA_COLUMN}' of type "
            f"{column.type}.",
        )
        return block

    return block.set_column(index, SCHEMA_DATA_COLUMN, encoded)
