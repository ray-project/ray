"""MCAP (Message Capture) datasource for Ray Data.

MCAP is a standardized format for storing timestamped messages from robotics and
autonomous systems, commonly used for sensor data, control commands, and other
time-series data.
"""

import json
import logging
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
        # Collected alongside the builder rather than inside `_message_to_dict`:
        # Arrow infers `struct` from a Python dict, and a struct's fields are
        # the union of the keys it happens to see, so two files with different
        # metadata keys yield blocks that will not concatenate.
        # `_add_channel_metadata` builds a `map` column instead.
        channel_metadata: Optional[List[List[Tuple[str, str]]]] = (
            [] if self._include_metadata else None
        )

        for schema, channel, message in messages:
            # Apply filters that couldn't be pushed down to MCAP level
            if not self._should_include_message(schema, channel, message):
                continue

            # Convert message to dictionary format
            message_data = self._message_to_dict(schema, channel, message, path)
            builder.add(message_data)
            if channel_metadata is not None:
                channel_metadata.append(list(channel.metadata.items()))

        # Yield the block if we have any messages
        if builder.num_rows() > 0:
            yield _add_channel_metadata(builder.build(), channel_metadata)

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


CHANNEL_METADATA_COLUMN = "channel_metadata"


def _add_channel_metadata(
    block: Block, values: Optional[List[List[Tuple[str, str]]]]
) -> Block:
    """Attach each message's channel metadata as a ``map<string, string>`` column.

    A Channel record carries a ``metadata`` field of type ``Map<string, string>``
    (MCAP specification, op=0x04) holding whatever the recorder chose to record
    about the topic. ROS 2 writes ``offered_qos_profiles`` there, and rigs
    commonly add sensor serials or calibration identifiers. No message record
    reproduces it, so without this column that information is not reachable from
    the dataset at all.

    The column is typed explicitly rather than inferred. Arrow reads a Python
    dict as a ``struct`` whose fields are the union of the keys it happens to
    see: blocks from two files recording different keys then carry different
    types and fail to concatenate, and a channel that never had a key becomes
    indistinguishable from one whose value is null. A ``map`` matches the
    specification's own type, keeps the block schema stable whatever the keys
    are, and concatenates across files.
    """
    import pyarrow as pa

    if values is None or not isinstance(block, pa.Table):
        # `include_metadata=False`, or a pandas block, which
        # `DelegatingBlockBuilder` produces for row values Arrow cannot hold.
        return block

    column = pa.array(values, type=pa.map_(pa.string(), pa.string()))
    return block.append_column(CHANNEL_METADATA_COLUMN, column)
