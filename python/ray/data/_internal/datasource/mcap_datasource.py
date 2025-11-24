"""MCAP (Message Capture) datasource for Ray Data.

MCAP is a standardized format for storing timestamped messages from robotics and
autonomous systems, commonly used for sensor data, control commands, and other
time-series data.
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
        if not isinstance(self.start_time, (int, float)) or not isinstance(self.end_time, (int, float)):
            raise TypeError(
                f"Time range values must be numeric, got start_time={type(self.start_time)}, "
                f"end_time={type(self.end_time)}"
            )
        # Convert to int for consistency
        self.start_time = int(self.start_time)
        self.end_time = int(self.end_time)

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
        # Check for integer overflow (2^63 - 1 is max safe int64)
        max_safe_time = (2**63) - 1
        if self.start_time > max_safe_time or self.end_time > max_safe_time:
            raise ValueError(
                f"time values exceed maximum safe value ({max_safe_time}), "
                f"got start_time={self.start_time}, end_time={self.end_time}"
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

        # Validate include_metadata is boolean
        if not isinstance(include_metadata, bool):
            raise TypeError(f"include_metadata must be bool, got {type(include_metadata)}")

        # Validate time_range type
        if time_range is not None and not isinstance(time_range, TimeRange):
            raise TypeError(f"time_range must be TimeRange or None, got {type(time_range)}")

        # Convert to sets for faster lookup, filtering out None and non-string values
        if topics is not None:
            if isinstance(topics, (list, tuple)):
                # Filter None and validate all are strings
                filtered_topics = []
                for i, t in enumerate(topics):
                    if t is None:
                        continue
                    if not isinstance(t, str):
                        raise TypeError(f"topics[{i}] must be str, got {type(t)}")
                    filtered_topics.append(t)
                self._topics = set(filtered_topics)
            elif isinstance(topics, set):
                # Filter None and validate all are strings
                filtered_topics = []
                for t in topics:
                    if t is None:
                        continue
                    if not isinstance(t, str):
                        raise TypeError(f"topics must contain only strings, got {type(t)}")
                    filtered_topics.append(t)
                self._topics = set(filtered_topics)
            else:
                raise TypeError(f"topics must be list, tuple, or set, got {type(topics)}")
            # Empty set means filter to nothing, None means no filter
            self._topics = self._topics if self._topics else None
        else:
            self._topics = None

        if message_types is not None:
            if isinstance(message_types, (list, tuple)):
                # Filter None and validate all are strings
                filtered_types = []
                for i, mt in enumerate(message_types):
                    if mt is None:
                        continue
                    if not isinstance(mt, str):
                        raise TypeError(f"message_types[{i}] must be str, got {type(mt)}")
                    filtered_types.append(mt)
                self._message_types = set(filtered_types)
            elif isinstance(message_types, set):
                # Filter None and validate all are strings
                filtered_types = []
                for mt in message_types:
                    if mt is None:
                        continue
                    if not isinstance(mt, str):
                        raise TypeError(f"message_types must contain only strings, got {type(mt)}")
                    filtered_types.append(mt)
                self._message_types = set(filtered_types)
            else:
                raise TypeError(f"message_types must be list, tuple, or set, got {type(message_types)}")
            # Empty set means filter to nothing, None means no filter
            self._message_types = self._message_types if self._message_types else None
        else:
            self._message_types = None

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
            RuntimeError: If the MCAP reader fails or file is corrupted.
        """
        from mcap.reader import make_reader

        if f is None:
            raise ValueError(f"File handle is None for {path}")
        if not path:
            raise ValueError("Path cannot be empty or None")
        if not isinstance(path, str):
            raise TypeError(f"Path must be str, got {type(path)}")

        try:
            reader = make_reader(f)
        except Exception as e:
            raise RuntimeError(f"Failed to create MCAP reader for {path}: {e}") from e

        if reader is None:
            raise RuntimeError(f"MCAP reader is None for {path}")

        # Note: MCAP summaries are optional and iter_messages works without them
        # We don't need to validate the summary since it's not required

        # Validate time_range attributes before accessing
        start_time = None
        end_time = None
        if self._time_range is not None:
            if not hasattr(self._time_range, "start_time") or not hasattr(self._time_range, "end_time"):
                raise ValueError(f"Invalid TimeRange object for {path}")
            start_time = self._time_range.start_time
            end_time = self._time_range.end_time
            # Validate time values are non-negative
            if start_time is not None and start_time < 0:
                raise ValueError(f"Invalid start_time {start_time} for {path}: must be non-negative")
            if end_time is not None and end_time < 0:
                raise ValueError(f"Invalid end_time {end_time} for {path}: must be non-negative")

        # Use MCAP's built-in filtering for topics and time range
        # Empty set means exclude all, None means include all
        topics_list = list(self._topics) if self._topics is not None else None

        try:
            messages = reader.iter_messages(
                topics=topics_list,
                start_time=start_time,
                end_time=end_time,
                log_time_order=True,
                reverse=False,
            )
        except Exception as e:
            raise RuntimeError(f"Failed to iterate MCAP messages from {path}: {e}") from e

        builder = DelegatingBlockBuilder()

        try:
            message_count = 0
            for schema, channel, message in messages:
                message_count += 1
                # Skip None values (shouldn't happen but be defensive)
                if channel is None or message is None:
                    logger.warning(f"Skipping message with None channel or message in {path}")
                    continue

                # Validate schema, channel, message are proper types
                if not hasattr(channel, "topic"):
                    logger.warning(f"Skipping message with invalid channel object in {path}")
                    continue
                if not hasattr(message, "data"):
                    logger.warning(f"Skipping message with invalid message object in {path}")
                    continue

                # Apply filters that couldn't be pushed down to MCAP level
                if not self._should_include_message(schema, channel, message):
                    continue

                try:
                    # Convert message to dictionary format
                    message_data = self._message_to_dict(schema, channel, message, path)
                    builder.add(message_data)
                except (ValueError, TypeError, AttributeError) as e:
                    logger.warning(f"Failed to convert MCAP message to dict from {path}: {e}. Skipping message.")
                    continue
                except Exception as e:
                    # Unexpected error - log and skip but don't fail entire file
                    logger.error(f"Unexpected error converting MCAP message from {path}: {e}. Skipping message.", exc_info=True)
                    continue
        except StopIteration:
            # Normal end of iterator
            pass
        except Exception as e:
            raise RuntimeError(f"Error while reading MCAP messages from {path}: {e}") from e

        # Log if no messages were found (could indicate empty file or all filtered out)
        if message_count == 0:
            logger.debug(f"No messages found in MCAP file {path}")

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
        if self._message_types is not None:
            # Empty set means exclude all
            if not self._message_types:
                return False
            # Check schema name matches filter (empty string is valid schema name)
            if schema and hasattr(schema, "name") and schema.name is not None:
                if schema.name not in self._message_types:
                    return False
            elif self._message_types:
                # Schema is None or has no name, but we have a filter - exclude
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

        Raises:
            ValueError: If required message or channel attributes are None.
        """
        if channel is None:
            raise ValueError(f"Channel is None for message in {path}")
        if message is None:
            raise ValueError(f"Message is None in {path}")

        # Decode message data based on encoding
        if not hasattr(message, "data"):
            raise ValueError(f"Message object missing 'data' attribute in {path}")
        if message.data is None:
            decoded_data = b""
        else:
            decoded_data = message.data

        # Check message_encoding is not None before comparing
        if (
            hasattr(channel, "message_encoding")
            and channel.message_encoding is not None
            and isinstance(channel.message_encoding, str)
            and channel.message_encoding.lower() == "json"
            and isinstance(message.data, bytes)
            and len(message.data) > 0
        ):
            try:
                # Try UTF-8 first, fall back to other encodings if needed
                decoded_data = json.loads(message.data.decode("utf-8"))
            except UnicodeDecodeError as e:
                logger.debug(f"Failed to decode message bytes as UTF-8 from {path}: {e}. Using raw bytes.")
                decoded_data = message.data
            except json.JSONDecodeError as e:
                logger.debug(f"Failed to parse JSON message from {path}: {e}. Using raw bytes.")
                # Keep raw bytes if JSON parsing fails
                decoded_data = message.data

        # Core message data with None-safe defaults
        # Validate and extract topic
        if not hasattr(channel, "topic"):
            raise ValueError(f"Channel object missing 'topic' attribute in {path}")
        topic = channel.topic if channel.topic is not None else ""

        # Validate and extract time fields
        log_time = 0
        if hasattr(message, "log_time"):
            if message.log_time is not None:
                if not isinstance(message.log_time, (int, float)):
                    logger.warning(f"Invalid log_time type {type(message.log_time)} in {path}, using 0")
                else:
                    log_time = int(message.log_time)
                    if log_time < 0:
                        logger.warning(f"Negative log_time {log_time} in {path}, using 0")
                        log_time = 0

        publish_time = 0
        if hasattr(message, "publish_time"):
            if message.publish_time is not None:
                if not isinstance(message.publish_time, (int, float)):
                    logger.warning(f"Invalid publish_time type {type(message.publish_time)} in {path}, using 0")
                else:
                    publish_time = int(message.publish_time)
                    if publish_time < 0:
                        logger.warning(f"Negative publish_time {publish_time} in {path}, using 0")
                        publish_time = 0

        sequence = 0
        if hasattr(message, "sequence"):
            if message.sequence is not None:
                if not isinstance(message.sequence, (int, float)):
                    logger.warning(f"Invalid sequence type {type(message.sequence)} in {path}, using 0")
                else:
                    sequence = int(message.sequence)
                    if sequence < 0:
                        logger.warning(f"Negative sequence {sequence} in {path}, using 0")
                        sequence = 0

        message_data = {
            "data": decoded_data,
            "topic": topic,
            "log_time": log_time,
            "publish_time": publish_time,
            "sequence": sequence,
        }

        # Add metadata if requested
        if self._include_metadata:
            metadata_dict = {}
            if hasattr(message, "channel_id"):
                channel_id = message.channel_id
                if channel_id is not None:
                    if not isinstance(channel_id, (int, float)):
                        logger.warning(f"Invalid channel_id type {type(channel_id)} in {path}, using 0")
                        channel_id = 0
                    else:
                        channel_id = int(channel_id)
                        if channel_id < 0:
                            logger.warning(f"Negative channel_id {channel_id} in {path}, using 0")
                            channel_id = 0
                metadata_dict["channel_id"] = channel_id if channel_id is not None else 0

            if hasattr(channel, "message_encoding"):
                encoding = channel.message_encoding
                metadata_dict["message_encoding"] = encoding if encoding is not None and isinstance(encoding, str) else ""

            if schema:
                if hasattr(schema, "name"):
                    schema_name = schema.name
                    metadata_dict["schema_name"] = schema_name if schema_name is not None else None
                if hasattr(schema, "encoding"):
                    schema_encoding = schema.encoding
                    metadata_dict["schema_encoding"] = schema_encoding if schema_encoding is not None else None
                if hasattr(schema, "data"):
                    schema_data = schema.data
                    # schema.data can be bytes or None
                    metadata_dict["schema_data"] = schema_data if schema_data is not None else None
            else:
                # Schema is None - set all schema fields to None
                metadata_dict["schema_name"] = None
                metadata_dict["schema_encoding"] = None
                metadata_dict["schema_data"] = None

            message_data.update(metadata_dict)

        # Add file path if include_paths is enabled (from FileBasedDatasource)
        # Use _include_paths attribute set by base class, not getattr
        # Note: FileBasedDatasource automatically adds path column, but we check here
        # to avoid duplicate columns if the base class doesn't handle it
        if hasattr(self, "_include_paths") and self._include_paths:
            if not isinstance(path, str):
                logger.warning(f"Path is not a string in {path}, skipping path column")
            else:
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
