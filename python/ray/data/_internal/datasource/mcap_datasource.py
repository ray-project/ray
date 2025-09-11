"""MCAP (Message Capture) datasource for Ray Data.

MCAP is a standardized format for storing timestamped messages from robotics and
autonomous systems, commonly used for sensor data, control commands, and other
time-series data.
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Set, Union

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.util import _check_import
from ray.data.block import Block
from ray.data.datasource.file_based_datasource import FileBasedDatasource
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import pyarrow

logger = logging.getLogger(__name__)


@DeveloperAPI
class MCAPDatasource(FileBasedDatasource):
    """MCAP (Message Capture) datasource for Ray Data.
    
    This datasource provides reading of MCAP files with predicate pushdown
    optimization for filtering by channels, topics, time ranges, and message types.
    
    MCAP is a standardized format for storing timestamped messages from robotics and
    autonomous systems, commonly used for sensor data, control commands, and other
    time-series data.
    
    Examples:
        Basic usage:
        
        >>> import ray  # doctest: +SKIP
        >>> ds = ray.data.read_mcap("/path/to/data.mcap")  # doctest: +SKIP
        
        With channel filtering:
        
        >>> ds = ray.data.read_mcap(  # doctest: +SKIP
        ...     "/path/to/data.mcap",
        ...     channels={"camera", "lidar"},
        ...     time_range=(1000000000, 2000000000)
        ... )  # doctest: +SKIP
        
        With topic filtering and metadata:
        
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
        channels: Optional[Union[List[str], Set[str]]] = None,
        topics: Optional[Union[List[str], Set[str]]] = None,
        time_range: Optional[tuple] = None,
        message_types: Optional[Union[List[str], Set[str]]] = None,
        include_metadata: bool = True,
        **file_based_datasource_kwargs,
    ):
        """Initialize MCAP datasource.
        
        Args:
            paths: Path or list of paths to MCAP files.
            channels: Optional list/set of channel names to include. If specified,
                only messages from these channels will be read. Mutually exclusive
                with ``topics``.
            topics: Optional list/set of topic names to include. If specified,
                only messages from these topics will be read. Mutually exclusive
                with ``channels``.
            time_range: Optional tuple of (start_time, end_time) in nanoseconds
                for filtering messages by timestamp. Both values must be non-negative
                and start_time < end_time.
            message_types: Optional list/set of message type names (schema names)
                to include. Only messages with matching schema names will be read.
            include_metadata: Whether to include MCAP metadata fields in the output.
                Defaults to True. When True, includes schema, channel, and message
                metadata.
            **file_based_datasource_kwargs: Additional arguments for FileBasedDatasource.
                
        Raises:
            ValueError: If both channels and topics are specified, or if time_range
                is invalid.
        """
        super().__init__(paths, **file_based_datasource_kwargs)
        
        _check_import(self, module="mcap", package="mcap")
        
        # Validate mutually exclusive filters
        if channels is not None and topics is not None:
            raise ValueError("Cannot specify both 'channels' and 'topics' - they are mutually exclusive")
        
        # Convert to sets for faster lookup
        self._channels = set(channels) if channels else None
        self._topics = set(topics) if topics else None
        self._message_types = set(message_types) if message_types else None
        self._time_range = time_range
        self._include_metadata = include_metadata
        
        # Validate time range
        if self._time_range:
            start_time, end_time = self._time_range
            if start_time >= end_time:
                raise ValueError("start_time must be less than end_time")
            if start_time < 0 or end_time < 0:
                raise ValueError("time values must be non-negative")
    
    def _read_stream(self, f: "pyarrow.NativeFile", path: str) -> Iterator[Block]:
        """Read MCAP file and yield blocks of message data.
        
        This method implements efficient MCAP reading with predicate pushdown.
        It uses MCAP's built-in filtering capabilities for optimal performance
        and applies additional filters when needed.
        
        Args:
            f: File-like object to read from. Must be seekable for MCAP reading.
            path: Path to the MCAP file being processed.
            
        Yields:
            Blocks of MCAP message data as pyarrow Tables.
            
        Raises:
            ValueError: If the MCAP file cannot be read or has invalid format.
        """
        from mcap.reader import make_reader
        
        try:
            reader = make_reader(f)
            
            # Determine which topics to filter on for MCAP's built-in filtering
            # Use topics if specified, otherwise use channels (which map to topics)
            filter_topics = None
            if self._topics:
                filter_topics = list(self._topics)
            elif self._channels:
                # For channels, we'll need to apply filtering after reading
                # since MCAP filters by topic, not channel name
                filter_topics = None
            
            # Use MCAP's built-in filtering for topics and time range
            messages = reader.iter_messages(
                topics=filter_topics,
                start_time=self._time_range[0] if self._time_range else None,
                end_time=self._time_range[1] if self._time_range else None,
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
                
        except Exception as e:
            logger.error(f"Error reading MCAP file {path}: {e}")
            raise ValueError(f"Failed to read MCAP file {path}: {e}") from e
    
    def _should_include_message(self, schema, channel, message) -> bool:
        """Check if a message should be included based on filters.
        
        This method applies Python-level filtering that cannot be pushed down
        to the MCAP library level.
        
        Args:
            schema: MCAP schema object containing message type information.
            channel: MCAP channel object containing topic and metadata.
            message: MCAP message object containing the actual data.
            
        Returns:
            True if the message should be included, False otherwise.
        """
        # Message type filter
        if (self._message_types and 
            schema and 
            schema.name not in self._message_types):
            return False
        
        # Channel filter (only apply if topics weren't used for MCAP filtering)
        if (self._channels and 
            not self._topics and 
            channel.topic not in self._channels):
            return False
        
        return True
    
    def _message_to_dict(self, schema, channel, message, path: str) -> Dict[str, Any]:
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
        # Core message data
        message_data = {
            "data": message.data,
            "topic": channel.topic,
            "log_time": message.log_time,
            "publish_time": message.publish_time,
            "sequence": message.sequence,
        }
        
        # Add metadata if requested
        if self._include_metadata:
            message_data.update({
                "channel_id": message.channel_id,
                "message_encoding": channel.message_encoding,
                "schema_name": schema.name if schema else None,
                "schema_encoding": schema.encoding if schema else None,
                "schema_data": schema.data if schema else None,
            })
        
        # Add file path if include_paths is enabled
        if hasattr(self, 'include_paths') and self.include_paths:
            message_data["_file_path"] = path
        
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