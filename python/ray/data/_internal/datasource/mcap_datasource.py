"""
MCAP (Message Capture) datasource for Ray Data.

This datasource provides efficient reading of MCAP files with predicate pushdown
optimization for channel/topic filtering and time range filtering, plus support
for external indexing to further optimize reads.
"""

import logging
import os
from dataclasses import dataclass
from enum import Enum
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

if TYPE_CHECKING:
    import pyarrow as pa
    import mcap

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MCAPFilterConfig:
    """Configuration for MCAP filtering operations.

    This configuration object controls how MCAP data is filtered during reading,
    enabling predicate pushdown optimization for better performance.

    Args:
        channels: Optional set of channel names to include. If None, all channels are included.
        topics: Optional set of topic names to include. If None, all topics are included.
        time_range: Optional tuple of (start_time, end_time) in nanoseconds.
                   If None, all messages are included.
        message_types: Optional set of message type names to include.
                      If None, all message types are included.
        include_metadata: Whether to include MCAP metadata fields (channel, topic, timestamp, etc.).
        batch_size: Number of messages to read per batch.
    """

    channels: Optional[Set[str]] = None
    topics: Optional[Set[str]] = None
    time_range: Optional[Tuple[float, float]] = None
    message_types: Optional[Set[str]] = None
    include_metadata: bool = True
    batch_size: int = 1000

    def __post_init__(self):
        """Validate filter configuration."""
        if self.time_range is not None:
            start_time, end_time = self.time_range
            if start_time >= end_time:
                raise ValueError("start_time must be less than end_time")
            if start_time < 0 or end_time < 0:
                raise ValueError("time values must be non-negative")


class IndexType(Enum):
    """Valid index types for external indexing."""

    AUTO = "auto"
    PARQUET = "parquet"
    SQLITE = "sqlite"
    CUSTOM = "custom"


@dataclass(frozen=True)
class ExternalIndexConfig:
    """Configuration for external indexing to optimize MCAP reads.

    External indexes can provide faster access to specific data ranges,
    further optimizing predicate pushdown operations.

    Args:
        index_path: Path to the external index file.
        index_type: Type of external index (e.g., "sqlite", "parquet", "custom").
        validate_index: Whether to validate the index against the MCAP file.
        cache_index: Whether to cache the index in memory for repeated access.
    """

    index_path: str
    index_type: str = "auto"  # auto-detect based on file extension
    validate_index: bool = True
    cache_index: bool = True

    def __post_init__(self):
        """Validate external index configuration."""
        # Validate index_path is not empty
        if not self.index_path:
            raise ValueError("index_path cannot be empty")

        # Validate index_type is one of the allowed values
        valid_types = [t.value for t in IndexType]
        if self.index_type not in valid_types:
            raise ValueError(
                f"index_type must be one of {valid_types}, got '{self.index_type}'"
            )


class MCAPDatasource(FileBasedDatasource):
    """MCAP (Message Capture) datasource for reading MCAP files.

    This datasource provides efficient reading of MCAP files with predicate pushdown
    optimization. It supports filtering by:
    - Channel names
    - Topic names
    - Time ranges
    - Message types

    The datasource leverages MCAP's indexing capabilities to optimize reads and
    can use external indexing functions for further optimization.

    Examples:
        :noindex:

        >>> import ray
        >>> from ray.data.datasource import MCAPDatasource
        >>>
        >>> # Read all MCAP files in a directory
        >>> ds = ray.data.read_datasource(
        ...     MCAPDatasource("s3://bucket/mcap-data/")
        ... )
        >>>
        >>> # Read with filtering and external indexing
        >>> filter_config = MCAPFilterConfig(
        ...     channels={"camera", "lidar"},
        ...     time_range=(1000000000, 2000000000),  # 1-2 seconds in nanoseconds
        ...     include_metadata=True
        ... )
        >>> external_index = ExternalIndexConfig("s3://bucket/mcap-data/index.parquet")
        >>> ds = ray.data.read_datasource(
        ...     MCAPDatasource("s3://bucket/mcap-data/",
        ...                    filter_config=filter_config,
        ...                    external_index_config=external_index)
        ... )
    """

    _FILE_EXTENSIONS = ["mcap"]

    def __init__(
        self,
        paths: Union[str, List[str]],
        *,
        filter_config: Optional[MCAPFilterConfig] = None,
        external_index_config: Optional[ExternalIndexConfig] = None,
        include_paths: bool = False,
        **file_based_datasource_kwargs,
    ):
        """Initialize the MCAP datasource.

        Args:
            paths: Path or list of paths to MCAP files or directories.
            filter_config: Optional filter configuration for predicate pushdown.
            external_index_config: Optional external index configuration for optimization.
            include_paths: Whether to include file paths in the output.
            **file_based_datasource_kwargs: Additional arguments for FileBasedDatasource.
        """
        super().__init__(paths, **file_based_datasource_kwargs)

        self._filter_config = filter_config or MCAPFilterConfig()
        self._external_index_config = external_index_config
        self._include_paths = include_paths

        # Validate MCAP library availability
        _check_import(self, module="mcap", package="mcap")

        # Initialize external index if provided
        self._external_index = None
        if self._external_index_config:
            self._load_external_index()

    def _load_external_index(self):
        """Load and validate external index for optimization."""
        try:
            if self._external_index_config.index_type == "auto":
                # Auto-detect index type based on file extension
                ext = os.path.splitext(self._external_index_config.index_path)[
                    1
                ].lower()
                if ext == ".parquet":
                    self._external_index = self._load_parquet_index()
                elif ext == ".sqlite":
                    self._external_index = self._load_sqlite_index()
                else:
                    self._external_index = self._load_custom_index()
            else:
                # Use specified index type
                if self._external_index_config.index_type == "parquet":
                    self._external_index = self._load_parquet_index()
                elif self._external_index_config.index_type == "sqlite":
                    self._external_index = self._load_sqlite_index()
                else:
                    self._external_index = self._load_custom_index()

            logger.info(
                f"Loaded external index: {self._external_index_config.index_path}"
            )

        except Exception as e:
            logger.warning(f"Failed to load external index: {e}")
            self._external_index = None

    def _load_parquet_index(self):
        """Load Parquet-based external index."""
        try:
            import pyarrow.parquet as pq

            index_table = pq.read_table(self._external_index_config.index_path)
            return {
                "type": "parquet",
                "data": index_table,
                "cached": self._external_index_config.cache_index,
            }
        except Exception as e:
            logger.warning(f"Failed to load Parquet index: {e}")
            return None

    def _load_sqlite_index(self):
        """Load SQLite-based external index."""
        try:
            import sqlite3

            conn = sqlite3.connect(self._external_index_config.index_path)
            cursor = conn.cursor()

            # Query index structure
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()

            index_data = {
                "type": "sqlite",
                "connection": conn,
                "tables": [table[0] for table in tables],
                "cached": self._external_index_config.cache_index,
            }

            return index_data
        except Exception as e:
            logger.warning(f"Failed to load SQLite index: {e}")
            return None

    def _load_custom_index(self):
        """Load custom format external index."""
        try:
            # This is a placeholder for custom index formats
            # Users can extend this method for their specific index types
            logger.info("Loading custom index format")
            return {
                "type": "custom",
                "path": self._external_index_config.index_path,
                "cached": self._external_index_config.cache_index,
            }
        except Exception as e:
            logger.warning(f"Failed to load custom index: {e}")
            return None

    def _read_stream(self, f: "pyarrow.NativeFile", path: str) -> Iterator[Block]:
        """Streaming read a single MCAP file with filtering and external indexing.

        This method implements the core MCAP reading logic with predicate pushdown
        optimization for channels, topics, and time ranges, plus external indexing
        for further optimization.

        Args:
            f: PyArrow file handle.
            path: Path to the MCAP file.

        Yields:
            Blocks of MCAP message data.
        """
        import mcap

        try:
            # MCAP can read directly from seekable file-like objects
            # pyarrow.NativeFile is seekable, so we can pass it directly
            with mcap.open(f, "r") as reader:
                # Get summary for optimization
                summary = reader.get_summary()

                # Apply predicate pushdown filters with external index optimization
                filtered_messages = self._apply_filters_with_external_index(
                    reader, summary, path
                )

                # Process messages in batches
                batch = []
                for message in filtered_messages:
                    # Convert message to PyArrow-compatible format
                    message_data = self._message_to_pyarrow_format(message)

                    if self._include_paths:
                        message_data["_file_path"] = path

                    batch.append(message_data)

                    # Yield batch when it reaches the configured size
                    if len(batch) >= self._filter_config.batch_size:
                        yield self._create_block(batch)
                        batch = []

                # Yield remaining messages
                if batch:
                    yield self._create_block(batch)

        except Exception as e:
            logger.error(f"Error reading MCAP file {path}: {e}")
            raise

    def _apply_filters_with_external_index(
        self, reader: "mcap.MCAPReader", summary: "mcap.Summary", file_path: str
    ) -> Iterator["mcap.Message"]:
        """Apply predicate pushdown filters with external index optimization.

        This method implements the core filtering logic, leveraging MCAP's
        indexing capabilities and external indexes for efficient reads.

        Args:
            reader: MCAP reader instance.
            summary: MCAP file summary for optimization.
            file_path: Path to the MCAP file for external index lookup.

        Yields:
            Filtered MCAP messages.
        """
        # Build filter expression for efficient reading
        filter_expr = self._build_filter_expression(summary)

        # Use external index for additional optimization if available
        if self._external_index:
            optimized_filter = self._optimize_filter_with_external_index(
                filter_expr, file_path
            )
            if optimized_filter:
                filter_expr = optimized_filter

        # Use MCAP's optimized reading with filters
        if filter_expr:
            messages = reader.read_messages(filter=filter_expr)
        else:
            messages = reader.read_messages()

        # Apply additional filters that can't be pushed down
        for message in messages:
            if self._should_include_message(message):
                yield message

    def _optimize_filter_with_external_index(
        self, base_filter: Optional["mcap.Filter"], file_path: str
    ) -> Optional["mcap.Filter"]:
        """Optimize MCAP filter using external index information.

        Args:
            base_filter: Base MCAP filter.
            file_path: Path to the MCAP file.

        Returns:
            Optimized filter or None if no optimization possible.
        """
        if not self._external_index:
            return base_filter

        try:
            if self._external_index["type"] == "parquet":
                return self._optimize_with_parquet_index(base_filter, file_path)
            elif self._external_index["type"] == "sqlite":
                return self._optimize_with_sqlite_index(base_filter, file_path)
            elif self._external_index["type"] == "custom":
                return self._optimize_with_custom_index(base_filter, file_path)
        except Exception as e:
            logger.warning(f"Failed to optimize filter with external index: {e}")

        return base_filter

    def _optimize_with_parquet_index(
        self, base_filter: Optional["mcap.Filter"], file_path: str
    ) -> Optional["mcap.Filter"]:
        """Optimize filter using Parquet-based external index."""
        try:
            import mcap

            # Query the Parquet index for file-specific information
            index_table = self._external_index["data"]

            # Example: Look up optimal time ranges or channel mappings
            # This is a simplified example - real implementation would depend on index structure
            if self._filter_config.time_range:
                start_time, end_time = self._filter_config.time_range

                # Use index to find optimal time boundaries
                # This could involve querying the index for actual data ranges
                optimized_start = start_time
                optimized_end = end_time

                # Create optimized filter
                return mcap.Filter(start_time=optimized_start, end_time=optimized_end)

        except Exception as e:
            logger.warning(f"Failed to optimize with Parquet index: {e}")

        return base_filter

    def _optimize_with_sqlite_index(
        self, base_filter: Optional["mcap.Filter"], file_path: str
    ) -> Optional["mcap.Filter"]:
        """Optimize filter using SQLite-based external index."""
        try:
            import mcap

            conn = self._external_index["connection"]
            cursor = conn.cursor()

            # Example: Query SQLite index for optimization hints
            # This is a simplified example - real implementation would depend on index structure
            if self._filter_config.channels:
                # Query index for channel mappings using safe parameter substitution
                placeholders = ",".join("?" for _ in self._filter_config.channels)
                params = list(self._filter_config.channels) + [file_path]
                cursor.execute(
                    f"""
                    SELECT channel_id, topic FROM channels 
                    WHERE topic IN ({placeholders}) AND file_path = ?
                """,
                    params,
                )

                channel_mappings = cursor.fetchall()
                if channel_mappings:
                    channel_ids = [row[0] for row in channel_mappings]
                    return mcap.Filter(channel_ids=channel_ids)

        except Exception as e:
            logger.warning(f"Failed to optimize with SQLite index: {e}")

        return base_filter

    def _optimize_with_custom_index(
        self, base_filter: Optional["mcap.Filter"], file_path: str
    ) -> Optional["mcap.Filter"]:
        """Optimize filter using custom format external index."""
        try:
            # This is a placeholder for custom index optimization
            # Users can extend this method for their specific index types
            logger.debug("Using custom index for optimization")

            # Example: Custom index could provide:
            # - Pre-computed channel mappings
            # - Time range optimizations
            # - Message type filters
            # - Spatial or other domain-specific optimizations

        except Exception as e:
            logger.warning(f"Failed to optimize with custom index: {e}")

        return base_filter

    def _apply_filters(
        self, reader: "mcap.MCAPReader", summary: "mcap.Summary"
    ) -> Iterator["mcap.Message"]:
        """Apply predicate pushdown filters to MCAP messages (legacy method).

        This method is kept for backward compatibility but delegates to
        the enhanced method with external index support.

        Args:
            reader: MCAP reader instance.
            summary: MCAP file summary for optimization.

        Yields:
            Filtered MCAP messages.
        """
        # Delegate to enhanced method
        for message in self._apply_filters_with_external_index(reader, summary, ""):
            yield message

    def _build_filter_expression(
        self, summary: "mcap.Summary"
    ) -> Optional["mcap.Filter"]:
        """Build MCAP filter expression for predicate pushdown.

        Args:
            summary: MCAP file summary.

        Returns:
            MCAP filter expression or None if no filters.
        """
        import mcap

        filters = []

        # Channel filter
        if self._filter_config.channels:
            channel_ids = [
                channel.id
                for channel in summary.channels.values()
                if channel.topic in self._filter_config.channels
            ]
            if channel_ids:
                filters.append(mcap.Filter(channel_ids=channel_ids))

        # Topic filter (if channels not specified)
        elif self._filter_config.topics:
            channel_ids = [
                channel.id
                for channel in summary.channels.values()
                if channel.topic in self._filter_config.topics
            ]
            if channel_ids:
                filters.append(mcap.Filter(channel_ids=channel_ids))

        # Time range filter
        if self._filter_config.time_range:
            start_time, end_time = self._filter_config.time_range
            filters.append(mcap.Filter(start_time=start_time, end_time=end_time))

        # Combine filters
        if len(filters) == 1:
            return filters[0]
        elif len(filters) > 1:
            # MCAP doesn't support complex filter combinations, so we'll apply
            # the most restrictive filter and do additional filtering in Python
            return filters[0]

        return None

    def _should_include_message(self, message: "mcap.Message") -> bool:
        """Check if a message should be included based on filters.

        This method applies filters that couldn't be pushed down to MCAP level.

        Args:
            message: MCAP message.

        Returns:
            True if message should be included.
        """
        # Time range filter (if not already applied)
        if self._filter_config.time_range:
            start_time, end_time = self._filter_config.time_range
            if not (start_time <= message.log_time <= end_time):
                return False

        # Message type filter
        if self._filter_config.message_types:
            if message.schema.name not in self._filter_config.message_types:
                return False

        return True

    def _message_to_pyarrow_format(self, message: "mcap.Message") -> Dict[str, Any]:
        """Convert MCAP message to PyArrow-compatible format.

        This method converts MCAP messages to a format that can be directly
        converted to PyArrow tables, focusing on data parsing without complex
        protobuf or dictionary handling.

        Args:
            message: MCAP message.

        Returns:
            Message data in PyArrow-compatible format.
        """
        # Basic message data - raw bytes that can be converted to PyArrow
        message_data = {
            "data": message.data,
            "channel_id": message.channel_id,
            "log_time": message.log_time,
            "publish_time": message.publish_time,
            "sequence": message.sequence,
            "schema_id": message.schema_id,
        }

        # Add schema information if requested
        if self._filter_config.include_metadata:
            message_data.update(
                {
                    "schema_name": message.schema.name,
                    "schema_encoding": message.schema.encoding,
                    "schema_data": message.schema.data,
                }
            )

        return message_data

    def _create_block(self, batch: List[Dict[str, Any]]) -> Block:
        """Create a PyArrow block from a batch of messages.

        Args:
            batch: List of message data dictionaries.

        Returns:
            PyArrow table block.
        """
        import pyarrow as pa

        # Convert to PyArrow table
        try:
            return pa.Table.from_pylist(batch)
        except Exception as e:
            logger.warning(f"Failed to create PyArrow table from batch. Error: {e}")
            # Re-raising the exception provides a clearer error to the user
            # than falling back to a raw string representation.
            # This helps users diagnose issues like inconsistent schema within the batch.
            raise

    def __del__(self):
        """Clean up external index resources."""
        if hasattr(self, "_external_index") and self._external_index:
            if (
                self._external_index.get("type") == "sqlite"
                and "connection" in self._external_index
            ):
                try:
                    self._external_index["connection"].close()
                except Exception as e:
                    logger.warning(
                        f"Failed to close SQLite connection for external index: {e}"
                    )
