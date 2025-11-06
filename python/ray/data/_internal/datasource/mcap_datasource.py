"""MCAP (Message Capture) datasource for Ray Data.

MCAP is a standardized format for storing timestamped messages from robotics and
autonomous systems, commonly used for sensor data, control commands, and other
time-series data.

See https://mcap.dev/ for more information about the MCAP format.
"""

import json
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Set, Tuple, Union

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.datasource.file_based_datasource import FileBasedDatasource
from ray.data.datasource.file_meta_provider import (
    BaseFileMetadataProvider,
    DefaultFileMetadataProvider,
)
from ray.data.expressions import Expr
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import pyarrow
    from mcap.reader import Channel, Message, Schema  # noqa: F401

    from ray.data._internal.util import RetryingPyFileSystem
    from ray.data.datasource.partitioning import Partitioning

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


@dataclass
class MCAPFileMetadata:
    """Metadata extracted from an MCAP file summary.

    Attributes:
        path: Path to the MCAP file.
        file_size: Size of the file in bytes.
        num_messages: Total number of messages in the file.
        topics: Set of topic names present in the file.
        message_types: Set of schema names (message types) present in the file.
        start_time: Earliest log_time in nanoseconds (inclusive).
        end_time: Latest log_time in nanoseconds (exclusive).
    """

    path: str
    file_size: int
    num_messages: int
    topics: Set[str]
    message_types: Set[str]
    start_time: Optional[int]
    end_time: Optional[int]


@DeveloperAPI
class MCAPFileMetadataProvider(BaseFileMetadataProvider):
    """File metadata provider that extracts metadata from MCAP file summaries.

    This provider reads MCAP file summaries to extract file-level metadata including
    topics, time ranges, message counts, and message types. This enables efficient
    file filtering based on predicate pushdown expressions.

    Examples:
        >>> from ray.data.datasource import MCAPFileMetadataProvider  # doctest: +SKIP
        >>> provider = MCAPFileMetadataProvider(  # doctest: +SKIP
        ...     topics={"/camera/image_raw"},  # doctest: +SKIP
        ...     time_range=TimeRange(start_time=1000000000, end_time=2000000000)  # doctest: +SKIP
        ... )  # doctest: +SKIP
    """

    def __init__(
        self,
        topics: Optional[Union[List[str], Set[str]]] = None,
        time_range: Optional[TimeRange] = None,
        message_types: Optional[Union[List[str], Set[str]]] = None,
    ):
        """Initialize MCAP file metadata provider.

        Args:
            topics: Optional set of topics to filter files by. Files that don't
                contain any of these topics will be filtered out.
            time_range: Optional time range to filter files by. Files that don't
                overlap with this time range will be filtered out.
            message_types: Optional set of message types to filter files by.
                Files that don't contain any of these message types will be filtered out.
        """
        super().__init__()
        _check_import(self, module="mcap", package="mcap")
        self._topics = set(topics) if topics else None
        self._time_range = time_range
        self._message_types = set(message_types) if message_types else None

    def expand_paths(
        self,
        paths: List[str],
        filesystem: "RetryingPyFileSystem",
        partitioning: Optional["Partitioning"] = None,
        ignore_missing_paths: bool = False,
    ) -> Iterator[Tuple[str, int]]:
        """Expand paths and filter based on MCAP file metadata.

        This method implements **file-level predicate pushdown** for MCAP files.
        It reads MCAP file summaries (efficient footer-only reads) to extract
        metadata (topics, message types, time ranges) and filters files that
        don't match the provided filters BEFORE reading their contents.

        This is called by FileBasedDatasource.__init__() during datasource
        initialization, enabling file-level filtering when using predicate
        pushdown via ray.data.filter().

        Example flow:
            1. User calls: ds = ray.data.read_mcap(dir).filter(col("topic") == "/cam")
            2. Predicate pushdown calls: apply_predicate() → creates new datasource
            3. New datasource.__init__() calls: meta_provider.expand_paths()
            4. expand_paths() reads metadata and filters files (THIS METHOD)
            5. Only matching files are included in the datasource

        Args:
            paths: List of file or directory paths.
            filesystem: Filesystem implementation for reading files.
            partitioning: Optional partitioning information.
            ignore_missing_paths: If True, skip missing files instead of raising.

        Yields:
            Tuples of (file_path, file_size) for files that match the filters.
            Files that don't match are excluded entirely (file-level filtering).
        """
        from ray.data.datasource.file_meta_provider import _expand_paths

        for path, file_size in _expand_paths(
            paths, filesystem, partitioning, ignore_missing_paths
        ):
            if not path.endswith(".mcap"):
                continue

            try:
                metadata = self._read_file_metadata(path, filesystem, file_size)
                if self._should_include_file(metadata):
                    yield path, metadata.file_size
            except Exception as e:
                if ignore_missing_paths:
                    logger.warning(f"Failed to read MCAP metadata from {path}: {e}")
                    continue
                raise

    def _read_file_metadata(
        self,
        path: str,
        filesystem: "RetryingPyFileSystem",
        file_size: Optional[int],
    ) -> MCAPFileMetadata:
        """Read metadata from an MCAP file summary.

        This method efficiently reads only the MCAP file's summary section (footer),
        similar to how Parquet metadata is read. It does NOT scan the entire file
        or read message data, keeping metadata extraction fast and lightweight.

        The MCAP library (https://github.com/foxglove/mcap) provides efficient
        summary reading via reader.get_summary(), which only reads the footer
        section of the file.

        Args:
            path: Path to the MCAP file.
            filesystem: Filesystem implementation.
            file_size: File size if already known, otherwise None.

        Returns:
            MCAPFileMetadata object containing extracted metadata.
        """
        from mcap.reader import make_reader  # noqa: F401

        topics = set()
        message_types = set()
        start_time = None
        end_time = None
        num_messages = 0

        try:
            # Get file size if not already known
            if file_size is None:
                file_size = self._get_file_size(path, filesystem)

            # Try to read summary from MCAP file
            # NOTE: get_summary() only reads the summary section at the end of the file,
            # not the entire file data. This is analogous to reading Parquet metadata.
            with filesystem.open_input_stream(path) as f:
                reader = make_reader(f)
                try:
                    summary = reader.get_summary()
                except Exception:
                    # If summary is not available, we'll just use file size for sizing.
                    # This is acceptable as not all MCAP files have summaries.
                    summary = None

            # Extract metadata from summary if available
            if summary is not None:
                topics, message_types = self._extract_from_summary(summary)
                num_messages, start_time, end_time = self._extract_statistics(summary)

            # Note: We do NOT scan the entire file if summary is unavailable.
            # This keeps metadata extraction efficient. Files without summaries will
            # still be processed, but won't have topic/time filtering applied at the
            # file level (filtering will happen at the message level during read).

        except Exception as e:
            logger.warning(f"Failed to read MCAP metadata from {path}: {e}")
            return self._empty_metadata(path, file_size)

        return MCAPFileMetadata(
            path=path,
            file_size=file_size or 0,
            num_messages=num_messages,
            topics=topics,
            message_types=message_types,
            start_time=start_time,
            end_time=end_time,
        )

    def _extract_from_summary(self, summary) -> Tuple[Set[str], Set[str]]:
        """Extract topics and message types from MCAP summary."""
        topics = set()
        message_types = set()

        if hasattr(summary, "channels") and summary.channels:
            for channel in summary.channels.values():
                topics.add(channel.topic)
                if hasattr(channel, "schema_id") and channel.schema_id:
                    if hasattr(summary, "schemas") and summary.schemas:
                        schema = summary.schemas.get(channel.schema_id)
                        if schema:
                            message_types.add(schema.name)

        return topics, message_types

    def _extract_statistics(self, summary) -> Tuple[int, Optional[int], Optional[int]]:
        """Extract statistics from MCAP summary."""
        num_messages = 0
        start_time = None
        end_time = None

        if hasattr(summary, "statistics") and summary.statistics:
            stat = summary.statistics
            if hasattr(stat, "message_count"):
                num_messages = stat.message_count or 0
            if hasattr(stat, "message_start_time"):
                start_time = stat.message_start_time
            if hasattr(stat, "message_end_time"):
                end_time = stat.message_end_time

        return num_messages, start_time, end_time

    def _get_file_size(
        self, path: str, filesystem: "RetryingPyFileSystem"
    ) -> Optional[int]:
        """Get file size from filesystem."""
        try:
            file_info = filesystem.get_file_info(path)
            return file_info.size if file_info.size >= 0 else None
        except Exception:
            return None

    def _empty_metadata(self, path: str, file_size: Optional[int]) -> MCAPFileMetadata:
        """Return empty metadata for failed reads."""
        return MCAPFileMetadata(
            path=path,
            file_size=file_size or 0,
            num_messages=0,
            topics=set(),
            message_types=set(),
            start_time=None,
            end_time=None,
        )

    def _should_include_file(self, metadata: MCAPFileMetadata) -> bool:
        """Check if a file should be included based on filters.

        If file metadata is empty (no summary available), we conservatively include
        the file since we can't determine if it matches the filter. Message-level
        filtering will be applied during read.

        Args:
            metadata: File metadata to check.

        Returns:
            True if the file should be included, False otherwise.
        """
        # If metadata is empty (no summary in file), include it conservatively
        # Filtering will happen at message-read time
        has_metadata = bool(
            metadata.topics or metadata.message_types or metadata.start_time
        )

        if not has_metadata:
            # No metadata available, include file and filter at message level
            return True

        # Filter by topics - file must contain at least one requested topic
        if self._topics and not metadata.topics.intersection(self._topics):
            return False

        # Filter by message types - file must contain at least one requested type
        if self._message_types and not metadata.message_types.intersection(
            self._message_types
        ):
            return False

        # Filter by time range - file must overlap with requested range
        if self._time_range and metadata.start_time and metadata.end_time:
            # No overlap if file ends before range starts or starts after range ends
            if (
                metadata.end_time <= self._time_range.start_time
                or metadata.start_time >= self._time_range.end_time
            ):
                return False

        return True

    def _get_block_metadata(
        self,
        paths: List[str],
        *,
        rows_per_file: Optional[int],
        file_sizes: List[Optional[int]],
    ) -> BlockMetadata:
        """Get block metadata for MCAP files.

        Args:
            paths: List of file paths for this block.
            rows_per_file: Ignored for MCAP (variable row counts).
            file_sizes: List of file sizes.

        Returns:
            BlockMetadata with file sizes but no row counts.
        """
        size_bytes = None if None in file_sizes else int(sum(file_sizes))
        return BlockMetadata(
            num_rows=None,  # Computed during execution
            size_bytes=size_bytes,
            input_files=paths,
            exec_stats=None,
        )


@DeveloperAPI
class MCAPDatasource(FileBasedDatasource):
    """MCAP (Message Capture) datasource for Ray Data.

    This datasource provides reading of MCAP files with predicate pushdown
    optimization for filtering by topics, time ranges, and message types.

    MCAP is a standardized format for storing timestamped messages from robotics and
    autonomous systems, commonly used for sensor data, control commands, and other
    time-series data.

    The MCAP format is maintained by Foxglove. See https://mcap.dev/ for more
    information about the format specification and https://github.com/foxglove/mcap
    for the Python library implementation.

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

        With predicate pushdown:

        >>> from ray.data.expressions import col  # doctest: +SKIP
        >>> ds = ray.data.read_mcap("/path/to/data.mcap")  # doctest: +SKIP
        >>> ds = ds.filter((col("topic") == "/camera/image_raw") & (col("log_time") > 1000000000))  # doctest: +SKIP
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
        # Use MCAP-specific metadata provider if none provided
        meta_provider = file_based_datasource_kwargs.get("meta_provider")
        if meta_provider is None or isinstance(
            meta_provider, DefaultFileMetadataProvider
        ):
            meta_provider = MCAPFileMetadataProvider(
                topics=topics,
                time_range=time_range,
                message_types=message_types,
            )
            file_based_datasource_kwargs["meta_provider"] = meta_provider

        super().__init__(paths, **file_based_datasource_kwargs)

        _check_import(self, module="mcap", package="mcap")

        # Convert to sets for faster lookup
        self._topics = set(topics) if topics else None
        self._message_types = set(message_types) if message_types else None
        self._time_range = time_range
        self._include_metadata = include_metadata

    def supports_predicate_pushdown(self) -> bool:
        """Whether this datasource supports predicate pushdown.

        MCAP datasource supports predicate pushdown for topic, time_range,
        and message_type filters.
        """
        return True

    def get_current_predicate(self) -> Optional[Expr]:
        """Get the current predicate expression."""
        return self._predicate_expr

    def apply_predicate(self, predicate_expr: Expr) -> "MCAPDatasource":
        """Apply a predicate expression to this datasource.

        Extracts topic, time_range, and message_type filters from the predicate
        expression and applies them to the datasource. Creates a new datasource
        with updated filters.

        **File-Level Filtering via Metadata**:
        This method enables file-level predicate pushdown. When a new datasource
        is created with updated filters, its `__init__()` calls `super().__init__()`,
        which invokes `meta_provider.expand_paths()`. The `MCAPFileMetadataProvider`
        reads MCAP file summaries and filters files that don't match the filters
        BEFORE reading their contents. This provides significant performance
        improvements when reading from directories with many files.

        Example:
            >>> ds = ray.data.read_mcap("/path/to/mcap/dir")
            >>> ds = ds.filter(col("topic") == "/camera/image")  # File-level filtering!
            >>> # Files without /camera/image topic are excluded before reading

        Args:
            predicate_expr: Predicate expression to apply.

        Returns:
            New MCAPDatasource instance with applied predicate. The new instance
            will filter files at the metadata level during path expansion.
        """
        # Extract and merge filters
        topics, time_range, message_types = self._extract_filters_from_predicate(
            predicate_expr
        )

        new_topics = self._merge_filters(self._topics, topics)
        new_message_types = self._merge_filters(self._message_types, message_types)
        new_time_range = self._merge_time_ranges(self._time_range, time_range)
        # Combine predicates using AND (matching default mixin behavior)
        # _predicate_expr is initialized by _DatasourcePredicatePushdownMixin.__init__()
        new_predicate_expr = (
            self._predicate_expr & predicate_expr
            if self._predicate_expr is not None
            else predicate_expr
        )

        # Create new datasource with updated filters
        # NOTE: Creating a new instance is critical - it triggers path re-expansion
        # via super().__init__() -> meta_provider.expand_paths(), which applies
        # file-level filtering based on metadata.
        filesystem = (
            self._filesystem.unwrap()
            if hasattr(self._filesystem, "unwrap")
            else self._filesystem
        )

        clone = MCAPDatasource(
            paths=self._source_paths,
            topics=new_topics,
            time_range=new_time_range,
            message_types=new_message_types,
            include_metadata=self._include_metadata,
            filesystem=filesystem,
            meta_provider=MCAPFileMetadataProvider(
                topics=new_topics,
                time_range=new_time_range,
                message_types=new_message_types,
            ),
            partition_filter=self._partition_filter,
            partitioning=self._partitioning,
            ignore_missing_paths=self._ignore_missing_paths,
            shuffle=None,
            include_paths=self._include_paths,
            file_extensions=self._FILE_EXTENSIONS,
        )
        clone._predicate_expr = new_predicate_expr

        return clone

    def _merge_filters(
        self, existing: Optional[Set[str]], new: Optional[Set[str]]
    ) -> Optional[Set[str]]:
        """Merge two filter sets by taking their intersection."""
        if not new:
            return existing
        if not existing:
            return new
        return existing.intersection(new)

    def _extract_filters_from_predicate(
        self, predicate_expr: Expr
    ) -> Tuple[Optional[Set[str]], Optional[TimeRange], Optional[Set[str]]]:
        """Extract MCAP-specific filters from a predicate expression.

        Supports:
        - Topic filters: col("topic") == value or col("topic").is_in([...])
        - Time range filters: col("log_time") > start and col("log_time") < end
        - Message type filters: col("schema_name") == value

        Args:
            predicate_expr: Predicate expression to extract filters from.

        Returns:
            Tuple of (topics, time_range, message_types) extracted from predicate.
        """
        from ray.data.expressions import BinaryExpr, ColumnExpr, LiteralExpr, Operation

        topics = None
        time_range_start = None
        time_range_end = None
        message_types = None

        def add_to_set(
            current: Optional[Set], new_values: Union[Any, Set, List]
        ) -> Set:
            """Helper to add values to a set."""
            if isinstance(new_values, (list, tuple)):
                new_values = set(new_values)
            elif not isinstance(new_values, set):
                new_values = {new_values}
            return new_values if current is None else current | new_values

        def extract_value_from_literal(expr: Expr) -> Optional[Union[Any, List[Any]]]:
            """Extract value from a literal expression or list."""
            if isinstance(expr, LiteralExpr):
                return expr.value
            # Handle list literals (for is_in operations)
            if hasattr(expr, "value") and isinstance(expr.value, (list, tuple)):
                return expr.value
            return None

        def get_column_name(expr: Expr) -> Optional[str]:
            """Recursively extract column name from an expression.

            This function traverses nested BinaryExpr objects to find ColumnExpr
            nodes. For example, it can extract "topic" from (col("topic") == value).

            Args:
                expr: Expression to extract column name from.

            Returns:
                Column name if found, None otherwise.
            """
            if isinstance(expr, ColumnExpr):
                return expr.name
            if isinstance(expr, BinaryExpr):
                # Recursively check left side (columns are typically on the left)
                left_col = get_column_name(expr.left)
                if left_col:
                    return left_col
                # Check right side as fallback
                return get_column_name(expr.right)
            return None

        def can_pushdown_or(left_expr: Expr, right_expr: Expr) -> bool:
            """Check if an OR expression can be pushed down.

            An OR expression can be pushed down if both sides reference the same
            column with equality comparisons. For example:
            - (col("topic") == "/a") | (col("topic") == "/b") -> True
            - (col("topic") == "/a") | (col("log_time") > 1000) -> False

            Args:
                left_expr: Left side of OR expression.
                right_expr: Right side of OR expression.

            Returns:
                True if both sides reference the same column, False otherwise.
            """
            left_col = get_column_name(left_expr)
            right_col = get_column_name(right_expr)
            return left_col is not None and left_col == right_col

        def visit_expr(expr: Expr):
            nonlocal topics, time_range_start, time_range_end, message_types

            if not isinstance(expr, BinaryExpr):
                return

            # Handle AND operations - recursively visit both sides
            if expr.op == Operation.AND:
                visit_expr(expr.left)
                visit_expr(expr.right)
                return

            # Handle OR operations
            if expr.op == Operation.OR:
                # Optimize OR expressions where both sides reference the same column
                # For example: (col("topic") == "/a") | (col("topic") == "/b")
                # This can be pushed down as topics={"/a", "/b"}
                if can_pushdown_or(expr.left, expr.right):
                    # Both sides reference the same column, extract values from both
                    visit_expr(expr.left)
                    visit_expr(expr.right)
                else:
                    # Different columns or complex expressions - recursively visit both sides
                    # This will still extract filters, but may not be optimal
                    visit_expr(expr.left)
                    visit_expr(expr.right)
                return

            # Process binary operations with column on left
            if isinstance(expr.left, ColumnExpr):
                col_name = expr.left.name

                # Handle is_in() operations
                if expr.op == Operation.IN:
                    value = extract_value_from_literal(expr.right)
                    if value is not None:
                        if col_name == "topic":
                            topics = add_to_set(topics, value)
                        elif col_name == "schema_name":
                            message_types = add_to_set(message_types, value)
                    return

                # Handle equality operations
                if expr.op == Operation.EQ and isinstance(expr.right, LiteralExpr):
                    value = expr.right.value
                    if col_name == "topic":
                        topics = add_to_set(topics, {value})
                    elif col_name == "schema_name":
                        message_types = add_to_set(message_types, {value})
                    return

                # Handle time range operations
                if col_name == "log_time" and isinstance(expr.right, LiteralExpr):
                    value = expr.right.value
                    if expr.op in (Operation.GT, Operation.GE):
                        # Update start time (take maximum for GT/GE)
                        if time_range_start is None or value > time_range_start:
                            time_range_start = value
                    elif expr.op in (Operation.LT, Operation.LE):
                        # Update end time (take minimum for LT/LE)
                        if time_range_end is None or value < time_range_end:
                            time_range_end = value

            # Recursively visit children for other cases
            visit_expr(expr.left)
            visit_expr(expr.right)

        visit_expr(predicate_expr)

        # Build time range from extracted bounds
        time_range = None
        if time_range_start is not None or time_range_end is not None:
            start = time_range_start if time_range_start is not None else 0
            end = time_range_end if time_range_end is not None else (2**63 - 1)
            if start < end:
                time_range = TimeRange(start_time=start, end_time=end)

        return topics, time_range, message_types

    def _merge_time_ranges(
        self, existing: Optional[TimeRange], new: Optional[TimeRange]
    ) -> Optional[TimeRange]:
        """Merge two time ranges by taking their intersection."""
        if not new:
            return existing
        if not existing:
            return new

        start_time = max(existing.start_time, new.start_time)
        end_time = min(existing.end_time, new.end_time)

        # If no overlap, return range that matches nothing
        if start_time >= end_time:
            return TimeRange(start_time=start_time, end_time=start_time + 1)

        return TimeRange(start_time=start_time, end_time=end_time)

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
        from mcap.reader import make_reader  # noqa: F401

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
            block = builder.build()

            # Apply any remaining predicate expression that couldn't be pushed down
            # This handles complex expressions that we couldn't extract into MCAP filters
            if self._predicate_expr is not None:
                try:
                    # Convert predicate to PyArrow expression and filter the block
                    filter_expr = self._predicate_expr.to_pyarrow()
                    block_accessor = BlockAccessor.for_block(block)
                    # Convert to Arrow table for filtering
                    table = block_accessor.to_arrow()
                    filtered_table = table.filter(filter_expr)
                    block = block_accessor.from_arrow(filtered_table)
                except Exception as e:
                    # If predicate conversion fails, log and continue without filtering
                    # This can happen with unsupported expressions
                    logger.debug(
                        f"Could not apply predicate expression to MCAP block: {e}"
                    )

            yield block

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
        if self._include_paths:
            message_data["path"] = path

        return message_data

    def get_name(self) -> str:
        """Return a human-readable name for this datasource."""
        return "MCAP"
