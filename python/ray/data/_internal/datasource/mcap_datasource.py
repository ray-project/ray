"""MCAP (Message Capture) datasource for Ray Data.

MCAP is a standardized format for storing timestamped messages from robotics and
autonomous systems, commonly used for sensor data, control commands, and other
time-series data.

This datasource uses the mcap Python library (https://github.com/foxglove/mcap)
for reading MCAP files. The library provides efficient footer-only reads for
metadata extraction and supports predicate pushdown for file-level filtering.

See https://mcap.dev/ for more information about the MCAP format.
"""

import json
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Set, Tuple, Union

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.planner.plan_expression.expression_visitors import (
    get_column_references,
)
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

# Maximum safe time value for nanoseconds (2^63 - 1)
_MAX_SAFE_TIME = (2**63) - 1

# Column names that support predicate pushdown
_FILTERABLE_COLUMN_TOPIC = "topic"
_FILTERABLE_COLUMN_LOG_TIME = "log_time"
_FILTERABLE_COLUMN_SCHEMA_NAME = "schema_name"
_FILTERABLE_COLUMNS = {
    _FILTERABLE_COLUMN_TOPIC,
    _FILTERABLE_COLUMN_LOG_TIME,
    _FILTERABLE_COLUMN_SCHEMA_NAME,
}

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
        if self.start_time > _MAX_SAFE_TIME or self.end_time > _MAX_SAFE_TIME:
            raise ValueError(
                f"time values exceed maximum safe value ({_MAX_SAFE_TIME}), "
                f"got start_time={self.start_time}, end_time={self.end_time}"
            )

    def overlaps(self, start: Optional[int], end: Optional[int]) -> bool:
        """Check if this time range overlaps with another range.

        Args:
            start: Start time of other range (inclusive), or None for unbounded start.
            end: End time of other range (exclusive), or None for unbounded end.

        Returns:
            True if ranges overlap, False otherwise.
        """
        if start is None and end is None:
            return True
        if start is None:
            return self.start_time < end
        if end is None:
            return self.end_time > start
        return self.start_time < end and self.end_time > start

    def intersection(self, other: "TimeRange") -> Optional["TimeRange"]:
        """Compute intersection of two time ranges.

        Args:
            other: Other time range to intersect with.

        Returns:
            Intersection time range, or None if ranges don't overlap.
        """
        start_time = max(self.start_time, other.start_time)
        end_time = min(self.end_time, other.end_time)
        if start_time >= end_time:
            return None
        try:
            return TimeRange(start_time=start_time, end_time=end_time)
        except ValueError:
            return None

    @staticmethod
    def from_bounds(
        start: Optional[int], end: Optional[int]
    ) -> Optional["TimeRange"]:
        """Create TimeRange from optional start and end bounds.

        Args:
            start: Start time (inclusive), or None for unbounded start.
            end: End time (exclusive), or None for unbounded end.

        Returns:
            TimeRange if valid, None if both bounds are None or invalid.
        """
        if start is None and end is None:
            return None
        start_val = start if start is not None else 0
        end_val = end if end is not None else _MAX_SAFE_TIME
        if start_val < end_val:
            try:
                return TimeRange(start_time=start_val, end_time=end_val)
            except ValueError:
                return None
        return None


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
    """File metadata provider that extracts metadata from MCAP file summaries for file-level filtering."""

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
        self._topics = MCAPDatasource._normalize_filter_set(topics, "topics")
        self._time_range = time_range
        self._message_types = MCAPDatasource._normalize_filter_set(message_types, "message_types")

    def expand_paths(
        self,
        paths: List[str],
        filesystem: "RetryingPyFileSystem",
        partitioning: Optional["Partitioning"] = None,
        ignore_missing_paths: bool = False,
    ) -> Iterator[Tuple[str, int]]:
        """Expand paths and filter based on MCAP file metadata (file-level predicate pushdown)."""
        from ray.data.datasource.file_meta_provider import _expand_paths

        if not paths:
            return

        if filesystem is None:
            raise ValueError("filesystem cannot be None")

        for path, file_size in _expand_paths(
            paths, filesystem, partitioning, ignore_missing_paths
        ):
            if not path or not isinstance(path, str):
                continue
            # Check for .mcap extension (case-insensitive)
            if not path.lower().endswith(".mcap"):
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
        """Read metadata from MCAP file summary (footer-only, efficient read).

        Uses mcap.reader.make_reader() to read only the file footer, which contains
        the summary with channels, schemas, and statistics. This avoids reading the
        entire file for metadata extraction.

        See https://github.com/foxglove/mcap/tree/main/python/mcap for MCAP library docs.
        """
        from mcap.reader import make_reader

        topics = set()
        message_types = set()
        start_time = None
        end_time = None
        num_messages = 0

        try:
            if file_size is None:
                file_size = self._get_file_size(path, filesystem)

            if file_size is not None and file_size <= 0:
                return self._empty_metadata(path, file_size)

            # Read summary within context manager to ensure file stays open
            with filesystem.open_input_stream(path) as f:
                reader = make_reader(f)
                # MCAP reader.get_summary() returns Summary object or None
                # Summary provides direct access to channels, schemas, and statistics
                summary = reader.get_summary()

                if summary:
                    topics, message_types = self._extract_from_summary(summary)
                    num_messages, start_time, end_time = self._extract_statistics(summary.statistics)

        except Exception as e:
            logger.warning(f"Failed to read MCAP metadata from {path}: {e}")
            return self._empty_metadata(path, file_size)

        # Ensure file_size is non-negative
        final_file_size = file_size if file_size is not None and file_size >= 0 else 0
        # Ensure num_messages is non-negative
        final_num_messages = num_messages if num_messages >= 0 else 0

        return MCAPFileMetadata(
            path=path,
            file_size=final_file_size,
            num_messages=final_num_messages,
            topics=topics,
            message_types=message_types,
            start_time=start_time,
            end_time=end_time,
        )

    def _extract_from_summary(self, summary) -> Tuple[Set[str], Set[str]]:
        """Extract topics and message types from MCAP summary.

        Uses MCAP's Summary object which provides direct access to channels and schemas.
        Also leverages Statistics.channel_message_counts for accurate topic filtering.
        """
        topics = set()
        message_types = set()

        # MCAP Summary provides channels and schemas as dictionaries (may be empty or None)
        channels = summary.channels if summary.channels is not None else {}
        schemas = summary.schemas if summary.schemas is not None else {}

        if not channels:
            return topics, message_types

        # Use Statistics.channel_message_counts to only include topics with messages
        # This ensures we only include channels that actually have messages
        stat = summary.statistics
        channel_message_counts = (
            stat.channel_message_counts if stat and hasattr(stat, "channel_message_counts") else None
        )

        for channel_id, channel in channels.items():
            # Only include topics that have messages (if channel_message_counts available)
            if channel_message_counts and channel_id not in channel_message_counts:
                continue
            # Extract topic (MCAP Channel always has topic attribute)
            # Empty string is a valid topic name, so check for None explicitly
            if channel.topic is not None:
                topics.add(channel.topic)
            # Extract schema name (message type) from channel's schema_id
            if channel.schema_id and schemas:
                schema = schemas.get(channel.schema_id)
                # Empty string is a valid schema name, so check for None explicitly
                if schema and schema.name is not None:
                    message_types.add(schema.name)
        return topics, message_types

    def _extract_statistics(self, statistics) -> Tuple[int, Optional[int], Optional[int]]:
        """Extract statistics from MCAP Statistics object.

        MCAP Statistics provides message_count, message_start_time, message_end_time,
        and channel_message_counts for accurate per-channel filtering.
        """
        if not statistics:
            return 0, None, None
        # MCAP Statistics object provides these attributes directly
        message_count = statistics.message_count or 0
        # Ensure non-negative message count
        if message_count < 0:
            message_count = 0
        start_time = statistics.message_start_time
        end_time = statistics.message_end_time
        # Validate time values are non-negative
        if start_time is not None and start_time < 0:
            start_time = None
        if end_time is not None and end_time < 0:
            end_time = None
        return (message_count, start_time, end_time)

    def _get_file_size(
        self, path: str, filesystem: "RetryingPyFileSystem"
    ) -> Optional[int]:
        """Get file size from filesystem."""
        try:
            file_info = filesystem.get_file_info(path)
            if file_info.size is not None and file_info.size >= 0:
                return file_info.size
        except Exception:
            pass
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
        """Check if file should be included based on filters. Includes conservatively if no metadata."""
        # If no metadata available (empty sets and None times), include conservatively
        has_metadata = (
            (metadata.topics and len(metadata.topics) > 0)
            or (metadata.message_types and len(metadata.message_types) > 0)
            or metadata.start_time is not None
        )
        if not has_metadata:
            return True

        # Check topic filter (empty set excludes all)
        if self._topics is not None:
            if not self._topics:
                # Empty filter set means exclude all
                return False
            if not metadata.topics or not metadata.topics.intersection(self._topics):
                return False

        # Check message type filter (empty set excludes all)
        if self._message_types is not None:
            if not self._message_types:
                # Empty filter set means exclude all
                return False
            if not metadata.message_types or not metadata.message_types.intersection(self._message_types):
                return False

        # Check time range filter
        if self._time_range and not self._time_range.overlaps(
            metadata.start_time, metadata.end_time
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
        if not paths:
            return BlockMetadata(
                num_rows=0,
                size_bytes=0,
                input_files=[],
                exec_stats=None,
            )

        # Validate file_sizes length matches paths length
        if len(file_sizes) != len(paths):
            logger.warning(
                f"file_sizes length ({len(file_sizes)}) doesn't match paths length ({len(paths)})"
            )

        # Filter out None and negative values, sum remaining
        valid_sizes = [s for s in file_sizes if s is not None and s >= 0]
        if None in file_sizes or len(valid_sizes) != len(file_sizes):
            size_bytes = None
        else:
            size_bytes = int(sum(valid_sizes))

        return BlockMetadata(
            num_rows=None,  # Computed during execution
            size_bytes=size_bytes,
            input_files=paths,
            exec_stats=None,
        )


@DeveloperAPI
class MCAPDatasource(FileBasedDatasource):
    """MCAP (Message Capture) datasource for Ray Data with predicate pushdown.

    MCAP is a standardized format for storing timestamped messages from robotics and
    autonomous systems. See https://mcap.dev/ for more information.
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
        # Normalize and validate paths
        paths = self._normalize_and_validate_paths(paths)

        # Normalize filter sets (empty sets allowed after predicate pushdown)
        topics = self._normalize_filter_set(topics, "topics")
        message_types = self._normalize_filter_set(message_types, "message_types")

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

        self._topics = topics
        self._message_types = message_types
        self._time_range = time_range
        self._include_metadata = include_metadata
        self._predicate_expr = None

    @staticmethod
    def _normalize_and_validate_paths(
        paths: Union[str, List[str], Tuple[str, ...]]
    ) -> List[str]:
        """Normalize and validate paths input."""
        if isinstance(paths, str):
            paths = [paths]
        elif not isinstance(paths, (list, tuple)):
            raise TypeError(f"paths must be str, list, or tuple, got {type(paths)}")

        if not paths:
            raise ValueError("paths cannot be empty")

        validated_paths = []
        seen_paths = set()
        for i, p in enumerate(paths):
            if not isinstance(p, str):
                raise TypeError(f"All paths must be strings, got {type(p)} at index {i}")
            p = p.strip()
            if not p:
                raise ValueError(f"Path at index {i} cannot be empty or whitespace")
            # Check for duplicate paths
            if p in seen_paths:
                logger.debug(f"Duplicate path at index {i}: {p}")
            seen_paths.add(p)
            validated_paths.append(p)
        return validated_paths

    @staticmethod
    def _normalize_filter_set(
        value: Optional[Union[List[str], Set[str], Tuple[str, ...]]], name: str
    ) -> Optional[Set[str]]:
        """Normalize filter set input to a set or None."""
        if value is None:
            return None
        if isinstance(value, set):
            # Filter out None values and empty strings if desired
            return {v for v in value if v is not None}
        if isinstance(value, (list, tuple)):
            # Filter out None values
            return {v for v in value if v is not None}
        raise TypeError(f"{name} must be list, tuple, or set, got {type(value)}")

    def supports_predicate_pushdown(self) -> bool:
        """Whether this datasource supports predicate pushdown."""
        return True

    def get_current_predicate(self) -> Optional[Expr]:
        """Get the current predicate expression."""
        return self._predicate_expr

    def apply_predicate(self, predicate_expr: Expr) -> "MCAPDatasource":
        """Apply predicate expression with file-level filtering via metadata.

        Extracts pushable filters (topics, time_range, message_types) from the predicate
        and merges them with existing filters. The metadata provider filters files during
        path expansion, and any remaining predicate parts are applied at block level.

        Returns:
            Shallow copy of datasource with updated filters if any filters were extracted,
            otherwise returns self to keep the Filter operator in the plan.
        """
        import copy

        if predicate_expr is None:
            return self

        # Extract pushable filters from predicate
        topics, time_range, message_types = self._extract_filters_from_predicate(
            predicate_expr
        )

        # Check if we extracted any filters
        has_extracted_filters = (
            topics is not None
            or time_range is not None
            or message_types is not None
        )

        # If no filters extracted, return self to keep Filter operator in plan
        if not has_extracted_filters:
            return self

        # Merge with existing filters
        new_topics = self._merge_filters(self._topics, topics)
        new_message_types = self._merge_filters(self._message_types, message_types)
        new_time_range = self._merge_time_ranges(self._time_range, time_range)

        # Create shallow copy and update filter attributes
        clone = copy.copy(self)
        if clone is None:
            raise RuntimeError("Failed to create datasource clone")

        clone._topics = new_topics
        clone._message_types = new_message_types
        clone._time_range = new_time_range

        # Update metadata provider with new filters
        clone._meta_provider = MCAPFileMetadataProvider(
            topics=new_topics,
            time_range=new_time_range,
            message_types=new_message_types,
        )

        # Store remaining predicate for block-level filtering
        try:
            clone._predicate_expr = (
                self._predicate_expr & predicate_expr
                if self._predicate_expr is not None
                else predicate_expr
            )
        except Exception as e:
            raise RuntimeError(f"Failed to combine predicate expressions: {e}") from e

        return clone

    def _merge_filters(
        self, existing: Optional[Set[str]], new: Optional[Set[str]]
    ) -> Optional[Set[str]]:
        """Merge two filter sets by taking their intersection.

        Args:
            existing: Existing filter set, or None for no filter.
            new: New filter set, or None for no filter.

        Returns:
            Intersection of filters, or None if both are None.
            Empty set means "filter to nothing" (no matches).
        """
        if new is None:
            return existing
        if existing is None:
            return new
        return existing.intersection(new)  # Empty intersection is valid (no matching data)

    def _extract_filters_from_predicate(
        self, predicate_expr: Expr
    ) -> Tuple[Optional[Set[str]], Optional[TimeRange], Optional[Set[str]]]:
        """Extract simple MCAP filters from predicate expression.

        Only extracts simple filters that can be pushed down to file-level:
        - EQ/IN operations on topics and message_types
        - Simple comparisons (GT, GE, LT, LE) on log_time
        - Handles AND chains only (OR and complex expressions handled at block level)

        Returns:
            Tuple of (topics, time_range, message_types) that can be pushed down.
        """
        from ray.data.expressions import BinaryExpr, ColumnExpr, LiteralExpr, Operation

        # Check if predicate references any filterable columns
        referenced_cols = set(get_column_references(predicate_expr))
        filterable_cols = referenced_cols & _FILTERABLE_COLUMNS
        if not filterable_cols:
            return None, None, None

        topics = None
        time_range_start = None
        time_range_end = None
        message_types = None

        def extract_simple_filter(expr: Expr):
            """Extract simple filter from a single binary expression."""
            nonlocal topics, time_range_start, time_range_end, message_types

            if not isinstance(expr, BinaryExpr):
                return

            # Handle commutative expressions: normalize so column is on left
            left, right = expr.left, expr.right
            op = expr.op

            # Swap if literal is on left (e.g., 1000 < col("log_time"))
            if isinstance(left, LiteralExpr) and isinstance(right, ColumnExpr):
                if op == Operation.LT:
                    left, right, op = right, left, Operation.GT
                elif op == Operation.LE:
                    left, right, op = right, left, Operation.GE
                elif op == Operation.GT:
                    left, right, op = right, left, Operation.LT
                elif op == Operation.GE:
                    left, right, op = right, left, Operation.LE
                elif op in (Operation.EQ, Operation.NE):
                    left, right, op = right, left, op

            if not isinstance(left, ColumnExpr):
                return

            col_name = left.name

            # Extract topics/message_types from EQ or IN
            if col_name in (_FILTERABLE_COLUMN_TOPIC, _FILTERABLE_COLUMN_SCHEMA_NAME):
                if op == Operation.EQ and isinstance(right, LiteralExpr):
                    value = {right.value}
                elif op == Operation.IN and isinstance(right, LiteralExpr):
                    # Handle IN operation: value must be iterable (list/tuple/set), not string
                    if isinstance(right.value, (list, tuple, set)):
                        value = set(right.value)
                    elif isinstance(right.value, str):
                        # String IN means single value, not set of characters
                        value = {right.value}
                    else:
                        return
                else:
                    return

                if col_name == _FILTERABLE_COLUMN_TOPIC:
                    topics = value if topics is None else topics & value
                else:
                    message_types = value if message_types is None else message_types & value
                return

            # Extract time range from comparisons
            if col_name == _FILTERABLE_COLUMN_LOG_TIME and isinstance(right, LiteralExpr):
                value = right.value
                if not isinstance(value, (int, float)):
                    return
                value = int(value)

                # Validate value is non-negative and within safe range
                if value < 0:
                    return
                if value >= _MAX_SAFE_TIME:
                    return

                if op == Operation.GT:
                    new_start = value + 1
                    if new_start > _MAX_SAFE_TIME:
                        return
                    time_range_start = max(time_range_start, new_start) if time_range_start is not None else new_start
                elif op == Operation.GE:
                    time_range_start = max(time_range_start, value) if time_range_start is not None else value
                elif op == Operation.LT:
                    time_range_end = min(time_range_end, value) if time_range_end is not None else value
                elif op == Operation.LE:
                    new_end = value + 1
                    if new_end > _MAX_SAFE_TIME:
                        return
                    time_range_end = min(time_range_end, new_end) if time_range_end is not None else new_end

        def visit_and_chain(expr: Expr):
            """Recursively visit AND chains to extract simple filters."""
            if isinstance(expr, BinaryExpr) and expr.op == Operation.AND:
                visit_and_chain(expr.left)
                visit_and_chain(expr.right)
            else:
                extract_simple_filter(expr)

        visit_and_chain(predicate_expr)

        # Validate extracted time bounds before creating TimeRange
        if time_range_start is not None and time_range_end is not None:
            if time_range_start >= time_range_end:
                # Invalid range, don't push down time filter
                time_range_start = None
                time_range_end = None

        # Build TimeRange from extracted bounds
        time_range = TimeRange.from_bounds(time_range_start, time_range_end)

        return topics, time_range, message_types

    def _merge_time_ranges(
        self, existing: Optional[TimeRange], new: Optional[TimeRange]
    ) -> Optional[TimeRange]:
        """Merge two time ranges by taking their intersection.

        Args:
            existing: Existing time range, or None for no filter.
            new: New time range, or None for no filter.

        Returns:
            Intersection of time ranges, or None if both are None or don't overlap.
        """
        if new is None:
            return existing
        if existing is None:
            return new
        return existing.intersection(new)

    def _read_stream(self, f: "pyarrow.NativeFile", path: str) -> Iterator[Block]:
        """Read MCAP file and yield blocks with predicate pushdown.

        Uses mcap.reader.make_reader() to create a reader, then iterates messages
        with pushdown filters applied. Supports both decoded messages (via
        iter_decoded_messages) and raw messages (via iter_messages) as fallback.

        See https://github.com/foxglove/mcap/tree/main/python/mcap for MCAP library docs.
        """
        from mcap.reader import make_reader

        if f is None:
            raise ValueError(f"File handle is None for {path}")
        if not path:
            raise ValueError("Path cannot be empty")

        try:
            reader = make_reader(f)
        except Exception as e:
            raise RuntimeError(f"Failed to create MCAP reader for {path}: {e}") from e

        if reader is None:
            raise RuntimeError(f"MCAP reader is None for {path}")

        # Early return if topics filter is empty set
        if self._topics is not None and not self._topics:
            return

        # Get message iterator with pushdown filters
        decoded_messages = self._get_message_iterator(reader, path)

        # Build block from messages
        block = self._build_block_from_messages(decoded_messages, path)
        if block is None:
            return

        # Apply remaining predicate if needed
        block = self._apply_block_predicate(block, path)
        if block is None:
            return

        yield block

    def _get_message_iterator(self, reader, path: str):
        """Get message iterator with pushdown filters applied."""
        if reader is None:
            raise ValueError(f"Reader is None for {path}")

        # Empty set means no topics match, pass empty list to exclude all
        # None means no filter, pass None to include all
        topics_list = list(self._topics) if self._topics is not None else None
        start_time = self._time_range.start_time if self._time_range else None
        end_time = self._time_range.end_time if self._time_range else None

        # Validate time values are non-negative
        if start_time is not None and start_time < 0:
            raise ValueError(f"Invalid start_time {start_time} for {path}: must be non-negative")
        if end_time is not None and end_time < 0:
            raise ValueError(f"Invalid end_time {end_time} for {path}: must be non-negative")

        # Common iterator arguments
        iterator_kwargs = {
            "topics": topics_list,
            "start_time": start_time,
            "end_time": end_time,
            "log_time_order": True,
            "reverse": False,
        }

        try:
            # Try decoded messages first (automatic decoding)
            return reader.iter_decoded_messages(**iterator_kwargs)
        except Exception as decode_error:
            # Fall back to raw messages if decoding fails
            logger.debug(
                f"Decoded messages not available for {path}, using raw messages: {decode_error}"
            )
            return reader.iter_messages(**iterator_kwargs)

    def _build_block_from_messages(self, decoded_messages, path: str) -> Optional[Block]:
        """Build block from MCAP messages."""
        builder = DelegatingBlockBuilder()

        for item in decoded_messages:
            try:
                schema, channel, message, decoded_data = self._unpack_message_item(item)
            except (ValueError, AttributeError, IndexError) as e:
                logger.warning(
                    f"Failed to unpack message item from {path}: {e}. Skipping message."
                )
                continue

            # Skip if required fields are None
            if channel is None or message is None:
                logger.debug(f"Skipping message with None channel or message in {path}")
                continue

            if not self._should_include_message(schema, channel, message):
                continue

            try:
                message_dict = self._message_to_dict(
                    schema, channel, message, path, decoded_data=decoded_data
                )
                builder.add(message_dict)
            except Exception as e:
                logger.warning(
                    f"Failed to convert MCAP message to dict from {path}: {e}. Skipping message."
                )
                continue

        if builder.num_rows() == 0:
            logger.debug(f"No messages matched filters in {path}")
            return None

        return builder.build()

    @staticmethod
    def _unpack_message_item(item) -> Tuple["Schema", "Channel", "Message", Optional[Any]]:
        """Unpack message item from iterator (handles both decoded and raw messages)."""
        if hasattr(item, "decoded_message"):
            # DecodedMessageTuple from iter_decoded_messages
            schema = getattr(item, "schema", None)
            channel = getattr(item, "channel", None)
            message = getattr(item, "message", None)
            decoded_data = getattr(item, "decoded_message", None)
            return schema, channel, message, decoded_data
        else:
            # Tuple from iter_messages
            if not isinstance(item, (tuple, list)) or len(item) < 3:
                raise ValueError(f"Invalid message item format: {type(item)}")
            schema, channel, message = item[0], item[1], item[2]
            return schema, channel, message, None

    def _apply_block_predicate(self, block: Block, path: str) -> Optional[Block]:
        """Apply remaining predicate expression to block."""
        if self._predicate_expr is None:
            return block

        if block is None:
            return None

        try:
            filter_expr = self._predicate_expr.to_pyarrow()
            if filter_expr is None:
                return block

            block_accessor = BlockAccessor.for_block(block)
            arrow_table = block_accessor.to_arrow()
            if arrow_table is None:
                return block

            filtered_table = arrow_table.filter(filter_expr)
            if filtered_table is None:
                return block

            # num_rows is an attribute, not a method
            if filtered_table.num_rows == 0:
                return None
            return block_accessor.from_arrow(filtered_table)
        except Exception as e:
            raise RuntimeError(
                f"Failed to apply predicate expression to MCAP block from {path}: {e}"
            ) from e

    @staticmethod
    def _decode_message_data(
        channel: "Channel", message: "Message", path: str, decoded_data: Optional[Any]
    ) -> Any:
        """Decode message data, using pre-decoded data if available."""
        if decoded_data is not None:
            return decoded_data

        if channel is None:
            raise ValueError(f"Channel is None for message in {path}")
        if message is None:
            raise ValueError(f"Message is None in {path}")

        # Fallback: manually decode JSON if needed
        if message.data is None:
            return b""

        final_data = message.data
        # Check message_encoding is not None before comparing
        if (
            channel.message_encoding is not None
            and channel.message_encoding == "json"
            and isinstance(message.data, bytes)
            and len(message.data) > 0
        ):
            try:
                final_data = json.loads(message.data.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.debug(
                    f"Failed to decode JSON message from {path}, using raw bytes: {e}"
                )
                # Keep original bytes on decode failure
                final_data = message.data
        return final_data

    def _should_include_message(
        self, schema: "Schema", channel: "Channel", message: "Message"
    ) -> bool:
        """Check if message should be included (applies message_types filter)."""
        if self._message_types is None:
            return True
        # Empty set excludes all messages
        if not self._message_types:
            return False
        # Check schema name matches filter (empty string is valid schema name)
        return schema and schema.name is not None and schema.name in self._message_types

    def _message_to_dict(
        self,
        schema: "Schema",
        channel: "Channel",
        message: "Message",
        path: str,
        decoded_data: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Convert MCAP message to dictionary format.

        Args:
            schema: MCAP schema for the message.
            channel: MCAP channel for the message.
            message: MCAP message.
            path: Path to the MCAP file.
            decoded_data: Pre-decoded message data (from iter_decoded_messages).
                If None, will attempt to decode manually.
        """
        if channel is None:
            raise ValueError(f"Channel is None for message in {path}")
        if message is None:
            raise ValueError(f"Message is None in {path}")

        # Decode message data
        final_data = self._decode_message_data(channel, message, path, decoded_data)

        # MCAP Message and Channel objects provide these attributes directly
        # Use 0 as default for numeric fields, empty string for string fields
        message_data = {
            "data": final_data,
            "topic": channel.topic if channel.topic is not None else "",
            "log_time": message.log_time if message.log_time is not None else 0,
            "publish_time": message.publish_time if message.publish_time is not None else 0,
            "sequence": message.sequence if message.sequence is not None else 0,
        }

        if self._include_metadata:
            # MCAP Schema provides name, encoding, and data attributes directly
            # MCAP Channel provides metadata attribute (dict) if available
            message_data.update(
                {
                    "channel_id": message.channel_id if message.channel_id is not None else 0,
                    "message_encoding": channel.message_encoding if channel.message_encoding is not None else "",
                    "schema_name": schema.name if schema and schema.name is not None else None,
                    "schema_encoding": schema.encoding if schema and schema.encoding is not None else None,
                    "schema_data": schema.data if schema and schema.data is not None else None,
                    "channel_metadata": channel.metadata if channel.metadata is not None else None,
                }
            )

        # Note: FileBasedDatasource automatically adds "path" column when include_paths=True
        # Do not add it here to avoid duplicate columns

        return message_data

    def get_name(self) -> str:
        """Return a human-readable name for this datasource."""
        return "MCAP"
