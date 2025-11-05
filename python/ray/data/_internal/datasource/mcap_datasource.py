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
        """Expand paths and filter based on MCAP file metadata (file-level predicate pushdown)."""
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
        """Read metadata from MCAP file summary (footer-only, efficient read)."""
        from mcap.reader import make_reader

        topics = set()
        message_types = set()
        start_time = None
        end_time = None
        num_messages = 0

        try:
            if file_size is None:
                file_size = self._get_file_size(path, filesystem)

            with filesystem.open_input_stream(path) as f:
                reader = make_reader(f)
                try:
                    summary = reader.get_summary()
                except Exception:
                    summary = None

            if summary is not None:
                topics, message_types = self._extract_from_summary(summary)
                num_messages, start_time, end_time = self._extract_statistics(summary)

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
                if (
                    hasattr(channel, "schema_id")
                    and channel.schema_id
                    and hasattr(summary, "schemas")
                    and summary.schemas
                ):
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
        """Check if file should be included based on filters. Includes conservatively if no metadata."""
        has_metadata = bool(
            metadata.topics or metadata.message_types or metadata.start_time
        )
        if not has_metadata:
            return True

        if self._topics and not metadata.topics.intersection(self._topics):
            return False
        if self._message_types and not metadata.message_types.intersection(
            self._message_types
        ):
            return False
        if self._time_range and metadata.start_time and metadata.end_time:
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
        self._predicate_expr = None

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
        """Apply predicate expression with file-level filtering via metadata."""
        topics, time_range, message_types = self._extract_filters_from_predicate(
            predicate_expr
        )

        new_topics = self._merge_filters(self._topics, topics)
        new_message_types = self._merge_filters(self._message_types, message_types)
        new_time_range = self._merge_time_ranges(self._time_range, time_range)
        new_predicate_expr = (
            self._predicate_expr & predicate_expr
            if self._predicate_expr
            else predicate_expr
        )

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
        """Extract MCAP filters from predicate expression.

        Handles topics, time_range, message_types. Correctly handles AND/OR predicates
        and commutative expressions (e.g., 1000 < col("log_time")).
        """
        from ray.data.expressions import BinaryExpr, ColumnExpr, LiteralExpr, Operation

        topics = None
        time_range_start = None
        time_range_end = None
        message_types = None

        def normalize_to_set(values: Union[Any, Set, List]) -> Set:
            """Normalize values to a set."""
            if isinstance(values, (list, tuple)):
                return set(values)
            return {values} if not isinstance(values, set) else values

        def update_set(
            current: Optional[Set], new_values: Union[Any, Set, List], is_and: bool
        ) -> Set:
            """Update set with intersection (AND) or union (OR)."""
            new_set = normalize_to_set(new_values)
            return (
                new_set
                if current is None
                else (current.intersection(new_set) if is_and else current | new_set)
            )

        def extract_value(expr: Expr) -> Optional[Union[Any, List[Any]]]:
            """Extract value from literal expression."""
            if isinstance(expr, LiteralExpr):
                return expr.value
            if hasattr(expr, "value") and isinstance(expr.value, (list, tuple)):
                return expr.value
            return None

        def get_column_name(expr: Expr) -> Optional[str]:
            """Extract column name from expression."""
            return expr.name if isinstance(expr, ColumnExpr) else None

        def can_pushdown_or(expr: Expr) -> bool:
            """Check if OR expression can be pushed down (both sides must reference same column)."""
            if not isinstance(expr, BinaryExpr) or expr.op != Operation.OR:
                return False
            left_col = get_column_name(expr.left)
            right_col = get_column_name(expr.right)
            return left_col == right_col and left_col in (
                "topic",
                "log_time",
                "schema_name",
            )

        def visit_expr(expr: Expr, is_and_context: bool = False):
            """Visit expression tree and extract filters."""
            nonlocal topics, time_range_start, time_range_end, message_types

            if not isinstance(expr, BinaryExpr):
                return

            if expr.op == Operation.AND:
                visit_expr(expr.left, is_and_context=True)
                visit_expr(expr.right, is_and_context=True)
                return

            if expr.op == Operation.OR:
                if can_pushdown_or(expr):
                    visit_expr(expr.left, is_and_context=False)
                    visit_expr(expr.right, is_and_context=False)
                return

            # Handle commutative expressions: 1000 < col("log_time") -> col("log_time") > 1000
            left, right = expr.left, expr.right
            op = expr.op

            if isinstance(right, ColumnExpr) and isinstance(left, LiteralExpr):
                left, right = right, left
                op_map = {
                    Operation.LT: Operation.GT,
                    Operation.LE: Operation.GE,
                    Operation.GT: Operation.LT,
                    Operation.GE: Operation.LE,
                    Operation.EQ: Operation.EQ,
                    Operation.NE: Operation.NE,
                }
                if op not in op_map:
                    return
                op = op_map[op]

            if isinstance(left, ColumnExpr):
                col_name = left.name

                if op == Operation.IN:
                    value = extract_value(right)
                    if value is not None:
                        if col_name == "topic":
                            topics = update_set(topics, value, is_and_context)
                        elif col_name == "schema_name":
                            message_types = update_set(
                                message_types, value, is_and_context
                            )
                    return

                if op == Operation.EQ and isinstance(right, LiteralExpr):
                    if col_name == "topic":
                        topics = update_set(topics, {right.value}, is_and_context)
                    elif col_name == "schema_name":
                        message_types = update_set(
                            message_types, {right.value}, is_and_context
                        )
                    return

                if col_name == "log_time" and isinstance(right, LiteralExpr):
                    value = right.value
                    if op in (Operation.GT, Operation.GE):
                        if time_range_start is None or value > time_range_start:
                            time_range_start = value
                    elif op in (Operation.LT, Operation.LE):
                        if time_range_end is None or value < time_range_end:
                            time_range_end = value
                    return

            visit_expr(expr.left, is_and_context)
            visit_expr(expr.right, is_and_context)

        visit_expr(predicate_expr)

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
        if start_time >= end_time:
            return TimeRange(start_time=start_time, end_time=start_time + 1)
        return TimeRange(start_time=start_time, end_time=end_time)

    def _read_stream(self, f: "pyarrow.NativeFile", path: str) -> Iterator[Block]:
        """Read MCAP file and yield blocks with predicate pushdown."""
        from mcap.reader import make_reader

        reader = make_reader(f)
        messages = reader.iter_messages(
            topics=list(self._topics) if self._topics else None,
            start_time=self._time_range.start_time if self._time_range else None,
            end_time=self._time_range.end_time if self._time_range else None,
            log_time_order=True,
            reverse=False,
        )

        builder = DelegatingBlockBuilder()
        for schema, channel, message in messages:
            if not self._should_include_message(schema, channel, message):
                continue
            builder.add(self._message_to_dict(schema, channel, message, path))

        if builder.num_rows() > 0:
            block = builder.build()
            # Apply predicate expression if it contains unpushed parts
            if self._predicate_expr is not None:
                try:
                    filter_expr = self._predicate_expr.to_pyarrow()
                    block_accessor = BlockAccessor.for_block(block)
                    table = block_accessor.to_arrow()
                    filtered_table = table.filter(filter_expr)
                    block = block_accessor.from_arrow(filtered_table)
                except Exception as e:
                    logger.debug(
                        f"Could not apply predicate expression to MCAP block: {e}"
                    )
            yield block

    def _should_include_message(
        self, schema: "Schema", channel: "Channel", message: "Message"
    ) -> bool:
        """Check if message should be included (applies message_types filter)."""
        if self._message_types and schema and schema.name not in self._message_types:
            return False
        return True

    def _message_to_dict(
        self, schema: "Schema", channel: "Channel", message: "Message", path: str
    ) -> Dict[str, Any]:
        """Convert MCAP message to dictionary format."""
        decoded_data = message.data
        if channel.message_encoding == "json" and isinstance(message.data, bytes):
            try:
                decoded_data = json.loads(message.data.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                decoded_data = message.data

        message_data = {
            "data": decoded_data,
            "topic": channel.topic,
            "log_time": message.log_time,
            "publish_time": message.publish_time,
            "sequence": message.sequence,
        }

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

        if self._include_paths:
            message_data["path"] = path

        return message_data

    def get_name(self) -> str:
        """Return a human-readable name for this datasource."""
        return "MCAP"
