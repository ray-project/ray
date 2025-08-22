"""
Window functions for Ray Data.

This module provides window function capabilities for Ray Data, including
sliding windows, tumbling windows, and session windows. The API is designed
to be familiar to users of Pandas, PySpark, and Flink.
"""

from datetime import datetime, timedelta
from typing import Any, List, Optional, Union, Dict, Tuple
import re

from ray.util.annotations import PublicAPI
from ray.data._internal.compute import (
    ComputeStrategy,
    TaskPoolStrategy,
    ActorPoolStrategy,
)


class WindowSpec:
    """Base class for window specifications.

    This class defines the interface for different types of windows that can be
    applied to Ray Data datasets.
    """

    def __init__(
        self,
        on: str,
        partition_by: Optional[List[str]] = None,
        order_by: Optional[List[str]] = None,
        compute_strategy: Optional[ComputeStrategy] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        """Initialize a window specification.

        Args:
            on: The column name to use for windowing (usually a timestamp column)
            partition_by: Columns to partition by before applying windows (equivalent to PARTITION BY)
            order_by: Columns to order by within each partition (equivalent to ORDER BY)
            compute_strategy: The compute strategy to use (TaskPoolStrategy or ActorPoolStrategy)
            ray_remote_args: Additional resource requirements for Ray tasks/actors
        """
        self.on = on
        self.partition_by = partition_by or []
        self.order_by = order_by or []
        self.compute_strategy = compute_strategy
        self.ray_remote_args = ray_remote_args or {}

    def __repr__(self) -> str:
        partition_str = (
            f", partition_by={self.partition_by}" if self.partition_by else ""
        )
        order_str = f", order_by={self.order_by}" if self.order_by else ""
        return f"{self.__class__.__name__}(on='{self.on}'{partition_str}{order_str})"


class SlidingWindow(WindowSpec):
    """Specification for sliding windows.

    Sliding windows create a window for each row that includes adjacent rows
    within a specified range.
    """

    def __init__(
        self,
        on: str,
        size: Union[str, timedelta, int],
        offset: Optional[Union[str, timedelta, int]] = None,
        alignment: str = "TRAILING",
        partition_by: Optional[List[str]] = None,
        order_by: Optional[List[str]] = None,
        compute_strategy: Optional[ComputeStrategy] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        """Initialize a sliding window specification.

        Args:
            on: The column name to use for windowing (usually a timestamp column)
            size: The size of the window. Can be:
                - A string like "1 hour", "30 minutes", "2 days", "3 months"
                - A timedelta object
                - An integer (number of rows)
            offset: The offset from the current row. Can be:
                - A string like "15 minutes", "1 hour"
                - A timedelta object
                - An integer (number of rows)
                - None (defaults to 0 for time-based, size for row-based)
            alignment: How to align the window relative to the current row:
                - "TRAILING": Window extends into the past from current row
                - "LEADING": Window extends into the future from current row
                - "CENTERED": Window is centered on current row
            partition_by: Columns to partition by before applying windows
            order_by: Columns to order by within each partition
            compute_strategy: The compute strategy to use (TaskPoolStrategy or ActorPoolStrategy)
            ray_remote_args: Additional resource requirements for Ray tasks/actors
        """
        super().__init__(on, partition_by, order_by, compute_strategy, ray_remote_args)
        self.size = size
        self.offset = offset
        self.alignment = alignment.upper()

        if self.alignment not in ["TRAILING", "LEADING", "CENTERED"]:
            raise ValueError(
                f"alignment must be one of 'TRAILING', 'LEADING', 'CENTERED', got {alignment}"
            )

        # Validate that we can't have unbounded centered windows
        if self.alignment == "CENTERED" and size == "UNBOUNDED":
            raise ValueError("Cannot have unbounded centered windows")

    def _parse_time_interval(self, interval: Union[str, timedelta]) -> timedelta:
        """Parse a time interval string or timedelta into a timedelta.

        Supports more robust parsing including months and years using approximate conversions.
        """
        if isinstance(interval, timedelta):
            return interval

        if isinstance(interval, str):
            interval = interval.lower().strip()

            if interval == "unbounded":
                return timedelta.max

            # Enhanced parsing with more units and better error handling
            # Pattern: number + unit (e.g., "3 months", "2.5 hours")
            pattern = r"^(\d+(?:\.\d+)?)\s*(\w+)$"
            match = re.match(pattern, interval)

            if match:
                value = float(match.group(1))
                unit = match.group(2)

                if unit in ["second", "seconds", "sec", "s"]:
                    return timedelta(seconds=value)
                elif unit in ["minute", "minutes", "min", "m"]:
                    return timedelta(minutes=value)
                elif unit in ["hour", "hours", "hr", "h"]:
                    return timedelta(hours=value)
                elif unit in ["day", "days", "d"]:
                    return timedelta(days=value)
                elif unit in ["week", "weeks", "w"]:
                    return timedelta(weeks=value)
                elif unit in ["month", "months", "mo"]:
                    # Approximate: 1 month ≈ 30.44 days
                    return timedelta(days=value * 30.44)
                elif unit in ["year", "years", "yr", "y"]:
                    # Approximate: 1 year ≈ 365.25 days
                    return timedelta(days=value * 365.25)
                else:
                    raise ValueError(f"Unknown time unit: {unit}")

            # Fallback to simple parsing for backward compatibility
            if "hour" in interval:
                hours = int(interval.split()[0])
                return timedelta(hours=hours)
            elif "minute" in interval:
                minutes = int(interval.split()[0])
                return timedelta(minutes=minutes)
            elif "day" in interval:
                days = int(interval.split()[0])
                return timedelta(days=days)
            elif "second" in interval:
                seconds = int(interval.split()[0])
                return timedelta(seconds=seconds)
            else:
                raise ValueError(
                    f"Unable to parse time interval: {interval}. Supported formats: '3 months', '2.5 hours', '1 day', etc."
                )

        raise ValueError(f"Invalid time interval type: {type(interval)}")


class TumblingWindow(WindowSpec):
    """Specification for tumbling windows.

    Tumbling windows divide the data into non-overlapping, fixed-size intervals.
    """

    def __init__(
        self,
        on: str,
        size: Union[str, timedelta, int],
        step: Optional[Union[str, timedelta, int]] = None,
        start: Optional[Union[str, datetime]] = None,
        partition_by: Optional[List[str]] = None,
        order_by: Optional[List[str]] = None,
        compute_strategy: Optional[ComputeStrategy] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        """Initialize a tumbling window specification.

        Args:
            on: The column name to use for windowing (usually a timestamp column)
            size: The size of each window
            step: The step size between windows (defaults to size for non-overlapping)
            start: The start time for the first window
            partition_by: Columns to partition by before applying windows
            order_by: Columns to order by within each partition
            compute_strategy: The compute strategy to use (TaskPoolStrategy or ActorPoolStrategy)
            ray_remote_args: Additional resource requirements for Ray tasks/actors
        """
        super().__init__(on, partition_by, order_by, compute_strategy, ray_remote_args)
        self.size = size
        self.step = step or size
        self.start = start

    def _parse_time_interval(self, interval: Union[str, timedelta]) -> timedelta:
        """Parse a time interval string or timedelta into a timedelta."""
        if isinstance(interval, timedelta):
            return interval

        if isinstance(interval, str):
            # Use the enhanced parsing from SlidingWindow
            sliding_win = SlidingWindow("temp")
            return sliding_win._parse_time_interval(interval)

        raise ValueError(f"Invalid time interval type: {type(interval)}")


class SessionWindow(WindowSpec):
    """Specification for session windows.

    Session windows group consecutive events that are within a specified gap.
    """

    def __init__(
        self,
        on: str,
        gap: Union[str, timedelta, int],
        partition_by: Optional[List[str]] = None,
        order_by: Optional[List[str]] = None,
        compute_strategy: Optional[ComputeStrategy] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        """Initialize a session window specification.

        Args:
            on: The column name to use for windowing (usually a timestamp column)
            gap: The maximum gap between events to be considered in the same session
            partition_by: Columns to partition by before applying windows
            order_by: Columns to order by within each partition
            compute_strategy: The compute strategy to use (TaskPoolStrategy or ActorPoolStrategy)
            ray_remote_args: Additional resource requirements for Ray tasks/actors
        """
        super().__init__(on, partition_by, order_by, compute_strategy, ray_remote_args)
        self.gap = gap

    def _parse_time_interval(self, interval: Union[str, timedelta]) -> timedelta:
        """Parse a time interval string or timedelta into a timedelta."""
        if isinstance(interval, timedelta):
            return interval

        if isinstance(interval, str):
            # Use the enhanced parsing from SlidingWindow
            sliding_win = SlidingWindow("temp")
            return sliding_win._parse_time_interval(interval)

        raise ValueError(f"Invalid time interval type: {type(interval)}")


# Convenience constructors for common window types
def sliding_window(
    on: str,
    size: Union[str, timedelta, int],
    offset: Optional[Union[str, timedelta, int]] = None,
    alignment: str = "TRAILING",
    partition_by: Optional[List[str]] = None,
    order_by: Optional[List[str]] = None,
    compute_strategy: Optional[ComputeStrategy] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
) -> SlidingWindow:
    """Create a sliding window specification.

    Args:
        on: The column name to use for windowing
        size: The size of the window
        offset: The offset from the current row
        alignment: How to align the window
        partition_by: Columns to partition by before applying windows
        order_by: Columns to order by within each partition
        compute_strategy: The compute strategy to use (TaskPoolStrategy or ActorPoolStrategy)
        ray_remote_args: Additional resource requirements for Ray tasks/actors

    Returns:
        A SlidingWindow specification

    Examples:
        >>> from ray.data.window import sliding_window

        # Trailing 1-hour window
        >>> win = sliding_window("timestamp", "1 hour")

        # Centered 5-row window
        >>> win = sliding_window("row_id", 5, alignment="CENTERED")

        # GPU-accelerated sliding window
        >>> win = sliding_window("timestamp", "1 hour", ray_remote_args={"num_gpus": 1})

        # Stateful sliding window with actor pool
        >>> win = sliding_window("timestamp", "1 hour", compute_strategy=ActorPoolStrategy(size=2))

        # Partitioned sliding window
        >>> win = sliding_window("timestamp", "1 hour", partition_by=["user_id"], order_by=["timestamp"])
    """
    return SlidingWindow(
        on,
        size,
        offset,
        alignment,
        partition_by,
        order_by,
        compute_strategy,
        ray_remote_args,
    )


def tumbling_window(
    on: str,
    size: Union[str, timedelta, int],
    step: Optional[Union[str, timedelta, int]] = None,
    start: Optional[Union[str, datetime]] = None,
    partition_by: Optional[List[str]] = None,
    order_by: Optional[List[str]] = None,
    compute_strategy: Optional[ComputeStrategy] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
) -> TumblingWindow:
    """Create a tumbling window specification.

    Args:
        on: The column name to use for windowing
        size: The size of each window
        step: The step size between windows
        start: The start time for the first window
        partition_by: Columns to partition by before applying windows
        order_by: Columns to order by within each partition
        compute_strategy: The compute strategy to use (TaskPoolStrategy or ActorPoolStrategy)
        ray_remote_args: Additional resource requirements for Ray tasks/actors

    Returns:
        A TumblingWindow specification

    Examples:
        >>> from ray.data.window import tumbling_window

        # Daily windows
        >>> win = tumbling_window("timestamp", "1 day")

        # 1-hour windows with 30-minute step (overlapping)
        >>> win = tumbling_window("timestamp", "1 hour", "30 minutes")

        # Row-based windows of 1000 rows
        >>> win = tumbling_window("row_id", 1000)

        # GPU-accelerated tumbling window
        >>> win = tumbling_window("timestamp", "1 day", ray_remote_args={"num_gpus": 1})

        # Partitioned tumbling window
        >>> win = tumbling_window("timestamp", "1 day", partition_by=["region"], order_by=["timestamp"])
    """
    return TumblingWindow(
        on, size, step, start, partition_by, order_by, compute_strategy, ray_remote_args
    )


def session_window(
    on: str,
    gap: Union[str, timedelta, int],
    partition_by: Optional[List[str]] = None,
    order_by: Optional[List[str]] = None,
    compute_strategy: Optional[ComputeStrategy] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
) -> SessionWindow:
    """Create a session window specification.

    Args:
        on: The column name to use for windowing
        gap: The maximum gap between events to be considered in the same session
        partition_by: Columns to partition by before applying windows
        order_by: Columns to order by within each partition
        compute_strategy: The compute strategy to use (TaskPoolStrategy or ActorPoolStrategy)
        ray_remote_args: Additional resource requirements for Ray tasks/actors

    Returns:
        A SessionWindow specification

    Examples:
        >>> from ray.data.window import session_window

        # 15-minute session gap
        >>> win = session_window("timestamp", "15 minutes")

        # 1-hour session gap
        >>> win = session_window("timestamp", "1 hour")

        # GPU-accelerated session window
        >>> win = session_window("timestamp", "15 minutes", ray_remote_args={"num_gpus": 1})

        # Partitioned session window
        >>> win = session_window("timestamp", "15 minutes", partition_by=["user_id"], order_by=["timestamp"])
    """
    return SessionWindow(
        on, gap, partition_by, order_by, compute_strategy, ray_remote_args
    )


# Additional window functions for PySpark/Flink compatibility
def rank_window(
    partition_by: Optional[List[str]] = None,
    order_by: Optional[List[str]] = None,
    compute_strategy: Optional[ComputeStrategy] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
) -> "RankWindowSpec":
    """Create a rank window specification for ranking functions.

    This is equivalent to PySpark's Window.partitionBy().orderBy() for ranking functions.

    Args:
        partition_by: Columns to partition by before applying ranking
        order_by: Columns to order by within each partition
        compute_strategy: The compute strategy to use
        ray_remote_args: Additional resource requirements for Ray tasks/actors

    Returns:
        A RankWindowSpec for use with ranking functions

    Examples:
        >>> from ray.data.window import rank_window

        # Rank users by amount within each region
        >>> rank_win = rank_window(partition_by=["region"], order_by=["amount"])

        # Rank all rows by timestamp
        >>> rank_win = rank_window(order_by=["timestamp"])
    """
    return RankWindowSpec(partition_by, order_by, compute_strategy, ray_remote_args)


def lag_window(
    column: str,
    offset: int = 1,
    partition_by: Optional[List[str]] = None,
    order_by: Optional[List[str]] = None,
    compute_strategy: Optional[ComputeStrategy] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
) -> "LagWindowSpec":
    """Create a lag window specification for accessing previous row values.

    This is equivalent to PySpark's lag() function.

    Args:
        column: The column to lag
        offset: Number of rows to look back (default: 1)
        partition_by: Columns to partition by before applying lag
        order_by: Columns to order by within each partition
        compute_strategy: The compute strategy to use
        ray_remote_args: Additional resource requirements for Ray tasks/actors

    Returns:
        A LagWindowSpec for use with lag operations

    Examples:
        >>> from ray.data.window import lag_window

        # Get previous day's value for each user
        >>> lag_win = lag_window("amount", 1, partition_by=["user_id"], order_by=["timestamp"])

        # Get value from 3 rows ago
        >>> lag_win = lag_window("price", 3, order_by=["row_id"])
    """
    return LagWindowSpec(
        column, offset, partition_by, order_by, compute_strategy, ray_remote_args
    )


def lead_window(
    column: str,
    offset: int = 1,
    partition_by: Optional[List[str]] = None,
    order_by: Optional[List[str]] = None,
    compute_strategy: Optional[ComputeStrategy] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
) -> "LeadWindowSpec":
    """Create a lead window specification for accessing future row values.

    This is equivalent to PySpark's lead() function.

    Args:
        column: The column to lead
        offset: Number of rows to look forward (default: 1)
        partition_by: Columns to partition by before applying lead
        order_by: Columns to order by within each partition
        compute_strategy: The compute strategy to use
        ray_remote_args: Additional resource requirements for Ray tasks/actors

    Returns:
        A LeadWindowSpec for use with lead operations

    Examples:
        >>> from ray.data.window import lead_window

        # Get next day's value for each user
        >>> lead_win = lead_window("amount", 1, partition_by=["user_id"], order_by=["timestamp"])

        # Get value from 2 rows ahead
        >>> lead_win = lead_window("price", 2, order_by=["row_id"])
    """
    return LeadWindowSpec(
        column, offset, partition_by, order_by, compute_strategy, ray_remote_args
    )


# New window specification classes for ranking and lag/lead functions
class RankWindowSpec(WindowSpec):
    """Specification for ranking window functions.

    This is used for functions like rank(), dense_rank(), row_number().
    """

    def __init__(
        self,
        partition_by: Optional[List[str]] = None,
        order_by: Optional[List[str]] = None,
        compute_strategy: Optional[ComputeStrategy] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        # For ranking functions, we don't need a specific 'on' column
        super().__init__(
            "_rank", partition_by, order_by, compute_strategy, ray_remote_args
        )

    def __repr__(self) -> str:
        partition_str = (
            f", partition_by={self.partition_by}" if self.partition_by else ""
        )
        order_str = f", order_by={self.order_by}" if self.order_by else ""
        return f"RankWindowSpec({partition_str}{order_str})"


class LagWindowSpec(WindowSpec):
    """Specification for lag window functions.

    This is used for accessing previous row values.
    """

    def __init__(
        self,
        column: str,
        offset: int = 1,
        partition_by: Optional[List[str]] = None,
        order_by: Optional[List[str]] = None,
        compute_strategy: Optional[ComputeStrategy] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            column, partition_by, order_by, compute_strategy, ray_remote_args
        )
        self.offset = offset

    def __repr__(self) -> str:
        partition_str = (
            f", partition_by={self.partition_by}" if self.partition_by else ""
        )
        order_str = f", order_by={self.order_by}" if self.order_by else ""
        return f"LagWindowSpec(column='{self.on}', offset={self.offset}{partition_str}{order_str})"


class LeadWindowSpec(WindowSpec):
    """Specification for lead window functions.

    This is used for accessing future row values.
    """

    def __init__(
        self,
        column: str,
        offset: int = 1,
        partition_by: Optional[List[str]] = None,
        order_by: Optional[List[str]] = None,
        compute_strategy: Optional[ComputeStrategy] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            column, partition_by, order_by, compute_strategy, ray_remote_args
        )
        self.offset = offset

    def __repr__(self) -> str:
        partition_str = (
            f", partition_by={self.partition_by}" if self.partition_by else ""
        )
        order_str = f", order_by={self.order_by}" if self.order_by else ""
        return f"LeadWindowSpec(column='{self.on}', offset={self.offset}{partition_str}{order_str})"
