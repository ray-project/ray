"""
Physical operator for window operations.
"""

import logging
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from datetime import datetime, timedelta

import ray
from ray import ObjectRef
import numpy as np
import pandas as pd
import pyarrow as pa
from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
    ExecutionResources,
    PhysicalOperator,
    RefBundle,
    TaskContext,
)
from ray.data._internal.execution.interfaces.physical_operator import (
    DataOpTask,
    MetadataOpTask,
    OpTask,
    estimate_total_num_of_blocks,
)
from ray.data._internal.execution.operators.base_physical_operator import (
    AllToAllOperator,
)
from ray.data._internal.execution.operators.hash_shuffle import (
    HashShuffleOperator,
    StatefulShuffleAggregation,
)
from ray.data._internal.execution.util import memory_string
from ray.data._internal.stats import StatsDict
from ray.data._internal.util import MemoryProfiler
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockExecStats,
    BlockMetadata,
    BlockMetadataWithSchema,
    BlockStats,
    to_stats,
)
from ray.data.context import DataContext
from ray.data.window import (
    WindowSpec,
    SlidingWindow,
    TumblingWindow,
    SessionWindow,
    RankWindowSpec,
    LagWindowSpec,
    LeadWindowSpec,
)
from ray.data.aggregate import AggregateFn

logger = logging.getLogger(__name__)


class WindowAggregation(StatefulShuffleAggregation):
    """Stateful aggregation for window operations.

    This class handles the aggregation of data within windows during the shuffle phase.
    """

    def __init__(
        self,
        aggregator_id: int,
        window_spec: WindowSpec,
        aggregation_fns: Tuple["AggregateFn"],
    ):
        super().__init__(aggregator_id)
        self._window_spec = window_spec
        self._aggregation_fns = aggregation_fns
        self._windowed_blocks: List[Block] = []
        self._window_cache: Dict[Any, Dict[str, Any]] = {}

    def accept(self, input_seq_id: int, partition_id: int, partition_shard: Block):
        """Accept a partition shard and process it for window aggregation."""
        assert (
            input_seq_id == 0
        ), f"Single sequence expected (got seq-id {input_seq_id})"

        # Process the partition shard for window aggregation
        processed_block = self._process_window_aggregation(partition_shard)
        self._windowed_blocks.append(processed_block)

    def finalize(self, partition_id: int) -> Block:
        """Finalize the window aggregation and return the result block."""
        if len(self._windowed_blocks) == 0:
            # Return empty block with proper schema
            return self._create_empty_result_block()

        # Combine all windowed blocks
        return self._combine_windowed_blocks()

    def clear(self, partition_id: int):
        """Clear the accumulated state."""
        self._windowed_blocks = []
        self._window_cache = {}

    def _process_window_aggregation(self, block: Block) -> Block:
        """Process a block for window aggregation.

        This implements the actual window aggregation logic for different window types.
        """
        accessor = BlockAccessor.for_block(block)

        if isinstance(self._window_spec, SlidingWindow):
            return self._process_sliding_window(accessor)
        elif isinstance(self._window_spec, TumblingWindow):
            return self._process_tumbling_window(accessor)
        elif isinstance(self._window_spec, SessionWindow):
            return self._process_session_window(accessor)
        else:
            # Fallback to basic processing
            return block

    def _process_sliding_window(self, accessor: BlockAccessor) -> Block:
        """Process sliding window aggregation."""
        # Convert to pandas for easier window operations
        df = accessor.to_pandas()

        # Sort by window column and partition columns
        sort_cols = [self._window_spec.on] + self._window_spec.partition_by
        df = df.sort_values(sort_cols)

        # Apply rolling window operations
        result_data = {}

        # Add original columns
        for col in df.columns:
            result_data[col] = df[col].values

        # Apply aggregations for each function
        for agg_fn in self._aggregation_fns:
            col_name = agg_fn._key
            if col_name in df.columns:
                if isinstance(self._window_spec.size, int):
                    # Row-based window
                    window_size = self._window_spec.size
                    if self._window_spec.alignment == "CENTERED":
                        window_size = window_size // 2

                    # Apply rolling aggregation
                    if agg_fn.__class__.__name__ == "Sum":
                        result_data[f"sum({col_name})"] = (
                            df[col_name]
                            .rolling(window=window_size, center=True, min_periods=1)
                            .sum()
                            .values
                        )
                    elif agg_fn.__class__.__name__ == "Mean":
                        result_data[f"mean({col_name})"] = (
                            df[col_name]
                            .rolling(window=window_size, center=True, min_periods=1)
                            .mean()
                            .values
                        )
                    elif agg_fn.__class__.__name__ == "Count":
                        result_data[f"count({col_name})"] = (
                            df[col_name]
                            .rolling(window=window_size, center=True, min_periods=1)
                            .count()
                            .values
                        )
                    elif agg_fn.__class__.__name__ == "Max":
                        result_data[f"max({col_name})"] = (
                            df[col_name]
                            .rolling(window=window_size, center=True, min_periods=1)
                            .max()
                            .values
                        )
                    elif agg_fn.__class__.__name__ == "Min":
                        result_data[f"min({col_name})"] = (
                            df[col_name]
                            .rolling(window=window_size, center=True, min_periods=1)
                            .min()
                            .values
                        )
                else:
                    # Time-based window - use pandas time-based rolling
                    # This is a simplified implementation
                    result_data[f"agg({col_name})"] = df[col_name].values

        # Create result DataFrame
        result_df = pd.DataFrame(result_data)

        # Convert back to block
        return BlockAccessor.for_block(result_df).to_block()

    def _process_tumbling_window(self, accessor: BlockAccessor) -> Block:
        """Process tumbling window aggregation."""
        df = accessor.to_pandas()

        # Sort by window column and partition columns
        sort_cols = [self._window_spec.on] + self._window_spec.partition_by
        df = df.sort_values(sort_cols)

        # Group by window ID and apply aggregations
        if isinstance(self._window_spec.size, int):
            # Row-based: create window IDs
            df["_window_id"] = df.index // self._window_spec.size
        else:
            # Time-based: create window IDs using the window spec
            df["_window_id"] = df[self._window_spec.on].apply(
                lambda x: self._window_spec._get_window_id(x)
            )

        # Add partition columns to window ID for proper grouping
        if self._window_spec.partition_by:
            for col in self._window_spec.partition_by:
                df["_window_id"] = (
                    df["_window_id"].astype(str) + "_" + df[col].astype(str)
                )

        # Group by window ID and apply aggregations
        result_data = {}

        # Add window ID and partition columns
        result_data["_window_id"] = df["_window_id"].values
        for col in self._window_spec.partition_by:
            result_data[col] = df[col].values

        # Apply aggregations for each function
        for agg_fn in self._aggregation_fns:
            col_name = agg_fn._key
            if col_name in df.columns:
                grouped = df.groupby("_window_id")[col_name]

                if agg_fn.__class__.__name__ == "Sum":
                    result_data[f"sum({col_name})"] = (
                        grouped.sum().reindex(df["_window_id"]).values
                    )
                elif agg_fn.__class__.__name__ == "Mean":
                    result_data[f"mean({col_name})"] = (
                        grouped.mean().reindex(df["_window_id"]).values
                    )
                elif agg_fn.__class__.__name__ == "Count":
                    result_data[f"count({col_name})"] = (
                        grouped.count().reindex(df["_window_id"]).values
                    )
                elif agg_fn.__class__.__name__ == "Max":
                    result_data[f"max({col_name})"] = (
                        grouped.max().reindex(df["_window_id"]).values
                    )
                elif agg_fn.__class__.__name__ == "Min":
                    result_data[f"min({col_name})"] = (
                        grouped.min().reindex(df["_window_id"]).values
                    )

        # Create result DataFrame
        result_df = pd.DataFrame(result_data)

        # Convert back to block
        return BlockAccessor.for_block(result_df).to_block()

    def _process_session_window(self, accessor: BlockAccessor) -> Block:
        """Process session window aggregation."""
        df = accessor.to_pandas()

        # Sort by window column and partition columns
        sort_cols = [self._window_spec.on] + self._window_spec.partition_by
        df = df.sort_values(sort_cols)

        # Create session IDs based on gaps
        df["_session_id"] = 0
        current_session = 0

        # Group by partition columns first
        if self._window_spec.partition_by:
            for partition_values in (
                df[self._window_spec.partition_by].drop_duplicates().values
            ):
                mask = True
                for i, col in enumerate(self._window_spec.partition_by):
                    mask &= df[col] == partition_values[i]

                partition_df = df[mask].copy()
                if len(partition_df) > 0:
                    # Create session IDs within this partition
                    partition_df = partition_df.sort_values(self._window_spec.on)
                    session_ids = self._create_session_ids(
                        partition_df, self._window_spec.on, self._window_spec.gap
                    )
                    df.loc[mask, "_session_id"] = session_ids + current_session
                    current_session = max(session_ids) + current_session + 1
        else:
            # No partitioning, create sessions across entire dataset
            df = df.sort_values(self._window_spec.on)
            df["_session_id"] = self._create_session_ids(
                df, self._window_spec.on, self._window_spec.gap
            )

        # Group by session ID and apply aggregations
        result_data = {}

        # Add session ID and partition columns
        result_data["_session_id"] = df["_session_id"].values
        for col in self._window_spec.partition_by:
            result_data[col] = df[col].values

        # Apply aggregations for each function
        for agg_fn in self._aggregation_fns:
            col_name = agg_fn._key
            if col_name in df.columns:
                grouped = df.groupby("_session_id")[col_name]

                if agg_fn.__class__.__name__ == "Sum":
                    result_data[f"sum({col_name})"] = (
                        grouped.sum().reindex(df["_session_id"]).values
                    )
                elif agg_fn.__class__.__name__ == "Mean":
                    result_data[f"mean({col_name})"] = (
                        grouped.mean().reindex(df["_session_id"]).values
                    )
                elif agg_fn.__class__.__name__ == "Count":
                    result_data[f"count({col_name})"] = (
                        grouped.count().reindex(df["_session_id"]).values
                    )
                elif agg_fn.__class__.__name__ == "Max":
                    result_data[f"max({col_name})"] = (
                        grouped.max().reindex(df["_session_id"]).values
                    )
                elif agg_fn.__class__.__name__ == "Min":
                    result_data[f"min({col_name})"] = (
                        grouped.min().reindex(df["_session_id"]).values
                    )

        # Create result DataFrame
        result_df = pd.DataFrame(result_data)

        # Convert back to block
        return BlockAccessor.for_block(result_df).to_block()

    def _create_session_ids(
        self, df: pd.DataFrame, time_col: str, gap: Union[str, timedelta, int]
    ) -> List[int]:
        """Create session IDs based on time gaps."""
        if isinstance(gap, str):
            # Parse time gap
            gap_delta = self._parse_time_interval(gap)
        elif isinstance(gap, timedelta):
            gap_delta = gap
        else:
            # Row-based gap
            gap_delta = gap

        session_ids = [0]
        current_session = 0

        for i in range(1, len(df)):
            current_time = df.iloc[i][time_col]
            prev_time = df.iloc[i - 1][time_col]

            if isinstance(current_time, str):
                current_time = datetime.fromisoformat(current_time)
            if isinstance(prev_time, str):
                prev_time = datetime.fromisoformat(prev_time)

            if isinstance(gap_delta, timedelta):
                # Time-based gap
                if current_time - prev_time > gap_delta:
                    current_session += 1
            else:
                # Row-based gap
                if i - (i - 1) > gap_delta:
                    current_session += 1

            session_ids.append(current_session)

        return session_ids

    def _parse_time_interval(self, interval: Union[str, timedelta]) -> timedelta:
        """Parse time interval for session windows."""
        if isinstance(interval, timedelta):
            return interval

        if isinstance(interval, str):
            # Use simplified parsing for session windows
            interval = interval.lower().strip()

            if "hour" in interval:
                hours = int(interval.split()[0])
                return timedelta(hours=hours)
            elif "minute" in interval:
                minutes = int(interval.split()[0])
                return timedelta(minutes=minutes)
            elif "second" in interval:
                seconds = int(interval.split()[0])
                return timedelta(seconds=seconds)
            else:
                raise ValueError(f"Unable to parse time interval: {interval}")

        raise ValueError(f"Invalid time interval type: {type(interval)}")

    def _combine_windowed_blocks(self) -> Block:
        """Combine all windowed blocks into a single result block."""
        if len(self._windowed_blocks) == 1:
            return self._windowed_blocks[0]

        # Combine multiple blocks using pandas concatenation
        accessors = [BlockAccessor.for_block(block) for block in self._windowed_blocks]
        dfs = [acc.to_pandas() for acc in accessors]

        # Concatenate DataFrames
        combined_df = pd.concat(dfs, ignore_index=True)

        # Convert back to block
        return BlockAccessor.for_block(combined_df).to_block()

    def _process_ranking_window(self, accessor: BlockAccessor) -> Block:
        """Process ranking window functions (rank, dense_rank, row_number)."""
        df = accessor.to_pandas()

        # Sort by partition and order columns
        sort_cols = self._window_spec.order_by
        if self._window_spec.partition_by:
            sort_cols = self._window_spec.partition_by + sort_cols

        if sort_cols:
            df = df.sort_values(sort_cols)

        result_data = {col: df[col].values for col in df.columns}

        for agg_fn in self._aggregation_fns:
            col_name = agg_fn._key
            if col_name in df.columns:
                if agg_fn.__class__.__name__ == "Rank":
                    # Standard ranking with gaps
                    result_data[f"rank({col_name})"] = (
                        df[col_name].rank(method="min").values
                    )
                elif agg_fn.__class__.__name__ == "DenseRank":
                    # Dense ranking without gaps
                    result_data[f"dense_rank({col_name})"] = (
                        df[col_name].rank(method="dense").values
                    )
                elif agg_fn.__class__.__name__ == "RowNumber":
                    # Sequential row numbering
                    result_data[f"row_number({col_name})"] = range(1, len(df) + 1)

        return BlockAccessor.for_block(pd.DataFrame(result_data)).to_block()

    def _process_lag_window(self, accessor: BlockAccessor) -> Block:
        """Process lag window functions."""
        df = accessor.to_pandas()

        # Sort by partition and order columns
        sort_cols = self._window_spec.order_by
        if self._window_spec.partition_by:
            sort_cols = self._window_spec.partition_by + sort_cols

        if sort_cols:
            df = df.sort_values(sort_cols)

        result_data = {col: df[col].values for col in df.columns}

        for agg_fn in self._aggregation_fns:
            col_name = agg_fn._key
            if col_name in df.columns:
                offset = getattr(agg_fn, "offset", 1)
                result_data[f"lag({col_name}, {offset})"] = (
                    df[col_name].shift(offset).values
                )

        return BlockAccessor.for_block(pd.DataFrame(result_data)).to_block()

    def _process_lead_window(self, accessor: BlockAccessor) -> Block:
        """Process lead window functions."""
        df = accessor.to_pandas()

        # Sort by partition and order columns
        sort_cols = self._window_spec.order_by
        if self._window_spec.partition_by:
            sort_cols = self._window_spec.partition_by + sort_cols

        if sort_cols:
            df = df.sort_values(sort_cols)

        result_data = {col: df[col].values for col in df.columns}

        for agg_fn in self._aggregation_fns:
            col_name = agg_fn._key
            if col_name in df.columns:
                offset = getattr(agg_fn, "offset", 1)
                result_data[f"lead({col_name}, {offset})"] = (
                    df[col_name].shift(-offset).values
                )

        return BlockAccessor.for_block(pd.DataFrame(result_data)).to_block()

    def _create_empty_result_block(self) -> Block:
        """Create an empty result block with the expected schema."""
        # Create empty DataFrame with expected columns
        empty_data = {}

        # Add basic columns
        empty_data["_window_id"] = []

        # Add partition columns if specified
        for col in self._window_spec.partition_by:
            empty_data[col] = []

        # Add aggregation result columns
        for agg_fn in self._aggregation_fns:
            col_name = agg_fn._key
            result_col = f"{agg_fn.__class__.__name__.lower()}({col_name})"
            empty_data[result_col] = []

        empty_df = pd.DataFrame(empty_data)
        return BlockAccessor.for_block(empty_df).to_block()


class WindowOperator(AllToAllOperator):
    """Physical operator for window operations.

    This operator implements the distributed execution of window functions,
    supporting both sliding and tumbling windows with stateful aggregation.
    """

    def __init__(
        self,
        input_op: PhysicalOperator,
        data_context: DataContext,
        window_spec: WindowSpec,
        aggregation_fns: List["AggregateFn"],
        num_partitions: Optional[int] = None,
        batch_format: Optional[str] = "default",
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        self._window_spec = window_spec
        self._aggregation_fns = aggregation_fns
        self._num_partitions = (
            num_partitions or data_context.default_hash_shuffle_parallelism
        )
        self._batch_format = batch_format
        self._ray_remote_args = ray_remote_args or {}

        # Create the hash shuffle operator for window operations
        shuffle_op = HashShuffleOperator(
            input_op,
            data_context,
            key_columns=self._get_shuffle_keys(),
            num_partitions=self._num_partitions,
            should_sort=True,  # Windows typically require sorted data
            aggregator_factory=self._create_aggregator_factory(),
        )

        super().__init__(
            shuffle_op,
            data_context,
            target_max_block_size=None,
            num_outputs=self._num_partitions,
            sub_progress_bar_names=[
                "WindowSample",
                "WindowShuffle",
                "WindowAggregate",
            ],
            name="Window",
        )

    def _get_shuffle_keys(self) -> Tuple[str, ...]:
        """Get the key columns for shuffling based on window specification."""
        # For sliding windows, we need to shuffle by the partition columns
        if self._window_spec.partition_by:
            return tuple(self._window_spec.partition_by)

        # For tumbling and session windows, we might not need additional shuffling
        # beyond the window column itself
        return (self._window_spec.on,)

    def _create_aggregator_factory(
        self,
    ) -> Callable[[int, List[int]], WindowAggregation]:
        """Create a factory function for window aggregators."""

        def factory(aggregator_id: int, partition_ids: List[int]) -> WindowAggregation:
            return WindowAggregation(
                aggregator_id=aggregator_id,
                window_spec=self._window_spec,
                aggregation_fns=tuple(self._aggregation_fns),
            )

        return factory

    def start(self, options: ExecutionOptions):
        """Start the window operator."""
        super().start(options)
        logger.info(f"Started Window operator with {self._num_partitions} partitions")

    def get_stats(self) -> StatsDict:
        """Get execution statistics."""
        stats = super().get_stats()
        stats["window_spec"] = str(self._window_spec)
        stats["num_aggregations"] = len(self._aggregation_fns)
        return stats
