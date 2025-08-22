"""
Tests for window functions in Ray Data.
"""

import pytest
from datetime import datetime, timedelta
from typing import Dict, Any

import numpy as np
import pandas as pd

import ray
from ray.data.window import (
    window,
    sliding_window,
    tumbling_window,
    session_window,
    WindowSpec,
    SlidingWindow,
    TumblingWindow,
    SessionWindow,
)
from ray.data.aggregate import (
    Sum,
    Mean,
    Count,
    Max,
    Min,
    Rank,
    DenseRank,
    RowNumber,
    Lag,
    Lead,
)
from ray.data._internal.compute import TaskPoolStrategy, ActorPoolStrategy


@pytest.fixture
def sample_time_series_data():
    """Create sample time series data for testing."""
    data = []
    base_time = datetime(2023, 1, 1, 9, 0, 0)

    for i in range(100):
        timestamp = base_time + timedelta(minutes=i * 5)
        user_id = f"user_{i % 10}"
        amount = 10 + (i % 100)

        # Add some anomalies
        if i % 20 == 0:
            amount = 1000 + (i % 500)

        data.append(
            {
                "timestamp": timestamp,
                "user_id": user_id,
                "amount": amount,
                "transaction_id": f"tx_{i}",
                "row_id": i,
            }
        )

    return ray.data.from_items(data)


@pytest.fixture
def sample_iot_data():
    """Create sample IoT sensor data for testing."""
    data = []
    base_time = datetime(2023, 1, 1, 0, 0, 0)

    for i in range(50):
        timestamp = base_time + timedelta(minutes=i * 2)
        device_id = f"device_{i % 5}"
        temperature = 20 + (i % 30) + (i % 10) * 0.1
        humidity = 40 + (i % 30) + (i % 15) * 0.5

        data.append(
            {
                "timestamp": timestamp,
                "device_id": device_id,
                "temperature": temperature,
                "humidity": humidity,
                "reading_id": i,
            }
        )

    return ray.data.from_items(data)


class TestWindowSpec:
    """Test window specification classes."""

    def test_window_spec_base(self):
        """Test base WindowSpec class."""
        spec = WindowSpec("timestamp")
        assert spec.on == "timestamp"
        assert str(spec) == "WindowSpec(on='timestamp')"
        assert spec.compute_strategy is None
        assert spec.ray_remote_args == {}

    def test_sliding_window_creation(self):
        """Test SlidingWindow creation and validation."""
        # Basic sliding window
        win = sliding_window("timestamp", "1 hour")
        assert isinstance(win, SlidingWindow)
        assert win.on == "timestamp"
        assert win.size == "1 hour"
        assert win.alignment == "TRAILING"
        assert win.offset is None
        assert win.partition_by == []

        # With offset and alignment
        win = sliding_window("timestamp", "2 hours", "30 minutes", "CENTERED")
        assert win.size == "2 hours"
        assert win.offset == "30 minutes"
        assert win.alignment == "CENTERED"

        # Row-based window
        win = sliding_window("row_id", 100, alignment="CENTERED")
        assert win.size == 100
        assert win.alignment == "CENTERED"

    def test_sliding_window_validation(self):
        """Test SlidingWindow validation."""
        # Invalid alignment
        with pytest.raises(ValueError, match="alignment must be one of"):
            sliding_window("timestamp", "1 hour", alignment="INVALID")

        # Unbounded centered window (not allowed)
        with pytest.raises(ValueError, match="Cannot have unbounded centered windows"):
            sliding_window("timestamp", "UNBOUNDED", alignment="CENTERED")

    def test_tumbling_window_creation(self):
        """Test TumblingWindow creation."""
        # Basic tumbling window
        win = tumbling_window("timestamp", "1 day")
        assert isinstance(win, TumblingWindow)
        assert win.on == "timestamp"
        assert win.size == "1 day"
        assert win.step == "1 day"  # Defaults to size
        assert win.start is None
        assert win.partition_by == []

        # With custom step
        win = tumbling_window("timestamp", "1 hour", "30 minutes")
        assert win.size == "1 hour"
        assert win.step == "30 minutes"

        # With start time
        start_time = datetime(2023, 1, 1, 0, 0, 0)
        win = tumbling_window("timestamp", "1 day", start=start_time)
        assert win.start == start_time

    def test_session_window_creation(self):
        """Test SessionWindow creation."""
        win = session_window("timestamp", "15 minutes")
        assert isinstance(win, SessionWindow)
        assert win.on == "timestamp"
        assert win.gap == "15 minutes"
        assert win.partition_by == []

    def test_window_convenience_function(self):
        """Test the generic window() function."""
        # Sliding window
        win = window("timestamp", "1 hour", window_type="sliding")
        assert isinstance(win, SlidingWindow)
        assert win.size == "1 hour"

        # Tumbling window
        win = window("timestamp", "1 day", window_type="tumbling")
        assert isinstance(win, TumblingWindow)
        assert win.size == "1 day"

        # Session window
        win = window("timestamp", gap="15 minutes", window_type="session")
        assert isinstance(win, SessionWindow)
        assert win.gap == "15 minutes"

        # Invalid window type
        with pytest.raises(ValueError, match="Unknown window type"):
            window("timestamp", "1 hour", window_type="invalid")

    def test_time_interval_parsing(self):
        """Test time interval parsing."""
        win = sliding_window("timestamp", "2 hours")

        # Test string parsing
        delta = win._parse_time_interval("1 hour")
        assert delta == timedelta(hours=1)

        delta = win._parse_time_interval("30 minutes")
        assert delta == timedelta(minutes=30)

        delta = win._parse_time_interval("2 days")
        assert delta == timedelta(days=2)

        # Test timedelta passthrough
        td = timedelta(hours=3)
        delta = win._parse_time_interval(td)
        assert delta == td

        # Test invalid interval
        with pytest.raises(ValueError, match="Unable to parse time interval"):
            win._parse_time_interval("invalid")

    def test_window_bounds_calculation(self):
        """Test window boundary calculations."""
        # Time-based trailing window
        win = sliding_window("timestamp", "1 hour")
        start, end = win._get_window_bounds(
            datetime(2023, 1, 1, 12, 0), is_time_based=True
        )
        assert start == datetime(2023, 1, 1, 11, 0)
        assert end == datetime(2023, 1, 1, 12, 0)

        # Time-based centered window
        win = sliding_window("timestamp", "2 hours", alignment="CENTERED")
        start, end = win._get_window_bounds(
            datetime(2023, 1, 1, 12, 0), is_time_based=True
        )
        assert start == datetime(2023, 1, 1, 11, 0)
        assert end == datetime(2023, 1, 1, 13, 0)

        # Row-based window
        win = sliding_window("row_id", 5, alignment="CENTERED")
        start, end = win._get_window_bounds(10, is_time_based=False)
        assert start == 8
        assert end == 12

    def test_tumbling_window_id_calculation(self):
        """Test tumbling window ID calculation."""
        # Row-based
        win = tumbling_window("row_id", 1000)
        assert win._get_window_id(500) == 0
        assert win._get_window_id(1500) == 1
        assert win._get_window_id(2500) == 2

        # Time-based
        win = tumbling_window("timestamp", "1 hour")
        epoch = datetime(1970, 1, 1)
        hour1 = epoch + timedelta(hours=1)
        hour2 = epoch + timedelta(hours=2)

        assert win._get_window_id(hour1) == 1
        assert win._get_window_id(hour2) == 2


class TestWindowFunctions:
    """Test window function operations."""

    def test_sliding_window_with_slide_over(self, sample_time_series_data):
        """Test sliding windows using slide_over method."""
        ds = sample_time_series_data

        # Basic sliding window
        result = ds.slide_over(sliding_window("timestamp", "1 hour")).aggregate(
            Mean("amount")
        )

        assert result.count() > 0
        assert "mean(amount)" in result.schema().names

        # With sharding
        result = ds.slide_over(
            sliding_window("timestamp", "1 hour"), partition_by=["user_id"]
        ).aggregate(Mean("amount"))

        assert result.count() > 0
        assert "user_id" in result.schema().names
        assert "mean(amount)" in result.schema().names

    def test_tumbling_window_with_window_method(self, sample_time_series_data):
        """Test tumbling windows using window method."""
        ds = sample_time_series_data

        result = ds.window(
            tumbling_window("timestamp", "1 day"),
            Sum("amount"),
            Count("transaction_id"),
            Mean("amount"),
        )

        assert result.count() > 0
        assert "sum(amount)" in result.schema().names
        assert "count(transaction_id)" in result.schema().names
        assert "mean(amount)" in result.schema().names

    def test_session_window_with_groupby(self, sample_time_series_data):
        """Test session windows within groups."""
        ds = sample_time_series_data

        result = ds.groupby("user_id").window(
            session_window("timestamp", "15 minutes"),
            Sum("amount"),
            Count("transaction_id"),
        )

        assert result.count() > 0
        assert "user_id" in result.schema().names
        assert "sum(amount)" in result.schema().names
        assert "count(transaction_id)" in result.schema().names

    def test_hopping_windows(self, sample_time_series_data):
        """Test hopping windows (tumbling with step < size)."""
        ds = sample_time_series_data

        result = ds.window(
            tumbling_window("timestamp", "1 hour", "30 minutes"), Sum("amount")
        )

        assert result.count() > 0
        assert "sum(amount)" in result.schema().names

    def test_row_based_windows(self, sample_time_series_data):
        """Test row-based sliding windows."""
        ds = sample_time_series_data

        result = ds.slide_over(
            sliding_window("row_id", 100, alignment="CENTERED")
        ).aggregate(Mean("amount"))

        assert result.count() > 0
        assert "mean(amount)" in result.schema().names

    def test_expanding_windows(self, sample_time_series_data):
        """Test expanding (unbounded) windows."""
        ds = sample_time_series_data

        result = ds.slide_over(
            sliding_window("timestamp", "UNBOUNDED", alignment="TRAILING")
        ).aggregate(Sum("amount"))

        assert result.count() > 0
        assert "sum(amount)" in result.schema().names


class TestStatefulWindowFunctions:
    """Test window functions with stateful objects and compute strategies."""

    def test_stateful_sliding_window_with_actor_pool(self, sample_time_series_data):
        """Test sliding windows with stateful actor pool strategy."""
        ds = sample_time_series_data

        # Use ActorPoolStrategy for stateful operations
        result = ds.slide_over(
            sliding_window("timestamp", "1 hour"),
            partition_by=["user_id"],
            compute_strategy=ActorPoolStrategy(size=2),
            ray_remote_args={"num_cpus": 1},
        ).aggregate(Sum("amount"))

        assert result.count() > 0
        assert "sum(amount)" in result.schema().names

    def test_gpu_accelerated_window_operation(self, sample_iot_data):
        """Test GPU-accelerated window operations."""
        ds = sample_iot_data

        # Test GPU acceleration (if available)
        result = ds.window(
            sliding_window("timestamp", "10 minutes"),
            Mean("temperature"),
            ray_remote_args={"num_gpus": 0.1},  # Use small GPU fraction for testing
        )

        assert result.count() > 0
        assert "mean(temperature)" in result.schema().names

    def test_window_with_custom_resource_requirements(self, sample_time_series_data):
        """Test window operations with custom resource requirements."""
        ds = sample_time_series_data

        result = ds.window(
            tumbling_window("timestamp", "1 day"),
            Sum("amount"),
            ray_remote_args={"memory": 1000000, "num_cpus": 2},
        )

        assert result.count() > 0
        assert "sum(amount)" in result.schema().names


class TestWindowFunctionEdgeCases:
    """Test edge cases and error conditions."""

    def test_empty_dataset_windows(self):
        """Test window functions with empty datasets."""
        empty_ds = ray.data.from_items([])

        # Should handle empty datasets gracefully
        result = empty_ds.window(tumbling_window("timestamp", "1 day"), Count("amount"))

        assert result.count() == 0

    def test_single_row_windows(self):
        """Test window functions with single-row datasets."""
        single_row_ds = ray.data.from_items(
            [{"timestamp": "2023-01-01", "amount": 100}]
        )

        result = single_row_ds.slide_over(
            sliding_window("timestamp", "1 hour")
        ).aggregate(Sum("amount"))

        assert result.count() == 1

    def test_invalid_window_specifications(self):
        """Test error handling for invalid window specifications."""
        ds = ray.data.from_items([{"timestamp": "2023-01-01", "amount": 100}])

        # Invalid window spec type
        with pytest.raises(ValueError):
            ds.slide_over("invalid_window_spec")

        # Window spec that's not a SlidingWindow
        with pytest.raises(ValueError, match="slide_over only supports SlidingWindow"):
            ds.slide_over(tumbling_window("timestamp", "1 day"))


class TestWindowExpressions:
    """Test window expressions API."""

    def test_window_expr_creation(self):
        """Test creating window expressions."""
        from ray.data.expressions import col, window_expr

        # Test basic window expression
        expr = window_expr(col("amount"), sliding_window("timestamp", "1 hour"), "mean")
        assert expr.column is not None
        assert expr.window_spec is not None
        assert expr.function == "mean"

    def test_rolling_expr_creation(self):
        """Test creating rolling expressions."""
        from ray.data.expressions import col, rolling

        # Test basic rolling expression
        expr = rolling(col("amount"), "1 hour", "timestamp", function="sum")
        assert expr.column is not None
        assert expr.window_size == "1 hour"
        assert expr.window_column == "timestamp"
        assert expr.function == "sum"

    def test_window_expressions_with_dataset(self, sample_time_series_data):
        """Test using window expressions with Dataset.with_column()."""
        ds = sample_time_series_data

        # Test window expression
        from ray.data.expressions import col, window_expr

        result = ds.with_column(
            "rolling_avg",
            window_expr(col("amount"), sliding_window("timestamp", "1 hour"), "mean"),
        )

        assert result.count() > 0
        assert "rolling_avg" in result.schema().names


class TestAdvancedWindowFunctions:
    """Test advanced window functions like ranking, lag, and lead."""

    def test_ranking_functions(self, sample_time_series_data):
        """Test ranking window functions."""
        ds = sample_time_series_data

        # Test rank function
        result = ds.window(
            rank_window(partition_by=["user_id"], order_by=["amount"]), Rank("amount")
        )

        assert result.count() > 0
        assert "rank(amount)" in result.schema().names

        # Test dense_rank function
        result = ds.window(
            rank_window(partition_by=["user_id"], order_by=["amount"]),
            DenseRank("amount"),
        )

        assert result.count() > 0
        assert "dense_rank(amount)" in result.schema().names

        # Test row_number function
        result = ds.window(
            rank_window(partition_by=["user_id"], order_by=["timestamp"]),
            RowNumber("timestamp"),
        )

        assert result.count() > 0
        assert "row_number(timestamp)" in result.schema().names

    def test_lag_lead_functions(self, sample_time_series_data):
        """Test lag and lead window functions."""
        ds = sample_time_series_data

        # Test lag function
        result = ds.window(
            lag_window("amount", 1, partition_by=["user_id"], order_by=["timestamp"]),
            Lag("amount"),
        )

        assert result.count() > 0
        assert "lag(amount, 1)" in result.schema().names

        # Test lead function
        result = ds.window(
            lead_window("amount", 2, partition_by=["user_id"], order_by=["timestamp"]),
            Lead("amount"),
        )

        assert result.count() > 0
        assert "lead(amount, 2)" in result.schema().names

    def test_ranking_without_partitioning(self, sample_time_series_data):
        """Test ranking functions without partitioning."""
        ds = sample_time_series_data

        # Test rank across entire dataset
        result = ds.window(rank_window(order_by=["amount"]), Rank("amount"))

        assert result.count() > 0
        assert "rank(amount)" in result.schema().names

    def test_lag_lead_with_custom_offsets(self, sample_time_series_data):
        """Test lag and lead with custom offset values."""
        ds = sample_time_series_data

        # Test lag with offset 3
        result = ds.window(
            lag_window("amount", 3, partition_by=["user_id"], order_by=["timestamp"]),
            Lag("amount", offset=3),
        )

        assert result.count() > 0
        assert "lag(amount, 3)" in result.schema().names

        # Test lead with offset 5
        result = ds.window(
            lead_window("amount", 5, partition_by=["user_id"], order_by=["timestamp"]),
            Lead("amount", offset=5),
        )

        assert result.count() > 0
        assert "lead(amount, 5)" in result.schema().names


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__])
