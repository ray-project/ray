"""
Advanced Window Functions Example for Ray Data.

This example demonstrates the advanced window functions including:
- Ranking functions (rank, dense_rank, row_number)
- Lag and Lead functions
- Partitioning and ordering
- GPU acceleration and stateful operations
"""

import ray
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

from ray.data.window import (
    sliding_window,
    tumbling_window,
    session_window,
    rank_window,
    lag_window,
    lead_window,
)
from ray.data.aggregate import (
    Std,
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
from ray.data._internal.compute import ActorPoolStrategy


def create_sample_data():
    """Create sample data for demonstrating window functions."""
    data = []
    base_time = datetime(2023, 1, 1, 9, 0, 0)

    # Create time series data with multiple users and regions
    for i in range(1000):
        timestamp = base_time + timedelta(minutes=i * 5)
        user_id = f"user_{i % 20}"
        region = f"region_{i % 5}"
        amount = 10 + (i % 100) + np.random.normal(0, 10)
        transaction_id = f"tx_{i}"

        # Add some anomalies
        if i % 50 == 0:
            amount = 1000 + (i % 500)

        data.append(
            {
                "timestamp": timestamp,
                "user_id": user_id,
                "region": region,
                "amount": amount,
                "transaction_id": transaction_id,
                "row_id": i,
            }
        )

    return ray.data.from_items(data)


def demonstrate_ranking_functions(ds):
    """Demonstrate ranking window functions."""
    print("\n=== Ranking Functions ===")

    # 1. Rank users by amount within each region
    print("1. Ranking users by amount within each region:")
    result = ds.window(
        rank_window(partition_by=["region"], order_by=["amount"]), Rank("amount")
    )

    print(f"   Result count: {result.count()}")
    print(f"   Schema: {result.schema().names}")

    # Show sample results
    sample = result.take(5)
    print("   Sample results:")
    for row in sample:
        print(f"     {row}")

    # 2. Dense rank users by amount within each region
    print("\n2. Dense ranking users by amount within each region:")
    result = ds.window(
        rank_window(partition_by=["region"], order_by=["amount"]), DenseRank("amount")
    )

    print(f"   Result count: {result.count()}")

    # 3. Row numbering within each user partition
    print("\n3. Row numbering within each user partition:")
    result = ds.window(
        rank_window(partition_by=["user_id"], order_by=["timestamp"]),
        RowNumber("timestamp"),
    )

    print(f"   Result count: {result.count()}")

    # 4. Global ranking (no partitioning)
    print("\n4. Global ranking by amount:")
    result = ds.window(rank_window(order_by=["amount"]), Rank("amount"))

    print(f"   Result count: {result.count()}")


def demonstrate_lag_lead_functions(ds):
    """Demonstrate lag and lead window functions."""
    print("\n=== Lag and Lead Functions ===")

    # 1. Get previous day's amount for each user
    print("1. Previous day's amount for each user:")
    result = ds.window(
        lag_window("amount", 1, partition_by=["user_id"], order_by=["timestamp"]),
        Lag("amount"),
    )

    print(f"   Result count: {result.count()}")
    print(f"   Schema: {result.schema().names}")

    # Show sample results
    sample = result.take(5)
    print("   Sample results:")
    for row in sample:
        print(f"     {row}")

    # 2. Get next day's amount for each user
    print("\n2. Next day's amount for each user:")
    result = ds.window(
        lead_window("amount", 1, partition_by=["user_id"], order_by=["timestamp"]),
        Lead("amount"),
    )

    print(f"   Result count: {result.count()}")

    # 3. Get amount from 3 rows ago
    print("\n3. Amount from 3 rows ago:")
    result = ds.window(
        lag_window("amount", 3, partition_by=["user_id"], order_by=["timestamp"]),
        Lag("amount", offset=3),
    )

    print(f"   Result count: {result.count()}")

    # 4. Get amount from 2 rows ahead
    print("\n4. Amount from 2 rows ahead:")
    result = ds.window(
        lead_window("amount", 2, partition_by=["user_id"], order_by=["timestamp"]),
        Lead("amount", offset=2),
    )

    print(f"   Result count: {result.count()}")


def demonstrate_advanced_sliding_windows(ds):
    """Demonstrate advanced sliding window features."""
    print("\n=== Advanced Sliding Windows ===")

    # 1. Sliding window with partitioning and ordering
    print("1. Sliding window with partitioning and ordering:")
    result = ds.window(
        sliding_window(
            "timestamp", "1 hour", partition_by=["user_id"], order_by=["timestamp"]
        ),
        Mean("amount"),
        Sum("amount"),
        Count("transaction_id"),
    )

    print(f"   Result count: {result.count()}")
    print(f"   Schema: {result.schema().names}")

    # 2. GPU-accelerated sliding window
    print("\n2. GPU-accelerated sliding window:")
    result = ds.window(
        sliding_window("timestamp", "30 minutes", partition_by=["region"]),
        Mean("amount"),
        ray_remote_args={"num_gpus": 0.1},  # Use small GPU fraction for testing
    )

    print(f"   Result count: {result.count()}")

    # 3. Stateful sliding window with actor pool
    print("\n3. Stateful sliding window with actor pool:")
    result = ds.window(
        sliding_window("timestamp", "2 hours", partition_by=["user_id"]),
        Sum("amount"),
        compute_strategy=ActorPoolStrategy(size=2),
        ray_remote_args={"num_cpus": 1},
    )

    print(f"   Result count: {result.count()}")


def demonstrate_advanced_tumbling_windows(ds):
    """Demonstrate advanced tumbling window features."""
    print("\n=== Advanced Tumbling Windows ===")

    # 1. Tumbling window with custom step (overlapping)
    print("1. Tumbling window with custom step (overlapping):")
    result = ds.window(
        tumbling_window("timestamp", "1 hour", "30 minutes", partition_by=["region"]),
        Sum("amount"),
        Mean("amount"),
        Count("transaction_id"),
    )

    print(f"   Result count: {result.count()}")
    print(f"   Schema: {result.schema().names}")

    # 2. Tumbling window with custom start time
    print("\n2. Tumbling window with custom start time:")
    start_time = datetime(2023, 1, 1, 0, 0, 0)
    result = ds.window(
        tumbling_window(
            "timestamp", "1 day", start=start_time, partition_by=["region"]
        ),
        Sum("amount"),
    )

    print(f"   Result count: {result.count()}")

    # 3. Row-based tumbling windows
    print("\n3. Row-based tumbling windows:")
    result = ds.window(
        tumbling_window("row_id", 100, partition_by=["user_id"]),
        Mean("amount"),
        Max("amount"),
        Min("amount"),
    )

    print(f"   Result count: {result.count()}")


def demonstrate_session_windows(ds):
    """Demonstrate session window features."""
    print("\n=== Session Windows ===")

    # 1. Session window with 15-minute gap
    print("1. Session window with 15-minute gap:")
    result = ds.window(
        session_window("timestamp", "15 minutes", partition_by=["user_id"]),
        Sum("amount"),
        Count("transaction_id"),
        Mean("amount"),
    )

    print(f"   Result count: {result.count()}")
    print(f"   Schema: {result.schema().names}")

    # 2. Session window with 1-hour gap
    print("\n2. Session window with 1-hour gap:")
    result = ds.window(
        session_window("timestamp", "1 hour", partition_by=["region"]), Sum("amount")
    )

    print(f"   Result count: {result.count()}")


def demonstrate_performance_features(ds):
    """Demonstrate performance and resource management features."""
    print("\n=== Performance Features ===")

    # 1. Custom resource requirements
    print("1. Custom resource requirements:")
    result = ds.window(
        sliding_window("timestamp", "1 hour", partition_by=["user_id"]),
        Sum("amount"),
        ray_remote_args={
            "memory": 1000000,  # 1GB memory
            "num_cpus": 2,
            "resources": {"custom_resource": 1},
        },
    )

    print(f"   Result count: {result.count()}")

    # 2. Multiple aggregations in single window
    print("\n2. Multiple aggregations in single window:")
    result = ds.window(
        sliding_window("timestamp", "30 minutes", partition_by=["region"]),
        Sum("amount"),
        Mean("amount"),
        Count("transaction_id"),
        Max("amount"),
        Min("amount"),
        Std("amount"),
    )

    print(f"   Result count: {result.count()}")
    print(f"   Schema: {result.schema().names}")


def main():
    """Main function to demonstrate all window functions."""
    print("Advanced Window Functions Example for Ray Data")
    print("=" * 50)

    # Initialize Ray
    if not ray.is_initialized():
        ray.init()

    # Create sample data
    print("Creating sample data...")
    ds = create_sample_data()
    print(f"Dataset created with {ds.count()} rows")
    print(f"Schema: {ds.schema().names}")

    # Demonstrate all window function types
    demonstrate_ranking_functions(ds)
    demonstrate_lag_lead_functions(ds)
    demonstrate_advanced_sliding_windows(ds)
    demonstrate_advanced_tumbling_windows(ds)
    demonstrate_session_windows(ds)
    demonstrate_performance_features(ds)

    print("\n" + "=" * 50)
    print("Advanced Window Functions demonstration completed!")
    print("\nKey Features Demonstrated:")
    print("- Ranking functions: rank(), dense_rank(), row_number()")
    print("- Lag/Lead functions: lag(), lead() with custom offsets")
    print("- Advanced partitioning and ordering")
    print("- GPU acceleration and stateful operations")
    print("- Custom resource management")
    print("- Multiple aggregation functions per window")

    # Shutdown Ray
    ray.shutdown()


if __name__ == "__main__":
    main()
