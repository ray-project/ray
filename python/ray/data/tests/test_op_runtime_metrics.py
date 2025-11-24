import time
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

import ray
from ray.data._internal.execution.interfaces import RefBundle
from ray.data._internal.execution.interfaces.op_runtime_metrics import (
    OpRuntimeMetrics,
)
from ray.data._internal.util import KiB
from ray.data.block import BlockExecStats, BlockMetadata


def test_average_max_uss_per_task():
    # No tasks submitted yet.
    metrics = OpRuntimeMetrics(MagicMock())
    assert metrics.average_max_uss_per_task is None

    def create_bundle(uss_bytes: int):
        block = ray.put(pa.Table.from_pydict({}))
        stats = BlockExecStats()
        stats.max_uss_bytes = uss_bytes
        stats.wall_time_s = 0
        metadata = BlockMetadata(
            num_rows=0,
            size_bytes=0,
            input_files=None,
            exec_stats=stats,
        )
        return RefBundle([(block, metadata)], owns_blocks=False, schema=None)

    # Submit two tasks.
    bundle = create_bundle(uss_bytes=0)
    metrics.on_task_submitted(0, bundle)
    metrics.on_task_submitted(1, bundle)
    assert metrics.average_max_uss_per_task is None

    # Generate one output for the first task.
    bundle = create_bundle(uss_bytes=1)
    metrics.on_task_output_generated(0, bundle)
    assert metrics.average_max_uss_per_task == 1

    # Generate one output for the second task.
    bundle = create_bundle(uss_bytes=3)
    metrics.on_task_output_generated(0, bundle)
    assert metrics.average_max_uss_per_task == 2  # (1 + 3) / 2 = 2


def test_task_completion_time_histogram():
    """Test task completion time histogram bucket assignment and counting."""
    metrics = OpRuntimeMetrics(MagicMock())

    # Test different completion times
    # Buckets: [0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 7.5, 10.0, 15.0, 20.0, 25.0, 50.0, 75.0, 100.0, 150.0, 500.0, 1000.0, 2500.0, 5000.0]
    test_cases = [
        (0.05, 0),  # Very fast task (0.05s) - should go to first bucket (0.1)
        (0.2, 1),  # Fast task (0.2s) - should go to second bucket (0.25)
        (0.6, 3),  # Medium task (0.6s) - should go to fourth bucket (1.0)
        (1.5, 4),  # Slower task (1.5s) - should go to fifth bucket (2.5)
        (3.0, 5),  # Slow task (3.0s) - should go to sixth bucket (5.0)
    ]

    for i, (completion_time, expected_bucket) in enumerate(test_cases):
        # Create input bundle
        input_bundle = RefBundle([], owns_blocks=False, schema=None)

        # Submit task (this will create the RunningTaskInfo with current time)
        metrics.on_task_submitted(i, input_bundle)

        # Manually adjust the start time to simulate the completion time
        metrics._running_tasks[i].start_time = time.perf_counter() - completion_time

        # Complete the task
        metrics.on_task_finished(i, None)  # None means no exception

        # Check that the correct bucket was incremented
        assert metrics.task_completion_time._bucket_counts[expected_bucket] == 1

        # Reset for next test
        metrics.task_completion_time._bucket_counts[expected_bucket] = 0


def test_block_completion_time_histogram():
    """Test block completion time histogram bucket assignment and counting."""
    metrics = OpRuntimeMetrics(MagicMock())

    # Test different block generation scenarios
    # Buckets: [0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 7.5, 10.0, 15.0, 20.0, 25.0, 50.0, 75.0, 100.0, 150.0, 500.0, 1000.0, 2500.0, 5000.0]
    test_cases = [
        (1, 0.1, 0),  # 1 block, 0.1s total time -> 0.1s per block -> bucket 0 (0.1)
        (2, 0.5, 1),  # 2 blocks, 0.5s total time -> 0.25s per block -> bucket 1 (0.25)
        (1, 0.6, 3),  # 1 block, 0.6s total time -> 0.6s per block -> bucket 3 (1.0)
        (3, 1.5, 2),  # 3 blocks, 1.5s total time -> 0.5s per block -> bucket 2 (0.5)
    ]

    for i, (num_blocks, total_time, expected_bucket) in enumerate(test_cases):
        # Create input bundle
        input_bundle = RefBundle([], owns_blocks=False, schema=None)

        # Submit task
        metrics.on_task_submitted(i, input_bundle)

        # Manually set the task info to simulate the block generation
        metrics._running_tasks[i].num_outputs = num_blocks
        metrics._running_tasks[i].cum_block_gen_time = total_time

        # Complete the task
        metrics.on_task_finished(i, None)  # None means no exception

        # Check that the correct bucket was incremented by the number of blocks
        assert (
            metrics.block_completion_time._bucket_counts[expected_bucket] == num_blocks
        )

        # Reset for next test
        metrics.block_completion_time._bucket_counts[expected_bucket] = 0


def test_block_size_bytes_histogram():
    """Test block size bytes histogram bucket assignment and counting."""
    metrics = OpRuntimeMetrics(MagicMock())

    def create_bundle_with_size(size_bytes):
        block = ray.put(pa.Table.from_pydict({}))
        stats = BlockExecStats()
        stats.max_uss_bytes = 0
        stats.wall_time_s = 0
        metadata = BlockMetadata(
            num_rows=0,
            size_bytes=size_bytes,
            input_files=None,
            exec_stats=stats,
        )
        return RefBundle([(block, metadata)], owns_blocks=False, schema=None)

    # Test different block sizes
    # Buckets: [1KB, 8KB, 64KB, 128KB, 256KB, 512KB, 1MB, 8MB, 64MB, 128MB, 256MB, 512MB, 1GB, 4GB, 16GB, 64GB, 128GB, 256GB, 512GB, 1024GB, 4096GB]
    test_cases = [
        (512, 0),  # 512 bytes -> first bucket (1KB)
        (2 * KiB, 1),  # 2 KiB -> second bucket (8KB)
        (32 * KiB, 2),  # 32 KiB -> third bucket (64KB)
        (100 * KiB, 3),  # 100 KiB -> fourth bucket (128KB)
        (500 * KiB, 5),  # 500 KiB -> sixth bucket (512KB)
    ]

    for i, (size_bytes, expected_bucket) in enumerate(test_cases):
        # Create input bundle (can be empty for this test)
        input_bundle = RefBundle([], owns_blocks=False, schema=None)

        # Submit task
        metrics.on_task_submitted(i, input_bundle)

        # Create output bundle with the size we want to test
        output_bundle = create_bundle_with_size(size_bytes)

        # Generate output
        metrics.on_task_output_generated(i, output_bundle)

        # Check that the correct bucket was incremented
        assert metrics.block_size_bytes._bucket_counts[expected_bucket] == 1

        # Reset for next test
        metrics.block_size_bytes._bucket_counts[expected_bucket] = 0


def test_block_size_rows_histogram():
    """Test block size rows histogram bucket assignment and counting."""
    metrics = OpRuntimeMetrics(MagicMock())

    def create_bundle_with_rows(num_rows):
        block = ray.put(pa.Table.from_pydict({}))
        stats = BlockExecStats()
        stats.max_uss_bytes = 0
        stats.wall_time_s = 0
        metadata = BlockMetadata(
            num_rows=num_rows,
            size_bytes=0,
            input_files=None,
            exec_stats=stats,
        )
        return RefBundle([(block, metadata)], owns_blocks=False, schema=None)

    # Test different row counts
    # Buckets: [1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000, 250000, 500000, 1000000, 2500000, 5000000, 10000000]
    test_cases = [
        (1, 0),  # 1 row -> first bucket (1)
        (3, 1),  # 3 rows -> second bucket (5)
        (7, 2),  # 7 rows -> third bucket (10)
        (15, 3),  # 15 rows -> fourth bucket (25)
        (30, 4),  # 30 rows -> fifth bucket (50)
        (75, 5),  # 75 rows -> sixth bucket (100)
    ]

    for i, (num_rows, expected_bucket) in enumerate(test_cases):
        # Create input bundle (can be empty for this test)
        input_bundle = RefBundle([], owns_blocks=False, schema=None)

        # Submit task
        metrics.on_task_submitted(i, input_bundle)

        # Create output bundle with the row count we want to test
        output_bundle = create_bundle_with_rows(num_rows)

        # Generate output
        metrics.on_task_output_generated(i, output_bundle)

        # Check that the correct bucket was incremented
        assert metrics.block_size_rows._bucket_counts[expected_bucket] == 1

        # Reset for next test
        metrics.block_size_rows._bucket_counts[expected_bucket] = 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
