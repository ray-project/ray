import time
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

import ray
from ray.data._internal.execution.interfaces import RefBundle
from ray.data._internal.execution.interfaces.op_runtime_metrics import (
    OpRuntimeMetrics,
)
from ray.data._internal.execution.interfaces.physical_operator import (
    TaskExecDriverStats,
)
from ray.data._internal.util import KiB
from ray.data.block import BlockExecStats, BlockMetadata, TaskExecStats
from ray.data.context import MAX_SAFE_BLOCK_SIZE_FACTOR, DataContext


def test_average_max_uss_per_task():
    # No tasks submitted yet.
    metrics = OpRuntimeMetrics(MagicMock())
    assert metrics.average_max_uss_per_task is None

    def create_bundle(uss_bytes: int):
        block = ray.put(pa.Table.from_pydict({}))
        stats = BlockExecStats()
        stats.max_uss_bytes = uss_bytes
        stats.wall_time_s = 0
        stats.block_ser_time_s = 0
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
        metrics.on_task_finished(
            i,
            None,
            TaskExecStats(task_wall_time_s=completion_time),
            TaskExecDriverStats(task_output_backpressure_s=0),
        )

        # Check that the correct bucket was incremented
        assert metrics.task_completion_time._bucket_counts[expected_bucket] == 1

        # Reset for next test
        metrics.task_completion_time._bucket_counts[expected_bucket] = 0


def test_block_completion_time_histogram():
    """Test block completion time histogram bucket assignment and counting.

    Block completion time = (cum_block_gen_time_s + cum_block_ser_time_s) / num_outputs
    """
    metrics = OpRuntimeMetrics(MagicMock())

    # Test different block generation scenarios
    # Buckets: [0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 7.5, 10.0, 15.0, 20.0, 25.0, 50.0, 75.0, 100.0, 150.0, 500.0, 1000.0, 2500.0, 5000.0]
    # Each test case: (num_blocks, gen_time, ser_time, expected_bucket)
    # Per-block time = (gen_time + ser_time) / num_blocks
    test_cases = [
        # 1 block, 0.08s gen + 0.02s ser = 0.1s total -> 0.1s per block -> bucket 0 (0.1)
        (1, 0.08, 0.02, 0),
        # 2 blocks, 0.4s gen + 0.1s ser = 0.5s total -> 0.25s per block -> bucket 1 (0.25)
        (2, 0.4, 0.1, 1),
        # 1 block, 0.5s gen + 0.1s ser = 0.6s total -> 0.6s per block -> bucket 3 (1.0)
        (1, 0.5, 0.1, 3),
        # 3 blocks, 1.2s gen + 0.3s ser = 1.5s total -> 0.5s per block -> bucket 2 (0.5)
        (3, 1.2, 0.3, 2),
    ]

    for i, (num_blocks, gen_time, ser_time, expected_bucket) in enumerate(test_cases):
        # Create input bundle
        input_bundle = RefBundle([], owns_blocks=False, schema=None)

        # Submit task
        metrics.on_task_submitted(i, input_bundle)

        # Manually set the task info to simulate the block generation
        metrics._running_tasks[i].num_outputs = num_blocks
        metrics._running_tasks[i].cum_block_gen_time_s = gen_time
        metrics._running_tasks[i].cum_block_ser_time_s = ser_time

        # Complete the task
        metrics.on_task_finished(
            i,
            None,
            TaskExecStats(task_wall_time_s=gen_time + ser_time),
            TaskExecDriverStats(task_output_backpressure_s=0),
        )

        # Check that the correct bucket was incremented by the number of blocks
        assert (
            metrics.block_completion_time._bucket_counts[expected_bucket] == num_blocks
        )

        # Reset for next test
        metrics.block_completion_time._bucket_counts[expected_bucket] = 0


@patch("time.perf_counter")
def test_task_completion_time_excl_backpressure(mock_perf_counter):
    """Test that average_task_completion_time_excl_backpressure_s correctly
    subtracts output backpressure from the driver's wall-clock task time.

    Scheduling time is estimated as the time from task submission to the first
    output arriving on the driver, minus the worker-side time to generate and
    serialize that first block.
    """
    op = MagicMock()
    op.data_context.enable_get_object_locations_for_metrics = False

    metrics = OpRuntimeMetrics(op)

    test_cases = [
        # (driver_wall_time_s, scheduling_time_s, backpressure_time_s, gen_time_s, ser_time_s, num_outputs)
        (2.0, 0.2, 0.5, 0.25, 0.05, 2),  # Task 0
        (1.5, 0.2, 0.2, 0.3, 0.05, 1),  # Task 1
        (3.0, 0.2, 1.0, 0.3, 0.05, 3),  # Task 2
    ]

    def create_output_bundle(gen_time_s, ser_time_s):
        block = ray.put(pa.Table.from_pydict({}))
        stats = BlockExecStats()
        stats.wall_time_s = gen_time_s
        stats.block_ser_time_s = ser_time_s
        stats.max_uss_bytes = 0
        metadata = BlockMetadata(
            num_rows=1,
            size_bytes=0,
            input_files=None,
            exec_stats=stats,
        )
        return RefBundle([(block, metadata)], owns_blocks=False, schema=None)

    total_gen_ser = 0
    clock = 0.0
    for i, tc in enumerate(test_cases):
        (
            driver_wall_time_s,
            scheduling_time_s,
            output_bp_time_s,
            gen_time_s,
            ser_time_s,
            num_outputs,
        ) = tc

        input_bundle = RefBundle([], owns_blocks=False, schema=None)

        # Freeze time at task submission
        submit_time = clock
        mock_perf_counter.return_value = clock
        metrics.on_task_submitted(i, input_bundle)

        # Advance clock to first output arrival on driver:
        #   time_to_first_block = scheduling + gen + ser
        clock = submit_time + scheduling_time_s + gen_time_s + ser_time_s
        mock_perf_counter.return_value = clock
        metrics.on_task_output_generated(
            i, create_output_bundle(gen_time_s, ser_time_s)
        )

        # Generate remaining outputs (won't affect scheduling time)
        for _ in range(num_outputs - 1):
            clock += gen_time_s + ser_time_s
            mock_perf_counter.return_value = clock
            metrics.on_task_output_generated(
                i, create_output_bundle(gen_time_s, ser_time_s)
            )

        total_gen_ser += num_outputs * (gen_time_s + ser_time_s)

        # Advance clock to task finish
        clock = submit_time + driver_wall_time_s
        mock_perf_counter.return_value = clock

        metrics.on_task_finished(
            i,
            None,
            TaskExecStats(task_wall_time_s=driver_wall_time_s - scheduling_time_s),
            TaskExecDriverStats(task_output_backpressure_s=output_bp_time_s),
        )

    num_tasks = len(test_cases)

    total_driver_wall_time_s = sum(t[0] for t in test_cases)
    total_scheduling_time_s = sum(t[1] for t in test_cases)
    total_output_bp_time_s = sum(t[2] for t in test_cases)

    # Raw counters
    assert metrics.task_block_gen_and_ser_time_s == pytest.approx(total_gen_ser)
    assert metrics.task_completion_time_s == pytest.approx(total_driver_wall_time_s)
    assert metrics.task_scheduling_time_s == pytest.approx(total_scheduling_time_s)
    assert metrics.task_output_backpressure_time_s == pytest.approx(
        total_output_bp_time_s
    )

    # Derived averages
    assert metrics.average_total_task_completion_time_s == pytest.approx(
        total_driver_wall_time_s / num_tasks
    )
    assert metrics.average_task_scheduling_time_s == pytest.approx(
        total_scheduling_time_s / num_tasks
    )
    assert metrics.average_task_output_backpressure_time_s == pytest.approx(
        total_output_bp_time_s / num_tasks
    )
    assert metrics.average_task_completion_time_excl_backpressure_s == pytest.approx(
        (total_driver_wall_time_s - total_output_bp_time_s) / num_tasks
    )


def test_block_size_bytes_histogram():
    """Test block size bytes histogram bucket assignment and counting."""
    metrics = OpRuntimeMetrics(MagicMock())

    def create_bundle_with_size(size_bytes):
        block = ray.put(pa.Table.from_pydict({}))
        stats = BlockExecStats()
        stats.max_uss_bytes = 0
        stats.wall_time_s = 0
        stats.block_ser_time_s = 0
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
        stats.block_ser_time_s = 0
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


@pytest.fixture
def metrics_config_no_sample_with_target(restore_data_context):  # noqa: F811
    """Fixture for no-sample scenario with target_max_block_size set."""
    ctx = DataContext.get_current()
    ctx.target_max_block_size = 128 * 1024 * 1024  # 128MB
    ctx._max_num_blocks_in_streaming_gen_buffer = 2

    op = MagicMock()
    op.data_context = ctx
    metrics = OpRuntimeMetrics(op)
    return metrics


@pytest.fixture
def metrics_config_no_sample_with_none(restore_data_context):  # noqa: F811
    """Fixture for no-sample scenario with target_max_block_size=None."""
    ctx = DataContext.get_current()
    ctx.target_max_block_size = None
    ctx._max_num_blocks_in_streaming_gen_buffer = 1

    op = MagicMock()
    op.data_context = ctx
    metrics = OpRuntimeMetrics(op)
    return metrics


@pytest.fixture
def metrics_config_with_sample(restore_data_context):  # noqa: F811
    """Fixture for scenario with average_bytes_per_output available."""
    ctx = DataContext.get_current()
    ctx.target_max_block_size = 128 * 1024 * 1024  # 128MB
    ctx._max_num_blocks_in_streaming_gen_buffer = 1

    op = MagicMock()
    op.data_context = ctx
    metrics = OpRuntimeMetrics(op)

    # Simulate having samples: set bytes_task_outputs_generated and
    # num_task_outputs_generated to make average_bytes_per_output available
    actual_block_size = 150 * 1024 * 1024  # 150MB
    metrics.bytes_task_outputs_generated = actual_block_size
    metrics.num_task_outputs_generated = 1

    return metrics


@pytest.fixture
def metrics_config_pending_outputs_no_sample(
    restore_data_context,  # noqa: F811
):
    """Fixture for pending outputs during no-sample with target set."""
    ctx = DataContext.get_current()
    ctx.target_max_block_size = 64 * 1024 * 1024  # 64MB
    ctx._max_num_blocks_in_streaming_gen_buffer = 2

    op = MagicMock()
    op.data_context = ctx
    metrics = OpRuntimeMetrics(op)
    metrics.num_tasks_running = 3
    return metrics


@pytest.fixture
def metrics_config_pending_outputs_none(restore_data_context):  # noqa: F811
    """Fixture for pending outputs during no-sample with target=None."""
    ctx = DataContext.get_current()
    ctx.target_max_block_size = None
    ctx._max_num_blocks_in_streaming_gen_buffer = 1

    op = MagicMock()
    op.data_context = ctx
    metrics = OpRuntimeMetrics(op)
    metrics.num_tasks_running = 2
    return metrics


@pytest.mark.parametrize(
    "metrics_fixture,test_property,expected_calculator",
    [
        # When no sample is available but target_max_block_size is set, uses fallback
        (
            "metrics_config_no_sample_with_target",
            "obj_store_mem_max_pending_output_per_task",
            lambda m: (
                m._op.data_context.target_max_block_size
                * MAX_SAFE_BLOCK_SIZE_FACTOR
                * m._op.data_context._max_num_blocks_in_streaming_gen_buffer
            ),
        ),
        # When sample is available, uses average_bytes_per_output
        (
            "metrics_config_with_sample",
            "obj_store_mem_max_pending_output_per_task",
            lambda m: (
                m.average_bytes_per_output
                * m._op.data_context._max_num_blocks_in_streaming_gen_buffer
            ),
        ),
        # When no sample is available but target_max_block_size is set, uses fallback
        (
            "metrics_config_pending_outputs_no_sample",
            "obj_store_mem_pending_task_outputs",
            lambda m: (
                m.num_tasks_running
                * m._op.data_context.target_max_block_size
                * MAX_SAFE_BLOCK_SIZE_FACTOR
                * m._op.data_context._max_num_blocks_in_streaming_gen_buffer
            ),
        ),
    ],
)
def test_obj_store_mem_estimation(
    request, metrics_fixture, test_property, expected_calculator
):
    """Test object store memory estimation for various scenarios."""
    metrics = request.getfixturevalue(metrics_fixture)
    actual = getattr(metrics, test_property)
    expected = expected_calculator(metrics)

    assert (
        actual == expected
    ), f"Expected {test_property} to be {expected}, got {actual}"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
