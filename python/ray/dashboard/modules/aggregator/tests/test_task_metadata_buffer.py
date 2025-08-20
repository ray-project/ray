import pytest
import sys

from ray.dashboard.modules.aggregator.task_metadata_buffer import TaskMetadataBuffer
from ray.core.generated.events_event_aggregator_service_pb2 import TaskEventsMetadata


def _create_test_metadata(dropped_task_ids: list = None, attempt_number=1):
    """Helper function to create test metadata"""
    metadata = TaskEventsMetadata()
    if dropped_task_ids:
        for task_id in dropped_task_ids:
            attempt = metadata.dropped_task_attempts.add()
            attempt.task_id = task_id.encode()
            attempt.attempt_number = attempt_number
    return metadata


def _result_to_attempts_list(result):
    """Normalize return value from buffer.get() to a python list of attempts."""
    if hasattr(result, "dropped_task_attempts"):
        attempts = result.dropped_task_attempts
    else:
        attempts = result
    return list(attempts)


def _drain_all_attempts(buffer: TaskMetadataBuffer):
    """Drain the buffer completely via public API and return list of bytes task_ids.

    Continues calling get() until it returns an empty set of attempts.
    """
    collected_ids = []
    while True:
        result = buffer.get()
        attempts = _result_to_attempts_list(result)
        if len(attempts) == 0:
            break
        collected_ids.extend([a.task_id for a in attempts])
    return collected_ids


class TestTaskMetadataBuffer:
    """tests for TaskMetadataBuffer class"""

    def test_merge_and_get(self):
        """Test merging multiple metadata objects and verify task attempts are combined."""
        buffer = TaskMetadataBuffer(
            max_buffer_size=100, max_dropped_attempts_per_metadata_entry=10
        )

        # Create two separate metadata objects with different task IDs
        metadata1 = _create_test_metadata(["task_1", "task_2"])
        metadata2 = _create_test_metadata(["task_3", "task_4"])

        # Merge both metadata objects
        buffer.merge(metadata1)
        buffer.merge(metadata2)

        # Get the merged results
        result = buffer.get()
        attempts = _result_to_attempts_list(result)

        # Verify we have all 4 task attempts
        assert len(attempts) == 4

        # Verify all expected task IDs are present
        task_ids = [attempt.task_id for attempt in attempts]
        assert sorted(task_ids) == [b"task_1", b"task_2", b"task_3", b"task_4"]

    @pytest.mark.parametrize(
        "max_attempts,num_tasks,max_buffer_size,expected_drop_attempts",
        [
            # No overflow, possibly multiple flushes. Ensure all tasks preserved.
            (2, 3, 100, 0),
            (5, 10, 100, 0),
            # Overflow scenario: buffer too small, ensure drop count is tracked.
            (1, 4, 2, 1),
        ],
    )
    def test_buffer_flushing_and_drop_count(
        self, max_attempts, num_tasks, max_buffer_size, expected_drop_attempts
    ):
        buffer = TaskMetadataBuffer(
            max_buffer_size=max_buffer_size,
            max_dropped_attempts_per_metadata_entry=max_attempts,
        )

        for i in range(num_tasks):
            test_metadata = _create_test_metadata([f"task_{i}"])
            buffer.merge(test_metadata)

        # Verify dropped count
        dropped_count = buffer.get_and_reset_dropped_metadata_count()
        assert dropped_count == expected_drop_attempts
        # Subsequent call should be reset to 0
        assert buffer.get_and_reset_dropped_metadata_count() == 0

        # Drain everything and verify conservation of attempts
        drained_ids = _drain_all_attempts(buffer)
        assert len(drained_ids) == num_tasks - expected_drop_attempts

        # Draining again should yield nothing
        assert len(_result_to_attempts_list(buffer.get())) == 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
