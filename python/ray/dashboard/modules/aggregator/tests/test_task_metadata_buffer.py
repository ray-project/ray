import sys

import pytest

from ray.core.generated.events_event_aggregator_service_pb2 import TaskEventsMetadata
from ray.dashboard.modules.aggregator.task_metadata_buffer import TaskMetadataBuffer


def _create_test_metadata(dropped_task_ids: list = None, attempt_number=1):
    """Helper function to create test metadata"""
    metadata = TaskEventsMetadata()
    if dropped_task_ids:
        for task_id in dropped_task_ids:
            attempt = metadata.dropped_task_attempts.add()
            attempt.task_id = task_id.encode()
            attempt.attempt_number = attempt_number
    return metadata


async def _result_to_attempts_list(result):
    """Normalize return value from buffer.get() to a python list of attempts."""
    if hasattr(result, "dropped_task_attempts"):
        attempts = result.dropped_task_attempts
    else:
        attempts = result
    return list(attempts)


async def _drain_all_attempts(buffer: TaskMetadataBuffer):
    """Drain the buffer completely via public API and return list of bytes task_ids.

    Continues calling get() until it returns an empty set of attempts.
    """
    collected_ids = []
    num_metadata_entries = 0
    while True:
        result = await buffer.get()
        attempts = await _result_to_attempts_list(result)
        if len(attempts) == 0:
            break

        num_metadata_entries += 1
        collected_ids.extend([a.task_id for a in attempts])
    return collected_ids, num_metadata_entries


class TestTaskMetadataBuffer:
    """tests for TaskMetadataBuffer class"""

    @pytest.mark.asyncio
    async def test_merge_and_get(self):
        """Test merging multiple metadata objects and verify task attempts are combined."""
        buffer = TaskMetadataBuffer(
            max_buffer_size=100, max_dropped_attempts_per_metadata_entry=10
        )

        # Create two separate metadata objects with different task IDs
        metadata1 = _create_test_metadata(["task_1", "task_2"])
        metadata2 = _create_test_metadata(["task_3", "task_4"])

        # Merge both metadata objects
        await buffer.merge(metadata1)
        await buffer.merge(metadata2)

        # Get the merged results
        result = await buffer.get()
        attempts = await _result_to_attempts_list(result)

        # Verify we have all 4 task attempts
        assert len(attempts) == 4

        # Verify all expected task IDs are present
        task_ids = [attempt.task_id for attempt in attempts]
        assert sorted(task_ids) == [b"task_1", b"task_2", b"task_3", b"task_4"]

    @pytest.mark.parametrize(
        "max_attempts_per_metadata_entry,num_tasks,max_buffer_size,expected_drop_attempts,expected_num_metadata_entries",
        [
            # No overflow, two metadata entries should be created
            (2, 3, 100, 0, 2),
            # No overflow, three metadata entries should be created
            (5, 15, 100, 0, 3),
            # Overflow scenario: buffer too small, ensure drop count is tracked.
            (1, 4, 2, 2, 2),
        ],
    )
    @pytest.mark.asyncio
    async def test_buffer_merge_and_overflow(
        self,
        max_attempts_per_metadata_entry,
        num_tasks,
        max_buffer_size,
        expected_drop_attempts,
        expected_num_metadata_entries,
    ):
        buffer = TaskMetadataBuffer(
            max_buffer_size=max_buffer_size,
            max_dropped_attempts_per_metadata_entry=max_attempts_per_metadata_entry,
        )

        for i in range(num_tasks):
            test_metadata = _create_test_metadata([f"task_{i}"])
            await buffer.merge(test_metadata)

        # Drain everything and verify number of attempts in buffer is as expected
        drained_ids, num_metadata_entries = await _drain_all_attempts(buffer)
        assert len(drained_ids) == num_tasks - expected_drop_attempts
        assert num_metadata_entries == expected_num_metadata_entries

        # Buffer should now be empty
        assert len(await _result_to_attempts_list(await buffer.get())) == 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
