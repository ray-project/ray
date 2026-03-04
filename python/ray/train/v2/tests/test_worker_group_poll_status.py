from unittest.mock import create_autospec

import pytest

import ray
from ray.train.v2._internal.execution.worker_group.poll import (
    ERR_CHAR_LIMIT,
    PollTask,
    WorkerGroupPollStatus,
    WorkerStatus,
    WorldRankToOngoingPoll,
    _normalize_error_string,
)


def test_get_error_string_basic():
    """
    Simulate four workers, two with the same error, one with a different error,
    and one without an error.
    """

    statuses = {
        0: WorkerStatus(running=False, error=ValueError("An error")),
        1: WorkerStatus(running=False, error=None),
        2: WorkerStatus(running=False, error=RuntimeError("Different error")),
        3: WorkerStatus(running=False, error=ValueError("An error")),
    }
    poll_status = WorkerGroupPollStatus(worker_statuses=statuses)
    error_str = poll_status.get_error_string()

    expected_error_str = (
        "[Rank 0,3 Error Snippet]:\nAn error\n[Rank 2 Error Snippet]:\nDifferent error"
    )
    assert error_str == expected_error_str


def test_get_error_string_with_numbers():
    """
    Simulate workers with similar errors that differ only by numbers.
    These should be grouped together.
    """
    statuses = {
        0: WorkerStatus(
            running=False, error=ValueError("Error parsing object at 0x7f8b12345678")
        ),
        1: WorkerStatus(
            running=False, error=ValueError("Error parsing object at 0x7f8b12345679")
        ),
    }
    poll_status = WorkerGroupPollStatus(worker_statuses=statuses)
    error_str = poll_status.get_error_string()

    assert (
        error_str == "[Rank 0,1 Error Snippet]:\nError parsing object at 0x7f8b12345678"
    )


def test_get_error_string_long_error():
    """
    Simulate two workers with identical long error string.
    """
    long_error_str = "test string" * 200
    statuses = {
        0: WorkerStatus(running=False, error=long_error_str),
        1: WorkerStatus(running=False, error=long_error_str),
    }
    poll_status = WorkerGroupPollStatus(worker_statuses=statuses)
    error_str = poll_status.get_error_string()

    expected_error_str = (
        "[Rank 0,1 Error Snippet]:\n"
        + long_error_str[: ERR_CHAR_LIMIT // 2]
        + "...\n... (Output truncated. See individual worker logs for full details) ...\n"
        + long_error_str[len(long_error_str) - ERR_CHAR_LIMIT // 2 :]
    )
    assert error_str == expected_error_str


def test_normalize_error_string():
    """Test that _normalize_error_string properly handles all types of numbers."""
    error = """Traceback (most recent call last):
File "/home/ray/default/train_benchmark.py", line 35, in train_fn_per_worker
File "/tmp/ray/session_2025-08-07_23-49-55_617067_2585/runtime_resources/working_dir_files/_ray_pkg_5abd79ca51ba0ed4/runner.py", line 282, in run"""
    result = _normalize_error_string(error)

    assert (
        result
        == """Traceback (most recent call last):
File "/home/ray/default/train_benchmark.py", line <NUM>, in train_fn_per_worker
File "/tmp/ray/session_<NUM>-<NUM>-<NUM>_<NUM>-<NUM>-<NUM>_<NUM>_<NUM>/runtime_resources/working_dir_files/_ray_pkg_<NUM>abd<NUM>ca<NUM>ba<NUM>ed<NUM>/runner.py", line <NUM>, in run"""
    )


def test_world_rank_to_ongoing_poll():
    """Test all methods on WorldRankToOngoingPoll class."""
    polls = WorldRankToOngoingPoll()

    # Empty state
    assert polls.get(0) is None
    assert 0 not in polls

    # setdefault inserts and returns the task
    task0 = PollTask(start_time=1.0, task=create_autospec(ray.ObjectRef, instance=True))
    result = polls.setdefault(0, task0)
    assert result is task0
    assert polls.get(0) is task0
    assert 0 in polls

    # setdefault does not overwrite existing entry
    task0_new = PollTask(
        start_time=2.0, task=create_autospec(ray.ObjectRef, instance=True)
    )
    result = polls.setdefault(0, task0_new)
    assert result is task0

    # pop removes and returns the task
    popped = polls.pop(0)
    assert popped is task0
    assert 0 not in polls

    # pop on missing rank returns None
    assert polls.pop(99) is None

    # remove_ranks removes specified ranks
    task1 = PollTask(start_time=1.0, task=create_autospec(ray.ObjectRef, instance=True))
    task2 = PollTask(start_time=2.0, task=create_autospec(ray.ObjectRef, instance=True))
    task3 = PollTask(start_time=3.0, task=create_autospec(ray.ObjectRef, instance=True))
    polls.setdefault(1, task1)
    polls.setdefault(2, task2)
    polls.setdefault(3, task3)
    polls.remove_ranks([1, 3])
    assert 1 not in polls
    assert 2 in polls
    assert 3 not in polls

    # clear removes all
    polls.clear()
    assert 2 not in polls


def test_failing_replica_group_indices():
    """Test that failing_replica_group_indices maps worker failures to replica groups."""
    statuses = {
        0: WorkerStatus(running=True),
        1: WorkerStatus(running=True),
        2: WorkerStatus(running=True),
        3: WorkerStatus(running=False, error=RuntimeError("Worker 3 failed")),
    }
    poll_status = WorkerGroupPollStatus(
        worker_statuses=statuses,
        worker_rank_to_replica_group_rank={0: 0, 1: 0, 2: 1, 3: 1},
    )
    assert poll_status.failing_replica_group_indices == {1}


def test_failing_replica_group_indices_no_mapping():
    """Test that failing_replica_group_indices returns empty set when no mapping."""
    statuses = {
        0: WorkerStatus(running=False, error=RuntimeError("fail")),
    }
    poll_status = WorkerGroupPollStatus(worker_statuses=statuses)
    assert poll_status.failing_replica_group_indices == set()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
