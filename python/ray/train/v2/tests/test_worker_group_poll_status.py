import pytest

from ray.train.v2._internal.execution.worker_group.poll import (
    ERR_CHAR_LIMIT,
    WorkerGroupPollStatus,
    WorkerStatus,
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
