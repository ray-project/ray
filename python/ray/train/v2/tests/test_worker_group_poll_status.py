import pytest

from ray.train.v2._internal.execution.worker_group.poll import (
    ERR_CHAR_LIMIT,
    WorkerGroupPollStatus,
    WorkerStatus,
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

    expected_error_str = "[Rank 0, 3]:\nAn error\n[Rank 2]:\nDifferent error"
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

    assert error_str == "[Rank 0, 1]:\nError parsing object at 0x7f8b12345678"


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
        "[Rank 0, 1]:\n"
        + long_error_str[:ERR_CHAR_LIMIT]
        + "...\nView individual worker logs for more details."
    )
    assert error_str == expected_error_str


def test_get_error_string_running_worker():
    """
    Simulate two workers with identical error strings but one is running
    and the other is not.
    """
    statuses = {
        0: WorkerStatus(running=False, error="error"),
        1: WorkerStatus(running=True, error="error"),
    }
    poll_status = WorkerGroupPollStatus(worker_statuses=statuses)
    error_str = poll_status.get_error_string()

    expected_error_str = "[Rank 0]:\nerror"
    assert error_str == expected_error_str


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
