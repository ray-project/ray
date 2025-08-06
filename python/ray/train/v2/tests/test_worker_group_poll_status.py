import pytest

from ray.train.v2._internal.execution.worker_group.poll import (
    WorkerGroupPollStatus,
    WorkerStatus,
)


def test_get_error_string_basic():
    """
    Simulate four workers, two with the same error, one with a different error,
    and one without an error.
    """

    statuses = {
        0: WorkerStatus(running=False, error=ValueError("Test error 1")),
        1: WorkerStatus(running=False, error=None),
        2: WorkerStatus(running=False, error=RuntimeError("Test error 2")),
        3: WorkerStatus(running=False, error=ValueError("Test error 1")),
    }
    poll_status = WorkerGroupPollStatus(worker_statuses=statuses)
    error_str = poll_status.get_error_string()

    expected_error_str = "[Rank 0, 3]: \nTest error 1\n[Rank 2]: \nTest error 2"
    assert error_str == expected_error_str


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
