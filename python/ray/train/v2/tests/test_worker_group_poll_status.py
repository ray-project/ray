from ray.train.v2._internal.execution.worker_group.poll import (
    WorkerGroupPollStatus,
    WorkerStatus,
)


def test_get_error_string_basic():
    """
    Simulate four workers, two with the same error, one with a different error,
    and one without an error.
    """

    error1 = ValueError("Test error 1")
    error2 = RuntimeError("Test error 2")
    statuses = {
        0: WorkerStatus(running=False, error=error1),
        1: WorkerStatus(running=False, error=None),
        2: WorkerStatus(running=False, error=error2),
        3: WorkerStatus(running=False, error=error1),  # Same error as rank 0
    }
    poll_status = WorkerGroupPollStatus(worker_statuses=statuses)
    error_str = poll_status.get_error_string()

    expected_error_str = "[Rank 0, 3]: \nTest error 1\n[Rank 2]: \nTest error 2"
    assert error_str == expected_error_str
