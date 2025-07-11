import pytest

from ray.train import FailureConfig
from ray.train.v2._internal.exceptions import WorkerGroupStartupTimeoutError
from ray.train.v2._internal.execution.failure_handling import (
    FailureDecision,
    create_failure_policy,
)
from ray.train.v2._internal.execution.worker_group import (
    WorkerGroupPollStatus,
    WorkerGroupSchedulingStatus,
    WorkerStatus,
)


def _worker_group_status_from_errors(errors):
    return WorkerGroupPollStatus(
        worker_statuses={
            i: WorkerStatus(running=False, error=errors[i]) for i in range(len(errors))
        },
    )


def _worker_group_resize_status_from_errors():
    return WorkerGroupSchedulingStatus(
        error=WorkerGroupStartupTimeoutError(0),
    )


def _non_retryable_scheduling_error():
    return WorkerGroupSchedulingStatus(
        error=Exception("Non-retryable scheduling error")
    )


@pytest.mark.parametrize("max_failures", [0, 1, 10])
def test_max_failures(max_failures):
    policy = create_failure_policy(FailureConfig(max_failures=max_failures))
    status = _worker_group_status_from_errors(
        [RuntimeError(f"Worker {i} failed") if i % 2 == 0 else None for i in range(8)]
    )
    for _ in range(max_failures):
        assert policy.make_decision(status.errors) == FailureDecision.RETRY
    assert policy.make_decision(status.errors) == FailureDecision.RAISE


@pytest.mark.parametrize("scheduling_failure_limit", [0, 1, 10])
def test_reschedule(scheduling_failure_limit):
    policy = create_failure_policy(
        FailureConfig(scheduling_failure_limit=scheduling_failure_limit)
    )
    status = _worker_group_resize_status_from_errors()
    for _ in range(scheduling_failure_limit):
        assert policy.make_decision(status.error) == FailureDecision.RETRY
    assert policy.make_decision(status.error) == FailureDecision.RAISE


def test_infinite_retry():
    policy = create_failure_policy(FailureConfig(max_failures=-1))
    status = _worker_group_status_from_errors(
        [RuntimeError(f"Worker {i} failed") if i % 2 == 0 else None for i in range(8)]
    )
    for _ in range(10):
        assert policy.make_decision(status.errors) == FailureDecision.RETRY


def test_non_retryable_scheduling_error():
    policy = create_failure_policy(FailureConfig(scheduling_failure_limit=10))
    status = _non_retryable_scheduling_error()
    assert policy.make_decision(status.error) == FailureDecision.RAISE


def test_infinite_reschedule():
    policy = create_failure_policy(FailureConfig(scheduling_failure_limit=-1))
    status = _worker_group_resize_status_from_errors()
    for _ in range(10):
        assert policy.make_decision(status.error) == FailureDecision.RETRY


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
