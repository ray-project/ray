import pytest

from ray.air.config import FailureConfig as FailureConfigV1
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
from ray.train.v2.api.config import FailureConfig


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


@pytest.mark.parametrize("max_failures", [0, 1, 10])
def test_max_failures(max_failures):
    policy = create_failure_policy(FailureConfig(max_failures=max_failures))
    status = _worker_group_status_from_errors(
        [RuntimeError(f"Worker {i} failed") if i % 2 == 0 else None for i in range(8)]
    )
    for _ in range(max_failures):
        assert policy.make_decision(status) == FailureDecision.RETRY
    assert policy.make_decision(status) == FailureDecision.RAISE


@pytest.mark.parametrize("scheduling_failure_limit", [0, 1, 10])
def test_reschedule(scheduling_failure_limit):
    policy = create_failure_policy(
        FailureConfig(scheduling_failure_limit=scheduling_failure_limit)
    )
    status = _worker_group_resize_status_from_errors()
    for _ in range(scheduling_failure_limit):
        assert policy.make_decision(status) == FailureDecision.RETRY
    assert policy.make_decision(status) == FailureDecision.RAISE


def test_infinite_retry():
    policy = create_failure_policy(FailureConfig(max_failures=-1))
    status = _worker_group_status_from_errors(
        [RuntimeError(f"Worker {i} failed") if i % 2 == 0 else None for i in range(8)]
    )
    for _ in range(10):
        assert policy.make_decision(status) == FailureDecision.RETRY


def test_infinite_reschedule():
    policy = create_failure_policy(FailureConfig(scheduling_failure_limit=-1))
    status = _worker_group_resize_status_from_errors()
    for _ in range(10):
        assert policy.make_decision(status) == FailureDecision.RETRY


def test_failure_config_v1():
    policy = create_failure_policy(FailureConfigV1(max_failures=-1))
    status = _worker_group_resize_status_from_errors()
    for _ in range(10):
        assert policy.make_decision(status) == FailureDecision.RETRY


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
