import pytest

from ray.exceptions import RayActorError
from ray.train import FailureConfig
from ray.train.v2._internal.exceptions import WorkerHealthCheckFailedError
from ray.train.v2._internal.execution.failure_handling import (
    DefaultFailurePolicy,
    FailureDecision,
)
from ray.train.v2._internal.execution.worker_group import (
    WorkerGroupStatus,
    WorkerStatus,
)


def _worker_group_status_from_errors(errors):
    return WorkerGroupStatus(
        num_workers=len(errors),
        latest_start_time=0,
        worker_statuses={
            i: WorkerStatus(running=False, error=errors[i]) for i in range(len(errors))
        },
    )


def test_worker_group_status_has_preemption():
    class PreemptionRayActorError(RayActorError):
        def preempted(self) -> bool:
            return True

    worker_health_check_failure_error = WorkerHealthCheckFailedError(
        message="Worker health check failed due to node preemption.",
        failure=PreemptionRayActorError(),
    )
    status = _worker_group_status_from_errors(
        [None, worker_health_check_failure_error, None, RuntimeError(), None]
    )
    assert status.has_preemption_error

    status = _worker_group_status_from_errors(
        [None, RuntimeError(), None, RuntimeError(), None]
    )
    assert not status.has_preemption_error

    status = _worker_group_status_from_errors([None, None, None, None])
    assert not status.has_preemption_error


@pytest.mark.parametrize("max_failures", [0, 1, 10])
def test_max_failures(max_failures):
    policy = DefaultFailurePolicy(FailureConfig(max_failures=max_failures))
    status = _worker_group_status_from_errors(
        [RuntimeError(f"Worker {i} failed") if i % 2 == 0 else None for i in range(8)]
    )
    for _ in range(max_failures):
        assert policy.make_decision(status) == FailureDecision.RESTART
    assert policy.make_decision(status) == FailureDecision.RAISE


def test_infinite_retry():
    policy = DefaultFailurePolicy(FailureConfig(max_failures=-1))
    status = _worker_group_status_from_errors(
        [RuntimeError(f"Worker {i} failed") if i % 2 == 0 else None for i in range(8)]
    )
    for _ in range(10):
        assert policy.make_decision(status) == FailureDecision.RESTART


def test_failure_on_preemption_errors():
    """Check that the failure counts preemption errors correctly."""

    policy = DefaultFailurePolicy(FailureConfig(max_failures=0))

    class PreemptionRayActorError(RayActorError):
        def preempted(self) -> bool:
            return True

    worker_health_check_failure_error = WorkerHealthCheckFailedError(
        message="Worker health check failed due to node preemption.",
        failure=PreemptionRayActorError(),
    )

    status = _worker_group_status_from_errors(
        [None, worker_health_check_failure_error, None, RuntimeError(), None]
    )

    assert policy.make_decision(status) == FailureDecision.RESTART

    status = _worker_group_status_from_errors(
        [None, RuntimeError(), None, RuntimeError(), None]
    )

    assert policy.make_decision(status) == FailureDecision.RAISE


if __name__ == "__main__":
    pytest.main(["-v", __file__])
