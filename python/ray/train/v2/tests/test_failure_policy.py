import pytest

from ray.train import FailureConfig
from ray.train.v2._internal.exceptions import WorkerGroupStartupTimeoutError
from ray.train.v2._internal.execution.failure_handling import (
    FailureDecision,
    create_failure_policy,
)
from ray.train.v2._internal.execution.preemption import PreemptionInfo
from ray.train.v2.api.exceptions import (
    ControllerError,
    PreemptionError,
    WorkerGroupError,
)


def _preemption_error():
    return PreemptionError(
        preemption_info=PreemptionInfo(
            deadline_ms=None, preempted_node_to_ranks={"node-a": [0, 1]}
        )
    )


def _controller_error(retryable):
    return ControllerError(
        controller_failure=WorkerGroupStartupTimeoutError(0)
        if retryable
        else Exception("Non-retryable error")
    )


def _worker_group_error_from_errors(errors):
    return WorkerGroupError(
        "Worker group failed",
        dict(enumerate(errors)),
    )


@pytest.mark.parametrize("max_failures", [0, 1, 10])
def test_max_failures(max_failures):
    policy = create_failure_policy(FailureConfig(max_failures=max_failures))

    for _ in range(max_failures):
        assert (
            policy.make_decision(
                training_failed_error=_worker_group_error_from_errors(
                    [RuntimeError(f"Worker {i} failed") for i in range(8)]
                )
            )
            == FailureDecision.RETRY
        )
    assert (
        policy.make_decision(
            training_failed_error=_worker_group_error_from_errors(
                [RuntimeError(f"Worker {i} failed") for i in range(8)]
            )
        )
        == FailureDecision.RAISE
    )


@pytest.mark.parametrize("controller_failure_limit", [0, 1, 10])
def test_max_controller_failures(controller_failure_limit):
    policy = create_failure_policy(
        FailureConfig(controller_failure_limit=controller_failure_limit)
    )
    controller_error = _controller_error(retryable=True)
    for _ in range(controller_failure_limit):
        assert (
            policy.make_decision(training_failed_error=controller_error)
            == FailureDecision.RETRY
        )
    assert (
        policy.make_decision(training_failed_error=controller_error)
        == FailureDecision.RAISE
    )


def test_infinite_retry():
    policy = create_failure_policy(FailureConfig(max_failures=-1))
    for _ in range(10):
        assert (
            policy.make_decision(
                training_failed_error=WorkerGroupError(
                    "Worker group resize failed",
                    {0: WorkerGroupStartupTimeoutError(0)},
                )
            )
            == FailureDecision.RETRY
        )


def test_non_retryable_error():
    policy = create_failure_policy(FailureConfig(controller_failure_limit=10))
    controller_error = _controller_error(retryable=False)
    assert (
        policy.make_decision(training_failed_error=controller_error)
        == FailureDecision.RAISE
    )


def test_infinite_controller_failure_retry():
    policy = create_failure_policy(FailureConfig(controller_failure_limit=-1))
    controller_error = _controller_error(retryable=True)
    for _ in range(10):
        assert (
            policy.make_decision(training_failed_error=controller_error)
            == FailureDecision.RETRY
        )


@pytest.mark.parametrize("max_preemption_failures", [0, 1, 10])
def test_max_preemption_failures(max_preemption_failures):
    policy = create_failure_policy(
        FailureConfig(max_preemption_failures=max_preemption_failures)
    )
    for _ in range(max_preemption_failures):
        assert (
            policy.make_decision(training_failed_error=_preemption_error())
            == FailureDecision.RETRY
        )
    assert (
        policy.make_decision(training_failed_error=_preemption_error())
        == FailureDecision.RAISE
    )


def test_infinite_preemption_retry_by_default():
    # max_preemption_failures defaults to -1 (unlimited).
    policy = create_failure_policy(FailureConfig())
    for _ in range(10):
        assert (
            policy.make_decision(training_failed_error=_preemption_error())
            == FailureDecision.RETRY
        )


def test_preemption_budget_is_separate_from_max_failures():
    # Preemptions should not be charged against `max_failures`: with no worker
    # failure budget, preemptions still retry while a real worker error raises.
    policy = create_failure_policy(
        FailureConfig(max_failures=0, max_preemption_failures=-1)
    )
    for _ in range(5):
        assert (
            policy.make_decision(training_failed_error=_preemption_error())
            == FailureDecision.RETRY
        )
    assert (
        policy.make_decision(
            training_failed_error=_worker_group_error_from_errors(
                [RuntimeError("boom")]
            )
        )
        == FailureDecision.RAISE
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
