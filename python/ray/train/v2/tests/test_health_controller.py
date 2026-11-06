import asyncio
import sys
from unittest.mock import MagicMock

import pytest

import ray
from ray.train.health import (
    Diagnose,
    Evict,
    HealthCheck,
    HealthConfig,
    Noop,
    Reattempt,
)
from ray.train.v2._internal.callbacks.health_callback import HealthCallback
from ray.train.v2._internal.callbacks.user_callback import UserCallbackHandler
from ray.train.v2._internal.constants import HEALTH_CHECK_INTERVAL_S_ENV_VAR
from ray.train.v2._internal.execution.callback import ControllerCallback
from ray.train.v2._internal.execution.controller import (
    TrainController,
    controller as controller_module,
)
from ray.train.v2._internal.execution.controller.state import (
    ReschedulingState,
    RestartingState,
    RunningState,
    SchedulingState,
)
from ray.train.v2._internal.execution.failure_handling import FailureDecision
from ray.train.v2._internal.execution.scaling_policy import ResizeDecision
from ray.train.v2.api.callback import UserCallback
from ray.train.v2.api.config import RunConfig, ScalingConfig
from ray.train.v2.api.exceptions import HealthCheckError
from ray.train.v2.tests.util import (
    DummyObjectRefWrapper,
    DummyWorkerGroup,
    MockFailurePolicy,
    MockScalingPolicy,
    create_dummy_run_context,
)

pytestmark = pytest.mark.usefixtures("mock_runtime_context")


class FakeHealthManager:
    """Returns the queued decisions, and records the started on-demand checks."""

    worker_probes = []

    def __init__(self, inflight_checks):
        self.decisions = []
        self.on_demand_checks = []
        self.worker_statuses = None

    def evaluate(self, worker_statuses):
        self.worker_statuses = worker_statuses
        return self.decisions.pop(0) if self.decisions else Noop()

    def start_on_demand_checks(self, checks):
        self.on_demand_checks.append(checks)


class RecordingCallback(ControllerCallback):
    def __init__(self):
        self.decisions = []

    def after_health_decision(self, health_decision):
        self.decisions.append(health_decision)


@pytest.fixture(autouse=True)
def patch_controller(monkeypatch):
    monkeypatch.setattr(TrainController, "worker_group_cls", DummyWorkerGroup)
    monkeypatch.setattr(controller_module, "HealthManager", FakeHealthManager)
    monkeypatch.setenv(HEALTH_CHECK_INTERVAL_S_ENV_VAR, "0")
    ray.init()
    yield
    ray.shutdown()


def _step(controller):
    asyncio.run(controller._run_control_loop_iteration())


def _controller(failure_policy):
    scaling_policy = MockScalingPolicy(scaling_config=ScalingConfig())
    callback = RecordingCallback()
    controller = TrainController(
        train_fn_ref=DummyObjectRefWrapper(lambda: None),
        train_run_context=create_dummy_run_context(
            run_config=RunConfig(name="test", health_config=HealthConfig()),
            scaling_config=ScalingConfig(num_workers=2),
        ),
        scaling_policy=scaling_policy,
        failure_policy=failure_policy,
        callbacks=[callback],
    )
    for _ in range(2):
        scaling_policy.queue_recovery_decision(
            ResizeDecision(num_workers=2, resources_per_worker={})
        )
    return controller, callback


def _running_controller(failure_policy):
    controller, callback = _controller(failure_policy)
    _step(controller)
    _step(controller)
    assert isinstance(controller.get_state(), RunningState)
    return controller, callback


def test_diagnose_starts_its_checks_while_training_continues():
    controller, callback = _running_controller(MockFailurePolicy(None))
    check = HealthCheck()
    controller._health_manager.decisions = [Diagnose(reason="slow", checks=[check])]

    _step(controller)
    assert isinstance(controller.get_state(), RunningState)
    assert controller._health_manager.on_demand_checks == [[check]]
    assert [d.reason for d in callback.decisions] == ["slow"]


def test_a_diagnose_that_requires_idle_workers_restarts_outside_the_failure_policy():
    failure_policy = MockFailurePolicy(None)
    failure_policy.make_decision = MagicMock()
    controller, callback = _running_controller(failure_policy)
    checks = [HealthCheck(requires_idle_workers=True), HealthCheck()]
    controller._health_manager.decisions = [Diagnose(reason="slow", checks=checks)]

    _step(controller)
    assert isinstance(controller.get_state(), RestartingState)
    failure_policy.make_decision.assert_not_called()
    assert controller._health_manager.on_demand_checks == []
    assert controller._health_callback._next_start_checks == checks
    assert [type(d) for d in callback.decisions] == [Diagnose]


def test_reattempt_goes_through_the_failure_policy():
    failure_policy = MockFailurePolicy(None)
    failure_policy.make_decision = MagicMock(return_value=FailureDecision.RETRY)
    controller, callback = _running_controller(failure_policy)
    controller._health_manager.decisions = [Reattempt(reason="loss spiked")]

    _step(controller)
    state = controller.get_state()
    assert isinstance(state, RestartingState)
    error = failure_policy.make_decision.call_args.kwargs["training_failed_error"]
    assert isinstance(error, HealthCheckError)
    assert "loss spiked" in str(error)
    assert state.training_failed_error is error
    assert [type(d) for d in callback.decisions] == [Reattempt]


def test_evict_restarts_outside_the_failure_policy():
    failure_policy = MockFailurePolicy(None)
    failure_policy.make_decision = MagicMock()
    controller, callback = _running_controller(failure_policy)
    controller._health_manager.decisions = [
        Evict(reason="hot GPU", target_nodes=["n1"])
    ]

    _step(controller)
    assert isinstance(controller.get_state(), RestartingState)
    failure_policy.make_decision.assert_not_called()
    assert [type(d) for d in callback.decisions] == [Evict]

    _step(controller)
    assert isinstance(controller.get_state(), SchedulingState)
    _step(controller)
    assert isinstance(controller.get_state(), RunningState)


@pytest.mark.parametrize(
    "decision, uses_retry_budget",
    [(Evict(reason="bad GPU", target_nodes=["n1"]), False), (Reattempt(), True)],
)
def test_a_preflight_decision_keeps_training_from_starting(
    monkeypatch, decision, uses_retry_budget
):
    failure_policy = MockFailurePolicy(None)
    failure_policy.make_decision = MagicMock(return_value=FailureDecision.RETRY)
    controller, callback = _controller(failure_policy)
    _step(controller)
    assert isinstance(controller.get_state(), SchedulingState)

    monkeypatch.setattr(DummyWorkerGroup, "_start_failure", HealthCheckError(decision))
    _step(controller)
    assert isinstance(controller.get_state(), ReschedulingState)
    assert failure_policy.make_decision.called == uses_retry_budget
    assert callback.decisions == [decision]


def _worker_group(*node_ids):
    worker_group = MagicMock()
    worker_group.get_workers.return_value = [
        MagicMock(metadata=MagicMock(node_id=node_id)) for node_id in node_ids
    ]
    return worker_group


def _health_manager(preflight_decision=None):
    health_manager = MagicMock()
    health_manager.start_and_evaluate_on_demand_checks.return_value = preflight_decision
    return health_manager


def test_health_callback_resets_each_attempt():
    health_manager = _health_manager()
    callback = HealthCallback(health_manager, preflight_checks=[])
    worker_group = _worker_group("b", "a", "b")
    callback.after_worker_group_start(worker_group)
    health_manager.reset.assert_called_once_with(["a", "b"], worker_group)
    health_manager.start_on_demand_checks.assert_not_called()

    callback.before_worker_group_shutdown(worker_group)
    health_manager.shutdown.assert_called_once()


@pytest.mark.parametrize(
    "decision", [Noop(), Diagnose(reason="slow", checks=[])], ids=["noop", "diagnose"]
)
def test_training_starts_once_the_preflight_checks_that_require_idle_workers_decide(
    decision,
):
    health_manager = _health_manager(decision)
    blocking, other = HealthCheck(requires_idle_workers=True), HealthCheck()
    callback = HealthCallback(health_manager, preflight_checks=[other, blocking])

    callback.after_worker_group_start(_worker_group("a"))
    health_manager.start_and_evaluate_on_demand_checks.assert_called_once_with(
        [blocking]
    )
    health_manager.start_on_demand_checks.assert_called_once_with([other])
    health_manager.shutdown.assert_not_called()


def test_preflight_checks_that_dont_require_idle_workers_run_alongside_training():
    health_manager = _health_manager()
    check = HealthCheck()
    callback = HealthCallback(health_manager, preflight_checks=[check])

    callback.after_worker_group_start(_worker_group("a"))
    health_manager.start_on_demand_checks.assert_called_once_with([check])
    health_manager.start_and_evaluate_on_demand_checks.assert_not_called()


@pytest.mark.parametrize(
    "decision", [Reattempt(), Evict(reason="bad GPU", target_nodes=["a"])]
)
def test_preflight_checks_that_restart_keep_training_from_starting(decision):
    health_manager = _health_manager(decision)
    callback = HealthCallback(
        health_manager,
        preflight_checks=[HealthCheck(requires_idle_workers=True), HealthCheck()],
    )

    with pytest.raises(HealthCheckError) as error:
        callback.after_worker_group_start(_worker_group("a"))
    assert error.value.decision == decision
    health_manager.start_on_demand_checks.assert_not_called()
    health_manager.shutdown.assert_called_once()


def test_checks_for_the_next_start_run_once_with_the_preflight_checks():
    health_manager = _health_manager(Noop())
    preflight = HealthCheck(requires_idle_workers=True)
    blocking, other = HealthCheck(requires_idle_workers=True), HealthCheck()
    callback = HealthCallback(health_manager, preflight_checks=[preflight])
    callback.run_at_next_start([blocking, other])

    callback.after_worker_group_start(_worker_group("a"))
    health_manager.start_and_evaluate_on_demand_checks.assert_called_once_with(
        [preflight, blocking]
    )
    health_manager.start_on_demand_checks.assert_called_once_with([other])

    callback.after_worker_group_start(_worker_group("a"))
    health_manager.start_and_evaluate_on_demand_checks.assert_called_with([preflight])


def test_health_callback_gives_each_worker_the_worker_probes():
    health_manager = MagicMock(worker_probes=["probe"])
    callback = HealthCallback(health_manager, preflight_checks=[])
    workers = [MagicMock(), MagicMock()]
    assert callback.before_init_train_context(workers) == {
        "worker_probes": [["probe"], ["probe"]]
    }

    health_manager.worker_probes = []
    assert callback.before_init_train_context(workers) == {}


def test_the_health_checks_get_the_worker_group_poll():
    controller, _ = _running_controller(MockFailurePolicy(None))
    _step(controller)
    worker_group_status = controller.get_worker_group().poll_status()
    assert controller._health_manager.worker_statuses == (
        worker_group_status.worker_statuses
    )


def test_user_callbacks_get_the_health_decision():
    user_callback = MagicMock(spec=UserCallback)
    run_context = MagicMock()
    decision = Reattempt(reason="loss spiked")
    UserCallbackHandler([user_callback], run_context).after_health_decision(decision)
    user_callback.after_health_decision.assert_called_once_with(
        run_context=run_context, health_decision=decision
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
