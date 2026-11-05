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
from ray.train.health._internal import manager as manager_module
from ray.train.v2._internal.callbacks.health_callback import HealthCallback
from ray.train.v2._internal.callbacks.user_callback import UserCallbackHandler
from ray.train.v2._internal.constants import HEALTH_CHECK_INTERVAL_S_ENV_VAR
from ray.train.v2._internal.execution.callback import ControllerCallback
from ray.train.v2._internal.execution.controller import TrainController
from ray.train.v2._internal.execution.controller.state import (
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
    """Returns the queued in-flight and diagnosis decisions, and records the
    started diagnoses and the evicted nodes."""

    def __init__(self, inflight_checks):
        self.decisions = []
        self.diagnosis_decisions = []
        self.diagnoses = []
        self.evicted_nodes = []

    def evaluate_inflight_checks(self):
        return self._next(self.decisions)

    def start_on_demand_checks(self, checks, timeout):
        self.diagnoses.append(checks)

    def evaluate_on_demand_checks(self):
        return self._next(self.diagnosis_decisions)

    def _next(self, decisions):
        decision = decisions.pop(0) if decisions else Noop()
        if isinstance(decision, Evict):
            self.evicted_nodes = sorted({*self.evicted_nodes, *decision.target_nodes})
        return decision


class RecordingWorkerGroup(DummyWorkerGroup):
    """Records the context of each worker group started."""

    contexts = []

    def __init__(self, train_run_context, worker_group_context, callbacks=None):
        super().__init__(train_run_context, worker_group_context, callbacks)
        RecordingWorkerGroup.contexts.append(worker_group_context)


class RecordingCallback(ControllerCallback):
    def __init__(self):
        self.decisions = []

    def after_health_decision(self, health_decision):
        self.decisions.append(health_decision)


@pytest.fixture(autouse=True)
def patch_controller(monkeypatch):
    monkeypatch.setattr(TrainController, "worker_group_cls", RecordingWorkerGroup)
    monkeypatch.setattr(manager_module, "HealthManager", FakeHealthManager)
    monkeypatch.setenv(HEALTH_CHECK_INTERVAL_S_ENV_VAR, "0")
    RecordingWorkerGroup.contexts = []
    ray.init()
    yield
    ray.shutdown()


def _step(controller):
    asyncio.run(controller._run_control_loop_iteration())


def _running_controller(failure_policy, scaling_config=None):
    scaling_policy = MockScalingPolicy(scaling_config=ScalingConfig())
    callback = RecordingCallback()
    controller = TrainController(
        train_fn_ref=DummyObjectRefWrapper(lambda: None),
        train_run_context=create_dummy_run_context(
            run_config=RunConfig(name="test", health_config=HealthConfig()),
            scaling_config=scaling_config or ScalingConfig(num_workers=2),
        ),
        scaling_policy=scaling_policy,
        failure_policy=failure_policy,
        callbacks=[callback],
    )
    for _ in range(2):
        scaling_policy.queue_recovery_decision(
            ResizeDecision(num_workers=2, resources_per_worker={})
        )
    _step(controller)
    _step(controller)
    assert isinstance(controller.get_state(), RunningState)
    return controller, callback


def test_diagnose_runs_its_checks_while_training_continues():
    controller, callback = _running_controller(MockFailurePolicy(None))
    health_manager = controller._health_manager
    check = HealthCheck()
    health_manager.decisions = [
        Diagnose(reason="slow", checks=[check]),
        Diagnose(reason="slower", checks=[]),
    ]
    # Not started, then running, then back with nothing to act on.
    health_manager.diagnosis_decisions = [Noop(), None, Noop()]

    for _ in range(3):
        _step(controller)
        assert isinstance(controller.get_state(), RunningState)
    assert health_manager.diagnoses == [[check]]
    assert [d.reason for d in callback.decisions] == ["slow"]


def test_a_diagnosis_can_evict():
    controller, callback = _running_controller(MockFailurePolicy(None))
    health_manager = controller._health_manager
    health_manager.decisions = [Diagnose(reason="slow", checks=[])]
    health_manager.diagnosis_decisions = [
        Noop(),
        Evict(reason="bad NIC", target_nodes=["n1"]),
    ]

    _step(controller)
    assert isinstance(controller.get_state(), RunningState)
    _step(controller)
    assert isinstance(controller.get_state(), RestartingState)
    assert [type(d) for d in callback.decisions] == [Diagnose, Evict]


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


def test_evict_restarts_without_the_evicted_nodes():
    failure_policy = MockFailurePolicy(None)
    failure_policy.make_decision = MagicMock()
    controller, _ = _running_controller(
        failure_policy,
        scaling_config=ScalingConfig(num_workers=2, label_selector={"zone": "a"}),
    )
    controller._health_manager.decisions = [
        Evict(reason="hot GPU", target_nodes=["n2", "n1"])
    ]

    _step(controller)
    assert isinstance(controller.get_state(), RestartingState)
    failure_policy.make_decision.assert_not_called()

    _step(controller)
    assert isinstance(controller.get_state(), SchedulingState)
    _step(controller)
    assert isinstance(controller.get_state(), RunningState)
    assert (
        RecordingWorkerGroup.contexts[-1].label_selector
        == [{"zone": "a", "ray.io/node-id": "!in(n1,n2)"}] * 2
    )


def test_health_callback_resets_each_attempt():
    health_manager = MagicMock()
    callback = HealthCallback(health_manager)
    worker_group = MagicMock()
    worker_group.get_workers.return_value = [
        MagicMock(metadata=MagicMock(node_id=node_id)) for node_id in ("b", "a", "b")
    ]
    callback.after_worker_group_start(worker_group)
    health_manager.reset.assert_called_once_with(["a", "b"])

    callback.before_worker_group_shutdown(worker_group)
    health_manager.shutdown.assert_called_once()


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
