import os
import sys
import time
from collections import defaultdict, deque
from unittest import mock

import pytest

import ray
from ray.train.health import (
    ControllerProbe,
    Diagnose,
    Evaluator,
    Evict,
    HealthCheck,
    HealthDecision,
    NodeProbe,
    Noop,
    PeriodicHealthCheck,
    ProbeResult,
    Reattempt,
    WorkerProbe,
)
from ray.train.health._internal.manager import (
    HealthManager,
    _to_health_state,
    merge_decisions,
)
from ray.train.health._internal.node_monitor import NodeMonitorStatus
from ray.train.health._internal.node_monitor_group import NodeMonitorGroupPollStatus
from ray.train.health._internal.probe_utils import ScheduledProbe, merge_probes
from ray.train.v2._internal.execution.worker_group import WorkerStatus


class Temp(NodeProbe):
    def poll(self):
        return ProbeResult(metrics={"temp_c": 60.0})


class Pid(NodeProbe):
    def poll(self):
        return ProbeResult(metrics={"pid": float(os.getpid())})


class Comms(ControllerProbe):
    def poll(self):
        return {"comm0": ProbeResult()}


class StepTime(WorkerProbe):
    def poll(self):
        return ProbeResult(metrics={"step_time_s": 1.0})


class Broken(ControllerProbe):
    def poll(self):
        raise RuntimeError("ncclras not found")


class Always(Evaluator):
    """Returns ``decision``, and records the states it decides on."""

    def __init__(self, decision):
        self.decision = decision
        self.seen = []
        self.resets = 0

    def evaluate(self, state):
        self.seen.append(state)
        return self.decision

    def on_reset(self):
        self.resets += 1


class Raises(Evaluator):
    def evaluate(self, state):
        raise RuntimeError("bug in evaluator")


class Custom(HealthDecision):
    pass


class EvictHotGpu(Evaluator):
    """Evicts a node whose last ``samples`` ``Temp`` results are all over 90C."""

    def __init__(self, samples=3):
        self.samples = samples
        self.history = defaultdict(lambda: deque(maxlen=self.samples))

    def evaluate(self, state):
        for node, result in state.probe_results.get("Temp", {}).items():
            history = self.history[node]
            # A result stays the latest until the probe samples again.
            if not history or history[-1].timestamp_s != result.timestamp_s:
                history.append(result)
        hot = [
            node
            for node, history in self.history.items()
            if len(history) == self.samples
            and all(r.metrics["temp_c"] > 90 for r in history)
        ]
        return Evict(reason="GPU over 90C", target_nodes=hot) if hot else Noop()

    def on_reset(self):
        self.history.clear()


def _check(poll_interval_s=1.0, probes=(), evaluators=()):
    return PeriodicHealthCheck(
        probe_creator=lambda: list(probes),
        evaluator_creator=lambda: list(evaluators),
        poll_interval_s=poll_interval_s,
    )


def _diagnose(reason):
    return Diagnose(reason=reason, checks=[HealthCheck(probe_creator=lambda: [Pid()])])


def _fake_temps(manager, results):
    """Replace the monitors of ``manager`` with a fake whose polls give the next
    of ``results`` as node n1's ``Temp`` result, and whose on-demand polls give
    nothing."""
    results = iter(results)
    manager._node_monitor_group = mock.Mock(
        poll_status=lambda timeout: NodeMonitorGroupPollStatus(
            node_monitor_statuses={
                "n1": NodeMonitorStatus(probe_results={"Temp": next(results)})
            }
        ),
        poll_probe_results=lambda: NodeMonitorGroupPollStatus(node_monitor_statuses={}),
    )


@pytest.fixture
def fake_node_id(monkeypatch):
    """Let ``reset()`` run without Ray, with a fake controller node ID."""
    context = mock.Mock(get_node_id=lambda: "controller")
    monkeypatch.setattr(ray, "get_runtime_context", lambda: context)


def _wait_for(fn, timeout_s=15.0):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        value = fn()
        if value:
            return value
        time.sleep(0.1)
    pytest.fail("timed out")


SLOW, HOT = _diagnose("slow"), _diagnose("hot")


@pytest.mark.parametrize(
    "decisions, merged",
    [
        ([Noop()], Noop()),
        (
            [SLOW, Reattempt(reason="hang"), Evict(reason="ecc", target_nodes=["n1"])],
            Evict(reason="ecc", target_nodes=["n1"]),
        ),
        (
            [
                Evict(reason="ecc", target_nodes=["n1"]),
                Evict(reason="nvlink", target_nodes=["n2", "n1"]),
            ],
            Evict(reason="ecc; nvlink", target_nodes=["n1", "n2"]),
        ),
        (
            [Reattempt(reason="hang"), Reattempt(reason="nan")],
            Reattempt(reason="hang; nan"),
        ),
        ([SLOW, HOT], Diagnose(reason="slow; hot", checks=SLOW.checks + HOT.checks)),
    ],
    ids=["noop", "most_severe_wins", "evicts", "reattempts", "diagnoses"],
)
def test_merge_decisions(decisions, merged):
    assert merge_decisions(decisions) == merged


def test_merge_decisions_rejects_an_unknown_decision():
    with pytest.raises(TypeError):
        merge_decisions([Noop(), Custom()])


def test_merge_probes_keeps_one_probe_per_name_at_the_shortest_interval():
    first, second, comms = Temp(), Temp(), Comms()
    merged = merge_probes(
        [_check(5.0, probes=[first, comms]), _check(2.0, probes=[second])]
    )
    assert merged == [ScheduledProbe(first, 2.0), ScheduledProbe(comms, 5.0)]


def test_merge_probes_rejects_two_different_probes_with_the_same_name():
    class Other(Pid):
        name = "Temp"

    with pytest.raises(ValueError, match="'Temp'"):
        merge_probes([_check(probes=[Temp()]), _check(probes=[Other()])])


def test_every_inflight_evaluator_decides_on_every_evaluation():
    fast, slow = Always(Noop()), Always(Noop())
    manager = HealthManager(
        [_check(2.0, evaluators=[fast]), _check(10.0, evaluators=[slow])]
    )
    _fake_temps(manager, [ProbeResult(timestamp_s=1.0), ProbeResult(timestamp_s=2.0)])
    manager.evaluate_inflight_checks(timeout=0)
    manager.evaluate_inflight_checks(timeout=0)
    for evaluator in (fast, slow):
        timestamps = [s.probe_results["Temp"]["n1"].timestamp_s for s in evaluator.seen]
        assert timestamps == [1.0, 2.0]


def test_worker_probes_are_the_inflight_worker_probes():
    step_time = StepTime()
    manager = HealthManager([_check(2.0, probes=[step_time, Temp(), Comms()])])
    assert manager.worker_probes == [ScheduledProbe(step_time, 2.0)]


def test_worker_results_and_health_reports_reach_the_evaluators():
    evaluator = Always(Noop())
    manager = HealthManager(
        [_check(probes=[StepTime(), Temp()], evaluators=[evaluator])]
    )
    temp = ProbeResult(timestamp_s=1.0)
    _fake_temps(manager, [temp])
    step_times = [ProbeResult(timestamp_s=2.0), ProbeResult(timestamp_s=3.0)]
    report = ProbeResult(step=3)

    manager.evaluate_inflight_checks(
        {
            0: WorkerStatus(
                running=True,
                probe_results={"StepTime": step_times[0]},
                health_report=report,
            ),
            1: WorkerStatus(running=True, probe_results={"StepTime": step_times[1]}),
        },
        timeout=0,
    )
    (state,) = evaluator.seen
    assert state.probe_results == {
        "Temp": {"n1": temp},
        "StepTime": {0: step_times[0], 1: step_times[1]},
    }
    assert state.health_reports == {0: report}


def test_a_worker_whose_poll_hangs_keeps_its_last_results():
    evaluator = Always(Noop())
    manager = HealthManager([_check(probes=[StepTime()], evaluators=[evaluator])])
    _fake_temps(manager, [ProbeResult(), ProbeResult(), ProbeResult()])
    step_times = [ProbeResult(timestamp_s=1.0), ProbeResult(timestamp_s=2.0)]
    reports = [ProbeResult(step=1), ProbeResult(step=2)]

    manager.evaluate_inflight_checks(
        {
            0: WorkerStatus(
                running=True,
                probe_results={"StepTime": step_times[0]},
                health_report=reports[0],
            )
        },
        timeout=0,
    )
    # A hanging poll gives an empty status.
    manager.evaluate_inflight_checks({0: WorkerStatus(running=True)}, timeout=0)
    manager.evaluate_inflight_checks(
        {
            0: WorkerStatus(
                running=True,
                probe_results={"StepTime": step_times[1]},
                health_report=reports[1],
            )
        },
        timeout=0,
    )

    assert [state.probe_results["StepTime"] for state in evaluator.seen] == [
        {0: step_times[0]},
        {0: step_times[0]},
        {0: step_times[1]},
    ]
    assert [state.health_reports for state in evaluator.seen] == [
        {0: reports[0]},
        {0: reports[0]},
        {0: reports[1]},
    ]


def test_an_evaluator_decides_on_the_history_it_keeps(fake_node_id):
    evaluator = EvictHotGpu(samples=3)
    manager = HealthManager([_check(evaluators=[evaluator])])
    # The probe samples at 1s, 2s, 3s and 4s; the 1s sample stays the latest for
    # 3 evaluations, and the 4s sample is cool.
    samples = [(1.0, 95.0)] * 3 + [(2.0, 95.0), (3.0, 95.0), (4.0, 60.0)]
    _fake_temps(
        manager,
        [ProbeResult(metrics={"temp_c": t}, timestamp_s=ts) for ts, t in samples],
    )

    def evaluate():
        decision = manager.evaluate_inflight_checks(timeout=0)
        return decision, [r.timestamp_s for r in evaluator.history["n1"]]

    assert evaluate() == (Noop(), [1.0])
    assert evaluate() == (Noop(), [1.0])
    assert evaluate() == (Noop(), [1.0])
    assert evaluate() == (Noop(), [1.0, 2.0])
    hot = Evict(reason="GPU over 90C", target_nodes=["n1"])
    assert evaluate() == (hot, [1.0, 2.0, 3.0])
    assert evaluate() == (Noop(), [2.0, 3.0, 4.0])

    manager.reset([])
    assert evaluator.history == {}


def test_reset_drops_the_results_and_resets_the_evaluators(fake_node_id):
    evaluator = Always(Noop())
    manager = HealthManager([_check(evaluators=[evaluator])])
    _fake_temps(manager, [ProbeResult(timestamp_s=1.0)])
    manager.evaluate_inflight_checks(
        {0: WorkerStatus(running=True, health_report=ProbeResult(step=1))},
        timeout=0,
    )
    assert manager._last_node_monitor_group_status.node_monitor_statuses
    assert manager._last_worker_statuses

    manager.reset([])
    assert manager._last_node_monitor_group_status.node_monitor_statuses == {}
    assert manager._last_worker_statuses == {}
    assert evaluator.resets == 1


def test_an_evicted_node_is_not_evicted_again(fake_node_id):
    evaluator = Always(Evict(reason="hot", target_nodes=["n1"]))
    manager = HealthManager([_check(evaluators=[evaluator])])
    _fake_temps(manager, [ProbeResult()] * 3)
    assert manager.evaluate_inflight_checks(timeout=0) == Evict(
        reason="hot", target_nodes=["n1"]
    )
    assert manager.evaluate_inflight_checks(timeout=0) == Noop()

    manager.reset([])
    evaluator.decision = Evict(reason="hot", target_nodes=["n1", "n2"])
    assert manager.evaluate_inflight_checks(timeout=0) == Evict(
        reason="hot", target_nodes=["n2"]
    )
    assert manager.evicted_nodes == ["n1", "n2"]


@pytest.mark.parametrize(
    "broken",
    [Raises(), Always(None), Always(Custom())],
    ids=["raises", "none", "unknown-decision"],
)
def test_a_failing_evaluator_is_skipped_and_logged_once(broken, caplog, propagate_logs):
    manager = HealthManager([_check(evaluators=[broken, Always(Reattempt())])])
    _fake_temps(manager, [ProbeResult()] * 2)
    assert manager.evaluate_inflight_checks(timeout=0) == Reattempt()
    assert manager.evaluate_inflight_checks(timeout=0) == Reattempt()
    assert len([r for r in caplog.records if "failed" in r.getMessage()]) == 1


def test_on_demand_checks_run_apart_from_the_inflight_checks():
    manager = HealthManager([_check(evaluators=[Always(Reattempt(reason="hang"))])])
    _fake_temps(manager, [ProbeResult()])
    diagnosis = Always(Evict(reason="diagnosed", target_nodes=["n1"]))
    manager.start_on_demand_checks(
        [HealthCheck(evaluator_creator=lambda: [diagnosis])], timeout=10
    )

    assert manager.evaluate_inflight_checks(timeout=0) == Reattempt(reason="hang")
    assert manager.evaluate_on_demand_checks() == Evict(
        reason="diagnosed", target_nodes=["n1"]
    )


def test_evaluate_waits_for_the_on_demand_checks_before_another_diagnose():
    diagnose = _diagnose("slow")
    manager = HealthManager([_check(evaluators=[Always(diagnose)])])
    _fake_temps(manager, [ProbeResult()] * 3)
    evict = Evict(reason="bad NIC", target_nodes=["n1"])
    manager.start_on_demand_checks(
        [HealthCheck(evaluator_creator=lambda: [Always(evict)])], timeout=10
    )
    manager._node_monitor_group.poll_probe_results = mock.Mock(
        side_effect=[None, NodeMonitorGroupPollStatus(node_monitor_statuses={})]
    )

    # Running: the in-flight Diagnose is dropped.
    assert manager.evaluate(timeout=0) == Noop()
    # Back: their Evict beats the in-flight Diagnose.
    assert manager.evaluate(timeout=0) == evict
    # Done: the in-flight Diagnose goes through.
    assert manager.evaluate(timeout=0) == diagnose


def test_evaluate_keeps_a_diagnose_that_requires_idle_workers_while_others_run():
    diagnose = Diagnose(reason="slow", checks=[HealthCheck(requires_idle_workers=True)])
    manager = HealthManager([_check(evaluators=[Always(diagnose)])])
    _fake_temps(manager, [ProbeResult()])
    manager.start_on_demand_checks(
        [HealthCheck(evaluator_creator=lambda: [Always(Noop())])], timeout=10
    )
    manager._node_monitor_group.poll_probe_results = mock.Mock(return_value=None)

    # It restarts the worker group rather than start more on-demand checks.
    assert manager.evaluate(timeout=0) == diagnose


def test_start_and_evaluate_on_demand_checks_waits_for_their_decision():
    manager = HealthManager([])
    _fake_temps(manager, [])
    evict = Evict(reason="bad GPU", target_nodes=["n1"])
    manager._node_monitor_group.poll_probe_results = mock.Mock(
        side_effect=[None, NodeMonitorGroupPollStatus(node_monitor_statuses={})]
    )

    check = HealthCheck(evaluator_creator=lambda: [Always(evict)])
    assert manager.start_and_evaluate_on_demand_checks([check]) == evict
    assert manager._node_monitor_group.poll_probe_results.call_count == 2


def test_on_demand_worker_probes_run_on_the_worker_group(fake_node_id):
    evaluator = Always(Noop())
    manager = HealthManager([])
    _fake_temps(manager, [])
    worker_group = mock.Mock()
    step_time = ProbeResult(metrics={"step_time_s": 1.0})
    worker_group.poll_probe_results.side_effect = [None, {"StepTime": {0: step_time}}]
    manager.reset(["n1"], worker_group)
    probe = StepTime()
    manager.start_on_demand_checks(
        [
            HealthCheck(
                probe_creator=lambda: [probe, Temp()],
                evaluator_creator=lambda: [evaluator],
            )
        ],
        timeout=10,
    )
    worker_group.poll_probes_async.assert_called_once_with([probe], timeout_s=10)

    assert manager.evaluate_on_demand_checks() is None
    assert manager.evaluate_on_demand_checks() == Noop()
    (state,) = evaluator.seen
    assert state.probe_results["StepTime"] == {0: step_time}


def test_on_demand_checks_decide_noop_before_any_start():
    assert HealthManager([]).evaluate_on_demand_checks() == Noop()


def test_on_demand_checks_decide_once(caplog, propagate_logs):
    manager = HealthManager([])
    _fake_temps(manager, [])
    manager.start_on_demand_checks(
        [HealthCheck(evaluator_creator=lambda: [Always(Noop())])], timeout=10
    )
    assert manager.evaluate_on_demand_checks() == Noop()
    assert "found nothing to act on" in caplog.text

    manager._node_monitor_group.poll_probe_results = mock.Mock()
    assert manager.evaluate_on_demand_checks() == Noop()
    manager._node_monitor_group.poll_probe_results.assert_not_called()


def test_a_failed_monitor_gives_each_of_its_probes_its_error():
    error = RuntimeError("monitor gone")
    status = NodeMonitorGroupPollStatus(
        node_monitor_statuses={"n1": NodeMonitorStatus(monitor_error=error)},
        controller_node_monitor_status=NodeMonitorStatus(monitor_error=error),
    )
    state = _to_health_state(status, [Temp(), Comms()])
    assert list(state.probe_results) == ["Temp"]
    assert state.probe_results["Temp"]["n1"].error is error
    assert state.probe_errors == {"Comms": error}


def test_a_controller_monitor_that_answers_again_has_no_error():
    comms = {"comm0": ProbeResult()}
    statuses = iter(
        [
            NodeMonitorStatus(probe_results={"Comms": comms}),
            NodeMonitorStatus(monitor_error=TimeoutError("poll hangs")),
            NodeMonitorStatus(probe_results={"Comms": comms}),
        ]
    )
    evaluator = Always(Noop())
    manager = HealthManager([_check(probes=[Comms()], evaluators=[evaluator])])
    manager._node_monitor_group = mock.Mock(
        poll_status=lambda timeout: NodeMonitorGroupPollStatus(
            node_monitor_statuses={},
            controller_node_monitor_status=next(statuses),
        )
    )
    for _ in range(3):
        manager.evaluate_inflight_checks(timeout=0)

    answered, failed, answered_again = evaluator.seen
    assert answered.probe_results == {"Comms": comms}
    assert isinstance(failed.probe_errors["Comms"], TimeoutError)
    assert answered_again.probe_results == {"Comms": comms}
    assert answered_again.probe_errors == {}


def test_node_probes_are_sampled_on_the_nodes_of_each_attempt(ray_start_4_cpus):
    node_id = ray.get_runtime_context().get_node_id()
    evaluator = Always(Noop())
    manager = HealthManager([_check(0.1, probes=[Temp()], evaluators=[evaluator])])
    manager.reset([node_id])

    def latest_temp():
        manager.evaluate_inflight_checks(timeout=10)
        return evaluator.seen[-1].probe_results.get("Temp", {}).get(node_id)

    try:
        assert _wait_for(latest_temp).metrics == {"temp_c": 60.0}

        # A failed monitor stops, with its results, until the next attempt.
        ray.kill(manager._node_monitor_group._worker_node_monitors[node_id])
        _wait_for(lambda: latest_temp() is None)
        assert manager._node_monitor_group.worker_node_ids == []

        manager.reset([node_id])
        assert _wait_for(latest_temp).metrics == {"temp_c": 60.0}
    finally:
        manager.shutdown()


def test_controller_probes_are_sampled_on_the_controller_node(ray_start_4_cpus):
    evaluator = Always(Noop())
    manager = HealthManager([_check(0.1, probes=[Comms()], evaluators=[evaluator])])
    manager.reset([])

    def latest():
        manager.evaluate_inflight_checks(timeout=10)
        return evaluator.seen[-1]

    try:
        results = _wait_for(lambda: latest().probe_results)
        assert list(results["Comms"]) == ["comm0"]

        # While the monitor is down, each ControllerProbe has its error.
        ray.kill(manager._node_monitor_group._controller_node_monitor)
        errors = _wait_for(lambda: latest().probe_errors)
        assert isinstance(errors["Comms"], ray.exceptions.RayActorError)
        assert evaluator.seen[-1].probe_results == {}
    finally:
        manager.shutdown()


def test_on_demand_checks_poll_their_probes_once_on_each_node(ray_start_4_cpus):
    node_id = ray.get_runtime_context().get_node_id()
    manager = HealthManager([])
    manager.reset([node_id])
    evaluator = Always(Reattempt(reason="diagnosed"))
    check = HealthCheck(
        probe_creator=lambda: [Pid(), Comms(), Broken()],
        evaluator_creator=lambda: [evaluator],
    )
    try:
        manager.start_on_demand_checks([check], timeout=30)
        assert _wait_for(manager.evaluate_on_demand_checks) == Reattempt(
            reason="diagnosed"
        )
        (state,) = evaluator.seen
        assert state.probe_results["Pid"][node_id].metrics["pid"] != os.getpid()
        assert list(state.probe_results["Comms"]) == ["comm0"]
        assert isinstance(state.probe_errors["Broken"], RuntimeError)
    finally:
        manager.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
