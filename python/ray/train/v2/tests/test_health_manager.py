import sys
import time

import pytest

import ray
from ray.train.health import (
    ControllerProbe,
    Diagnose,
    Evaluator,
    Evict,
    HealthCheck,
    NodeProbe,
    Noop,
    PeriodicHealthCheck,
    ProbeResult,
    Reattempt,
    WorkerProbe,
)
from ray.train.health._internal.manager import HealthManager, merge_decisions
from ray.train.health._internal.node_monitor import NodeMonitorStatus
from ray.train.health._internal.node_monitor_group import NodeMonitorGroupPollStatus


class Temp(NodeProbe):
    def poll(self):
        return ProbeResult(metrics={"temp_c": 60.0})


class Comms(ControllerProbe):
    def __init__(self):
        self.polls = 0

    def poll(self):
        self.polls += 1
        return {"comm0": ProbeResult(metrics={"polls": float(self.polls)})}


class BrokenComms(ControllerProbe):
    def poll(self):
        raise RuntimeError("ncclras not found")


class StackDump(WorkerProbe):
    def poll(self):
        return ProbeResult()


class EvictHot(Evaluator):
    """Evicts every node whose ``Temp`` is over 90C."""

    def __init__(self):
        self.starts = 0

    def evaluate(self, state):
        temps = state.probe_results.get("Temp", {})
        hot = [n for n, r in temps.items() if r.metrics["temp_c"] > 90]
        return Evict(reason="hot", target_nodes=hot) if hot else Noop()

    def on_worker_group_start(self):
        self.starts += 1


class Always(Evaluator):
    def __init__(self, decision):
        self.decision = decision
        self.calls = 0

    def evaluate(self, state):
        self.calls += 1
        return self.decision


class Raises(Evaluator):
    def __init__(self):
        self.calls = 0

    def evaluate(self, state):
        self.calls += 1
        raise RuntimeError("bug in evaluator")


def _check(poll_interval_s=1e-6, probes=(), evaluators=()):
    return PeriodicHealthCheck(
        probe_creator=lambda: list(probes),
        evaluator_creator=lambda: list(evaluators),
        poll_interval_s=poll_interval_s,
    )


def _diagnose(**kwargs):
    return Diagnose(checks=[HealthCheck(probe_creator=lambda: [StackDump()])], **kwargs)


def test_the_most_severe_kind_wins():
    merged = merge_decisions(
        [Noop(), _diagnose(reason="slow"), Evict(reason="ecc", target_nodes=["n1"])]
    )
    assert merged == [Evict(reason="ecc", target_nodes=["n1"])]


@pytest.mark.parametrize("decisions", [[], [Noop(), Noop()]])
def test_nothing_to_do_merges_to_an_empty_list(decisions):
    assert merge_decisions(decisions) == []


@pytest.mark.parametrize(
    "decisions, merged",
    [
        (
            [
                Evict(reason="ecc", target_nodes=["n1"]),
                Evict(reason="nvlink", target_nodes=["n2", "n1"]),
                Reattempt(reason="hang"),
            ],
            Evict(reason="ecc; nvlink", target_nodes=["n1", "n2"]),
        ),
        (
            [Reattempt(reason="hang"), Reattempt(reason="nan"), _diagnose()],
            Reattempt(reason="hang; nan"),
        ),
    ],
)
def test_restarts_are_combined_into_one(decisions, merged):
    assert merge_decisions(decisions) == [merged]


def test_diagnoses_are_carried_out_one_by_one():
    first, second = _diagnose(reason="slow rank"), _diagnose(reason="hot node")
    assert merge_decisions([first, Noop(), second]) == [first, second]


def test_already_evicted_nodes_are_not_evicted_again():
    assert merge_decisions([Evict(target_nodes=["n1"])], evicted_nodes=["n1"]) == []
    merged = merge_decisions([Evict(target_nodes=["n1", "n2"])], evicted_nodes=["n1"])
    assert merged == [Evict(target_nodes=["n2"])]


def test_a_probe_in_two_checks_is_polled_once_at_the_shorter_interval():
    first, second = Comms(), Comms()
    manager = HealthManager(
        [_check(3600.0, probes=[first]), _check(1e-6, probes=[second])]
    )
    manager.poll(timeout=0)
    manager.poll(timeout=0)
    assert first.polls == 2
    assert second.polls == 0


def test_two_different_probes_with_the_same_name_are_rejected():
    class Other(Comms):
        name = "Comms"

    with pytest.raises(ValueError, match="'Comms'"):
        HealthManager([_check(probes=[Comms()]), _check(probes=[Other()])])


def test_controller_results_are_stamped_and_a_failing_probe_is_skipped():
    manager = HealthManager([_check(probes=[Comms(), BrokenComms()])])
    manager.poll(timeout=0)

    results = manager._results
    assert results["Comms"]["comm0"].metrics == {"polls": 1.0}
    assert results["Comms"]["comm0"].timestamp_s is not None
    assert "BrokenComms" not in results


def test_worker_results_are_stored_per_rank_and_stamped():
    manager = HealthManager([])
    manager.store_worker_results(0, {"Loss": ProbeResult(timestamp_s=1.0)})
    manager.store_worker_results(1, {"Loss": ProbeResult()})
    assert manager._results["Loss"][0].timestamp_s == 1.0
    assert manager._results["Loss"][1].timestamp_s is not None


def test_each_check_runs_its_evaluators_at_its_own_interval():
    fast, slow = Always(Noop()), Always(Noop())
    manager = HealthManager(
        [_check(1e-6, evaluators=[fast]), _check(3600.0, evaluators=[slow])]
    )
    for _ in range(3):
        manager.poll(timeout=0)
    assert fast.calls == 3
    assert slow.calls == 1


def test_an_evict_is_decided_once_and_the_node_stays_evicted_across_restarts():
    evaluator = EvictHot()
    manager = HealthManager([_check(evaluators=[evaluator])])
    manager._results["Temp"] = {"n1": ProbeResult(metrics={"temp_c": 95.0})}

    assert manager.poll(timeout=0) == [Evict(reason="hot", target_nodes=["n1"])]
    assert manager.poll(timeout=0) == []
    assert manager.evicted_nodes == ["n1"]

    manager.reset([])
    assert manager._results == {}
    assert evaluator.starts == 1
    assert manager.evicted_nodes == ["n1"]


@pytest.mark.parametrize(
    "broken",
    [Raises(), Always(None), Always([Reattempt()])],
    ids=["raises", "returns_none", "returns_a_list"],
)
def test_a_failing_evaluator_is_skipped_and_the_rest_keep_running(broken):
    healthy = Always(Reattempt(reason="hang"))
    manager = HealthManager([_check(evaluators=[broken, healthy])])
    assert manager.poll(timeout=0) == [Reattempt(reason="hang")]
    assert manager.poll(timeout=0) == [Reattempt(reason="hang")]
    assert broken.calls == 2
    assert healthy.calls == 2


def test_a_failing_node_probe_is_logged_once_until_it_recovers(caplog):
    manager = HealthManager([])
    failing = NodeMonitorStatus(
        probe_results={"Temp": ProbeResult(error=RuntimeError("sensor gone"))}
    )
    ok = NodeMonitorStatus(probe_results={"Temp": ProbeResult(timestamp_s=1.0)})
    statuses = iter([failing, failing, ok, failing])
    manager._node_monitor_group.poll_status = lambda timeout: (
        NodeMonitorGroupPollStatus(node_monitor_statuses={"n1": next(statuses)})
    )
    for _ in range(4):
        manager.poll(timeout=0)
    logged = [r for r in caplog.records if "Node probe Temp failed" in r.getMessage()]
    assert len(logged) == 2
    assert isinstance(manager._results["Temp"]["n1"].error, RuntimeError)


def _wait_for(fn, timeout_s=15.0):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        value = fn()
        if value:
            return value
        time.sleep(0.1)
    pytest.fail("timed out")


def _node_temps(manager):
    manager.poll(timeout=10)
    return manager._results.get("Temp", {})


def test_node_probes_are_sampled_on_the_nodes_of_each_attempt(ray_start_4_cpus):
    node_id = ray.get_runtime_context().get_node_id()
    manager = HealthManager([_check(0.1, probes=[Temp()])])
    manager.reset([node_id])
    try:
        temps = _wait_for(lambda: _node_temps(manager))
        assert temps[node_id].metrics == {"temp_c": 60.0}

        old_monitor = manager._node_monitor_group._monitors[node_id]
        killed_at = time.time()
        ray.kill(old_monitor)

        # The failed monitor is replaced, and samples resume from the new one.
        _wait_for(
            lambda: _node_temps(manager)[node_id].timestamp_s > killed_at
            and manager._node_monitor_group._monitors[node_id] is not old_monitor
        )

        manager.reset([])
        assert manager._node_monitor_group.node_ids == []
    finally:
        manager.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
