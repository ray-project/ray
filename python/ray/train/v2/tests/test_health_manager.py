import os
import sys
import time
from collections import defaultdict, deque

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
)
from ray.train.health._internal.manager import HealthManager, merge_decisions
from ray.train.health._internal.node_monitor import NodeMonitorStatus
from ray.train.health._internal.node_monitor_group import NodeMonitorGroupPollStatus
from ray.train.health._internal.probe_utils import ScheduledProbe, merge_probes


class Temp(NodeProbe):
    def poll(self):
        return ProbeResult(metrics={"temp_c": 60.0})


class Pid(NodeProbe):
    def poll(self):
        return ProbeResult(metrics={"pid": float(os.getpid())})


class Comms(ControllerProbe):
    def poll(self):
        return {"comm0": ProbeResult()}


class Always(Evaluator):
    """Returns ``decision``, and records the results it decides on."""

    def __init__(self, decision):
        self.decision = decision
        self.seen = []
        self.resets = 0

    def evaluate(self, state):
        self.seen.append(state.probe_results)
        return self.decision

    def on_reset(self):
        self.resets += 1


class Raises(Evaluator):
    def evaluate(self, state):
        raise RuntimeError("bug in evaluator")


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
    """Make each poll of ``manager`` get the next of ``results`` as node n1's
    ``Temp`` result."""
    results = iter(results)
    manager._node_monitor_group.poll_status = lambda timeout: (
        NodeMonitorGroupPollStatus(
            node_monitor_statuses={
                "n1": NodeMonitorStatus(probe_results={"Temp": next(results)})
            }
        )
    )


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


def test_every_evaluator_decides_on_every_poll():
    fast, slow = Always(Noop()), Always(Noop())
    manager = HealthManager(
        [_check(2.0, evaluators=[fast]), _check(10.0, evaluators=[slow])]
    )
    _fake_temps(manager, [ProbeResult(timestamp_s=1.0), ProbeResult(timestamp_s=2.0)])
    manager.poll(timeout=0)
    manager.poll(timeout=0)
    for evaluator in (fast, slow):
        assert [seen["Temp"]["n1"].timestamp_s for seen in evaluator.seen] == [1.0, 2.0]


def test_an_evaluator_decides_on_the_history_it_keeps():
    manager = HealthManager([_check(evaluators=[EvictHotGpu(samples=3)])])
    # The probe samples at 1s, 2s and 3s; the 1s sample is the latest for 3 polls.
    timestamps = [1.0, 1.0, 1.0, 2.0, 3.0]
    _fake_temps(
        manager,
        [ProbeResult(metrics={"temp_c": 95.0}, timestamp_s=t) for t in timestamps],
    )
    decisions = [manager.poll(timeout=0) for _ in timestamps]
    assert decisions == [Noop()] * 4 + [
        Evict(reason="GPU over 90C", target_nodes=["n1"])
    ]


def test_reset_drops_the_results_and_resets_the_evaluators():
    evaluator = Always(Noop())
    manager = HealthManager([_check(evaluators=[evaluator])])
    _fake_temps(manager, [ProbeResult(timestamp_s=1.0)])
    manager.poll(timeout=0)
    assert manager._latest_results

    manager.reset([])
    assert manager._latest_results == {}
    assert evaluator.resets == 1


def test_an_evicted_node_is_not_evicted_again():
    evaluator = Always(Evict(reason="hot", target_nodes=["n1"]))
    manager = HealthManager([_check(evaluators=[evaluator])])
    assert manager.poll(timeout=0) == Evict(reason="hot", target_nodes=["n1"])
    assert manager.poll(timeout=0) == Noop()

    manager.reset([])
    evaluator.decision = Evict(reason="hot", target_nodes=["n1", "n2"])
    assert manager.poll(timeout=0) == Evict(reason="hot", target_nodes=["n2"])
    assert manager.evicted_nodes == ["n1", "n2"]


@pytest.mark.parametrize("broken", [Raises(), Always(None)], ids=["raises", "none"])
def test_a_failing_evaluator_is_skipped_and_logged_once(broken, caplog):
    manager = HealthManager([_check(evaluators=[broken, Always(Reattempt())])])
    assert manager.poll(timeout=0) == Reattempt()
    assert manager.poll(timeout=0) == Reattempt()
    assert len([r for r in caplog.records if "failed" in r.getMessage()]) == 1


def test_node_probes_are_sampled_on_the_nodes_of_each_attempt(ray_start_4_cpus):
    node_id = ray.get_runtime_context().get_node_id()
    manager = HealthManager([_check(0.1, probes=[Temp()])])
    manager.reset([node_id])

    def latest_temp():
        manager.poll(timeout=10)
        return manager._latest_results.get("Temp", {}).get(node_id)

    try:
        assert _wait_for(latest_temp).metrics == {"temp_c": 60.0}

        # A failed monitor stops, with its results, until the next attempt.
        ray.kill(manager._node_monitor_group._monitors[node_id])
        _wait_for(lambda: latest_temp() is None)
        assert manager._node_monitor_group.node_ids == []

        manager.reset([node_id])
        assert _wait_for(latest_temp).metrics == {"temp_c": 60.0}
    finally:
        manager.shutdown()


@ray.remote(num_cpus=0)
class UnresponsiveMonitor:
    def poll_probes(self, probes):
        time.sleep(600)


def test_diagnose_polls_its_probes_once_on_every_node(ray_start_4_cpus):
    node_id = ray.get_runtime_context().get_node_id()
    manager = HealthManager([])
    manager.reset([node_id])
    monitors = manager._node_monitor_group._monitors
    ray.get(monitors[node_id].poll_status.remote())
    monitors["hung-node"] = UnresponsiveMonitor.remote()

    evaluator = Always(Reattempt(reason="diagnosed"))
    check = HealthCheck(
        probe_creator=lambda: [Pid()], evaluator_creator=lambda: [evaluator]
    )
    try:
        assert manager.diagnose([check], timeout=3) == Reattempt(reason="diagnosed")
        pids = evaluator.seen[0]["Pid"]
        assert list(pids) == [node_id]
        assert pids[node_id].metrics["pid"] != os.getpid()
    finally:
        manager.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
