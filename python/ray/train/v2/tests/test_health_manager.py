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
    ProbeResult,
    Reattempt,
    WorkerProbe,
)
from ray.train.health._internal.manager import (
    MAX_WINDOW_SIZE,
    HealthManager,
    merge_decisions,
)


class Loss(WorkerProbe):
    def poll(self):
        return ProbeResult(metrics={"loss": 1.0})


class Temp(NodeProbe):
    poll_interval_s = 0.1

    def poll(self):
        return ProbeResult(metrics={"temp_c": 60.0})


class Comms(ControllerProbe):
    poll_interval_s = 3600.0

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


def _latest(state, probe_cls):
    """{key: latest reading} of one probe."""
    windows = state.probe_results.get(probe_cls.probe_name(), {})
    return {key: window[-1] for key, window in windows.items() if window}


class EvictHot(Evaluator):
    """Evicts every node whose latest ``Temp`` reading is over 90C."""

    def __init__(self):
        self.starts = 0

    def evaluate(self, state):
        hot = [n for n, r in _latest(state, Temp).items() if r.metrics["temp_c"] > 90]
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
    first = _diagnose(target_ranks=[3])
    second = _diagnose(target_nodes=["n1"])
    assert merge_decisions([first, Noop(), second]) == [first, second]


def test_already_evicted_nodes_are_not_acted_on_again():
    assert merge_decisions([Evict(target_nodes=["n1"])], evicted_nodes=["n1"]) == []
    merged = merge_decisions([Evict(target_nodes=["n1", "n2"])], evicted_nodes=["n1"])
    assert merged == [Evict(target_nodes=["n2"])]
    assert merge_decisions([_diagnose(target_nodes=["n1"])], evicted_nodes=["n1"]) == []
    every_node = _diagnose()
    assert merge_decisions([every_node], evicted_nodes=["n1"]) == [every_node]


def test_an_evict_with_no_nodes_is_dropped():
    assert merge_decisions([Evict(target_nodes=[])]) == []


def test_probes_are_split_by_kind():
    manager = HealthManager(
        [HealthCheck(probe_creator=lambda: [Loss(), Temp(), Comms()])]
    )
    assert [type(p) for p in manager.worker_probes()] == [Loss]
    assert [type(p) for p in manager.node_probes()] == [Temp]
    assert [type(p) for p in manager.controller_probes()] == [Comms]


def test_no_checks_means_disabled():
    assert not HealthManager([]).enabled


def test_creators_are_called_once_per_manager():
    calls = []
    check = HealthCheck(probe_creator=lambda: calls.append(1) or [Temp()])
    HealthManager([check])
    HealthManager([check])
    assert len(calls) == 2


@pytest.mark.parametrize("window_size", [0, MAX_WINDOW_SIZE + 1])
def test_a_window_size_out_of_range_is_rejected(window_size):
    class Odd(Loss):
        pass

    Odd.window_size = window_size
    with pytest.raises(ValueError, match="window_size"):
        HealthManager([HealthCheck(probe_creator=lambda: [Odd()])])


def _loss(value, timestamp_s):
    return ProbeResult(metrics={"loss": value}, timestamp_s=timestamp_s)


def test_worker_readings_are_stored_per_rank_and_stamped():
    manager = HealthManager([HealthCheck(probe_creator=lambda: [Loss()])])
    manager.store_worker_results(0, {"Loss": _loss(2.0, 1.0)})
    manager.store_worker_results(1, {"Loss": ProbeResult()})

    latest = _latest(manager.build_state(), Loss)
    assert latest[0] == _loss(2.0, 1.0)
    assert latest[1].timestamp_s is not None


def test_a_probe_keeps_its_window_and_a_repeated_reading_is_stored_once():
    class LossWindow(Loss):
        window_size = 3

    manager = HealthManager([HealthCheck(probe_creator=lambda: [LossWindow()])])
    for t, value in enumerate([4.0, 3.0, 2.0, 1.0]):
        manager.store_worker_results(0, {"LossWindow": _loss(value, float(t))})
    manager.store_worker_results(0, {"LossWindow": _loss(1.0, 3.0)})

    assert manager.build_state().probe_results["LossWindow"][0] == [
        _loss(3.0, 1.0),
        _loss(2.0, 2.0),
        _loss(1.0, 3.0),
    ]


def test_controller_probes_are_polled_when_due_and_a_failing_one_is_skipped():
    comms = Comms()
    manager = HealthManager([HealthCheck(probe_creator=lambda: [comms, BrokenComms()])])
    manager.poll_probes()
    manager.poll_probes()

    state = manager.build_state()
    assert comms.polls == 1
    assert _latest(state, Comms)["comm0"].metrics == {"polls": 1.0}
    assert _latest(state, BrokenComms) == {}


def _wait_for(fn, timeout_s=15.0):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        value = fn()
        if value:
            return value
        time.sleep(0.1)
    pytest.fail("timed out")


def _node_temps(manager):
    manager.poll_probes(timeout=10)
    return _latest(manager.build_state(), Temp)


def test_node_probes_are_sampled_on_each_node_and_a_failed_monitor_restarts(
    ray_start_4_cpus,
):
    node_id = ray.get_runtime_context().get_node_id()
    manager = HealthManager([HealthCheck(probe_creator=lambda: [Temp()])])
    manager.start([node_id])
    try:
        temps = _wait_for(lambda: _node_temps(manager))
        assert temps[node_id].metrics == {"temp_c": 60.0}

        old_monitor = manager._node_monitors._monitors[node_id]
        killed_at = time.time()
        ray.kill(old_monitor)

        # The failed monitor is replaced, and readings resume from the new one.
        _wait_for(
            lambda: _node_temps(manager)[node_id].timestamp_s > killed_at
            and manager._node_monitors._monitors[node_id] is not old_monitor
        )
    finally:
        manager.shutdown()


def test_reset_moves_the_monitors_to_the_new_nodes(ray_start_4_cpus):
    node_id = ray.get_runtime_context().get_node_id()
    manager = HealthManager([HealthCheck(probe_creator=lambda: [Temp()])])
    manager.start([node_id])
    try:
        manager.reset([])
        assert manager._node_monitors.node_ids == []
        manager.reset([node_id])
        assert manager._node_monitors.node_ids == [node_id]
    finally:
        manager.shutdown()


def test_an_evict_is_decided_once_and_the_node_stays_evicted_across_restarts():
    evaluator = EvictHot()
    manager = HealthManager([HealthCheck(evaluator_creator=lambda: [evaluator])])
    manager._store({"Temp": {"n1": ProbeResult(metrics={"temp_c": 95.0})}})

    assert manager.poll_decisions() == [Evict(reason="hot", target_nodes=["n1"])]
    assert manager.poll_decisions() == []
    assert manager.evicted_nodes == ["n1"]

    manager.reset([])
    assert manager.build_state().probe_results == {}
    assert evaluator.starts == 1
    assert manager.evicted_nodes == ["n1"]


@pytest.mark.parametrize(
    "broken", [Raises(), Always(None), Always([Reattempt()])], ids=repr
)
def test_a_failing_evaluator_is_disabled_and_the_rest_keep_running(broken):
    healthy = Always(Reattempt(reason="hang"))
    manager = HealthManager([HealthCheck(evaluator_creator=lambda: [broken, healthy])])
    assert manager.poll_decisions() == [Reattempt(reason="hang")]
    assert manager.poll_decisions() == [Reattempt(reason="hang")]
    assert broken.calls == 1
    assert healthy.calls == 2


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
