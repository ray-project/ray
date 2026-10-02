import sys

import pytest

from ray.train.health import (
    ControllerProbe,
    Diagnose,
    Evaluator,
    Evict,
    HealthPolicy,
    NodeProbe,
    Noop,
    ProbeResult,
    Reattempt,
    WorkerProbe,
)
from ray.train.health._internal.manager import HealthManager, merge_decisions


class Loss(WorkerProbe):
    def poll(self):
        return ProbeResult(metrics={"loss": 1.0})


class Temp(NodeProbe):
    def poll(self):
        return ProbeResult(metrics={"temp_c": 60.0})


class Comms(ControllerProbe):
    def poll(self, ctx):
        return {}


class StackDump(WorkerProbe):
    def poll(self):
        return ProbeResult()


class EvictHot(Evaluator):
    """Evicts every node whose latest ``Temp`` reading is over 90C."""

    def __init__(self):
        self.starts = 0

    def evaluate(self, state):
        hot = [n for n, r in state.results(Temp).items() if r.metrics["temp_c"] > 90]
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
    return Diagnose(probe_creator=lambda: [StackDump()], **kwargs)


def test_the_most_severe_decision_wins():
    merged = merge_decisions(
        [Noop(), _diagnose(reason="slow"), Evict(reason="ecc", target_nodes=["n1"])]
    )
    assert merged == Evict(reason="ecc", target_nodes=["n1"])


@pytest.mark.parametrize("decisions", [[], [Noop(), Noop()]])
def test_nothing_to_do_merges_to_none(decisions):
    assert merge_decisions(decisions) is None


def test_decisions_of_the_winning_kind_are_combined():
    merged = merge_decisions(
        [
            Evict(reason="ecc", target_nodes=["n1"]),
            Evict(reason="nvlink", target_nodes=["n2", "n1"]),
            Reattempt(reason="hang"),
        ]
    )
    assert merged == Evict(reason="ecc; nvlink", target_nodes=["n1", "n2"])


def test_diagnoses_poll_every_probe_on_the_union_of_targets():
    merged = merge_decisions(
        [
            Diagnose(probe_creator=lambda: [StackDump()], target_ranks=[3]),
            Diagnose(probe_creator=lambda: [Temp()], target_ranks=[1, 3]),
        ]
    )
    assert [type(p) for p in merged.probe_creator()] == [StackDump, Temp]
    assert merged.target_ranks == [3, 1]


def test_an_empty_target_list_means_all_and_wins_the_union():
    merged = merge_decisions([_diagnose(target_ranks=[3]), _diagnose()])
    assert merged.target_ranks == []


def test_already_evicted_nodes_are_not_acted_on_again():
    assert merge_decisions([Evict(target_nodes=["n1"])], evicted_nodes=["n1"]) is None
    merged = merge_decisions([Evict(target_nodes=["n1", "n2"])], evicted_nodes=["n1"])
    assert merged.target_nodes == ["n2"]
    assert (
        merge_decisions([_diagnose(target_nodes=["n1"])], evicted_nodes=["n1"]) is None
    )


def test_an_evict_with_no_nodes_is_dropped():
    assert merge_decisions([Evict(reason="?")]) is None


def test_policies_are_split_by_probe_kind_and_by_stage():
    manager = HealthManager(
        [
            HealthPolicy(probe_creator=lambda: [Loss(), Temp(), Comms()]),
            HealthPolicy(
                probe_creator=lambda: [Temp()],
                evaluator_creator=lambda: [EvictHot()],
                preflight=True,
            ),
        ]
    )
    assert manager.enabled
    assert [type(p) for p in manager.worker_probes()] == [Loss]
    assert [type(p) for p in manager.node_probes()] == [Temp]
    assert [type(p) for p in manager.controller_probes()] == [Comms]
    assert [type(p) for p in manager.preflight_probes()] == [Temp]


def test_no_policies_means_disabled():
    assert not HealthManager([]).enabled


def test_creators_are_called_once_per_manager():
    calls = []
    policy = HealthPolicy(probe_creator=lambda: calls.append(1) or [Temp()])
    HealthManager([policy])
    HealthManager([policy])
    assert len(calls) == 2


def test_the_state_holds_the_latest_result_and_stamps_it():
    manager = HealthManager([HealthPolicy(probe_creator=lambda: [Loss()])])
    manager.ingest("Loss", {0: ProbeResult(metrics={"loss": 2.0})})
    manager.ingest("Loss", {0: ProbeResult(metrics={"loss": 1.5}), 1: ProbeResult()})
    manager.ingest("Temp", {"n1": ProbeResult(timestamp_s=5.0)})

    state = manager.build_state()
    assert state.results(Loss)[0].metrics == {"loss": 1.5}
    assert sorted(state.results(Loss)) == [0, 1]
    assert state.results(Loss)[1].timestamp_s is not None
    assert state.results(Temp)["n1"].timestamp_s == 5.0


def test_an_evict_is_decided_once_and_the_node_stays_evicted_across_restarts():
    evaluator = EvictHot()
    manager = HealthManager([HealthPolicy(evaluator_creator=lambda: [evaluator])])
    manager.ingest("Temp", {"n1": ProbeResult(metrics={"temp_c": 95.0})})

    assert manager.poll_decision() == Evict(reason="hot", target_nodes=["n1"])
    assert manager.poll_decision() is None
    assert manager.evicted_nodes == ["n1"]

    manager.on_worker_group_start()
    assert manager.build_state().probe_results == {}
    assert evaluator.starts == 1
    assert manager.evicted_nodes == ["n1"]


@pytest.mark.parametrize(
    "broken", [Raises(), Always(None), Always([Reattempt()])], ids=repr
)
def test_a_failing_evaluator_is_disabled_and_the_rest_keep_running(broken):
    healthy = Always(Reattempt(reason="hang"))
    manager = HealthManager([HealthPolicy(evaluator_creator=lambda: [broken, healthy])])
    assert manager.poll_decision() == Reattempt(reason="hang")
    assert manager.poll_decision() == Reattempt(reason="hang")
    assert broken.calls == 1
    assert healthy.calls == 2


def test_preflight_runs_only_the_preflight_evaluators_and_only_evicts():
    training = Always(Reattempt())
    manager = HealthManager(
        [
            HealthPolicy(evaluator_creator=lambda: [training]),
            HealthPolicy(
                probe_creator=lambda: [Temp()],
                evaluator_creator=lambda: [EvictHot(), Always(Reattempt())],
                preflight=True,
            ),
        ]
    )
    results = {
        "Temp": {
            "n1": ProbeResult(metrics={"temp_c": 95.0}),
            "n2": ProbeResult(metrics={"temp_c": 60.0}),
        }
    }
    assert manager.evaluate_preflight(results) == Evict(
        reason="hot", target_nodes=["n1"]
    )
    assert manager.evicted_nodes == ["n1"]
    assert training.calls == 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
