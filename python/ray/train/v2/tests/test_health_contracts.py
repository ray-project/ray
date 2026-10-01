import sys

import pytest

from ray.train.health import (
    ControllerProbe,
    ControllerProbeContext,
    Diagnose,
    Evaluator,
    Evict,
    HealthConfig,
    HealthDecision,
    HealthPolicy,
    HealthState,
    NodeProbe,
    Noop,
    ProbeResult,
    Reattempt,
    WorkerProbe,
)


class HostTemp(NodeProbe):
    def poll(self):
        return ProbeResult(metrics={"gpu0_temp_c": 60.0})


class Named(NodeProbe):
    name = "custom"

    def poll(self):
        return ProbeResult()


class Loss(WorkerProbe):
    def poll(self):
        return ProbeResult(metrics={"loss": 1.0})


class CommProgress(ControllerProbe):
    def __init__(self):
        self.polls = 0

    def poll(self, ctx):
        self.polls += 1
        return {"comm0": ProbeResult(metrics={"ranks": float(len(ctx.rank_to_node))})}


class Healthy(Evaluator):
    def evaluate(self, state):
        return Noop()


def test_probe_name_defaults_to_the_class_name():
    assert HostTemp.probe_name() == "HostTemp"
    assert Named.probe_name() == "custom"


def test_probe_defaults():
    assert HostTemp.poll_interval_s == 10.0
    assert Loss.poll_interval_s == 10.0
    assert ProbeResult().timestamp_s is None


def test_health_decision_is_abstract():
    with pytest.raises(TypeError):
        HealthDecision(reason="x")


def test_decisions_are_constructed_as_subclasses():
    assert Noop().reason == ""
    assert Reattempt(reason="hang").reason == "hang"
    assert Evict(target_nodes=["n1"]).target_nodes == ["n1"]
    assert Diagnose(probe_creator=lambda: [Loss()]).target_ranks == []


def test_results_are_read_by_probe_class():
    state = HealthState(
        probe_results={
            "Loss": {0: ProbeResult(metrics={"loss": 2.0}), 1: ProbeResult()},
            "HostTemp": {"nB": ProbeResult(metrics={"gpu0_temp_c": 71.0})},
        }
    )
    assert state.results(Loss)[0].metrics == {"loss": 2.0}
    assert sorted(state.results(Loss)) == [0, 1]
    assert state.results(HostTemp) == {"nB": ProbeResult(metrics={"gpu0_temp_c": 71.0})}
    assert state.results(Named) == {}


def test_a_controller_probe_reports_its_own_keys_and_keeps_state():
    probe = CommProgress()
    ctx = ControllerProbeContext(rank_to_node={0: "nA", 1: "nB"})
    assert probe.poll(ctx) == {"comm0": ProbeResult(metrics={"ranks": 2.0})}
    probe.poll(ctx)
    assert probe.polls == 2
    assert ControllerProbeContext().rank_to_node == {}


def test_results_of_a_controller_probe_are_keyed_by_its_keys():
    state = HealthState(probe_results={"CommProgress": {"comm0": ProbeResult()}})
    assert state.results(CommProgress) == {"comm0": ProbeResult()}


def test_a_controller_probe_must_implement_poll():
    class NoPoll(ControllerProbe):
        pass

    with pytest.raises(TypeError):
        NoPoll()


def test_an_evaluator_returns_one_decision():
    assert Healthy().evaluate(HealthState()) == Noop()


def test_diagnose_needs_probes():
    with pytest.raises(TypeError):
        Diagnose(target_ranks=[0])


@pytest.mark.parametrize(
    "fields",
    [
        {"probe_creator": lambda: [HostTemp()]},
        {"evaluator_creator": lambda: [Healthy()]},
        {"probe_creator": lambda: [HostTemp()], "preflight": True},
    ],
)
def test_valid_policies(fields):
    policy = HealthPolicy(**fields)
    assert HealthConfig(policies=[policy]).policies == [policy]


@pytest.mark.parametrize("preflight", [False, True])
def test_a_policy_with_neither_probes_nor_evaluators_is_rejected(preflight):
    with pytest.raises(ValueError):
        HealthPolicy(preflight=preflight)


def test_health_config_defaults_to_no_policies():
    assert HealthConfig().policies == []


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
