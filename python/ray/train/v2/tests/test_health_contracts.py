import sys

import pytest

from ray.train.health import (
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
