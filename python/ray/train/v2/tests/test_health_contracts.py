import sys

import pytest

from ray.train.health import (
    Diagnose,
    Evaluator,
    Evict,
    HealthCheck,
    HealthConfig,
    HealthDecision,
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
    check = HealthCheck(probe_creator=lambda: [Loss()])
    assert Diagnose(checks=[check]).target_ranks == []


def test_a_probe_must_implement_poll():
    class NoPoll(WorkerProbe):
        pass

    with pytest.raises(TypeError):
        NoPoll()


def test_evict_takes_its_nodes_by_keyword():
    with pytest.raises(TypeError):
        Evict(["n1"])
    with pytest.raises(TypeError):
        Evict(reason="hot")


def test_an_evaluator_returns_one_decision():
    assert Healthy().evaluate(HealthState()) == Noop()


def test_diagnose_needs_checks():
    with pytest.raises(TypeError):
        Diagnose(target_ranks=[0])


@pytest.mark.parametrize(
    "fields",
    [
        {"probe_creator": lambda: [HostTemp()]},
        {"evaluator_creator": lambda: [Healthy()]},
    ],
)
def test_valid_checks(fields):
    check = HealthCheck(**fields)
    assert HealthConfig(mid_training_checks=[check]).mid_training_checks == [check]


def test_a_check_with_neither_probes_nor_evaluators_is_rejected():
    with pytest.raises(ValueError):
        HealthCheck()


def test_health_config_defaults_to_no_checks():
    assert HealthConfig().mid_training_checks == []
    assert HealthConfig().preflight_checks == []


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
