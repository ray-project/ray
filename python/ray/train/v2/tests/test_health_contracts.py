import sys

import pytest

from ray.train.health import (
    ControllerProbe,
    Diagnose,
    Evaluator,
    Evict,
    HealthCheck,
    HealthConfig,
    HealthDecision,
    HealthState,
    NodeProbe,
    Noop,
    PeriodicHealthCheck,
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

    def poll(self):
        self.polls += 1
        return {"comm0": ProbeResult(metrics={"polls": float(self.polls)})}


class Healthy(Evaluator):
    def evaluate(self, state):
        return Noop()


def test_probe_name_defaults_to_the_class_name():
    assert HostTemp.probe_name() == "HostTemp"
    assert Named.probe_name() == "custom"


def test_probe_result_defaults_to_no_timestamp_and_no_error():
    assert ProbeResult().timestamp_s is None
    assert ProbeResult().error is None


def test_a_probe_must_implement_poll():
    class NoPoll(WorkerProbe):
        pass

    with pytest.raises(TypeError):
        NoPoll()


def test_health_decision_is_abstract():
    with pytest.raises(TypeError):
        HealthDecision(reason="x")


def test_decisions_are_constructed_as_subclasses():
    assert Noop().reason == ""
    assert Reattempt(reason="hang").reason == "hang"
    assert Evict(target_nodes=["n1"]).target_nodes == ["n1"]
    check = HealthCheck(probe_creator=lambda: [Loss()])
    assert Diagnose(checks=[check]).checks == [check]


def test_evict_takes_its_nodes_by_keyword():
    with pytest.raises(TypeError):
        Evict(["n1"])
    with pytest.raises(TypeError):
        Evict(reason="hot")


def test_diagnose_needs_checks():
    with pytest.raises(TypeError):
        Diagnose(reason="slow")


def test_a_controller_probe_reports_its_own_keys_and_keeps_state():
    probe = CommProgress()
    assert probe.poll() == {"comm0": ProbeResult(metrics={"polls": 1.0})}
    assert probe.poll() == {"comm0": ProbeResult(metrics={"polls": 2.0})}


def test_a_controller_probe_must_implement_poll():
    class NoPoll(ControllerProbe):
        pass

    with pytest.raises(TypeError):
        NoPoll()


def test_an_evaluator_returns_one_decision():
    assert Healthy().evaluate(HealthState()) == Noop()


@pytest.mark.parametrize(
    "check", [HealthCheck(), PeriodicHealthCheck(poll_interval_s=1)], ids=repr
)
def test_a_check_defaults_to_no_probes_and_no_evaluators(check):
    assert check.probe_creator() == []
    assert check.evaluator_creator() == []


def test_a_periodic_check_needs_its_poll_interval_s_by_keyword():
    with pytest.raises(TypeError):
        PeriodicHealthCheck(probe_creator=lambda: [HostTemp()])
    with pytest.raises(TypeError):
        PeriodicHealthCheck(lambda: [HostTemp()], lambda: [], 5)


@pytest.mark.parametrize("poll_interval_s", [0, -1.0])
def test_poll_interval_s_must_be_positive(poll_interval_s):
    with pytest.raises(ValueError):
        PeriodicHealthCheck(poll_interval_s=poll_interval_s)


def test_health_config_holds_its_checks():
    inflight = PeriodicHealthCheck(
        probe_creator=lambda: [HostTemp()],
        evaluator_creator=lambda: [Healthy()],
        poll_interval_s=5,
    )
    preflight = HealthCheck(probe_creator=lambda: [HostTemp()])
    config = HealthConfig(inflight_checks=[inflight], preflight_checks=[preflight])
    assert config.inflight_checks == [inflight]
    assert config.preflight_checks == [preflight]


def test_health_config_defaults_to_no_checks():
    assert HealthConfig().inflight_checks == []
    assert HealthConfig().preflight_checks == []


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
