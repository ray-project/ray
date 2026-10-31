import os
import sys
import time

import pytest

import ray
from ray.train.health import NodeProbe, ProbeResult
from ray.train.health._internal.node_monitor import NodeMonitor
from ray.train.health._internal.node_monitor_group import NodeMonitorGroup
from ray.train.health._internal.probe_utils import ScheduledProbe


class Temp(NodeProbe):
    def poll(self):
        return ProbeResult(metrics={"temp_c": 60.0})


class Pid(NodeProbe):
    def poll(self):
        return ProbeResult(metrics={"pid": float(os.getpid())})


class Broken(NodeProbe):
    def poll(self):
        raise RuntimeError("sensor gone")


class Slow(NodeProbe):
    def poll(self):
        time.sleep(5)
        return ProbeResult()


def _temp():
    return ScheduledProbe(Temp(), poll_interval_s=0.05)


def _pid():
    return ScheduledProbe(Pid(), poll_interval_s=3600.0)


def _slow_probe(name, seconds):
    def poll(self):
        time.sleep(seconds)
        return ProbeResult(metrics={"seconds": seconds})

    probe = type(name, (NodeProbe,), {"name": name, "poll": poll})()
    return ScheduledProbe(probe, poll_interval_s=10.0)


def _wait_for(fn, timeout_s=10.0):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        value = fn()
        if value:
            return value
        time.sleep(0.05)
    pytest.fail("timed out")


def test_each_probe_is_sampled_on_its_own_interval():
    monitor = NodeMonitor([_temp(), _pid()])
    first = _wait_for(
        lambda: len(monitor.poll_status().probe_results) == 2 and monitor.poll_status()
    )
    time.sleep(0.3)
    second = monitor.poll_status().probe_results

    assert second["Temp"].timestamp_s > first.probe_results["Temp"].timestamp_s
    assert second["Pid"] == first.probe_results["Pid"]


def test_a_failing_probe_is_reported_and_the_others_keep_sampling():
    monitor = NodeMonitor([ScheduledProbe(Broken(), poll_interval_s=10.0), _temp()])
    results = _wait_for(
        lambda: len(monitor.poll_status().probe_results) == 2
        and monitor.poll_status().probe_results
    )
    assert isinstance(results["Broken"].error, RuntimeError)
    assert results["Broken"].timestamp_s is not None
    assert results["Temp"].metrics == {"temp_c": 60.0}
    assert results["Temp"].error is None


def test_poll_status_does_not_wait_for_a_slow_probe():
    monitor = NodeMonitor([_slow_probe("Slow", 2.0)])
    started = time.monotonic()
    assert monitor.poll_status().probe_results == {}
    assert time.monotonic() - started < 0.5


def _local_node_id():
    return ray.get_runtime_context().get_node_id()


def test_monitors_sample_outside_the_driver_and_stop_on_shutdown(ray_start_4_cpus):
    node_id = _local_node_id()
    group = NodeMonitorGroup([_temp(), _pid()])
    group.start([node_id])

    def results():
        return (
            group.poll_status(timeout=30).node_monitor_statuses[node_id].probe_results
        )

    latest = _wait_for(lambda: len(results()) == 2 and results())
    assert latest["Pid"].metrics["pid"] != os.getpid()

    group.shutdown()
    assert group.poll_status(timeout=1).node_monitor_statuses == {}


def test_start_adds_monitors_only_where_missing_and_shutdown_can_stop_some(
    ray_start_4_cpus,
):
    node_id = _local_node_id()
    group = NodeMonitorGroup([_temp()])
    group.start([node_id])
    monitor = group._monitors[node_id]

    group.start([node_id])
    assert group._monitors[node_id] is monitor

    group.shutdown(["some-other-node"])
    assert group.node_ids == [node_id]
    group.shutdown([node_id])
    assert group.node_ids == []
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(monitor.poll_status.remote())


@ray.remote(num_cpus=0)
class UnresponsiveMonitor:
    def poll_status(self):
        time.sleep(600)

    def poll_probes(self, probes):
        time.sleep(600)


def test_an_unresponsive_monitor_is_reported_after_the_health_check_timeout(
    ray_start_4_cpus,
):
    group = NodeMonitorGroup([], health_check_timeout_s=3.0)
    group._monitors["n1"] = UnresponsiveMonitor.remote()

    # The default timeout keeps a monitor that never answers from blocking.
    started = time.monotonic()
    first = group.poll_status().node_monitor_statuses["n1"]
    assert first.monitor_error is None
    assert time.monotonic() - started < 3

    time.sleep(2.5)
    second = group.poll_status(timeout=0.2).node_monitor_statuses["n1"]
    assert isinstance(second.monitor_error, TimeoutError)


def test_a_dead_monitor_is_reported(ray_start_4_cpus):
    node_id = _local_node_id()
    group = NodeMonitorGroup([_temp()])
    group.start([node_id])
    group.poll_status(timeout=30)

    ray.kill(group._monitors[node_id])

    # The kill is asynchronous, so a poll right after it may still be answered.
    errors = _wait_for(lambda: group.poll_status(timeout=30).errors)
    assert isinstance(errors[node_id], ray.exceptions.RayActorError)
    group.shutdown()


def test_poll_probes_async_polls_probes_once_on_every_node_or_on_some(
    ray_start_4_cpus,
):
    node_id = _local_node_id()
    group = NodeMonitorGroup([_temp()])
    group.start([node_id])

    group.poll_probes_async([Pid(), Broken()], timeout_s=30)
    results = _wait_for(group.poll_probe_results)
    pid = results["Pid"][node_id]
    assert pid.metrics["pid"] != os.getpid()
    assert pid.timestamp_s is not None
    assert isinstance(results["Broken"][node_id].error, RuntimeError)

    group.poll_probes_async([Pid()], timeout_s=30, node_ids=[node_id])
    assert _wait_for(group.poll_probe_results)["Pid"][node_id].metrics == pid.metrics
    with pytest.raises(ValueError, match="No NodeMonitor"):
        group.poll_probes_async([Pid()], timeout_s=30, node_ids=["some-other-node"])
    group.shutdown()


def test_poll_status_answers_while_probes_are_polled_on_demand(ray_start_4_cpus):
    node_id = _local_node_id()
    group = NodeMonitorGroup([_temp()])
    group.start([node_id])
    _wait_for(
        lambda: group.poll_status(timeout=30)
        .node_monitor_statuses[node_id]
        .probe_results
    )

    group.poll_probes_async([Slow()], timeout_s=30)
    status = group.poll_status(timeout=2).node_monitor_statuses[node_id]
    assert "Temp" in status.probe_results
    group.shutdown()


def test_a_monitor_stopped_during_on_demand_probes_answers_with_its_error(
    ray_start_4_cpus,
):
    node_id = _local_node_id()
    group = NodeMonitorGroup([])
    group.start([node_id])

    group.poll_probes_async([Slow()], timeout_s=30)
    group.shutdown([node_id])
    results = _wait_for(group.poll_probe_results)
    assert isinstance(results["Slow"][node_id].error, ray.exceptions.RayActorError)


def test_a_node_that_does_not_answer_its_probes_in_time_gets_a_timeout_error(
    ray_start_4_cpus,
):
    group = NodeMonitorGroup([])
    group._monitors["n1"] = UnresponsiveMonitor.remote()

    group.poll_probes_async([Pid()], timeout_s=1.0)
    assert group.poll_probe_results() is None
    time.sleep(1.5)
    assert isinstance(group.poll_probe_results()["Pid"]["n1"].error, TimeoutError)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
