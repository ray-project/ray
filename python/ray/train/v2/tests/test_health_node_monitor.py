import os
import sys
import time

import pytest

import ray
from ray.train.health import ControllerProbe, NodeProbe, ProbeResult
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


class Comms(ControllerProbe):
    def poll(self):
        return {
            "comm0": ProbeResult(metrics={"pid": float(os.getpid())}),
            "comm1": ProbeResult(timestamp_s=1.0),
        }


class BrokenComms(ControllerProbe):
    def poll(self):
        raise RuntimeError("ncclras not found")


def _temp():
    return ScheduledProbe(Temp(), poll_interval_s=0.05)


def _pid():
    return ScheduledProbe(Pid(), poll_interval_s=3600.0)


def _comms():
    return ScheduledProbe(Comms(), poll_interval_s=0.05)


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
        lambda: len(monitor.poll_status().probe_results) == 2
        and monitor.poll_status().probe_results
    )
    time.sleep(0.3)
    second = monitor.poll_status().probe_results

    assert second["Temp"].timestamp_s > first["Temp"].timestamp_s
    assert second["Pid"] == first["Pid"]


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


def test_each_result_of_a_controller_probe_is_stamped():
    monitor = NodeMonitor([_comms()])
    results = _wait_for(lambda: monitor.poll_status().probe_results)["Comms"]
    assert results["comm0"].timestamp_s is not None
    assert results["comm1"].timestamp_s == 1.0


def test_poll_status_does_not_wait_for_a_slow_probe():
    monitor = NodeMonitor([_slow_probe("Slow", 2.0)])
    started = time.monotonic()
    assert monitor.poll_status().probe_results == {}
    assert time.monotonic() - started < 0.5


def _local_node_id():
    return ray.get_runtime_context().get_node_id()


def _start(group, node_id):
    group.start_worker_node_monitors([node_id])
    group.start_controller_node_monitor()


def _wait_for_status(group, node_id, field):
    """Poll ``group`` until both of its monitors have ``field`` set."""

    def status():
        status = group.poll_status(timeout=30)
        monitors = [
            status.node_monitor_statuses[node_id],
            status.controller_node_monitor_status,
        ]
        return all(getattr(m, field) for m in monitors) and status

    return _wait_for(status)


def test_each_monitor_samples_its_probes_outside_the_driver(ray_start_4_cpus):
    node_id = _local_node_id()
    group = NodeMonitorGroup([_pid(), _comms()])
    _start(group, node_id)

    status = _wait_for_status(group, node_id, "probe_results")
    (pid,) = status.node_monitor_statuses[node_id].probe_results.values()
    assert pid.metrics["pid"] != os.getpid()
    (comms,) = status.controller_node_monitor_status.probe_results.values()
    assert comms["comm0"].metrics["pid"] != os.getpid()

    group.shutdown()
    status = group.poll_status(timeout=1)
    assert status.node_monitor_statuses == {}
    assert status.controller_node_monitor_status is None


def test_start_adds_monitors_only_where_missing_and_shutdown_can_stop_some(
    ray_start_4_cpus,
):
    node_id = _local_node_id()
    group = NodeMonitorGroup([_temp()])
    _start(group, node_id)
    monitor = group._worker_node_monitors[node_id]
    controller_monitor = group._controller_node_monitor

    _start(group, node_id)
    assert group._worker_node_monitors[node_id] is monitor
    assert group._controller_node_monitor is controller_monitor

    group.shutdown_worker_node_monitors(["some-other-node"])
    assert group.worker_node_ids == [node_id]
    group.shutdown_worker_node_monitors([node_id])
    assert group.worker_node_ids == []
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(monitor.poll_status.remote())
    group.shutdown()


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
    group._worker_node_monitors["n1"] = UnresponsiveMonitor.remote()

    # The default timeout keeps a monitor that never answers from blocking.
    started = time.monotonic()
    first = group.poll_status().node_monitor_statuses["n1"]
    assert first.monitor_error is None
    assert time.monotonic() - started < 3

    time.sleep(2.5)
    second = group.poll_status(timeout=0.2).node_monitor_statuses["n1"]
    assert isinstance(second.monitor_error, TimeoutError)


def test_a_dead_monitor_gives_each_of_its_probes_its_error(ray_start_4_cpus):
    node_id = _local_node_id()
    group = NodeMonitorGroup([_temp(), _comms()])
    _start(group, node_id)
    ray.kill(group._worker_node_monitors[node_id])
    ray.kill(group._controller_node_monitor)

    status = _wait_for_status(group, node_id, "monitor_error")
    node_status = status.node_monitor_statuses[node_id]
    controller_status = status.controller_node_monitor_status
    assert isinstance(node_status.monitor_error, ray.exceptions.RayActorError)
    assert node_status.probe_results["Temp"].error is node_status.monitor_error
    assert controller_status.probe_results["Comms"].error is (
        controller_status.monitor_error
    )


def test_poll_probes_async_polls_probes_once_on_every_node_or_on_some(
    ray_start_4_cpus,
):
    node_id = _local_node_id()
    group = NodeMonitorGroup([_temp()])
    _start(group, node_id)

    group.poll_probes_async([Pid(), Broken(), Comms(), BrokenComms()], timeout_s=30)
    state = _wait_for(group.poll_probe_results)
    pid = state.probe_results["Pid"][node_id]
    assert pid.metrics["pid"] != os.getpid()
    assert isinstance(state.probe_results["Broken"][node_id].error, RuntimeError)
    assert list(state.controller_probe_results["Comms"]) == ["comm0", "comm1"]
    assert isinstance(state.controller_probe_results["BrokenComms"].error, RuntimeError)

    group.poll_probes_async([Pid()], timeout_s=30, node_ids=[node_id])
    state = _wait_for(group.poll_probe_results)
    assert state.probe_results["Pid"][node_id].metrics == pid.metrics
    with pytest.raises(ValueError, match="No NodeMonitor"):
        group.poll_probes_async([Pid()], timeout_s=30, node_ids=["some-other-node"])
    group.shutdown()


def test_poll_status_answers_while_probes_are_polled_on_demand(ray_start_4_cpus):
    node_id = _local_node_id()
    group = NodeMonitorGroup([_temp()])
    group.start_worker_node_monitors([node_id])
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
    group.start_worker_node_monitors([node_id])

    group.poll_probes_async([Slow()], timeout_s=30)
    group.shutdown_worker_node_monitors([node_id])
    state = _wait_for(group.poll_probe_results)
    error = state.probe_results["Slow"][node_id].error
    assert isinstance(error, ray.exceptions.RayActorError)


def test_a_monitor_that_does_not_answer_its_probes_in_time_gets_a_timeout_error(
    ray_start_4_cpus,
):
    group = NodeMonitorGroup([])
    group._worker_node_monitors["n1"] = UnresponsiveMonitor.remote()
    group._controller_node_monitor = UnresponsiveMonitor.remote()

    group.poll_probes_async([Pid(), Comms()], timeout_s=1.0)
    assert group.poll_probe_results() is None
    time.sleep(1.5)
    state = group.poll_probe_results()
    assert isinstance(state.probe_results["Pid"]["n1"].error, TimeoutError)
    assert isinstance(state.controller_probe_results["Comms"].error, TimeoutError)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
