import os
import sys
import time

import pytest

import ray
import ray.cloudpickle
from ray.train.health import NodeProbe, ProbeResult
from ray.train.health._internal.node_monitor import NodeMonitor, start_node_monitor

# The subprocess and the actor unpickle these probes; ship them by value.
ray.cloudpickle.register_pickle_by_value(sys.modules[__name__])


class Temp(NodeProbe):
    poll_interval_s = 0.05

    def poll(self):
        return ProbeResult(metrics={"temp_c": 60.0})


class Broken(NodeProbe):
    poll_interval_s = 0.05

    def poll(self):
        raise RuntimeError("sensor gone")


class WhereAmI(NodeProbe):
    def poll(self):
        return ProbeResult(metrics={"pid": float(os.getpid())})


class Hangs(NodeProbe):
    def poll(self):
        time.sleep(600)


class Raises(NodeProbe):
    def poll(self):
        raise ValueError("bad check")


def _wait_for(fn, timeout_s=10.0):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        value = fn()
        if value:
            return value
        time.sleep(0.05)
    pytest.fail("timed out")


@pytest.fixture
def monitor():
    m = NodeMonitor([Temp(), Broken()])
    yield m
    m.stop()


def test_probes_are_polled_and_a_broken_one_does_not_stop_the_rest(monitor):
    results = _wait_for(monitor.latest_results)
    assert results["Temp"].metrics == {"temp_c": 60.0}
    assert results["Temp"].timestamp_s is not None
    assert "Broken" not in results


def test_no_results_without_probes():
    assert NodeMonitor([]).latest_results() == {}


def test_poll_once_runs_the_probe_in_a_subprocess(monitor):
    result = monitor.poll_once(WhereAmI(), timeout_s=30)
    assert result.metrics["pid"] != os.getpid()
    assert result.timestamp_s is not None


def test_a_hanging_probe_is_killed_and_the_monitor_keeps_working(monitor):
    started = time.monotonic()
    with pytest.raises(TimeoutError):
        monitor.poll_once(Hangs(), timeout_s=2)
    assert time.monotonic() - started < 15
    assert monitor.poll_once(WhereAmI(), timeout_s=30).metrics
    assert monitor.latest_results()


def test_a_probe_that_raises_is_reported(monitor):
    with pytest.raises(RuntimeError, match="bad check"):
        monitor.poll_once(Raises(), timeout_s=30)


@ray.remote(num_cpus=0)
class Worker:
    def pid(self):
        return os.getpid()


def test_the_monitor_actor_keeps_reporting_when_a_worker_on_its_node_dies(
    ray_start_4_cpus,
):
    node_id = ray.get_runtime_context().get_node_id()
    monitor = start_node_monitor(node_id, [Temp()])
    _wait_for(lambda: ray.get(monitor.latest_results.remote()))

    worker = Worker.options(
        scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
            node_id=node_id, soft=False
        )
    ).remote()
    ray.get(worker.pid.remote())
    ray.kill(worker)

    before = ray.get(monitor.latest_results.remote())["Temp"].timestamp_s
    _wait_for(
        lambda: ray.get(monitor.latest_results.remote())["Temp"].timestamp_s > before
    )


def test_a_slow_poll_once_does_not_block_latest_results(ray_start_4_cpus):
    node_id = ray.get_runtime_context().get_node_id()
    monitor = start_node_monitor(node_id, [Temp()])
    _wait_for(lambda: ray.get(monitor.latest_results.remote()))

    slow = monitor.poll_once.remote(Hangs(), 5)
    started = time.monotonic()
    assert ray.get(monitor.latest_results.remote(), timeout=3)
    assert time.monotonic() - started < 3
    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(slow)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
