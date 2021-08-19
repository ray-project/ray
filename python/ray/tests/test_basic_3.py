# coding: utf-8
import logging
import os
import sys
import time

import numpy as np
import pytest

import ray.cluster_utils
from ray.test_utils import (
    dicts_equal,
    wait_for_pid_to_exit,
    wait_for_condition,
)
from pathlib import Path

import ray

logger = logging.getLogger(__name__)


def test_auto_global_gc(shutdown_only):
    # 100MB
    ray.init(num_cpus=1, object_store_memory=100 * 1024 * 1024)

    @ray.remote
    class Test:
        def __init__(self):
            self.collected = False
            import gc
            gc.disable()

            def gc_called(phase, info):
                self.collected = True

            gc.callbacks.append(gc_called)

        def circular_ref(self):
            # 20MB
            buf1 = b"0" * (10 * 1024 * 1024)
            buf2 = b"1" * (10 * 1024 * 1024)
            ref1 = ray.put(buf1)
            ref2 = ray.put(buf2)
            b = []
            a = []
            b.append(a)
            a.append(b)
            b.append(ref1)
            a.append(ref2)
            return a

        def collected(self):
            return self.collected

    test = Test.remote()
    # 60MB
    for i in range(3):
        ray.get(test.circular_ref.remote())
    time.sleep(2)
    assert not ray.get(test.collected.remote())
    # 80MB
    for _ in range(1):
        ray.get(test.circular_ref.remote())
    time.sleep(2)
    assert ray.get(test.collected.remote())


def test_many_fractional_resources(shutdown_only):
    ray.init(num_cpus=2, num_gpus=2, resources={"Custom": 2})

    @ray.remote
    def g():
        return 1

    @ray.remote
    def f(block, accepted_resources):
        true_resources = {
            resource: value[0][1]
            for resource, value in ray.worker.get_resource_ids().items()
        }
        if block:
            ray.get(g.remote())
        return dicts_equal(true_resources, accepted_resources)

    # Check that the resource are assigned correctly.
    result_ids = []
    for rand1, rand2, rand3 in np.random.uniform(size=(100, 3)):
        resource_set = {"CPU": int(rand1 * 10000) / 10000}
        result_ids.append(f._remote([False, resource_set], num_cpus=rand1))

        resource_set = {"CPU": 1, "GPU": int(rand1 * 10000) / 10000}
        result_ids.append(f._remote([False, resource_set], num_gpus=rand1))

        resource_set = {"CPU": 1, "Custom": int(rand1 * 10000) / 10000}
        result_ids.append(
            f._remote([False, resource_set], resources={"Custom": rand1}))

        resource_set = {
            "CPU": int(rand1 * 10000) / 10000,
            "GPU": int(rand2 * 10000) / 10000,
            "Custom": int(rand3 * 10000) / 10000
        }
        result_ids.append(
            f._remote(
                [False, resource_set],
                num_cpus=rand1,
                num_gpus=rand2,
                resources={"Custom": rand3}))
        result_ids.append(
            f._remote(
                [True, resource_set],
                num_cpus=rand1,
                num_gpus=rand2,
                resources={"Custom": rand3}))
    assert all(ray.get(result_ids))

    # Check that the available resources at the end are the same as the
    # beginning.
    stop_time = time.time() + 10
    correct_available_resources = False
    while time.time() < stop_time:
        available_resources = ray.available_resources()
        if ("CPU" in available_resources
                and ray.available_resources()["CPU"] == 2.0
                and "GPU" in available_resources
                and ray.available_resources()["GPU"] == 2.0
                and "Custom" in available_resources
                and ray.available_resources()["Custom"] == 2.0):
            correct_available_resources = True
            break
    if not correct_available_resources:
        assert False, "Did not get correct available resources."


def test_background_tasks_with_max_calls(shutdown_only):
    ray.init(num_cpus=2)

    @ray.remote
    def g():
        time.sleep(.1)
        return 0

    @ray.remote(max_calls=1, max_retries=0)
    def f():
        return [g.remote()]

    nested = ray.get([f.remote() for _ in range(10)])

    # Should still be able to retrieve these objects, since f's workers will
    # wait for g to finish before exiting.
    ray.get([x[0] for x in nested])

    @ray.remote(max_calls=1, max_retries=0)
    def f():
        return os.getpid(), g.remote()

    nested = ray.get([f.remote() for _ in range(10)])
    while nested:
        pid, g_id = nested.pop(0)
        ray.get(g_id)
        del g_id
        wait_for_pid_to_exit(pid)


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_fair_queueing(shutdown_only):
    ray.init(num_cpus=1)

    @ray.remote
    def h():
        return 0

    @ray.remote
    def g():
        return ray.get(h.remote())

    @ray.remote
    def f():
        return ray.get(g.remote())

    # This will never finish without fair queueing of {f, g, h}:
    # https://github.com/ray-project/ray/issues/3644
    ready, _ = ray.wait(
        [f.remote() for _ in range(1000)], timeout=60.0, num_returns=1000)
    assert len(ready) == 1000, len(ready)


def test_actor_killing(shutdown_only):
    # This is to test create and kill an actor immediately
    import ray
    ray.init(num_cpus=1)

    @ray.remote(num_cpus=1)
    class Actor:
        def foo(self):
            return None

    worker_1 = Actor.remote()
    ray.kill(worker_1)
    worker_2 = Actor.remote()
    assert ray.get(worker_2.foo.remote()) is None
    ray.kill(worker_2)

    worker_1 = Actor.options(max_restarts=1).remote()
    ray.kill(worker_1, no_restart=False)
    assert ray.get(worker_1.foo.remote()) is None

    ray.kill(worker_1, no_restart=False)
    worker_2 = Actor.remote()
    assert ray.get(worker_2.foo.remote()) is None


def test_actor_scheduling(shutdown_only):
    ray.init()

    @ray.remote
    class A:
        def run_fail(self):
            ray.actor.exit_actor()

        def get(self):
            return 1

    a = A.remote()
    a.run_fail.remote()
    with pytest.raises(Exception):
        ray.get([a.get.remote()])


@pytest.mark.parametrize(
    "ray_start_cluster", [{
        "_system_config": {
            "event_stats_print_interval_ms": 100,
            "debug_dump_period_milliseconds": 100,
            "event_stats": True
        }
    }],
    indirect=True)
def test_worker_startup_count(ray_start_cluster):
    """Test that no extra workers started while no available cpu resources
    in cluster."""

    cluster = ray_start_cluster
    # Cluster total cpu resources is 4.
    cluster.add_node(num_cpus=4, )
    ray.init(address=cluster.address)

    # A slow function never returns. It will hold cpu resources all the way.
    @ray.remote
    def slow_function():
        while True:
            time.sleep(1000)

    # Flood a large scale lease worker requests.
    for i in range(10000):
        # Use random cpu resources to make sure that all tasks are sent
        # to the raylet. Because core worker will cache tasks with the
        # same resource shape.
        num_cpus = 0.24 + np.random.uniform(0, 0.01)
        slow_function.options(num_cpus=num_cpus).remote()

    # Check "debug_state.txt" to ensure no extra workers were started.
    session_dir = ray.worker.global_worker.node.address_info["session_dir"]
    session_path = Path(session_dir)
    debug_state_path = session_path / "debug_state.txt"

    def get_num_workers():
        with open(debug_state_path) as f:
            for line in f.readlines():
                num_workers_prefix = "- num PYTHON workers: "
                if num_workers_prefix in line:
                    return int(line[len(num_workers_prefix):])
        return None

    # Wait for "debug_state.txt" to be updated to reflect the started worker.
    start = time.time()
    wait_for_condition(lambda: get_num_workers() == 16)
    time_waited = time.time() - start
    print(f"Waited {time_waited} for debug_state.txt to be updated")

    # Check that no more workers started for a while.
    for i in range(100):
        num = get_num_workers()
        assert num == 16
        time.sleep(0.1)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
