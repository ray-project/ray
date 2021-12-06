# coding: utf-8
import gc
import logging
import os
import sys
import time
import subprocess

import numpy as np
import pytest

import ray.cluster_utils
from ray._private.gcs_pubsub import gcs_pubsub_enabled, \
    GcsFunctionKeySubscriber
from ray._private.test_utils import (
    dicts_equal,
    wait_for_pid_to_exit,
    wait_for_condition,
)
from ray.autoscaler._private.constants import RAY_PROCESSES
from pathlib import Path

import ray
import psutil

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


@pytest.mark.skipif(sys.platform == "win32", reason="Hangs on windows")
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


@pytest.mark.skipif(sys.platform == "win32", reason="Fails on windows")
def test_background_tasks_with_max_calls(shutdown_only):
    ray.init(
        # TODO (Alex): We need to fix
        # https://github.com/ray-project/ray/issues/20203 to remove this flag.
        num_cpus=2,
        _system_config={"worker_cap_initial_backoff_delay_ms": 0})

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
        assert ray.get(g_id) == 0
        del g_id
        # Necessary to dereference the object via GC, so the worker can exit.
        gc.collect()
        wait_for_pid_to_exit(pid)


@pytest.mark.skipif(sys.platform == "win32", reason="Hangs on windows")
def test_fair_queueing(shutdown_only):
    ray.init(
        num_cpus=1,
        _system_config={
            # Having parallel leases is slow in this case
            # because tasks are scheduled FIFO,
            # the more parallism we have,
            # the more workers we need to start to execute f and g tasks
            # before we can execute the first h task.
            "max_pending_lease_requests_per_scheduling_category": 1,
            "worker_cap_enabled": True,
        })

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
    timeout = 510.0 if sys.platform == "win32" else 60.0
    ready, _ = ray.wait(
        [f.remote() for _ in range(1000)], timeout=timeout, num_returns=1000)
    assert len(ready) == 1000, len(ready)


@pytest.mark.skipif(sys.platform == "win32", reason="Hangs on windows")
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


def test_worker_startup_count(ray_start_cluster):
    """Test that no extra workers started while no available cpu resources
    in cluster."""

    cluster = ray_start_cluster
    # Cluster total cpu resources is 4.
    cluster.add_node(
        num_cpus=4, _system_config={
            "debug_dump_period_milliseconds": 100,
        })
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
    debug_state_path = session_path / "logs" / "debug_state.txt"

    def get_num_workers():
        with open(debug_state_path) as f:
            for line in f.readlines():
                num_workers_prefix = "- num PYTHON workers: "
                if num_workers_prefix in line:
                    num_workers = int(line[len(num_workers_prefix):])
                    return num_workers
        return None

    # Wait for "debug_state.txt" to be updated to reflect the started worker.
    timeout_limit = 15
    start = time.time()
    wait_for_condition(lambda: get_num_workers() == 16, timeout=timeout_limit)
    time_waited = time.time() - start
    print(f"Waited {time_waited} for debug_state.txt to be updated")

    # Check that no more workers started for a while.
    for i in range(100):
        # Sometimes the debug state file can be empty. Retry if needed.
        for _ in range(3):
            num = get_num_workers()
            if num is None:
                print("Retrying parse debug_state.txt")
                time.sleep(0.05)
            else:
                break
        assert num == 16
        time.sleep(0.1)


@pytest.mark.skipif(sys.platform == "win32", reason="Hangs on windows")
def test_function_unique_export(ray_start_regular):
    @ray.remote
    def f():
        pass

    @ray.remote
    def g():
        ray.get(f.remote())

    if gcs_pubsub_enabled():
        subscriber = GcsFunctionKeySubscriber(
            channel=ray.worker.global_worker.gcs_channel.channel())
        subscriber.subscribe()

        ray.get(g.remote())

        # Poll pubsub channel for messages generated from running task g().
        num_exports = 0
        while True:
            key = subscriber.poll(timeout=1)
            if key is None:
                break
            else:
                num_exports += 1
        print(f"num_exports after running g(): {num_exports}")

        ray.get([g.remote() for _ in range(5)])

        key = subscriber.poll(timeout=1)
        assert key is None, f"Unexpected function key export: {key}"
    else:
        ray.get(g.remote())
        num_exports = ray.worker.global_worker.redis_client.llen("Exports")
        ray.get([g.remote() for _ in range(5)])
        assert ray.worker.global_worker.redis_client.llen("Exports") == \
               num_exports


@pytest.mark.skipif(
    sys.platform not in ["win32", "darwin"],
    reason="Only listen on localhost by default on mac and windows.")
@pytest.mark.parametrize("start_ray", ["ray_start_regular", "call_ray_start"])
def test_listen_on_localhost(start_ray, request):
    """All ray processes should listen on localhost by default
       on mac and windows to prevent security popups.
    """
    request.getfixturevalue(start_ray)

    process_infos = []
    for proc in psutil.process_iter(["name", "cmdline"]):
        try:
            process_infos.append((proc, proc.name(), proc.cmdline()))
        except psutil.Error:
            pass

    for keyword, filter_by_cmd in RAY_PROCESSES:
        for candidate in process_infos:
            proc, proc_cmd, proc_cmdline = candidate
            corpus = (proc_cmd if filter_by_cmd else
                      subprocess.list2cmdline(proc_cmdline))
            if keyword not in corpus:
                continue

            for connection in proc.connections():
                if connection.status != psutil.CONN_LISTEN:
                    continue
                # ip can be 127.0.0.1 or ::127.0.0.1
                assert "127.0.0.1" in connection.laddr.ip


def test_job_id_consistency(ray_start_regular):
    @ray.remote
    def foo():
        return "bar"

    @ray.remote
    class Foo:
        def ping(self):
            return "pong"

    @ray.remote
    def verify_job_id(job_id, new_thread):
        def verify():
            current_task_id = ray.runtime_context.get_runtime_context().task_id
            assert job_id == current_task_id.job_id()
            obj1 = foo.remote()
            assert job_id == obj1.job_id()
            obj2 = ray.put(1)
            assert job_id == obj2.job_id()
            a = Foo.remote()
            assert job_id == a._actor_id.job_id
            obj3 = a.ping.remote()
            assert job_id == obj3.job_id()

        if not new_thread:
            verify()
        else:
            exc = []

            def run():
                try:
                    verify()
                except BaseException as e:
                    exc.append(e)

            import threading
            t = threading.Thread(target=run)
            t.start()
            t.join()
            if len(exc) > 0:
                raise exc[0]

    job_id = ray.runtime_context.get_runtime_context().job_id
    ray.get(verify_job_id.remote(job_id, False))
    ray.get(verify_job_id.remote(job_id, True))


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
