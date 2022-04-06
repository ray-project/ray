# coding: utf-8
from concurrent.futures import ThreadPoolExecutor
import logging
import random
import sys
import threading
import time

import numpy as np
import pytest

import ray.cluster_utils

from ray._private.test_utils import (
    client_test_enabled,
)

if client_test_enabled():
    from ray.util.client import ray
else:
    import ray

logger = logging.getLogger(__name__)


@pytest.mark.skipif(
    client_test_enabled(), reason="grpc interaction with releasing resources"
)
def test_multithreading(ray_start_2_cpus):
    # This test requires at least 2 CPUs to finish since the worker does not
    # release resources when joining the threads.

    def run_test_in_multi_threads(test_case, num_threads=10, num_repeats=25):
        """A helper function that runs test cases in multiple threads."""

        def wrapper():
            for _ in range(num_repeats):
                test_case()
                time.sleep(random.randint(0, 10) / 1000.0)
            return "ok"

        executor = ThreadPoolExecutor(max_workers=num_threads)
        futures = [executor.submit(wrapper) for _ in range(num_threads)]
        for future in futures:
            assert future.result() == "ok"

    @ray.remote
    def echo(value, delay_ms=0):
        if delay_ms > 0:
            time.sleep(delay_ms / 1000.0)
        return value

    def test_api_in_multi_threads():
        """Test using Ray api in multiple threads."""

        @ray.remote
        class Echo:
            def echo(self, value):
                return value

        # Test calling remote functions in multiple threads.
        def test_remote_call():
            value = random.randint(0, 1000000)
            result = ray.get(echo.remote(value))
            assert value == result

        run_test_in_multi_threads(test_remote_call)

        # Test multiple threads calling one actor.
        actor = Echo.remote()

        def test_call_actor():
            value = random.randint(0, 1000000)
            result = ray.get(actor.echo.remote(value))
            assert value == result

        run_test_in_multi_threads(test_call_actor)

        # Test put and get.
        def test_put_and_get():
            value = random.randint(0, 1000000)
            result = ray.get(ray.put(value))
            assert value == result

        run_test_in_multi_threads(test_put_and_get)

        # Test multiple threads waiting for objects.
        num_wait_objects = 10
        objects = [echo.remote(i, delay_ms=10) for i in range(num_wait_objects)]

        def test_wait():
            ready, _ = ray.wait(
                objects,
                num_returns=len(objects),
                timeout=1000.0,
            )
            assert len(ready) == num_wait_objects
            assert ray.get(ready) == list(range(num_wait_objects))

        run_test_in_multi_threads(test_wait, num_repeats=1)

    # Run tests in a driver.
    test_api_in_multi_threads()

    # Run tests in a worker.
    @ray.remote
    def run_tests_in_worker():
        test_api_in_multi_threads()
        return "ok"

    assert ray.get(run_tests_in_worker.remote()) == "ok"

    # Test actor that runs background threads.
    @ray.remote
    class MultithreadedActor:
        def __init__(self):
            self.lock = threading.Lock()
            self.thread_results = []

        def background_thread(self, wait_objects):
            try:
                # Test wait
                ready, _ = ray.wait(
                    wait_objects,
                    num_returns=len(wait_objects),
                    timeout=1000.0,
                )
                assert len(ready) == len(wait_objects)
                for _ in range(20):
                    num = 10
                    # Test remote call
                    results = [echo.remote(i) for i in range(num)]
                    assert ray.get(results) == list(range(num))
                    # Test put and get
                    objects = [ray.put(i) for i in range(num)]
                    assert ray.get(objects) == list(range(num))
                    time.sleep(random.randint(0, 10) / 1000.0)
            except Exception as e:
                with self.lock:
                    self.thread_results.append(e)
            else:
                with self.lock:
                    self.thread_results.append("ok")

        def spawn(self):
            wait_objects = [echo.remote(i, delay_ms=10) for i in range(10)]
            self.threads = [
                threading.Thread(target=self.background_thread, args=(wait_objects,))
                for _ in range(20)
            ]
            [thread.start() for thread in self.threads]

        def join(self):
            [thread.join() for thread in self.threads]
            assert self.thread_results == ["ok"] * len(self.threads)
            return "ok"

    actor = MultithreadedActor.remote()
    actor.spawn.remote()
    ray.get(actor.join.remote()) == "ok"


@pytest.mark.skipif(client_test_enabled(), reason="internal api")
def test_wait_makes_object_local(ray_start_cluster_enabled):
    cluster = ray_start_cluster_enabled
    cluster.add_node(num_cpus=0)
    cluster.add_node(num_cpus=2)
    ray.init(address=cluster.address)

    @ray.remote
    class Foo:
        def method(self):
            return np.zeros(1024 * 1024)

    a = Foo.remote()

    # Test get makes the object local.
    x_id = a.method.remote()
    assert not ray.worker.global_worker.core_worker.object_exists(x_id)
    ray.get(x_id)
    assert ray.worker.global_worker.core_worker.object_exists(x_id)

    # Test wait makes the object local.
    x_id = a.method.remote()
    assert not ray.worker.global_worker.core_worker.object_exists(x_id)
    ok, _ = ray.wait([x_id])
    assert len(ok) == 1
    assert ray.worker.global_worker.core_worker.object_exists(x_id)


@pytest.mark.skipif(client_test_enabled(), reason="internal api")
def test_future_resolution_skip_plasma(ray_start_cluster_enabled):
    cluster = ray_start_cluster_enabled
    # Disable worker caching so worker leases are not reused; set object
    # inlining size threshold so the borrowed ref is inlined.
    cluster.add_node(
        num_cpus=1,
        resources={"pin_head": 1},
        _system_config={
            "worker_lease_timeout_milliseconds": 0,
            "max_direct_call_object_size": 100 * 1024,
        },
    )
    cluster.add_node(num_cpus=1, resources={"pin_worker": 1})
    ray.init(address=cluster.address)

    @ray.remote(resources={"pin_head": 1})
    def f(x):
        return x + 1

    @ray.remote(resources={"pin_worker": 1})
    def g(x):
        borrowed_ref = x[0]
        f_ref = f.remote(borrowed_ref)
        f_result = ray.get(f_ref)
        # borrowed_ref should be inlined on future resolution and shouldn't be
        # in Plasma.
        assert ray.worker.global_worker.core_worker.object_exists(
            borrowed_ref, memory_store_only=True
        )
        return f_result * 2

    one = f.remote(0)
    g_ref = g.remote([one])
    assert ray.get(g_ref) == 4


def test_task_output_inline_bytes_limit(ray_start_cluster_enabled):
    cluster = ray_start_cluster_enabled
    # Disable worker caching so worker leases are not reused; set object
    # inlining size threshold and enable storing of small objects in in-memory
    # object store so the borrowed ref is inlined.
    # set task_rpc_inlined_bytes_limit which only allows inline 20 bytes.
    cluster.add_node(
        num_cpus=1,
        resources={"pin_head": 1},
        _system_config={
            "worker_lease_timeout_milliseconds": 0,
            "max_direct_call_object_size": 100 * 1024,
            "task_rpc_inlined_bytes_limit": 20,
        },
    )
    cluster.add_node(num_cpus=1, resources={"pin_worker": 1})
    ray.init(address=cluster.address)

    @ray.remote(num_returns=5, resources={"pin_head": 1})
    def f():
        return list(range(5))

    @ray.remote(resources={"pin_worker": 1})
    def sum():
        numbers = f.remote()
        result = 0
        for i, ref in enumerate(numbers):
            result += ray.get(ref)
            inlined = ray.worker.global_worker.core_worker.object_exists(
                ref, memory_store_only=True
            )
            if i < 2:
                assert inlined
            else:
                assert not inlined
        return result

    assert ray.get(sum.remote()) == 10


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
