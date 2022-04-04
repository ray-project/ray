# coding: utf-8
import logging
import os
import pickle
import sys
import time

import numpy as np
import pytest

import ray.cluster_utils
from ray._private.test_utils import (
    client_test_enabled,
    get_error_message,
    SignalActor,
    run_string_as_driver,
)

import ray

logger = logging.getLogger(__name__)


# https://github.com/ray-project/ray/issues/6662
@pytest.mark.skipif(client_test_enabled(), reason="interferes with grpc")
def test_ignore_http_proxy(shutdown_only):
    ray.init(num_cpus=1)
    os.environ["http_proxy"] = "http://example.com"
    os.environ["https_proxy"] = "http://example.com"

    @ray.remote
    def f():
        return 1

    assert ray.get(f.remote()) == 1


# https://github.com/ray-project/ray/issues/16025
def test_release_resources_race(shutdown_only):
    # This test fails with the flag set to false.
    ray.init(
        num_cpus=2,
        object_store_memory=700e6,
        _system_config={"inline_object_status_in_refs": True},
    )
    refs = []
    for _ in range(10):
        refs.append(ray.put(np.zeros(20 * 1024 * 1024, dtype=np.uint8)))

    @ray.remote
    def consume(refs):
        # Should work without releasing resources!
        ray.get(refs)
        return os.getpid()

    pids = set(ray.get([consume.remote(refs) for _ in range(1000)]))
    # Should not have started multiple workers.
    assert len(pids) <= 2, pids


# https://github.com/ray-project/ray/issues/22504
def test_worker_isolation_by_resources(shutdown_only):
    ray.init(num_cpus=1, num_gpus=1)

    @ray.remote(num_gpus=1)
    def gpu():
        return os.getpid()

    @ray.remote
    def cpu():
        return os.getpid()

    pid1 = ray.get(cpu.remote())
    pid2 = ray.get(gpu.remote())
    assert pid1 != pid2, (pid1, pid2)


# https://github.com/ray-project/ray/issues/10960
def test_max_calls_releases_resources(shutdown_only):
    ray.init(num_cpus=2, num_gpus=1)

    @ray.remote(num_cpus=0)
    def g():
        return 0

    @ray.remote(num_cpus=1, num_gpus=1, max_calls=1, max_retries=0)
    def f():
        return [g.remote()]

    for i in range(10):
        print(i)
        ray.get(f.remote())  # This will hang if GPU resources aren't released.


# https://github.com/ray-project/ray/issues/7263
def test_grpc_message_size(shutdown_only):
    ray.init(num_cpus=1)

    @ray.remote
    def bar(*a):
        return

    # 50KiB, not enough to spill to plasma, but will be inlined.
    def f():
        return np.zeros(50000, dtype=np.uint8)

    # Executes a 10MiB task spec
    ray.get(bar.remote(*[f() for _ in range(200)]))


# https://github.com/ray-project/ray/issues/7287
def test_omp_threads_set(shutdown_only):
    ray.init(num_cpus=1)
    # Should have been auto set by ray init.
    assert os.environ["OMP_NUM_THREADS"] == "1"


def test_submit_api(shutdown_only):
    ray.init(num_cpus=2, num_gpus=1, resources={"Custom": 1})

    @ray.remote
    def f(n):
        return list(range(n))

    @ray.remote
    def g():
        return ray.get_gpu_ids()

    assert f._remote([0], num_returns=0) is None
    id1 = f._remote(args=[1], num_returns=1)
    assert ray.get(id1) == [0]
    id1, id2 = f._remote(args=[2], num_returns=2)
    assert ray.get([id1, id2]) == [0, 1]
    id1, id2, id3 = f._remote(args=[3], num_returns=3)
    assert ray.get([id1, id2, id3]) == [0, 1, 2]
    assert ray.get(
        g._remote(args=[], num_cpus=1, num_gpus=1, resources={"Custom": 1})
    ) == [0]
    infeasible_id = g._remote(args=[], resources={"NonexistentCustom": 1})
    assert ray.get(g._remote()) == []
    ready_ids, remaining_ids = ray.wait([infeasible_id], timeout=0.05)
    assert len(ready_ids) == 0
    assert len(remaining_ids) == 1

    # Check mismatch with num_returns.
    with pytest.raises(ValueError):
        ray.get(f.options(num_returns=2).remote(3))
    with pytest.raises(ValueError):
        ray.get(f.options(num_returns=3).remote(2))

    @ray.remote
    class Actor:
        def __init__(self, x, y=0):
            self.x = x
            self.y = y

        def method(self, a, b=0):
            return self.x, self.y, a, b

        def gpu_ids(self):
            return ray.get_gpu_ids()

    @ray.remote
    class Actor2:
        def __init__(self):
            pass

        def method(self):
            pass

    a = Actor._remote(args=[0], kwargs={"y": 1}, num_gpus=1, resources={"Custom": 1})

    a2 = Actor2._remote()
    ray.get(a2.method._remote())

    id1, id2, id3, id4 = a.method._remote(args=["test"], kwargs={"b": 2}, num_returns=4)
    assert ray.get([id1, id2, id3, id4]) == [0, 1, "test", 2]


def test_invalid_arguments(shutdown_only):
    ray.init(num_cpus=2)

    for opt in [np.random.randint(-100, -1), np.random.uniform(0, 1)]:
        with pytest.raises(
            ValueError,
            match="The keyword 'num_returns' only accepts 0 or a positive integer",
        ):

            @ray.remote(num_returns=opt)
            def g1():
                return 1

    for opt in [np.random.randint(-100, -2), np.random.uniform(0, 1)]:
        with pytest.raises(
            ValueError,
            match="The keyword 'max_retries' only accepts 0, -1 or a"
            " positive integer",
        ):

            @ray.remote(max_retries=opt)
            def g2():
                return 1

    for opt in [np.random.randint(-100, -1), np.random.uniform(0, 1)]:
        with pytest.raises(
            ValueError,
            match="The keyword 'max_calls' only accepts 0 or a positive integer",
        ):

            @ray.remote(max_calls=opt)
            def g3():
                return 1

    for opt in [np.random.randint(-100, -2), np.random.uniform(0, 1)]:
        with pytest.raises(
            ValueError,
            match="The keyword 'max_restarts' only accepts -1, 0 or a"
            " positive integer",
        ):

            @ray.remote(max_restarts=opt)
            class A1:
                x = 1

    for opt in [np.random.randint(-100, -2), np.random.uniform(0, 1)]:
        with pytest.raises(
            ValueError,
            match="The keyword 'max_task_retries' only accepts -1, 0 or a"
            " positive integer",
        ):

            @ray.remote(max_task_retries=opt)
            class A2:
                x = 1


def test_user_setup_function():
    script = """
import ray
ray.init()
@ray.remote
def get_pkg_dir():
    return ray._private.runtime_env.VAR

print("remote", ray.get(get_pkg_dir.remote()))
print("local", ray._private.runtime_env.VAR)


"""

    env = {"RAY_USER_SETUP_FUNCTION": "ray._private.test_utils.set_setup_func"}
    out = run_string_as_driver(script, dict(os.environ, **env))
    (remote_out, local_out) = out.strip().splitlines()[-2:]
    assert remote_out == "remote hello world"
    assert local_out == "local hello world"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
