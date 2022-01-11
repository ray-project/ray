# coding: utf-8
import gc
import logging
import os
import sys
import time
import pytest

import psutil
import ray.cluster_utils
from ray._private.test_utils import client_test_enabled, wait_for_condition
from ray._private.test_utils import wait_for_pid_to_exit
from ray.experimental.internal_kv import _internal_kv_list

import ray

logger = logging.getLogger(__name__)


def test_background_tasks_with_max_calls(shutdown_only):
    ray.init(
        # TODO (Alex): We need to fix
        # https://github.com/ray-project/ray/issues/20203 to remove this flag.
        num_cpus=2,
        _system_config={"worker_cap_initial_backoff_delay_ms": 0})

    num_tasks = 3 if sys.platform == "win32" else 10

    @ray.remote
    def g():
        time.sleep(.1)
        return 0

    @ray.remote(max_calls=1, max_retries=0)
    def f():
        return [g.remote()]

    nested = ray.get([f.remote() for _ in range(num_tasks)])

    # Should still be able to retrieve these objects, since f's workers will
    # wait for g to finish before exiting.
    ray.get([x[0] for x in nested])

    @ray.remote(max_calls=1, max_retries=0)
    def f():
        return os.getpid(), g.remote()

    nested = ray.get([f.remote() for _ in range(num_tasks)])
    while nested:
        pid, g_id = nested.pop(0)
        assert ray.get(g_id) == 0
        del g_id
        # Necessary to dereference the object via GC, so the worker can exit.
        gc.collect()
        wait_for_pid_to_exit(pid)


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


@pytest.mark.skipif(
    client_test_enabled(),
    reason="client api doesn't support namespace right now.")
def test_internal_kv(ray_start_regular):
    import ray.experimental.internal_kv as kv
    assert kv._internal_kv_get("k1") is None
    assert kv._internal_kv_put("k1", "v1") is False
    assert kv._internal_kv_put("k1", "v1") is True
    assert kv._internal_kv_get("k1") == b"v1"

    assert kv._internal_kv_get("k1", namespace="n") is None
    assert kv._internal_kv_put("k1", "v1", namespace="n") is False
    assert kv._internal_kv_put("k1", "v1", namespace="n") is True
    assert kv._internal_kv_put("k1", "v2", True, namespace="n") is True
    assert kv._internal_kv_get("k1", namespace="n") == b"v2"

    assert kv._internal_kv_del("k1") == 1
    assert kv._internal_kv_del("k1") == 0
    assert kv._internal_kv_get("k1") is None

    assert kv._internal_kv_put("k2", "v2", namespace="n") is False
    assert kv._internal_kv_put("k3", "v3", namespace="n") is False

    assert set(kv._internal_kv_list("k",
                                    namespace="n")) == {b"k1", b"k2", b"k3"}
    assert kv._internal_kv_del("k", del_by_prefix=True, namespace="n") == 3
    assert kv._internal_kv_del("x", del_by_prefix=True, namespace="n") == 0
    assert kv._internal_kv_get("k1", namespace="n") is None
    assert kv._internal_kv_get("k2", namespace="n") is None
    assert kv._internal_kv_get("k3", namespace="n") is None

    with pytest.raises(RuntimeError):
        kv._internal_kv_put("@namespace_", "x", True)
    with pytest.raises(RuntimeError):
        kv._internal_kv_get("@namespace_", namespace="n")
    with pytest.raises(RuntimeError):
        kv._internal_kv_del("@namespace_def", namespace="n")
    with pytest.raises(RuntimeError):
        kv._internal_kv_list("@namespace_abc", namespace="n")


def get_gcs_memory_used():
    m = sum([
        process.memory_info().rss for process in psutil.process_iter()
        if process.name() in ("gcs_server", "redis-server")
    ])
    print(m)
    return m


def function_entry_num(job_id):
    from ray.ray_constants import KV_NAMESPACE_FUNCTION_TABLE
    return len(_internal_kv_list(b"IsolatedExports:" + job_id,
                                 namespace=KV_NAMESPACE_FUNCTION_TABLE)) + \
        len(_internal_kv_list(b"RemoteFunction:" + job_id,
                              namespace=KV_NAMESPACE_FUNCTION_TABLE)) + \
        len(_internal_kv_list(b"ActorClass:" + job_id,
                              namespace=KV_NAMESPACE_FUNCTION_TABLE))


@pytest.mark.skipif(
    client_test_enabled(),
    reason="client api doesn't support namespace right now.")
def test_function_table_gc(call_ray_start):
    """This test tries to verify that function table is cleaned up
    after job exits.
    """

    def f():
        data = "0" * 1024 * 1024  # 1MB

        @ray.remote
        def r():
            nonlocal data

            @ray.remote
            class Actor:
                pass

        return r.remote()

    ray.init(address="auto", namespace="b")

    # It should use > 500MB data
    ray.get([f() for _ in range(500)])

    assert get_gcs_memory_used() > 500 * 1024 * 1024
    job_id = ray.worker.global_worker.current_job_id.binary()

    ray.shutdown()

    # now check the function table is cleaned up after job finished
    ray.init(address="auto", namespace="a")
    wait_for_condition(lambda: function_entry_num(job_id) == 0)


@pytest.mark.skipif(
    client_test_enabled(),
    reason="client api doesn't support namespace right now.")
def test_function_table_gc_actor(call_ray_start):
    """If there is a detached actor, the table won't be cleaned up.
    """
    ray.init(address="auto", namespace="a")

    @ray.remote
    class Actor:
        def ready(self):
            return

    # If there is a detached actor, function won't be deleted
    a = Actor.options(lifetime="detached", name="a").remote()
    ray.get(a.ready.remote())
    job_id = ray.worker.global_worker.current_job_id.binary()
    ray.shutdown()

    ray.init(address="auto", namespace="b")
    with pytest.raises(Exception):
        wait_for_condition(lambda: function_entry_num(job_id) == 0)
    a = ray.get_actor("a", namespace="a")
    ray.kill(a)
    wait_for_condition(lambda: function_entry_num(job_id) == 0)

    # If it's not a detached actor, it'll be deleted once job finished
    a = Actor.remote()
    ray.get(a.ready.remote())
    job_id = ray.worker.global_worker.current_job_id.binary()
    ray.shutdown()
    ray.init(address="auto", namespace="c")
    wait_for_condition(lambda: function_entry_num(job_id) == 0)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
