# coding: utf-8
import gc
import logging
import os
import sys
import time
import pytest

import ray.cluster_utils
from ray._private.test_utils import (
    wait_for_pid_to_exit,
    client_test_enabled,
    run_string_as_driver,
)

import ray

logger = logging.getLogger(__name__)


def test_background_tasks_with_max_calls(shutdown_only):
    ray.init(
        # TODO (Alex): We need to fix
        # https://github.com/ray-project/ray/issues/20203 to remove this flag.
        num_cpus=2,
        _system_config={"worker_cap_initial_backoff_delay_ms": 0},
    )

    num_tasks = 3 if sys.platform == "win32" else 10

    @ray.remote
    def g():
        time.sleep(0.1)
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

    worker_1 = Actor.options(max_restarts=1, max_task_retries=-1).remote()
    ray.kill(worker_1, no_restart=False)
    assert ray.get(worker_1.foo.remote()) is None

    ray.kill(worker_1, no_restart=False)
    worker_2 = Actor.remote()
    assert ray.get(worker_2.foo.remote()) is None


@pytest.mark.skipif(
    client_test_enabled(), reason="client api doesn't support namespace right now."
)
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

    assert set(kv._internal_kv_list("k", namespace="n")) == {b"k1", b"k2", b"k3"}
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


def test_run_on_all_workers(ray_start_regular, tmp_path):
    # This test is to ensure run_function_on_all_workers are executed
    # on all workers.
    lock_file = tmp_path / "lock"
    data_file = tmp_path / "data"
    driver_script = f"""
import ray
from filelock import FileLock
from pathlib import Path
import pickle

lock_file = r"{str(lock_file)}"
data_file = Path(r"{str(data_file)}")

def init_func(worker_info):
    with FileLock(lock_file):
        if data_file.exists():
            old = pickle.loads(data_file.read_bytes())
        else:
            old = []
        old.append(worker_info['worker'].worker_id)
        data_file.write_bytes(pickle.dumps(old))

ray.worker.global_worker.run_function_on_all_workers(init_func)
ray.init(address='auto')

@ray.remote
def ready():
    with FileLock(lock_file):
        worker_ids = pickle.loads(data_file.read_bytes())
        assert ray.worker.global_worker.worker_id in worker_ids

ray.get(ready.remote())
"""
    run_string_as_driver(driver_script)
    run_string_as_driver(driver_script)
    run_string_as_driver(driver_script)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
