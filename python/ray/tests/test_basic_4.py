# coding: utf-8
import logging
import subprocess
import sys
import time
from pathlib import Path
import os

import pytest
from unittest import mock

import ray
import ray.cluster_utils
from ray._private.test_utils import wait_for_condition
from ray.autoscaler._private.constants import RAY_PROCESSES

import psutil

logger = logging.getLogger(__name__)


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
        num_cpus=4,
        _system_config={
            "debug_dump_period_milliseconds": 100,
        },
    )
    ray.init(address=cluster.address)

    # A slow function never returns. It will hold cpu resources all the way.
    @ray.remote
    def slow_function():
        while True:
            time.sleep(1000)

    # Flood a large scale lease worker requests.
    for i in range(10000):
        slow_function.options(num_cpus=0.25).remote()

    # Check "debug_state.txt" to ensure no extra workers were started.
    session_dir = ray._private.worker.global_worker.node.address_info["session_dir"]
    session_path = Path(session_dir)
    debug_state_path = session_path / "logs" / "debug_state.txt"

    def get_num_workers():
        with open(debug_state_path) as f:
            for line in f.readlines():
                num_workers_prefix = "- num PYTHON workers: "
                if num_workers_prefix in line:
                    num_workers = int(line[len(num_workers_prefix) :])
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


def test_function_import_without_importer_thread(shutdown_only):
    """Test that without background importer thread, dependencies can still be
    imported in workers."""
    ray.init(
        _system_config={
            "start_python_importer_thread": False,
        },
    )

    @ray.remote
    def f():
        import threading

        assert threading.get_ident() == threading.main_thread().ident
        # Make sure the importer thread is not running.
        for thread in threading.enumerate():
            assert "import" not in thread.name

    @ray.remote
    def g():
        ray.get(f.remote())

    ray.get(g.remote())
    ray.get([g.remote() for _ in range(5)])


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Fork is only supported on *nix systems.",
)
def test_fork_support(shutdown_only):
    """Test that fork support works."""
    ray.init(
        _system_config={
            "support_fork": True,
        },
    )

    @ray.remote
    def pool_factorial():
        import math
        import multiprocessing

        ctx = multiprocessing.get_context("fork")
        with ctx.Pool(processes=4) as pool:
            return sum(pool.map(math.factorial, range(8)))

    @ray.remote
    def g():
        import threading

        assert threading.get_ident() == threading.main_thread().ident
        # Make sure this is the only Python thread, because forking does not
        # work well under multi-threading.
        assert threading.active_count() == 1

        return ray.get(pool_factorial.remote())

    assert ray.get(g.remote()) == 5914


@pytest.mark.skipif(
    sys.platform not in ["win32", "darwin"],
    reason="Only listen on localhost by default on mac and windows.",
)
@mock.patch("ray._private.services.ray_constants.ENABLE_RAY_CLUSTER", False)
@mock.patch.dict(os.environ, {"RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER": "0"})
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
            corpus = (
                proc_cmd if filter_by_cmd else subprocess.list2cmdline(proc_cmdline)
            )
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
        },
    )

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
    timeout = 60.0
    ready, _ = ray.wait(
        [f.remote() for _ in range(1000)], timeout=timeout, num_returns=1000
    )
    assert len(ready) == 1000, len(ready)


if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
