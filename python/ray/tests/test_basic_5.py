# coding: utf-8
import gc
import logging
import os
import subprocess
import sys
import time
import unittest
from unittest.mock import Mock, patch

import pytest

import ray
import ray.cluster_utils
from ray._common.constants import HEAD_NODE_RESOURCE_NAME
from ray._private.test_utils import (
    client_test_enabled,
    run_string_as_driver,
    wait_for_pid_to_exit,
)

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


def test_internal_kv(ray_start_regular):
    import ray.experimental.internal_kv as kv

    assert kv._internal_kv_get("k1") is None
    assert kv._internal_kv_put("k1", "v1") is False
    assert kv._internal_kv_put("k1", "v1") is True
    assert kv._internal_kv_get("k1") == b"v1"
    assert kv._internal_kv_exists(b"k1") is True
    assert kv._internal_kv_exists(b"k2") is False

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

    with pytest.raises(ray.exceptions.RaySystemError):
        kv._internal_kv_put("@namespace_", "x", True)
    with pytest.raises(ray.exceptions.RaySystemError):
        kv._internal_kv_get("@namespace_", namespace="n")
    with pytest.raises(ray.exceptions.RaySystemError):
        kv._internal_kv_del("@namespace_def", namespace="n")
    with pytest.raises(ray.exceptions.RaySystemError):
        kv._internal_kv_list("@namespace_abc", namespace="n")


def test_exit_logging():
    log = run_string_as_driver(
        """
import ray

@ray.remote
class A:
    def pid(self):
        import os
        return os.getpid()


a = A.remote()
ray.get(a.pid.remote())
    """
    )
    assert "Traceback" not in log


def test_worker_sys_path_contains_driver_script_directory(tmp_path, monkeypatch):
    package_folder = tmp_path / "package"
    package_folder.mkdir()
    init_file = package_folder / "__init__.py"
    init_file.write_text("")

    module1_file = package_folder / "module1.py"
    module1_file.write_text(
        f"""
import sys
import ray
ray.init()

@ray.remote
def sys_path():
    return sys.path

remote_sys_path = ray.get(sys_path.remote())
assert r'{str(package_folder)}' in remote_sys_path, remote_sys_path
"""
    )

    # Ray's handling of sys.path does not work with PYTHONSAFEPATH.
    env = os.environ.copy()
    if env.get("PYTHONSAFEPATH", "") != "":
        env["PYTHONSAFEPATH"] = ""  # Set to empty string to disable.
    subprocess.check_call([sys.executable, str(module1_file)], env=env)

    # If the driver script is run via `python -m`,
    # the script directory is not included in sys.path.
    module2_file = package_folder / "module2.py"
    module2_file.write_text(
        f"""
import sys
import ray
ray.init()

@ray.remote
def sys_path():
    return sys.path

remote_sys_path = ray.get(sys_path.remote())
assert r'{str(package_folder)}' not in remote_sys_path, remote_sys_path
"""
    )
    monkeypatch.chdir(str(tmp_path))
    subprocess.check_call([sys.executable, "-m", "package.module2"], env=env)


def test_worker_kv_calls(monkeypatch, shutdown_only):
    monkeypatch.setenv("TEST_RAY_COLLECT_KV_FREQUENCY", "1")
    ray.init()

    @ray.remote
    def get_kv_metrics():
        from time import sleep

        sleep(2)
        return ray._private.utils._CALLED_FREQ

    freqs = ray.get(get_kv_metrics.remote())
    # So far we have the following gets
    """
    b'cluster' b'CLUSTER_METADATA'
    b'tracing' b'tracing_startup_hook'
    b'fun' b'RemoteFunction:01000000:\x00\x00\x00\x00\x00\x00\x00\x01'
    """
    # !!!If you want to increase this number, please let ray-core knows this!!!
    assert freqs["internal_kv_get"] == 3


@pytest.mark.skipif(sys.platform == "win32", reason="Fails on Windows.")
@pytest.mark.parametrize("root_process_no_site", [0, 1])
@pytest.mark.parametrize("root_process_no_user_site", [0, 1])
def test_site_flag_inherited(
    shutdown_only, monkeypatch, root_process_no_site, root_process_no_user_site
):
    # The flags we're testing could prevent Python workers in the test environment
    # from locating site packages; the workers would fail to find Ray and would thus
    # fail to start. To prevent that, set the PYTHONPATH env. The env will be inherited
    # by the Python workers, so that they are able to import Ray.
    monkeypatch.setenv("PYTHONPATH", ":".join(sys.path))

    @ray.remote
    def get_flags():
        return sys.flags.no_site, sys.flags.no_user_site

    with patch.multiple(
        "ray._private.services",
        _no_site=Mock(return_value=root_process_no_site),
        _no_user_site=Mock(return_value=root_process_no_user_site),
    ):
        ray.init()
        worker_process_no_site, worker_process_no_user_site = ray.get(
            get_flags.remote()
        )
        assert worker_process_no_site == root_process_no_site
        assert worker_process_no_user_site == root_process_no_user_site


@pytest.mark.parametrize("preload", [True, False])
def test_preload_workers(ray_start_cluster, preload):
    """
    Verify preload_python_modules actually preloads modules in the Ray workers.
    Also verify that it does not crash if a non-existent module is provided.
    """
    cluster = ray_start_cluster

    # Specifying imports not currently imported by default_worker.py
    expect_succeed_imports = ["html.parser", "webbrowser"]
    expect_fail_imports = ["fake_module_expect_ModuleNotFoundError"]

    if preload:
        cluster.add_node(
            _system_config={
                "preload_python_modules": [
                    *expect_succeed_imports,
                    *expect_fail_imports,
                ]
            }
        )
    else:
        cluster.add_node()

    @ray.remote(num_cpus=0)
    class Latch:
        """
        Used to ensure two separate worker processes.
        """

        def __init__(self, count):
            self.count = count

        def decr(self):
            self.count -= 1

        def is_ready(self):
            return self.count <= 0

    def wait_latch(latch):
        latch.decr.remote()
        while not ray.get(latch.is_ready.remote()):
            time.sleep(0.01)

    def assert_correct_imports():
        import sys

        imported_modules = set(sys.modules.keys())

        if preload:
            for expected_import in expect_succeed_imports:
                assert (
                    expected_import in imported_modules
                ), f"Expected {expected_import} to be in {imported_modules}"
            for unexpected_import in expect_fail_imports:
                assert (
                    unexpected_import not in imported_modules
                ), f"Expected {unexpected_import} to not be in {imported_modules}"
        else:
            for unexpected_import in expect_succeed_imports:
                assert (
                    unexpected_import not in imported_modules
                ), f"Expected {unexpected_import} to not be in {imported_modules}"

    @ray.remote(num_cpus=0)
    class Actor:
        def verify_imports(self, latch):
            wait_latch(latch)
            assert_correct_imports()

    @ray.remote(num_cpus=0)
    def verify_imports(latch):
        wait_latch(latch)
        assert_correct_imports()

    latch = Latch.remote(2)
    actor = Actor.remote()
    futures = [verify_imports.remote(latch), actor.verify_imports.remote(latch)]
    ray.get(futures)


@pytest.mark.skipif(client_test_enabled(), reason="only server mode")
def test_gcs_port_env(shutdown_only):
    try:
        with unittest.mock.patch.dict(os.environ):
            os.environ["RAY_GCS_SERVER_PORT"] = "12345"
            ray.init()
    except RuntimeError:
        pass
        # it's ok to throw runtime error for port conflicts


def test_head_node_resource(ray_start_cluster):
    """Test that the special head node resource is set."""
    cluster = ray_start_cluster
    # head node
    cluster.add_node(num_cpus=1)
    ray.init(address=cluster.address)

    assert ray.cluster_resources()[HEAD_NODE_RESOURCE_NAME] == 1

    # worker node
    cluster.add_node(num_cpus=1)

    assert ray.cluster_resources()[HEAD_NODE_RESOURCE_NAME] == 1


def test_head_node_resource_ray_init(shutdown_only):
    ray.init()

    assert ray.cluster_resources()[HEAD_NODE_RESOURCE_NAME] == 1


@pytest.mark.skipif(client_test_enabled(), reason="grpc deadlock with ray client")
def test_head_node_resource_ray_start(call_ray_start):
    ray.init(address=call_ray_start)

    assert ray.cluster_resources()[HEAD_NODE_RESOURCE_NAME] == 1


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
