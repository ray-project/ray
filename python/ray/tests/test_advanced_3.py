# coding: utf-8
import importlib
import logging
import os
import pickle
import socket
import sys
import time

import numpy as np
import pytest

import ray
import ray._private.ray_constants
import ray._private.utils
from ray._private.test_utils import check_call_ray, wait_for_num_actors
from ray.util.state import list_actors

import psutil

logger = logging.getLogger(__name__)


def test_global_state_api(shutdown_only):

    ray.init(
        num_cpus=5, num_gpus=3, resources={"CustomResource": 1}, include_dashboard=True
    )

    assert ray.cluster_resources()["CPU"] == 5
    assert ray.cluster_resources()["GPU"] == 3
    assert ray.cluster_resources()["CustomResource"] == 1

    job_id = ray._private.utils.compute_job_id_from_driver(
        ray.WorkerID(ray._private.worker.global_worker.worker_id)
    )

    client_table = ray.nodes()
    node_ip_address = ray._private.worker.global_worker.node_ip_address

    assert len(client_table) == 1
    assert client_table[0]["NodeManagerAddress"] == node_ip_address

    @ray.remote
    class Actor:
        def __init__(self):
            pass

    _ = Actor.options(name="test_actor").remote()  # noqa: F841
    # Wait for actor to be created
    wait_for_num_actors(1)

    actor_table = list_actors()  # should be using this API now for fetching actors
    assert len(actor_table) == 1

    actor_info = actor_table[0]
    assert actor_info.job_id == job_id.hex()
    assert actor_info.name == "test_actor"

    job_table = ray._private.state.jobs()

    assert len(job_table) == 1
    assert job_table[0]["JobID"] == job_id.hex()
    assert job_table[0]["DriverIPAddress"] == node_ip_address


def test_logging_to_driver(capsys, shutdown_only):
    ray.init(num_cpus=1, log_to_driver=True)

    @ray.remote
    def f():
        # It's important to make sure that these print statements occur even
        # without calling sys.stdout.flush() and sys.stderr.flush().
        for i in range(10):
            print(i, end=" ")
            print(100 + i, end=" ", file=sys.stderr)

    ray.get(f.remote())
    time.sleep(1)

    out, err = capsys.readouterr()
    for i in range(10):
        assert str(i) in out

    for i in range(100, 110):
        assert str(i) in err


def test_not_logging_to_driver_via_env_var(monkeypatch, capsys, shutdown_only):
    monkeypatch.setenv("RAY_LOG_TO_DRIVER", "0")
    importlib.reload(ray._private.ray_constants)
    ray.init(num_cpus=1)

    @ray.remote
    def f():
        for i in range(100):
            print(i)
            print(100 + i, file=sys.stderr)
            sys.stdout.flush()
            sys.stderr.flush()

    capsys.readouterr()
    ray.get(f.remote())
    time.sleep(1)

    out, err = capsys.readouterr()
    assert len(out) == 0
    assert len(err) == 0


def test_not_logging_to_driver(capsys, shutdown_only):
    ray.init(num_cpus=1, log_to_driver=False)

    @ray.remote
    def f():
        for i in range(100):
            print(i)
            print(100 + i, file=sys.stderr)
            sys.stdout.flush()
            sys.stderr.flush()

    capsys.readouterr()
    ray.get(f.remote())
    time.sleep(1)

    out, err = capsys.readouterr()
    assert len(out) == 0
    assert len(err) == 0


def test_workers(shutdown_only):
    num_workers = 3
    ray.init(num_cpus=num_workers)

    @ray.remote
    def f():
        return id(ray._private.worker.global_worker), os.getpid()

    # Wait until all of the workers have started.
    worker_ids = set()
    while len(worker_ids) != num_workers:
        worker_ids = set(ray.get([f.remote() for _ in range(10)]))


def test_object_ref_properties():
    id_bytes = b"0011223344556677889900001111"
    object_ref = ray.ObjectRef(id_bytes)
    assert object_ref.binary() == id_bytes
    object_ref = ray.ObjectRef.nil()
    assert object_ref.is_nil()
    with pytest.raises(ValueError, match=r".*needs to have length.*"):
        ray.ObjectRef(id_bytes + b"1234")
    with pytest.raises(ValueError, match=r".*needs to have length.*"):
        ray.ObjectRef(b"0123456789")
    object_ref = ray.ObjectRef.from_random()
    assert not object_ref.is_nil()
    assert object_ref.binary() != id_bytes
    id_dumps = pickle.dumps(object_ref)
    id_from_dumps = pickle.loads(id_dumps)
    assert id_from_dumps == object_ref


def test_wait_reconstruction(shutdown_only):
    ray.init(num_cpus=1, object_store_memory=int(10**8))

    @ray.remote
    def f():
        return np.zeros(6 * 10**7, dtype=np.uint8)

    x_id = f.remote()
    ray.wait([x_id])
    ray.wait([f.remote()])
    assert not ray._private.worker.global_worker.core_worker.object_exists(x_id)
    ready_ids, _ = ray.wait([x_id])
    assert len(ready_ids) == 1


@pytest.mark.skipif(
    sys.platform == "win32", reason="Windows doesn't support changing process title."
)
def test_ray_setproctitle(ray_start_2_cpus):
    @ray.remote
    class UniqueName:
        def __init__(self):
            assert psutil.Process().cmdline()[0] == "ray::UniqueName.__init__"

        def f(self):
            assert psutil.Process().cmdline()[0] == "ray::UniqueName.f"

    @ray.remote
    def unique_1():
        assert psutil.Process().cmdline()[0] == "ray::unique_1"

    actor = UniqueName.remote()
    ray.get(actor.f.remote())
    ray.get(unique_1.remote())


@pytest.mark.skipif(
    sys.platform == "win32", reason="Windows doesn't support changing process title."
)
def test_ray_task_name_setproctitle(ray_start_2_cpus):
    method_task_name = "foo"

    @ray.remote
    class UniqueName:
        def __init__(self):
            assert psutil.Process().cmdline()[0] == "ray::UniqueName.__init__"

        def f(self):
            assert psutil.Process().cmdline()[0] == f"ray::{method_task_name}"

    task_name = "bar"

    @ray.remote
    def unique_1():
        assert psutil.Process().cmdline()[0] == f"ray::{task_name}"

    actor = UniqueName.remote()
    ray.get(actor.f.options(name=method_task_name).remote())
    ray.get(unique_1.options(name=task_name).remote())


@pytest.mark.skipif(
    sys.platform == "win32", reason="Windows doesn't support changing process title."
)
def test_ray_task_generator_setproctitle(ray_start_2_cpus):
    @ray.remote
    def generator_task():
        for i in range(4):
            assert psutil.Process().cmdline()[0] == "ray::generator_task"
            yield i

    ray.get(generator_task.options(num_returns=2).remote()[0])
    ray.get(generator_task.options(num_returns="dynamic").remote())
    generator = generator_task.remote()
    for _ in range(4):
        ray.get(next(generator))

    @ray.remote
    class UniqueName:
        def f(self):
            for i in range(4):
                assert psutil.Process().cmdline()[0] == "ray::UniqueName.f"
                yield i

    actor = UniqueName.remote()
    ray.get(actor.f.options(num_returns=2).remote()[0])
    ray.get(actor.f.options(num_returns="dynamic").remote())
    generator = actor.f.remote()
    for _ in range(4):
        ray.get(next(generator))


@pytest.mark.skipif(
    sys.platform != "linux",
    reason=(
        "Test specifically targets the Linux /proc/self/stat fallback in "
        "src/ray/thirdparty/setproctitle/spt_setup.c."
    ),
)
def test_setproctitle_falls_back_to_proc_stat_when_environ_broken(tmp_path):
    """Regression test for the vendored-setproctitle deflake.

    spt_setup() in src/ray/thirdparty/setproctitle/spt_setup.c originally
    relied on find_argv_from_env(), which walks backward from environ[0] to
    locate the original argv memory region. That walk silently fails once
    libc setenv()/putenv() has moved environ, leaving every subsequent
    setproctitle() call as a no-op. We added find_argv_from_proc_stat() as
    a fallback that reads the kernel-recorded arg_start/arg_end from
    /proc/self/stat (set at execve() time, immune to env mutation).

    This test deliberately clears os.environ in a subprocess BEFORE the
    first setproctitle() call. Without the fallback, env[0] is NULL,
    find_argv_from_env() fails, and the title write is silently dropped.
    With the fallback in place, the kernel-recorded bounds are used and
    /proc/self/cmdline reflects the new title.
    """
    import subprocess
    import textwrap

    marker = "ray::PROC_STAT_FALLBACK_OK"
    script = textwrap.dedent(
        f"""
        import os, sys
        # Import _raylet BEFORE wiping environ — the .so load itself may
        # need a sane environment, and spt_setup() is lazy (only fires on
        # the first setproctitle() call), so we can safely break environ
        # afterwards.
        import ray._raylet

        # Break find_argv_from_env(): environ[0] becomes NULL once cleared,
        # so the backward walk bails out immediately.
        os.environ.clear()

        ray._raylet.setproctitle({marker!r})

        with open("/proc/self/cmdline", "rb") as f:
            cmdline = f.read()
        # /proc/self/cmdline is NUL-separated; the title is written into
        # argv[0]'s buffer and padded with NULs.
        first_arg = cmdline.split(b"\\x00", 1)[0].decode("utf-8", "replace")
        print("FIRST_ARG=" + first_arg)
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert (
        result.returncode == 0
    ), f"subprocess failed: stdout={result.stdout!r} stderr={result.stderr!r}"
    # Locate our marker line in stdout (other Ray imports may emit chatter).
    first_arg_lines = [
        line for line in result.stdout.splitlines() if line.startswith("FIRST_ARG=")
    ]
    assert first_arg_lines, (
        f"FIRST_ARG line missing from subprocess output. "
        f"stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    first_arg = first_arg_lines[-1][len("FIRST_ARG=") :]
    assert first_arg == marker, (
        f"setproctitle did not take effect after os.environ.clear(): "
        f"expected {marker!r}, got {first_arg!r}. "
        f"This typically means the /proc/self/stat fallback in "
        f"src/ray/thirdparty/setproctitle/spt_setup.c regressed."
    )


@pytest.mark.skipif(
    os.getenv("TRAVIS") is None, reason="This test should only be run on Travis."
)
def test_ray_stack(ray_start_2_cpus):
    def unique_name_1():
        time.sleep(1000)

    @ray.remote
    def unique_name_2():
        time.sleep(1000)

    @ray.remote
    def unique_name_3():
        unique_name_1()

    unique_name_2.remote()
    unique_name_3.remote()

    success = False
    start_time = time.time()
    while time.time() - start_time < 30:
        # Attempt to parse the "ray stack" call.
        output = ray._common.utils.decode(
            check_call_ray(["stack"], capture_stdout=True)
        )
        if (
            "unique_name_1" in output
            and "unique_name_2" in output
            and "unique_name_3" in output
        ):
            success = True
            break

    if not success:
        raise Exception("Failed to find necessary information with 'ray stack'")


def test_raylet_is_robust_to_random_messages(ray_start_regular):
    node_manager_address = None
    node_manager_port = None
    for client in ray.nodes():
        if "NodeManagerAddress" in client:
            node_manager_address = client["NodeManagerAddress"]
            node_manager_port = client["NodeManagerPort"]
    assert node_manager_address
    assert node_manager_port
    # Try to bring down the node manager:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((node_manager_address, node_manager_port))
    s.send(1000 * b"asdf")

    @ray.remote
    def f():
        return 1

    assert ray.get(f.remote()) == 1


def test_non_ascii_comment(ray_start_regular):
    @ray.remote
    def f():
        # 日本語 Japanese comment
        return 1

    assert ray.get(f.remote()) == 1


@pytest.mark.parametrize(
    "ray_start_object_store_memory", [150 * 1024 * 1024], indirect=True
)
def test_put_pins_object(ray_start_object_store_memory):
    obj = np.ones(200 * 1024, dtype=np.uint8)
    x_id = ray.put(obj)
    x_binary = x_id.binary()
    assert (ray.get(ray.ObjectRef(x_binary)) == obj).all()

    # x cannot be evicted since x_id pins it
    for _ in range(10):
        ray.put(np.zeros(10 * 1024 * 1024))
    assert (ray.get(x_id) == obj).all()
    assert (ray.get(ray.ObjectRef(x_binary)) == obj).all()

    # now it can be evicted since x_id pins it but x_binary does not
    del x_id
    for _ in range(10):
        ray.put(np.zeros(10 * 1024 * 1024))
    assert not ray._private.worker.global_worker.core_worker.object_exists(
        ray.ObjectRef(x_binary)
    )


def test_decorated_function(ray_start_regular):
    def function_invocation_decorator(f):
        def new_f(args, kwargs):
            # Reverse the arguments.
            return f(args[::-1], {"d": 5}), kwargs

        return new_f

    def f(a, b, c, d=None):
        return a, b, c, d

    f.__ray_invocation_decorator__ = function_invocation_decorator
    f = ray.remote(f)

    result_id, kwargs = f.remote(1, 2, 3, d=4)
    assert kwargs == {"d": 4}
    assert ray.get(result_id) == (3, 2, 1, 5)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
