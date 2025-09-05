import os
import re
import signal
import sys
import tempfile
import time

import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray._private.test_utils import (
    run_string_as_driver,
    run_string_as_driver_nonblocking,
    run_string_as_driver_stdout_stderr,
)
from ray.autoscaler.v2.utils import is_autoscaler_v2


def test_dedup_logs():
    script = """
import time

import ray
from ray._common.test_utils import SignalActor, wait_for_condition

signal = SignalActor.remote()

@ray.remote(num_cpus=0)
def verbose():
    ray.get(signal.wait.remote())
    print(f"hello world, id={time.time()}")

refs = [verbose.remote() for _ in range(4)]
wait_for_condition(
    lambda: ray.get(signal.cur_num_waiters.remote()) == 4
)
ray.get(signal.send.remote())
ray.get(refs)
"""

    proc = run_string_as_driver_nonblocking(script)
    out_str = proc.stdout.read().decode("ascii")

    assert out_str.count("hello") == 2, out_str
    assert out_str.count("RAY_DEDUP_LOGS") == 1, out_str
    assert out_str.count("[repeated 3x across cluster]") == 1, out_str


def test_dedup_error_warning_logs(ray_start_cluster, monkeypatch):
    with monkeypatch.context() as m:
        m.setenv("RAY_DEDUP_LOGS_AGG_WINDOW_S", 5)
        cluster = ray_start_cluster
        cluster.add_node(num_cpus=1)
        cluster.add_node(num_cpus=1)
        cluster.add_node(num_cpus=1)

        script = """
import ray
import time
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

ray.init()

@ray.remote(num_cpus=0)
class Foo:
    def __init__(self):
        time.sleep(1)

# NOTE: We should save actor, otherwise it will be out of scope.
actors = [Foo.remote() for _ in range(30)]
for actor in actors:
    try:
        ray.get(actor.__ray_ready__.remote())
    except ray.exceptions.OutOfMemoryError:
        # When running the test on a small machine,
        # some actors might be killed by the memory monitor.
        # We just catch and ignore the error.
        pass
"""
        out_str = run_string_as_driver(script)
        print(out_str)
    assert "PYTHON worker processes have been started" in out_str, out_str
    assert out_str.count("RAY_DEDUP_LOGS") == 1, out_str
    assert "[repeated" in out_str, out_str


def test_logger_config_with_ray_init():
    """Test that the logger is correctly configured when ray.init is called."""

    script = """
import ray

ray.init(num_cpus=1)
    """

    out_str = run_string_as_driver(script)
    assert "INFO" in out_str, out_str
    assert len(out_str) != 0


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_spill_logs():
    script = """
import ray

ray.init(object_store_memory=200e6)

x = []

for _ in range(10):
    x.append(ray.put(bytes(100 * 1024 * 1024)))

"""
    stdout_str, stderr_str = run_string_as_driver_stdout_stderr(
        script, env={"RAY_verbose_spill_logs": "1"}
    )
    out_str = stdout_str + stderr_str
    assert "Spilled " in out_str

    stdout_str, stderr_str = run_string_as_driver_stdout_stderr(
        script, env={"RAY_verbose_spill_logs": "0"}
    )
    out_str = stdout_str + stderr_str
    assert "Spilled " not in out_str


def _hook(env):
    return {"env_vars": {"HOOK_KEY": "HOOK_VALUE"}}


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@pytest.mark.parametrize("skip_hook", [True, False])
def test_runtime_env_hook(skip_hook):
    ray_init_snippet = "ray.init(_skip_env_hook=True)" if skip_hook else ""

    script = f"""
import ray
import os

{ray_init_snippet}

@ray.remote
def f():
    return os.environ.get("HOOK_KEY")

print(ray.get(f.remote()))
"""

    proc = run_string_as_driver_nonblocking(
        script, env={"RAY_RUNTIME_ENV_HOOK": "ray.tests.test_output._hook"}
    )
    out_str = proc.stdout.read().decode("ascii") + proc.stderr.read().decode("ascii")
    print(out_str)
    if skip_hook:
        assert "HOOK_VALUE" not in out_str
    else:
        assert "HOOK_VALUE" in out_str


def test_env_hook_skipped_for_ray_client(start_cluster, monkeypatch):
    monkeypatch.setenv("RAY_RUNTIME_ENV_HOOK", "ray.tests.test_output._hook")
    cluster, address = start_cluster
    ray.init(address)

    @ray.remote
    def f():
        return os.environ.get("HOOK_KEY")

    using_ray_client = address.startswith("ray://")
    if using_ray_client:
        assert ray.get(f.remote()) is None
    else:
        assert ray.get(f.remote()) == "HOOK_VALUE"


@pytest.mark.parametrize(
    "ray_start_cluster_head_with_env_vars",
    [
        {
            "num_cpus": 1,
            "env_vars": {
                "RAY_enable_autoscaler_v2": "0",
            },
        },
        {
            "num_cpus": 1,
            "env_vars": {
                "RAY_enable_autoscaler_v2": "1",
            },
        },
    ],
    indirect=True,
)
def test_autoscaler_warn_infeasible(ray_start_cluster_head_with_env_vars):
    script = """
import ray

@ray.remote(resources={{"does_not_exist": 1}})
class A:
    pass

ray.init(address="{address}")

# Will hang forever due to infeasible resource.
ray.get(A.remote().__ray_ready__.remote())
    """.format(
        address=ray_start_cluster_head_with_env_vars.address
    )

    proc = run_string_as_driver_nonblocking(script, env={"PYTHONUNBUFFERED": "1"})

    def _check_for_infeasible_msg():
        l = proc.stdout.readline().decode("ascii")
        if len(l) > 0:
            print(l)
        return "(autoscaler" in l and "No available node types can fulfill" in l

    wait_for_condition(_check_for_infeasible_msg, timeout=30)
    os.kill(proc.pid, signal.SIGTERM)
    proc.wait()


@pytest.mark.parametrize(
    "ray_start_cluster_head_with_env_vars",
    [
        {
            "num_cpus": 1,
            "env_vars": {
                "RAY_enable_autoscaler_v2": "0",
                "RAY_debug_dump_period_milliseconds": "1000",
            },
        },
        {
            "num_cpus": 1,
            "env_vars": {
                "RAY_enable_autoscaler_v2": "1",
                "RAY_debug_dump_period_milliseconds": "1000",
            },
        },
    ],
    indirect=True,
)
def test_autoscaler_warn_deadlock(ray_start_cluster_head_with_env_vars):
    script = """
import ray
import time

@ray.remote(num_cpus=1)
class A:
    pass

ray.init(address="{address}")

# Only one of a or b can be scheduled, so the other will hang.
a = A.remote()
b = A.remote()
ray.get([a.__ray_ready__.remote(), b.__ray_ready__.remote()])
    """.format(
        address=ray_start_cluster_head_with_env_vars.address
    )

    proc = run_string_as_driver_nonblocking(script, env={"PYTHONUNBUFFERED": "1"})

    if is_autoscaler_v2():
        infeasible_msg = "No available node types can fulfill resource requests"
    else:
        infeasible_msg = "Warning: The following resource request cannot"

    def _check_for_deadlock_msg():
        l = proc.stdout.readline().decode("ascii")
        if len(l) > 0:
            print(l)
        return "(autoscaler" in l and infeasible_msg in l

    wait_for_condition(_check_for_deadlock_msg, timeout=30)


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_autoscaler_v2_stream_events_with_filter(shutdown_only):
    """Test that autoscaler v2 events are streamed to the driver."""
    address = ray.init(_system_config={"enable_autoscaler_v2": True})["address"]
    script = """
import ray
import time
from ray.core.generated.event_pb2 import Event as RayEvent
from ray._private.event.event_logger import get_event_logger

ray.init(address="{address}")

# Get event logger to write autoscaler events.
log_dir = ray._private.worker.global_worker.node.get_logs_dir_path()
event_logger = get_event_logger(RayEvent.SourceType.AUTOSCALER, log_dir)
event_logger.trace("TRACE")
event_logger.debug("DEBUG")
event_logger.info("INFO")
event_logger.warning("WARNING")
event_logger.error("ERROR")
event_logger.fatal("FATAL")

# Sleep to allow the event logs to be streamed to the driver.
while True:
    time.sleep(1)
    """.format(
        address=address
    )
    test_cases = [
        ("TRACE", "TRACE,DEBUG,INFO,WARNING,ERROR,FATAL", ""),
        ("DEBUG", "DEBUG,INFO,WARNING,ERROR,FATAL", "TRACE"),
        ("INFO", "INFO,WARNING,ERROR,FATAL", "TRACE,DEBUG"),
        ("WARNING", "WARNING,ERROR,FATAL", "TRACE,DEBUG,INFO"),
        ("ERROR", "ERROR,FATAL", "TRACE,DEBUG,INFO,WARNING"),
        ("FATAL", "FATAL", "TRACE,DEBUG,INFO,WARNING,ERROR"),
    ]

    for event_level, expected_msg, unexpected_msg in test_cases:
        print("Running test case for level:", event_level)
        proc = run_string_as_driver_nonblocking(
            script,
            env={
                "PYTHONUNBUFFERED": "1",
                "RAY_LOG_TO_DRIVER_EVENT_LEVEL": event_level,
            },
        )

        out_str = ""

        def _check_events():
            nonlocal out_str

            # Incrementally read driver stdout.
            l = proc.stdout.readline().decode("ascii")
            if len(l) > 0 and "autoscaler" in l:
                out_str += l

            # Check that *all* expected messages are present.
            assert all([msg in out_str for msg in expected_msg.split(",")]), out_str

            # Check that *no* unexpected messages are present.
            if unexpected_msg:
                assert all(
                    [msg not in out_str for msg in unexpected_msg.split(",")]
                ), out_str

            return True

        wait_for_condition(_check_events)

        proc.kill()
        proc.wait()


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@pytest.mark.parametrize("async_actor", [True, False])
def test_fail_importing_actor(async_actor):
    script = f"""
import os
import sys
import tempfile
import ray

f = tempfile.NamedTemporaryFile("w+", suffix=".py", prefix="_", delete=False)
try:
    f.write('''
def temporary_helper_function():
   return 1
''')
    f.flush()
    f.close()

    # Get the module name and strip ".py" from the end.
    directory = os.path.dirname(f.name)
    module_name = os.path.basename(f.name)[:-3]
    sys.path.append(directory)
    module = __import__(module_name)

    # Define an actor that closes over this temporary module. This should
    # fail when it is unpickled.
    @ray.remote
    class Foo:
        def __init__(self):
            self.x = module.temporary_python_file()

        {"async " if async_actor else ""}def ready(self):
            pass
finally:
    os.unlink(f.name)

ray.get(Foo.remote().ready.remote())
"""
    proc = run_string_as_driver_nonblocking(script)
    err_str = proc.stderr.read().decode("ascii")
    print(err_str)
    assert "ModuleNotFoundError: No module named" in err_str
    assert "RuntimeError: The actor with name Foo failed to import" in err_str


def test_fail_importing_task():
    script = """
import os
import sys
import tempfile
import ray

f = tempfile.NamedTemporaryFile("w+", suffix=".py", prefix="_", delete=False)
try:
    f.write('''
def temporary_helper_function():
   return 1
''')
    f.flush()
    f.close()

    # Get the module name and strip ".py" from the end.
    directory = os.path.dirname(f.name)
    module_name = os.path.basename(f.name)[:-3]
    sys.path.append(directory)
    module = __import__(module_name)

    # Define a task that closes over this temporary module. This should
    # fail when it is unpickled.
    @ray.remote
    def foo():
        return module.temporary_python_file()
finally:
    os.unlink(f.name)

ray.get(foo.remote())
"""
    proc = run_string_as_driver_nonblocking(script)
    err_str = proc.stderr.read().decode("ascii")
    print(err_str)
    assert "ModuleNotFoundError: No module named" in err_str
    assert "RuntimeError: The remote function failed to import" in err_str


def test_core_worker_error_message():
    script = """
import ray
import sys

ray.init(local_mode=True)

# In local mode this generates an ERROR level log.
ray._private.utils.push_error_to_driver(
    ray._private.worker.global_worker, "type", "Hello there")
    """

    proc = run_string_as_driver_nonblocking(script)
    err_str = proc.stderr.read().decode("ascii")

    assert "Hello there" in err_str, err_str


def test_task_stdout_stderr():
    """Test that task stdout and stderr is streamed to the driver correctly."""
    script = """
import ray
import sys

@ray.remote
def foo():
    print("foo stdout")
    print("foo stderr", file=sys.stderr)

@ray.remote
def bar():
    print("bar stdout")
    print("bar stderr", file=sys.stderr)

@ray.remote
def baz():
    print("baz stdout")
    print("baz stderr", file=sys.stderr)

ray.init(num_cpus=3)
ray.get([foo.remote(), bar.remote(), baz.remote()])
    """

    proc = run_string_as_driver_nonblocking(script)
    out_str = proc.stdout.read().decode("ascii")
    err_str = proc.stderr.read().decode("ascii")

    assert re.search("(foo pid=.*) foo stdout", out_str), out_str
    assert re.search("(foo pid=.*) foo stderr", err_str), err_str
    assert re.search("(bar pid=.*) bar stdout", out_str), out_str
    assert re.search("(bar pid=.*) bar stderr", err_str), err_str
    assert re.search("(baz pid=.*) baz stdout", out_str), out_str
    assert re.search("(baz pid=.*) baz stderr", err_str), err_str


def test_actor_stdout_stderr():
    """Test that actor stdout and stderr is streamed to the driver correctly."""
    script = """
import ray
import sys

@ray.remote
class Actor1:
    def f(self):
        print("hi stdout")
        print("hi stderr", file=sys.stderr)

@ray.remote
class Actor2:
    def __init__(self):
        print("init stdout")
        print("init stderr", file=sys.stderr)
        self.name = "ActorX"

    def f(self):
        print("bye stdout")
        print("bye stderr", file=sys.stderr)

    def __repr__(self):
        return self.name

ray.init(num_cpus=2)
ray.get([Actor1.remote().f.remote(), Actor2.remote().f.remote()])
    """

    proc = run_string_as_driver_nonblocking(script)
    out_str = proc.stdout.read().decode("ascii")
    err_str = proc.stderr.read().decode("ascii")
    assert "stderr" not in out_str
    assert "stdout" not in err_str

    assert re.search("(Actor1 pid=.*) hi stdout", out_str), out_str
    assert re.search("(Actor1 pid=.*) hi stderr", err_str), err_str
    assert re.search("(Actor2 pid=.*) init stdout", out_str), out_str
    assert re.search("(Actor2 pid=.*) init stderr", err_str), err_str
    assert not re.search("(ActorX pid=.*) init", out_str), out_str
    assert not re.search("(ActorX pid=.*) init", err_str), err_str
    assert re.search("(ActorX pid=.*) bye stdout", out_str), out_str
    assert re.search("(ActorX pid=.*) bye stderr", err_str), err_str


def test_output_local_ray():
    script = """
import ray
ray.init()
    """
    output = run_string_as_driver(script)
    lines = output.strip("\n").split("\n")
    lines = [line for line in lines if "The object store is using /tmp" not in line]
    assert len(lines) >= 1
    assert "Started a local Ray instance." in output
    if os.environ.get("RAY_MINIMAL") == "1":
        assert "View the dashboard" not in output
    else:
        assert "View the dashboard" in output


def test_output_ray_cluster(call_ray_start):
    script = """
import ray
ray.init()
    """
    output = run_string_as_driver(script)
    lines = output.strip("\n").split("\n")
    assert len(lines) >= 1
    assert "Connecting to existing Ray cluster at address:" in output
    assert "Connected to Ray cluster." in output
    if os.environ.get("RAY_MINIMAL") == "1":
        assert "View the dashboard" not in output
    else:
        assert "View the dashboard" in output


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
# TODO: fix this test to support minimal installation
@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1",
    reason="This test currently fails with minimal install.",
)
def test_output_on_driver_shutdown():
    with tempfile.NamedTemporaryFile("w+", suffix=".py", prefix="_", delete=True) as f:
        script = """
import ray

@ray.remote
def t(i: int):
    return i

obj_refs = [t.remote(i) for i in range(10)]

with open("{ready_path}", "w") as f:
    f.write("ready")
    f.flush()

while True:
    assert len(obj_refs) == 10
    ready, pending = ray.wait(obj_refs, num_returns=2)
    for i in ray.get(ready):
        obj_refs[i] = t.remote(i)
        """.format(
            ready_path=f.name
        )

        # Start the driver and wait for it to start executing Ray code.
        proc = run_string_as_driver_nonblocking(script)
        wait_for_condition(lambda: len(f.read()) > 0)
        print(f"Script is running... pid: {proc.pid}")

        # Send multiple signals to terminate the driver like a real-world scenario.
        for _ in range(3):
            time.sleep(0.1)
            os.kill(proc.pid, signal.SIGINT)

        proc.wait(timeout=10)
        err_str = proc.stderr.read().decode("ascii")
        assert len(err_str) > 0
        assert "KeyboardInterrupt" in err_str
        assert "StackTrace Information" not in err_str


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1",
    reason="This test currently fails with minimal install.",
)
def test_empty_line_thread_safety_bug():
    """Make sure when new threads are used within __init__,
    the empty line is not printed.
    Related: https://github.com/ray-project/ray/pull/20987
    """
    actor_repr = "TESTER"
    script = f"""
import threading

import ray

@ray.remote
class A:
    def __init__(self, *, num_threads: int = 5):
        self._num_threads = num_threads
        self._done_count = 0
        self._done_lock = threading.Lock()
        self._done_event = threading.Event()

        def _spin():
            for _ in range(300000000):
                pass

        for _ in range(5):
            threading.Thread(target=self._spin, daemon=True).start()

    def _spin(self):
        for _ in range(300000000):
            pass

        with self._done_lock:
            self._done_count += 1
            if self._done_count == self._num_threads:
                self._done_event.set()

    def ready(self):
        self._done_event.wait()

    def __repr__(self):
        return "{actor_repr}"

a = A.remote()
ray.get(a.ready.remote())
    """
    out = run_string_as_driver(script)
    assert actor_repr not in out


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "_ray_instance":
        # Set object store memory very low so that it won't complain
        # about low shm memory in Linux environment.
        # The test failures currently complain it only has 2 GB memory,
        # so let's set it much lower than that.
        MB = 1000**2
        ray.init(num_cpus=1, object_store_memory=(100 * MB))
        ray.shutdown()
    else:
        sys.exit(pytest.main(["-sv", __file__]))
