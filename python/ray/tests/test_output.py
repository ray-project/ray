import os
import re
import signal
import subprocess
import sys
import time

import pytest

import ray
from ray._private.test_utils import (
    run_string_as_driver,
    run_string_as_driver_nonblocking,
)


def test_logger_config():
    script = """
import ray

ray.init(num_cpus=1)
    """

    proc = run_string_as_driver_nonblocking(script)
    out_str = proc.stdout.read().decode("ascii")
    err_str = proc.stderr.read().decode("ascii")

    print(out_str, err_str)
    assert "INFO worker.py:" in err_str, err_str


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_spill_logs():
    script = """
import ray
import numpy as np

ray.init(object_store_memory=200e6)

x = []

for _ in range(10):
    x.append(ray.put(np.ones(100 * 1024 * 1024, dtype=np.uint8)))
"""

    proc = run_string_as_driver_nonblocking(script, env={"RAY_verbose_spill_logs": "1"})
    out_str = proc.stdout.read().decode("ascii") + proc.stderr.read().decode("ascii")
    print(out_str)
    assert "Spilled " in out_str

    proc = run_string_as_driver_nonblocking(script, env={"RAY_verbose_spill_logs": "0"})
    out_str = proc.stdout.read().decode("ascii") + proc.stderr.read().decode("ascii")
    print(out_str)
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


def test_autoscaler_infeasible():
    script = """
import ray
import time

ray.init(num_cpus=1)

@ray.remote(num_gpus=1)
def foo():
    pass

x = foo.remote()
time.sleep(15)
    """

    proc = run_string_as_driver_nonblocking(script)
    out_str = proc.stdout.read().decode("ascii")
    err_str = proc.stderr.read().decode("ascii")

    print(out_str, err_str)
    assert "Tip:" in out_str
    assert "Error: No available node types can fulfill" in out_str


def test_autoscaler_warn_deadlock(enable_syncer_test):
    script = """
import ray
import time

ray.init(num_cpus=1)

@ray.remote(num_cpus=1)
class A:
    pass

a = A.remote()
b = A.remote()
time.sleep(25)
    """

    proc = run_string_as_driver_nonblocking(script)
    out_str = proc.stdout.read().decode("ascii")
    err_str = proc.stderr.read().decode("ascii")

    print(out_str, err_str)
    assert "Tip:" in out_str
    assert "Warning: The following resource request cannot" in out_str


def test_autoscaler_no_spam():
    script = """
import ray
import time

# Check that there are no false positives with custom resources.
ray.init(num_cpus=1, resources={"node:x": 1})

@ray.remote(num_cpus=1, resources={"node:x": 1})
def f():
    time.sleep(1)
    print("task done")

ray.get([f.remote() for _ in range(15)])
    """

    proc = run_string_as_driver_nonblocking(script)
    out_str = proc.stdout.read().decode("ascii")
    err_str = proc.stderr.read().decode("ascii")

    print(out_str, err_str)
    assert "Tip:" not in out_str
    assert "Tip:" not in err_str


def test_autoscaler_prefix():
    script = """
import ray
import time

ray.init(num_cpus=1)

@ray.remote(num_cpus=1)
class A:
    pass

a = A.remote()
b = A.remote()
time.sleep(25)
    """

    proc = run_string_as_driver_nonblocking(script)
    out_str = proc.stdout.read().decode("ascii")
    err_str = proc.stderr.read().decode("ascii")

    print(out_str, err_str)
    assert "(autoscaler" in out_str


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_fail_importing_actor(ray_start_regular, error_pubsub):
    script = f"""
import os
import sys
import tempfile
import ray

ray.init(address='{ray_start_regular.address_info["address"]}')
temporary_python_file = '''
def temporary_helper_function():
   return 1
'''

f = tempfile.NamedTemporaryFile("w+", suffix=".py", prefix="_", delete=True)
f_name = f.name
f.close()
f = open(f_name, "w+")
f.write(temporary_python_file)
f.flush()
directory = os.path.dirname(f_name)
# Get the module name and strip ".py" from the end.
module_name = os.path.basename(f_name)[:-3]
sys.path.append(directory)
module = __import__(module_name)

# Define an actor that closes over this temporary module. This should
# fail when it is unpickled.
@ray.remote(max_restarts=0)
class Foo:
    def __init__(self):
        self.x = module.temporary_python_file()
    def ready(self):
        pass
a = Foo.remote()
try:
    ray.get(a.ready.remote())
except Exception as e:
    pass
from time import sleep
sleep(3)
"""
    proc = run_string_as_driver_nonblocking(script)
    out_str = proc.stdout.read().decode("ascii")
    err_str = proc.stderr.read().decode("ascii")
    print(out_str)
    print("-----")
    print(err_str)
    assert "ModuleNotFoundError: No module named" in err_str
    assert "RuntimeError: The actor with name Foo failed to import" in err_str


def test_fail_importing_task(ray_start_regular, error_pubsub):
    script = f"""
import os
import sys
import tempfile
import ray

ray.init(address='{ray_start_regular.address_info["address"]}')
temporary_python_file = '''
def temporary_helper_function():
   return 1
'''

f = tempfile.NamedTemporaryFile("w+", suffix=".py", prefix="_", delete=True)
f_name = f.name
f.close()
f = open(f_name, "w+")
f.write(temporary_python_file)
f.flush()
directory = os.path.dirname(f_name)
# Get the module name and strip ".py" from the end.
module_name = os.path.basename(f_name)[:-3]
sys.path.append(directory)
module = __import__(module_name)

# Define an actor that closes over this temporary module. This should
# fail when it is unpickled.
@ray.remote
def foo():
    return module.temporary_python_file()

ray.get(foo.remote())
"""
    proc = run_string_as_driver_nonblocking(script)
    out_str = proc.stdout.read().decode("ascii")
    err_str = proc.stderr.read().decode("ascii")
    print(out_str)
    print(err_str)
    assert "ModuleNotFoundError: No module named" in err_str
    assert "RuntimeError: The remote function failed to import" in err_str


def test_worker_stdout():
    script = """
import ray
import sys

ray.init(num_cpus=2)

@ray.remote
def foo(out_str, err_str):
    print(out_str)
    print(err_str, file=sys.stderr)

ray.get(foo.remote("abc", "def"))
    """

    proc = run_string_as_driver_nonblocking(script)
    out_str = proc.stdout.read().decode("ascii")
    err_str = proc.stderr.read().decode("ascii")

    out_str = "".join(out_str.splitlines())
    assert out_str.endswith("abc"), out_str
    assert "(foo pid=" in out_str, out_str
    err_str_sec_last = "".join(err_str.split("\n")[-2].splitlines())
    assert err_str_sec_last.endswith("def")


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


def test_disable_driver_logs_breakpoint():
    script = """
import time
import os
import ray
import sys
import threading

ray.init(num_cpus=2)

@ray.remote
def f():
    while True:
        time.sleep(1)
        print("hello there")
        sys.stdout.flush()

def kill():
    time.sleep(5)
    sys.stdout.flush()
    time.sleep(1)
    os._exit(0)

t = threading.Thread(target=kill)
t.start()
x = f.remote()
time.sleep(2)  # Enough time to print one hello.
ray.util.rpdb._driver_set_trace()  # This should disable worker logs.
# breakpoint()  # Only works in Py3.7+
    """

    proc = run_string_as_driver_nonblocking(script)
    out_str = proc.stdout.read().decode("ascii")
    num_hello = out_str.count("hello")
    assert num_hello >= 1, out_str
    assert num_hello < 3, out_str
    assert "Temporarily disabling Ray worker logs" in out_str, out_str
    # TODO(ekl) nice to test resuming logs too, but it's quite complicated


@pytest.mark.parametrize("file", ["stdout", "stderr"])
def test_multi_stdout_err(file):
    if file == "stdout":
        file_handle = "sys.stdout"
    else:  # sys.stderr
        file_handle = "sys.stderr"

    script = f"""
import ray
import sys

ray.init(num_cpus=1)

@ray.remote
def foo():
    print(file={file_handle})

@ray.remote
def bar():
    print(file={file_handle})

@ray.remote
def baz():
    print(file={file_handle})

ray.get(foo.remote())
ray.get(bar.remote())
ray.get(baz.remote())
    """

    proc = run_string_as_driver_nonblocking(script)
    if file == "stdout":
        out_str = proc.stdout.read().decode("ascii")
    else:
        out_str = proc.stderr.read().decode("ascii")

    out_str = "".join(out_str.splitlines())
    assert "(foo pid=" in out_str, out_str
    assert "(bar pid=" in out_str, out_str
    assert "(baz pid=" in out_str, out_str


@pytest.mark.parametrize("file", ["stdout", "stderr"])
def test_actor_stdout(file):
    if file == "stdout":
        file_handle = "sys.stdout"
    else:  # sys.stderr
        file_handle = "sys.stderr"

    script = f"""
import ray
import sys

ray.init(num_cpus=2)

@ray.remote
class Actor1:
    def f(self):
        print("hi", file={file_handle})

@ray.remote
class Actor2:
    def __init__(self):
        print("init", file={file_handle})
        self.name = "ActorX"
    def f(self):
        print("bye", file={file_handle})
    def __repr__(self):
        return self.name

a = Actor1.remote()
ray.get(a.f.remote())
b = Actor2.remote()
ray.get(b.f.remote())
    """

    proc = run_string_as_driver_nonblocking(script)
    if file == "stdout":
        out_str = proc.stdout.read().decode("ascii")
    else:
        out_str = proc.stderr.read().decode("ascii")

    out_str = "".join(out_str.splitlines())
    assert "hi" in out_str, out_str
    assert "(Actor1 pid=" in out_str, out_str
    assert "bye" in out_str, out_str
    assert re.search("Actor2 pid=.*init", out_str), out_str
    assert not re.search("ActorX pid=.*init", out_str), out_str
    assert re.search("ActorX pid=.*bye", out_str), out_str
    assert re.search("Actor2 pid=.*bye", out_str), out_str


def test_output_local_ray():
    script = """
import ray
ray.init()
    """
    output = run_string_as_driver(script)
    lines = output.strip("\n").split("\n")
    for line in lines:
        print(line)
    lines = [line for line in lines if "The object store is using /tmp" not in line]
    assert len(lines) == 1
    line = lines[0]
    print(line)
    assert "Started a local Ray instance." in line
    if os.environ.get("RAY_MINIMAL") == "1":
        assert "View the dashboard" not in line
    else:
        assert "View the dashboard" in line


def test_output_ray_cluster(call_ray_start):
    script = """
import ray
ray.init()
    """
    output = run_string_as_driver(script)
    lines = output.strip("\n").split("\n")
    for line in lines:
        print(line)
    assert len(lines) == 2
    assert "Connecting to existing Ray cluster at address:" in lines[0]
    assert "Connected to Ray cluster." in lines[1]
    if os.environ.get("RAY_MINIMAL") == "1":
        assert "View the dashboard" not in lines[1]
    else:
        assert "View the dashboard" in lines[1]


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
# TODO: fix this test to support minimal installation
@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1",
    reason="This test currently fails with minimal install.",
)
def test_output_on_driver_shutdown(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=16)
    # many_ppo.py script.
    script = """
import ray
from ray.tune import run_experiments
from ray.tune.utils.release_test_util import ProgressCallback

num_redis_shards = 5
redis_max_memory = 10**8
object_store_memory = 10**9
num_nodes = 3

message = ("Make sure there is enough memory on this machine to run this "
           "workload. We divide the system memory by 2 to provide a buffer.")
assert (num_nodes * object_store_memory + num_redis_shards * redis_max_memory <
        ray._private.utils.get_system_memory() / 2), message

# Simulate a cluster on one machine.

ray.init(address="auto")

# Run the workload.

run_experiments(
    {
        "PPO": {
            "run": "PPO",
            "env": "CartPole-v0",
            "num_samples": 10,
            "config": {
                "framework": "torch",
                "num_workers": 1,
                "num_gpus": 0,
                "num_sgd_iter": 1,
            },
            "stop": {
                "timesteps_total": 1,
            },
        }
    },
    callbacks=[ProgressCallback()])
    """

    proc = run_string_as_driver_nonblocking(script)
    # Make sure the script is running before sending a sigterm.
    with pytest.raises(subprocess.TimeoutExpired):
        print(proc.wait(timeout=10))
        std_str = proc.stdout.read().decode("ascii")
        err_str = proc.stderr.read().decode("ascii")
        print(f"STDOUT:\n{std_str}")
        print(f"STDERR:\n{err_str}")
    print(f"Script is running... pid: {proc.pid}")
    # Send multiple signals to terminate it like real world scenario.
    for _ in range(10):
        time.sleep(0.1)
        os.kill(proc.pid, signal.SIGINT)
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        print("Script wasn't terminated by SIGINT. Try SIGTERM.")
        os.kill(proc.pid, signal.SIGTERM)
    print(proc.wait(timeout=10))
    err_str = proc.stderr.read().decode("ascii")
    assert len(err_str) > 0
    assert "StackTrace Information" not in err_str
    print(err_str)


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1",
    reason="This test currently fails with minimal install.",
)
@pytest.mark.parametrize("execution_number", range(3))
def test_empty_line_thread_safety_bug(execution_number, ray_start_cluster):
    """Make sure when new threads are used within __init__,
    the empty line is not printed.
    Related: https://github.com/ray-project/ray/pull/20987
    """
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=24)

    actor_repr = "TESTER"

    script = f"""
import time
import os
import threading
import torch

from filelock import FileLock

import ray

class Repro:
    pass

def do_lock():
    path = f"/tmp/lock"
    lock = FileLock(path, timeout=4)
    lock.acquire()

@ray.remote
class Train:
    def __init__(self, config: Repro):
        # print("b")
        def warmup():

            do_lock()
            torch.empty(0, device="cpu")
            for _ in range(300000000):
                pass

        threading.Thread(target=warmup, daemon=True).start()

    def ready(self):
        pass

    def __repr__(self):
        return "{actor_repr}"

ray.init("auto")
actors = [Train.remote(config=None) for i in range(24)]
for a in actors:
    ray.get(a.ready.remote())
time.sleep(5)
    """
    out = run_string_as_driver(script)
    assert actor_repr not in out


@pytest.mark.parametrize("pull_based", [True, False])
def test_node_name_in_raylet_death(pull_based):
    NODE_NAME = "RAY_TEST_RAYLET_DEATH_NODE_NAME"
    script = f"""
import time
import os

WAIT_BUFFER_SECONDS=5

if {pull_based}:
    os.environ["RAY_pull_based_healthcheck"]="true"
    os.environ["RAY_health_check_initial_delay_ms"]="0"
    os.environ["RAY_health_check_period_ms"]="1000"
    os.environ["RAY_health_check_timeout_ms"]="10"
    os.environ["RAY_health_check_failure_threshold"]="2"
    sleep_time = float(os.environ["RAY_health_check_period_ms"]) / 1000.0 * \
        int(os.environ["RAY_health_check_failure_threshold"])
    sleep_time += WAIT_BUFFER_SECONDS
else:
    NUM_HEARTBEATS=10
    HEARTBEAT_PERIOD=500
    os.environ["RAY_pull_based_healthcheck"]="false"
    os.environ["RAY_num_heartbeats_timeout"]=str(NUM_HEARTBEATS)
    os.environ["RAY_raylet_heartbeat_period_milliseconds"]=str(HEARTBEAT_PERIOD)
    sleep_time = NUM_HEARTBEATS * HEARTBEAT_PERIOD / 1000 + WAIT_BUFFER_SECONDS

import ray

ray.init(_node_name=\"{NODE_NAME}\")
# This will kill raylet without letting it exit gracefully.
ray._private.worker._global_node.kill_raylet()

time.sleep(sleep_time)
ray.shutdown()
    """
    out = run_string_as_driver(script)
    print(out)
    assert out.count(f"node name: {NODE_NAME} has been marked dead") == 1


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
        if os.environ.get("PARALLEL_CI"):
            sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
        else:
            sys.exit(pytest.main(["-sv", __file__]))
