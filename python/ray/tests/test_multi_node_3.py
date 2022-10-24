import asyncio
import os
import subprocess
import sys
from pathlib import Path

import psutil
import pytest

import ray
import ray._private.ray_constants as ray_constants
from ray._private.test_utils import (
    Semaphore,
    check_call_ray,
    check_call_subprocess,
    kill_process_by_name,
    start_redis_instance,
    run_string_as_driver,
    run_string_as_driver_nonblocking,
    wait_for_children_of_pid,
    wait_for_children_of_pid_to_exit,
)
from ray._private.utils import detect_fate_sharing_support


def test_calling_start_ray_head(call_ray_stop_only):

    # Test that we can call ray start with various command line
    # parameters.

    # Test starting Ray with a redis port specified.
    check_call_ray(["start", "--head", "--port", "0"])
    check_call_ray(["stop"])

    # Test starting Ray with a node IP address specified.
    check_call_ray(["start", "--head", "--node-ip-address", "127.0.0.1", "--port", "0"])
    check_call_ray(["stop"])

    # Test starting Ray with a system config parameter set.
    check_call_ray(
        [
            "start",
            "--head",
            "--system-config",
            '{"metrics_report_interval_ms":100}',
            "--port",
            "0",
        ]
    )
    check_call_ray(["stop"])

    # Test starting Ray with the object manager and node manager ports
    # specified.
    check_call_ray(
        [
            "start",
            "--head",
            "--object-manager-port",
            "22345",
            "--node-manager-port",
            "54321",
            "--port",
            "0",
        ]
    )
    check_call_ray(["stop"])

    # Test starting Ray with the worker port range specified.
    check_call_ray(
        [
            "start",
            "--head",
            "--min-worker-port",
            "51000",
            "--max-worker-port",
            "51050",
            "--port",
            "0",
        ]
    )
    check_call_ray(["stop"])

    # Test starting Ray with a worker port list.
    check_call_ray(["start", "--head", "--worker-port-list", "10002,10003"])
    check_call_ray(["stop"])

    # Test starting Ray with a non-int in the worker port list.
    with pytest.raises(subprocess.CalledProcessError):
        check_call_ray(["start", "--head", "--worker-port-list", "10002,a"])
    check_call_ray(["stop"])

    # Test starting Ray with an invalid port in the worker port list.
    with pytest.raises(subprocess.CalledProcessError):
        check_call_ray(["start", "--head", "--worker-port-list", "100"])
    check_call_ray(["stop"])

    # Test starting Ray with the number of CPUs specified.
    check_call_ray(["start", "--head", "--num-cpus", "2", "--port", "0"])
    check_call_ray(["stop"])

    # Test starting Ray with the number of GPUs specified.
    check_call_ray(["start", "--head", "--num-gpus", "100", "--port", "0"])
    check_call_ray(["stop"])

    # Test starting Ray with redis shard ports specified.
    check_call_ray(
        ["start", "--head", "--redis-shard-ports", "6380,6381,6382", "--port", "0"]
    )
    check_call_ray(["stop"])

    # Test starting Ray with all arguments specified.
    check_call_ray(
        [
            "start",
            "--head",
            "--redis-shard-ports",
            "6380,6381,6382",
            "--object-manager-port",
            "22345",
            "--num-cpus",
            "2",
            "--num-gpus",
            "0",
            "--resources",
            '{"Custom": 1}',
            "--port",
            "0",
        ]
    )
    check_call_ray(["stop"])

    temp_dir = ray._private.utils.get_ray_temp_dir()

    # Test starting Ray with RAY_REDIS_ADDRESS env.
    _, proc = start_redis_instance(
        temp_dir, 8888, password=ray_constants.REDIS_DEFAULT_PASSWORD
    )
    os.environ["RAY_REDIS_ADDRESS"] = "127.0.0.1:8888"
    check_call_ray(["start", "--head"])
    check_call_ray(["stop"])
    proc.process.terminate()
    del os.environ["RAY_REDIS_ADDRESS"]

    # Test --block. Killing a child process should cause the command to exit.
    blocked = subprocess.Popen(["ray", "start", "--head", "--block", "--port", "0"])

    blocked.poll()
    assert blocked.returncode is None
    # Make sure ray cluster is up
    run_string_as_driver(
        """
import ray
from time import sleep
for i in range(0, 5):
    try:
        ray.init(address='auto')
        break
    except:
        sleep(1)
"""
    )

    # Make sure ray cluster is up
    run_string_as_driver(
        """
import ray
from time import sleep
for i in range(0, 5):
    try:
        ray.init(address='auto')
        break
    except:
        sleep(1)
"""
    )

    kill_process_by_name("raylet", SIGKILL=True)
    wait_for_children_of_pid_to_exit(blocked.pid, timeout=30)
    blocked.wait()
    assert blocked.returncode != 0, "ray start shouldn't return 0 on bad exit"

    # Test --block. Killing the command should clean up all child processes.
    blocked = subprocess.Popen(["ray", "start", "--head", "--block", "--port", "0"])
    blocked.poll()
    assert blocked.returncode is None

    # Include GCS, autoscaler monitor, client server, dashboard, raylet and
    # log_monitor.py
    num_children = 6
    if not detect_fate_sharing_support():
        # Account for ray_process_reaper.py
        num_children += 1
    # Check a set of child process commands & scripts instead?
    wait_for_children_of_pid(blocked.pid, num_children=num_children, timeout=30)

    blocked.terminate()
    wait_for_children_of_pid_to_exit(blocked.pid, timeout=30)
    blocked.wait()
    assert blocked.returncode != 0, "ray start shouldn't return 0 on bad exit"


def test_ray_start_non_head(call_ray_stop_only, monkeypatch):

    # Test that we can call ray start to connect to an existing cluster.

    # Test starting Ray with a port specified.
    check_call_ray(["start", "--head", "--port", "7298", "--resources", '{"res_0": 1}'])

    # Test starting node connecting to the above cluster.
    check_call_ray(
        ["start", "--address", "127.0.0.1:7298", "--resources", '{"res_1": 1}']
    )

    # Test starting Ray with address `auto`.
    check_call_ray(["start", "--address", "auto", "--resources", '{"res_2": 1}'])

    # Run tasks to verify nodes with custom resources are available.
    driver_script = """
import ray
ray.init()

@ray.remote
def f():
    return 1

assert ray.get(f.remote()) == 1
assert ray.get(f.options(resources={"res_0": 1}).remote()) == 1
assert ray.get(f.options(resources={"res_1": 1}).remote()) == 1
assert ray.get(f.options(resources={"res_2": 1}).remote()) == 1
print("success")
"""
    monkeypatch.setenv("RAY_ADDRESS", "auto")
    out = run_string_as_driver(driver_script)
    # Make sure the driver succeeded.
    assert "success" in out

    check_call_ray(["stop"])


@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head --num-cpus=1 " + "--node-ip-address=localhost"],
    indirect=True,
)
def test_using_hostnames(call_ray_start):
    ray.init(_node_ip_address="localhost", address="localhost:6379")

    @ray.remote
    def f():
        return 1

    assert ray.get(f.remote()) == 1


def test_connecting_in_local_case(ray_start_regular):
    address_info = ray_start_regular

    # Define a driver that just connects to Redis.
    driver_script = """
import ray
ray.init(address="{}")
print("success")
""".format(
        address_info["address"]
    )

    out = run_string_as_driver(driver_script)
    # Make sure the other driver succeeded.
    assert "success" in out


def test_run_driver_twice(ray_start_regular):
    # We used to have issue 2165 and 2288:
    # https://github.com/ray-project/ray/issues/2165
    # https://github.com/ray-project/ray/issues/2288
    # both complain that driver will hang when run for the second time.
    # This test is used to verify the fix for above issue, it will run the
    # same driver for twice and verify whether both of them succeed.
    address_info = ray_start_regular
    driver_script = """
import ray
import ray.tune as tune
import os
import time

def train_func(config, reporter):  # add a reporter arg
    for i in range(2):
        time.sleep(0.1)
        reporter(timesteps_total=i, mean_accuracy=i+97)  # report metrics

os.environ["TUNE_RESUME_PROMPT_OFF"] = "True"
ray.init(address="{}", namespace="default_test_namespace")
ray.tune.register_trainable("train_func", train_func)

tune.run_experiments({{
    "my_experiment": {{
        "run": "train_func",
        "stop": {{"mean_accuracy": 99}},
        "config": {{
            "layer1": {{
                "class_name": tune.grid_search(["a"]),
                "config": {{"lr": tune.grid_search([1, 2])}}
            }},
        }},
        "local_dir": os.path.expanduser("~/tmp")
    }}
}})
print("success")
""".format(
        address_info["address"]
    )

    for i in range(2):
        out = run_string_as_driver(driver_script)
        assert "success" in out


@pytest.mark.skip(reason="fate sharing not implemented yet")
def test_driver_exiting_when_worker_blocked(call_ray_start):
    # This test will create some drivers that submit some tasks and then
    # exit without waiting for the tasks to complete.
    address = call_ray_start

    ray.init(address=address)

    # Define a driver that creates two tasks, one that runs forever and the
    # other blocked on the first in a `ray.get`.
    driver_script = """
import time
import ray
ray.init(address="{}")
@ray.remote
def f():
    time.sleep(10**6)
@ray.remote
def g():
    ray.get(f.remote())
g.remote()
time.sleep(1)
print("success")
""".format(
        address
    )

    # Create some drivers and let them exit and make sure everything is
    # still alive.
    for _ in range(3):
        out = run_string_as_driver(driver_script)
        # Make sure the first driver ran to completion.
        assert "success" in out

    # Define a driver that creates two tasks, one that runs forever and the
    # other blocked on the first in a `ray.wait`.
    driver_script = """
import time
import ray
ray.init(address="{}")
@ray.remote
def f():
    time.sleep(10**6)
@ray.remote
def g():
    ray.wait([f.remote()])
g.remote()
time.sleep(1)
print("success")
""".format(
        address
    )

    # Create some drivers and let them exit and make sure everything is
    # still alive.
    for _ in range(3):
        out = run_string_as_driver(driver_script)
        # Make sure the first driver ran to completion.
        assert "success" in out

    # Define a driver that creates one task that depends on a nonexistent
    # object. This task will be queued as waiting to execute.
    driver_script_template = """
import time
import ray
ray.init(address="{}")
@ray.remote
def g(x):
    return
g.remote(ray.ObjectRef(ray._private.utils.hex_to_binary("{}")))
time.sleep(1)
print("success")
"""

    # Create some drivers and let them exit and make sure everything is
    # still alive.
    for _ in range(3):
        nonexistent_id = ray.ObjectRef.from_random()
        driver_script = driver_script_template.format(address, nonexistent_id.hex())
        out = run_string_as_driver(driver_script)
        # Simulate the nonexistent dependency becoming available.
        ray._private.worker.global_worker.put_object(None, nonexistent_id)
        # Make sure the first driver ran to completion.
        assert "success" in out

    # Define a driver that calls `ray.wait` on a nonexistent object.
    driver_script_template = """
import time
import ray
ray.init(address="{}")
@ray.remote
def g():
    ray.wait(ray.ObjectRef(ray._private.utils.hex_to_binary("{}")))
g.remote()
time.sleep(1)
print("success")
"""

    # Create some drivers and let them exit and make sure everything is
    # still alive.
    for _ in range(3):
        nonexistent_id = ray.ObjectRef.from_random()
        driver_script = driver_script_template.format(address, nonexistent_id.hex())
        out = run_string_as_driver(driver_script)
        # Simulate the nonexistent dependency becoming available.
        ray._private.worker.global_worker.put_object(None, nonexistent_id)
        # Make sure the first driver ran to completion.
        assert "success" in out

    @ray.remote
    def f():
        return 1

    # Make sure we can still talk with the raylet.
    ray.get(f.remote())


def test_multi_driver_logging(ray_start_regular):
    address_info = ray_start_regular
    address = address_info["address"]

    # ray.init(address=address)
    driver1_wait = Semaphore.options(name="driver1_wait").remote(value=0)
    driver2_wait = Semaphore.options(name="driver2_wait").remote(value=0)
    main_wait = Semaphore.options(name="main_wait").remote(value=0)

    # The creation of an actor is asynchronous.
    # We need to wait for the completion of the actor creation,
    # otherwise we can't get the actor by name.
    ray.get(driver1_wait.locked.remote())
    ray.get(driver2_wait.locked.remote())
    ray.get(main_wait.locked.remote())

    # Params are address, semaphore name, output1, output2
    driver_script_template = """
import ray
import sys
from ray._private.test_utils import Semaphore

@ray.remote(num_cpus=0)
def remote_print(s, file=None):
    print(s, file=file)

ray.init(address="{}", namespace="default_test_namespace")

driver_wait = ray.get_actor("{}")
main_wait = ray.get_actor("main_wait")

ray.get(main_wait.release.remote())
ray.get(driver_wait.acquire.remote())

s1 = "{}"
ray.get(remote_print.remote(s1))

ray.get(main_wait.release.remote())
ray.get(driver_wait.acquire.remote())

s2 = "{}"
ray.get(remote_print.remote(s2))

ray.get(main_wait.release.remote())
    """

    p1 = run_string_as_driver_nonblocking(
        driver_script_template.format(address, "driver1_wait", "1", "2")
    )
    p2 = run_string_as_driver_nonblocking(
        driver_script_template.format(address, "driver2_wait", "3", "4")
    )

    ray.get(main_wait.acquire.remote())
    ray.get(main_wait.acquire.remote())
    # At this point both of the other drivers are fully initialized.

    ray.get(driver1_wait.release.remote())
    ray.get(driver2_wait.release.remote())

    # At this point driver1 should receive '1' and driver2 '3'
    ray.get(main_wait.acquire.remote())
    ray.get(main_wait.acquire.remote())

    ray.get(driver1_wait.release.remote())
    ray.get(driver2_wait.release.remote())

    # At this point driver1 should receive '2' and driver2 '4'
    ray.get(main_wait.acquire.remote())
    ray.get(main_wait.acquire.remote())

    driver1_out = p1.stdout.read().decode("ascii")
    driver2_out = p2.stdout.read().decode("ascii")
    if sys.platform == "win32":
        driver1_out = driver1_out.replace("\r", "")
        driver2_out = driver2_out.replace("\r", "")
    driver1_out_split = driver1_out.split("\n")
    driver2_out_split = driver2_out.split("\n")

    assert driver1_out_split[0][-1] == "1", driver1_out_split
    assert driver1_out_split[1][-1] == "2", driver1_out_split
    assert driver2_out_split[0][-1] == "3", driver2_out_split
    assert driver2_out_split[1][-1] == "4", driver2_out_split


@pytest.fixture
def redis_proc():
    """Download external redis and start the subprocess."""
    REDIS_SERVER_PATH = "core/src/ray/thirdparty/redis/src/redis-server"
    full_path = Path(ray.__file__).parents[0] / REDIS_SERVER_PATH
    check_call_subprocess(["cp", f"{full_path}", "redis-server"])
    proc = subprocess.Popen(["./redis-server", "--port", "7999"])
    yield proc
    subprocess.check_call(["ray", "stop"])
    os.kill(proc.pid, 9)
    subprocess.check_call(["rm", "-rf", "redis-server"])


@pytest.mark.skipif(
    sys.platform == "win32",
    reason=(
        "Feature not supported Windows because Redis "
        "is not officially supported by Windows. "
        "(There cannot be external Redis in Windows)"
    ),
)
def test_ray_stop_should_not_kill_external_redis(redis_proc):
    check_call_ray(["start", "--head"])
    subprocess.check_call(["ray", "stop"])
    assert redis_proc.poll() is None


def test_ray_stop_kill_workers():
    check_call_ray(["start", "--head"])

    ray.init(address="auto")

    @ray.remote
    class Actor:
        async def ping(self):
            return os.getpid()

        async def run_forever(self):
            while True:
                await asyncio.sleep(5)

    actor = Actor.options(lifetime="detached", name="A").remote()
    actor.run_forever.remote()
    actor_pid = ray.get(actor.ping.remote())
    ray.shutdown()

    check_call_ray(["stop", "--force"])

    assert not psutil.pid_exists(actor_pid)


if __name__ == "__main__":
    import pytest

    # Make subprocess happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
