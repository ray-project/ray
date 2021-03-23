from collections import Counter
import os
import pytest
import subprocess
import sys

import ray
from ray.test_utils import (
    check_call_ray, run_string_as_driver, run_string_as_driver_nonblocking,
    wait_for_children_of_pid, wait_for_children_of_pid_to_exit,
    wait_for_children_names_of_pid, kill_process_by_name, Semaphore)
from time import sleep


def test_calling_start_ray_head(call_ray_stop_only):

    # Test that we can call ray start with various command line
    # parameters. TODO(rkn): This test only tests the --head code path. We
    # should also test the non-head node code path.

    # Test starting Ray with a redis port specified.
    check_call_ray(["start", "--head", "--port", "0"])
    check_call_ray(["stop"])

    # Test starting Ray with a node IP address specified.
    check_call_ray(
        ["start", "--head", "--node-ip-address", "127.0.0.1", "--port", "0"])
    check_call_ray(["stop"])

    # Test starting Ray with a system config parameter set.
    check_call_ray([
        "start", "--head", "--system-config",
        "{\"metrics_report_interval_ms\":100}", "--port", "0"
    ])
    check_call_ray(["stop"])

    # Test starting Ray with the object manager and node manager ports
    # specified.
    check_call_ray([
        "start", "--head", "--object-manager-port", "12345",
        "--node-manager-port", "54321", "--port", "0"
    ])
    check_call_ray(["stop"])

    # Test starting Ray with the worker port range specified.
    check_call_ray([
        "start", "--head", "--min-worker-port", "50000", "--max-worker-port",
        "51000", "--port", "0"
    ])
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
    check_call_ray([
        "start", "--head", "--redis-shard-ports", "6380,6381,6382", "--port",
        "0"
    ])
    check_call_ray(["stop"])

    # Test starting Ray with all arguments specified.
    check_call_ray([
        "start", "--head", "--redis-shard-ports", "6380,6381,6382",
        "--object-manager-port", "12345", "--num-cpus", "2", "--num-gpus", "0",
        "--resources", "{\"Custom\": 1}", "--port", "0"
    ])
    check_call_ray(["stop"])

    # Test starting Ray with invalid arguments.
    with pytest.raises(subprocess.CalledProcessError):
        check_call_ray(
            ["start", "--head", "--address", "127.0.0.1:6379", "--port", "0"])
    check_call_ray(["stop"])

    # Test --block. Killing a child process should cause the command to exit.
    blocked = subprocess.Popen(
        ["ray", "start", "--head", "--block", "--port", "0"])

    wait_for_children_names_of_pid(blocked.pid, ["raylet"], timeout=30)

    blocked.poll()
    assert blocked.returncode is None

    kill_process_by_name("raylet")
    wait_for_children_of_pid_to_exit(blocked.pid, timeout=30)
    blocked.wait()
    assert blocked.returncode != 0, "ray start shouldn't return 0 on bad exit"

    # Test --block. Killing the command should clean up all child processes.
    blocked = subprocess.Popen(
        ["ray", "start", "--head", "--block", "--port", "0"])
    blocked.poll()
    assert blocked.returncode is None

    wait_for_children_of_pid(blocked.pid, num_children=7, timeout=30)

    blocked.terminate()
    wait_for_children_of_pid_to_exit(blocked.pid, timeout=30)
    blocked.wait()
    assert blocked.returncode != 0, "ray start shouldn't return 0 on bad exit"


@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head --num-cpus=1 " + "--node-ip-address=localhost"],
    indirect=True)
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
""".format(address_info["redis_address"])

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
ray.init(address="{}")
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
""".format(address_info["redis_address"])

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
""".format(address)

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
""".format(address)

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
        driver_script = driver_script_template.format(address,
                                                      nonexistent_id.hex())
        out = run_string_as_driver(driver_script)
        # Simulate the nonexistent dependency becoming available.
        ray.worker.global_worker.put_object(None, nonexistent_id)
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
        driver_script = driver_script_template.format(address,
                                                      nonexistent_id.hex())
        out = run_string_as_driver(driver_script)
        # Simulate the nonexistent dependency becoming available.
        ray.worker.global_worker.put_object(None, nonexistent_id)
        # Make sure the first driver ran to completion.
        assert "success" in out

    @ray.remote
    def f():
        return 1

    # Make sure we can still talk with the raylet.
    ray.get(f.remote())


def test_multi_driver_logging(ray_start_regular):
    address_info = ray_start_regular
    address = address_info["redis_address"]

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
from ray.test_utils import Semaphore

@ray.remote(num_cpus=0)
def remote_print(s, file=None):
    print(s, file=file)

ray.init(address="{}")

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
        driver_script_template.format(address, "driver1_wait", "1", "2"))
    p2 = run_string_as_driver_nonblocking(
        driver_script_template.format(address, "driver2_wait", "3", "4"))

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


def test_spillback_distribution(ray_start_cluster):
    cluster = ray_start_cluster
    # Create a head node and wait until it is up.
    cluster.add_node(
        num_cpus=0, _system_config={"scheduler_loadbalance_spillback": True})
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    num_nodes = 2
    # create 2 worker nodes.
    for _ in range(num_nodes):
        cluster.add_node(num_cpus=8)
    cluster.wait_for_nodes()

    assert ray.cluster_resources()["CPU"] == 16

    @ray.remote
    def task():
        sleep(1)
        return ray.worker.global_worker.current_node_id

    # Make sure tasks are spilled back non-deterministically.
    locations = ray.get([task.remote() for _ in range(8)])
    counter = Counter(locations)
    spread = max(counter.values()) - min(counter.values())
    # Ideally we'd want 4 tasks to go to each node, but we'll settle for
    # anything better than a 1-7 split since randomness is noisy.
    assert spread < 6
    assert len(counter) > 1

    @ray.remote(num_cpus=1)
    class Actor1:
        def __init__(self):
            pass

        def get_location(self):
            return ray.worker.global_worker.current_node_id

    actors = [Actor1.remote() for _ in range(10)]
    locations = ray.get([actor.get_location.remote() for actor in actors])
    counter = Counter(locations)
    spread = max(counter.values()) - min(counter.values())
    assert spread < 6
    assert len(counter) > 1


if __name__ == "__main__":
    import pytest
    # Make subprocess happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    sys.exit(pytest.main(["-v", __file__]))
