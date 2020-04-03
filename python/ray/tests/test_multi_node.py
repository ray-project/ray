import os
import pytest
import subprocess
import time

import ray
from ray.test_utils import (
    RayTestTimeoutException,
    run_string_as_driver,
    run_string_as_driver_nonblocking,
    wait_for_children_of_pid,
    wait_for_children_of_pid_to_exit,
    kill_process_by_name,
)


def test_error_isolation(call_ray_start):
    address = call_ray_start
    # Connect a driver to the Ray cluster.
    ray.init(address=address)

    # There shouldn't be any errors yet.
    assert len(ray.errors()) == 0

    error_string1 = "error_string1"
    error_string2 = "error_string2"

    @ray.remote
    def f():
        raise Exception(error_string1)

    # Run a remote function that throws an error.
    with pytest.raises(Exception):
        ray.get(f.remote())

    # Wait for the error to appear in Redis.
    while len(ray.errors()) != 1:
        time.sleep(0.1)
        print("Waiting for error to appear.")

    # Make sure we got the error.
    assert len(ray.errors()) == 1
    assert error_string1 in ray.errors()[0]["message"]

    # Start another driver and make sure that it does not receive this
    # error. Make the other driver throw an error, and make sure it
    # receives that error.
    driver_script = """
import ray
import time

ray.init(address="{}")

time.sleep(1)
assert len(ray.errors()) == 0

@ray.remote
def f():
    raise Exception("{}")

try:
    ray.get(f.remote())
except Exception as e:
    pass

while len(ray.errors()) != 1:
    print(len(ray.errors()))
    time.sleep(0.1)
assert len(ray.errors()) == 1

assert "{}" in ray.errors()[0]["message"]

print("success")
""".format(address, error_string2, error_string2)

    out = run_string_as_driver(driver_script)
    # Make sure the other driver succeeded.
    assert "success" in out

    # Make sure that the other error message doesn't show up for this
    # driver.
    assert len(ray.errors()) == 1
    assert error_string1 in ray.errors()[0]["message"]


def test_remote_function_isolation(call_ray_start):
    # This test will run multiple remote functions with the same names in
    # two different drivers. Connect a driver to the Ray cluster.
    address = call_ray_start

    ray.init(address=address)

    # Start another driver and make sure that it can define and call its
    # own commands with the same names.
    driver_script = """
import ray
import time
ray.init(address="{}")
@ray.remote
def f():
    return 3
@ray.remote
def g(x, y):
    return 4
for _ in range(10000):
    result = ray.get([f.remote(), g.remote(0, 0)])
    assert result == [3, 4]
print("success")
""".format(address)

    out = run_string_as_driver(driver_script)

    @ray.remote
    def f():
        return 1

    @ray.remote
    def g(x):
        return 2

    for _ in range(10000):
        result = ray.get([f.remote(), g.remote(0)])
        assert result == [1, 2]

    # Make sure the other driver succeeded.
    assert "success" in out


def test_driver_exiting_quickly(call_ray_start):
    # This test will create some drivers that submit some tasks and then
    # exit without waiting for the tasks to complete.
    address = call_ray_start

    ray.init(address=address)

    # Define a driver that creates an actor and exits.
    driver_script1 = """
import ray
ray.init(address="{}")
@ray.remote
class Foo:
    def __init__(self):
        pass
Foo.remote()
print("success")
""".format(address)

    # Define a driver that creates some tasks and exits.
    driver_script2 = """
import ray
ray.init(address="{}")
@ray.remote
def f():
    return 1
f.remote()
print("success")
""".format(address)

    # Create some drivers and let them exit and make sure everything is
    # still alive.
    for _ in range(3):
        out = run_string_as_driver(driver_script1)
        # Make sure the first driver ran to completion.
        assert "success" in out
        out = run_string_as_driver(driver_script2)
        # Make sure the first driver ran to completion.
        assert "success" in out


def test_cleanup_on_driver_exit(call_ray_start):
    # This test will create a driver that creates a bunch of objects and then
    # exits. The entries in the object table should be cleaned up.
    address = call_ray_start

    ray.init(address=address)

    # Define a driver that creates a bunch of objects and exits.
    driver_script = """
import time
import ray
ray.init(address="{}")
object_ids = [ray.put(i) for i in range(1000)]
start_time = time.time()
while time.time() - start_time < 30:
    if len(ray.objects()) == 1000:
        break
else:
    raise Exception("Objects did not appear in object table.")
print("success")
""".format(address)

    run_string_as_driver(driver_script)

    # Make sure the objects are removed from the object table.
    start_time = time.time()
    while time.time() - start_time < 30:
        if len(ray.objects()) == 0:
            break
    else:
        raise Exception("Objects were not all removed from object table.")


def test_drivers_named_actors(call_ray_start):
    # This test will create some drivers that submit some tasks to the same
    # named actor.
    address = call_ray_start

    ray.init(address=address)

    # Define a driver that creates a named actor then sleeps for a while.
    driver_script1 = """
import ray
import time
ray.init(address="{}")
@ray.remote
class Counter:
    def __init__(self):
        self.count = 0
    def increment(self):
        self.count += 1
        return self.count
counter = Counter.remote()
ray.util.register_actor("Counter", counter)
time.sleep(100)
""".format(address)

    # Define a driver that submits to the named actor and exits.
    driver_script2 = """
import ray
import time
ray.init(address="{}")
while True:
    try:
        counter = ray.util.get_actor("Counter")
        break
    except ValueError:
        time.sleep(1)
assert ray.get(counter.increment.remote()) == {}
print("success")
""".format(address, "{}")

    process_handle = run_string_as_driver_nonblocking(driver_script1)

    for i in range(3):
        driver_script = driver_script2.format(i + 1)
        out = run_string_as_driver(driver_script)
        assert "success" in out

    process_handle.kill()


def test_receive_late_worker_logs():
    # Make sure that log messages from tasks appear in the stdout even if the
    # script exits quickly.
    log_message = "some helpful debugging message"

    # Define a driver that creates a task that prints something, ensures that
    # the task runs, and then exits.
    driver_script = """
import ray
import random
import time

log_message = "{}"

@ray.remote
class Actor:
    def log(self):
        print(log_message)

@ray.remote
def f():
    print(log_message)

ray.init(num_cpus=2)

a = Actor.remote()
ray.get([a.log.remote(), f.remote()])
ray.get([a.log.remote(), f.remote()])
""".format(log_message)

    for _ in range(2):
        out = run_string_as_driver(driver_script)
        assert out.count(log_message) == 4


@pytest.mark.parametrize(
    "call_ray_start", ["ray start --head --num-cpus=1 --num-gpus=1"],
    indirect=True)
def test_drivers_release_resources(call_ray_start):
    address = call_ray_start

    # Define a driver that creates an actor and exits.
    driver_script1 = """
import time
import ray

ray.init(address="{}")

@ray.remote
def f(duration):
    time.sleep(duration)

@ray.remote(num_gpus=1)
def g(duration):
    time.sleep(duration)

@ray.remote(num_gpus=1)
class Foo:
    def __init__(self):
        pass

# Make sure some resources are available for us to run tasks.
ray.get(f.remote(0))
ray.get(g.remote(0))

# Start a bunch of actors and tasks that use resources. These should all be
# cleaned up when this driver exits.
foos = [Foo.remote() for _ in range(100)]
[f.remote(10 ** 6) for _ in range(100)]

print("success")
""".format(address)

    driver_script2 = (driver_script1 +
                      "import sys\nsys.stdout.flush()\ntime.sleep(10 ** 6)\n")

    def wait_for_success_output(process_handle, timeout=10):
        # Wait until the process prints "success" and then return.
        start_time = time.time()
        while time.time() - start_time < timeout:
            output_line = ray.utils.decode(
                process_handle.stdout.readline()).strip()
            print(output_line)
            if output_line == "success":
                return
        raise RayTestTimeoutException(
            "Timed out waiting for process to print success.")

    # Make sure we can run this driver repeatedly, which means that resources
    # are getting released in between.
    for _ in range(5):
        out = run_string_as_driver(driver_script1)
        # Make sure the first driver ran to completion.
        assert "success" in out
        # Also make sure that this works when the driver exits ungracefully.
        process_handle = run_string_as_driver_nonblocking(driver_script2)
        wait_for_success_output(process_handle)
        # Kill the process ungracefully.
        process_handle.kill()


def test_calling_start_ray_head(call_ray_stop_only):
    # Test that we can call ray start with various command line
    # parameters. TODO(rkn): This test only tests the --head code path. We
    # should also test the non-head node code path.

    # Test starting Ray with no arguments.
    subprocess.check_output(["ray", "start", "--head"])
    subprocess.check_output(["ray", "stop"])

    # Test starting Ray with a redis port specified.
    subprocess.check_output(["ray", "start", "--head", "--redis-port", "6379"])
    subprocess.check_output(["ray", "stop"])

    # Test starting Ray with a node IP address specified.
    subprocess.check_output(
        ["ray", "start", "--head", "--node-ip-address", "127.0.0.1"])
    subprocess.check_output(["ray", "stop"])

    # Test starting Ray with the object manager and node manager ports
    # specified.
    subprocess.check_output([
        "ray", "start", "--head", "--object-manager-port", "12345",
        "--node-manager-port", "54321"
    ])
    subprocess.check_output(["ray", "stop"])

    # Test starting Ray with the number of CPUs specified.
    subprocess.check_output(["ray", "start", "--head", "--num-cpus", "2"])
    subprocess.check_output(["ray", "stop"])

    # Test starting Ray with the number of GPUs specified.
    subprocess.check_output(["ray", "start", "--head", "--num-gpus", "100"])
    subprocess.check_output(["ray", "stop"])

    # Test starting Ray with the max redis clients specified.
    subprocess.check_output(
        ["ray", "start", "--head", "--redis-max-clients", "100"])
    subprocess.check_output(["ray", "stop"])

    if "RAY_USE_NEW_GCS" not in os.environ:
        # Test starting Ray with redis shard ports specified.
        subprocess.check_output([
            "ray", "start", "--head", "--redis-shard-ports", "6380,6381,6382"
        ])
        subprocess.check_output(["ray", "stop"])

        # Test starting Ray with all arguments specified.
        subprocess.check_output([
            "ray", "start", "--head", "--redis-port", "6379",
            "--redis-shard-ports", "6380,6381,6382", "--object-manager-port",
            "12345", "--num-cpus", "2", "--num-gpus", "0",
            "--redis-max-clients", "100", "--resources", "{\"Custom\": 1}"
        ])
        subprocess.check_output(["ray", "stop"])

    # Test starting Ray with invalid arguments.
    with pytest.raises(subprocess.CalledProcessError):
        subprocess.check_output(
            ["ray", "start", "--head", "--address", "127.0.0.1:6379"])
    subprocess.check_output(["ray", "stop"])

    # Test --block. Killing a child process should cause the command to exit.
    blocked = subprocess.Popen(["ray", "start", "--head", "--block"])

    wait_for_children_of_pid(blocked.pid, num_children=7, timeout=30)

    blocked.poll()
    assert blocked.returncode is None

    kill_process_by_name("raylet")
    wait_for_children_of_pid_to_exit(blocked.pid, timeout=120)
    blocked.wait()
    assert blocked.returncode != 0, "ray start shouldn't return 0 on bad exit"

    # Test --block. Killing the command should clean up all child processes.
    blocked = subprocess.Popen(["ray", "start", "--head", "--block"])
    blocked.poll()
    assert blocked.returncode is None

    wait_for_children_of_pid(blocked.pid, num_children=7, timeout=30)

    blocked.terminate()
    wait_for_children_of_pid_to_exit(blocked.pid, timeout=120)
    blocked.wait()
    assert blocked.returncode != 0, "ray start shouldn't return 0 on bad exit"


@pytest.mark.parametrize(
    "call_ray_start", [
        "ray start --head --num-cpus=1 " +
        "--node-ip-address=localhost --redis-port=6379"
    ],
    indirect=True)
def test_using_hostnames(call_ray_start):
    ray.init(node_ip_address="localhost", address="localhost:6379")

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
g.remote(ray.ObjectID(ray.utils.hex_to_binary("{}")))
time.sleep(1)
print("success")
"""

    # Create some drivers and let them exit and make sure everything is
    # still alive.
    for _ in range(3):
        nonexistent_id = ray.ObjectID.from_random()
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
    ray.wait(ray.ObjectID(ray.utils.hex_to_binary("{}")))
g.remote()
time.sleep(1)
print("success")
"""

    # Create some drivers and let them exit and make sure everything is
    # still alive.
    for _ in range(3):
        nonexistent_id = ray.ObjectID.from_random()
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


if __name__ == "__main__":
    import pytest
    import sys
    # Make subprocess happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    sys.exit(pytest.main(["-v", __file__]))
