from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import pytest
import subprocess
import sys
import tempfile
import time

import ray
from ray.test.test_utils import run_and_get_output


def run_string_as_driver(driver_script):
    """Run a driver as a separate process.

    Args:
        driver_script: A string to run as a Python script.

    Returns:
        The script's output.
    """
    # Save the driver script as a file so we can call it using subprocess.
    with tempfile.NamedTemporaryFile() as f:
        f.write(driver_script.encode("ascii"))
        f.flush()
        out = ray.utils.decode(
            subprocess.check_output([sys.executable, f.name]))
    return out


def run_string_as_driver_nonblocking(driver_script):
    """Start a driver as a separate process and return immediately.

    Args:
        driver_script: A string to run as a Python script.

    Returns:
        A handle to the driver process.
    """
    # Save the driver script as a file so we can call it using subprocess. We
    # do not delete this file because if we do then it may get removed before
    # the Python process tries to run it.
    with tempfile.NamedTemporaryFile(delete=False) as f:
        f.write(driver_script.encode("ascii"))
        f.flush()
        return subprocess.Popen(
            [sys.executable, f.name], stdout=subprocess.PIPE)


@pytest.fixture
def ray_start_head():
    out = run_and_get_output(["ray", "start", "--head", "--num-cpus=2"])
    # Get the redis address from the output.
    redis_substring_prefix = "redis_address=\""
    redis_address_location = (
        out.find(redis_substring_prefix) + len(redis_substring_prefix))
    redis_address = out[redis_address_location:]
    redis_address = redis_address.split("\"")[0]

    yield redis_address

    # Disconnect from the Ray cluster.
    ray.shutdown()
    # Kill the Ray cluster.
    subprocess.Popen(["ray", "stop"]).wait()


def test_error_isolation(ray_start_head):
    redis_address = ray_start_head
    # Connect a driver to the Ray cluster.
    ray.init(redis_address=redis_address)

    # There shouldn't be any errors yet.
    assert len(ray.error_info()) == 0

    error_string1 = "error_string1"
    error_string2 = "error_string2"

    @ray.remote
    def f():
        raise Exception(error_string1)

    # Run a remote function that throws an error.
    with pytest.raises(Exception):
        ray.get(f.remote())

    # Wait for the error to appear in Redis.
    while len(ray.error_info()) != 1:
        time.sleep(0.1)
        print("Waiting for error to appear.")

    # Make sure we got the error.
    assert len(ray.error_info()) == 1
    assert error_string1 in ray.error_info()[0]["message"]

    # Start another driver and make sure that it does not receive this
    # error. Make the other driver throw an error, and make sure it
    # receives that error.
    driver_script = """
import ray
import time

ray.init(redis_address="{}")

time.sleep(1)
assert len(ray.error_info()) == 0

@ray.remote
def f():
    raise Exception("{}")

try:
    ray.get(f.remote())
except Exception as e:
    pass

while len(ray.error_info()) != 1:
    print(len(ray.error_info()))
    time.sleep(0.1)
assert len(ray.error_info()) == 1

assert "{}" in ray.error_info()[0]["message"]

print("success")
""".format(redis_address, error_string2, error_string2)

    out = run_string_as_driver(driver_script)
    # Make sure the other driver succeeded.
    assert "success" in out

    # Make sure that the other error message doesn't show up for this
    # driver.
    assert len(ray.error_info()) == 1
    assert error_string1 in ray.error_info()[0]["message"]


def test_remote_function_isolation(ray_start_head):
    # This test will run multiple remote functions with the same names in
    # two different drivers. Connect a driver to the Ray cluster.
    redis_address = ray_start_head

    ray.init(redis_address=redis_address)

    # Start another driver and make sure that it can define and call its
    # own commands with the same names.
    driver_script = """
import ray
import time
ray.init(redis_address="{}")
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
""".format(redis_address)

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


def test_driver_exiting_quickly(ray_start_head):
    # This test will create some drivers that submit some tasks and then
    # exit without waiting for the tasks to complete.
    redis_address = ray_start_head

    ray.init(redis_address=redis_address)

    # Define a driver that creates an actor and exits.
    driver_script1 = """
import ray
ray.init(redis_address="{}")
@ray.remote
class Foo(object):
    def __init__(self):
        pass
Foo.remote()
print("success")
""".format(redis_address)

    # Define a driver that creates some tasks and exits.
    driver_script2 = """
import ray
ray.init(redis_address="{}")
@ray.remote
def f():
    return 1
f.remote()
print("success")
""".format(redis_address)

    # Create some drivers and let them exit and make sure everything is
    # still alive.
    for _ in range(3):
        out = run_string_as_driver(driver_script1)
        # Make sure the first driver ran to completion.
        assert "success" in out
        out = run_string_as_driver(driver_script2)
        # Make sure the first driver ran to completion.
        assert "success" in out
        assert ray.services.all_processes_alive()


@pytest.fixture
def ray_start_head_with_resources():
    out = run_and_get_output(
        ["ray", "start", "--head", "--num-cpus=1", "--num-gpus=1"])
    # Get the redis address from the output.
    redis_substring_prefix = "redis_address=\""
    redis_address_location = (
        out.find(redis_substring_prefix) + len(redis_substring_prefix))
    redis_address = out[redis_address_location:]
    redis_address = redis_address.split("\"")[0]

    yield redis_address

    # Kill the Ray cluster.
    subprocess.Popen(["ray", "stop"]).wait()


@pytest.mark.skipif(
    os.environ.get("RAY_USE_XRAY") != "1",
    reason="This test only works with xray.")
def test_drivers_release_resources(ray_start_head_with_resources):
    redis_address = ray_start_head_with_resources

    # Define a driver that creates an actor and exits.
    driver_script1 = """
import time
import ray

ray.init(redis_address="{}")

@ray.remote
def f(duration):
    time.sleep(duration)

@ray.remote(num_gpus=1)
def g(duration):
    time.sleep(duration)

@ray.remote(num_gpus=1)
class Foo(object):
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
""".format(redis_address)

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
        raise Exception("Timed out waiting for process to print success.")

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


def test_calling_start_ray_head():
    # Test that we can call start-ray.sh with various command line
    # parameters. TODO(rkn): This test only tests the --head code path. We
    # should also test the non-head node code path.

    # Test starting Ray with no arguments.
    run_and_get_output(["ray", "start", "--head"])
    subprocess.Popen(["ray", "stop"]).wait()

    # Test starting Ray with a number of workers specified.
    run_and_get_output(["ray", "start", "--head", "--num-workers", "20"])
    subprocess.Popen(["ray", "stop"]).wait()

    # Test starting Ray with a redis port specified.
    run_and_get_output(["ray", "start", "--head", "--redis-port", "6379"])
    subprocess.Popen(["ray", "stop"]).wait()

    # Test starting Ray with a node IP address specified.
    run_and_get_output(
        ["ray", "start", "--head", "--node-ip-address", "127.0.0.1"])
    subprocess.Popen(["ray", "stop"]).wait()

    # Test starting Ray with an object manager port specified.
    run_and_get_output(
        ["ray", "start", "--head", "--object-manager-port", "12345"])
    subprocess.Popen(["ray", "stop"]).wait()

    # Test starting Ray with the number of CPUs specified.
    run_and_get_output(["ray", "start", "--head", "--num-cpus", "2"])
    subprocess.Popen(["ray", "stop"]).wait()

    # Test starting Ray with the number of GPUs specified.
    run_and_get_output(["ray", "start", "--head", "--num-gpus", "100"])
    subprocess.Popen(["ray", "stop"]).wait()

    # Test starting Ray with the max redis clients specified.
    run_and_get_output(
        ["ray", "start", "--head", "--redis-max-clients", "100"])
    subprocess.Popen(["ray", "stop"]).wait()

    if "RAY_USE_NEW_GCS" not in os.environ:
        # Test starting Ray with redis shard ports specified.
        run_and_get_output([
            "ray", "start", "--head", "--redis-shard-ports", "6380,6381,6382"
        ])
        subprocess.Popen(["ray", "stop"]).wait()

        # Test starting Ray with all arguments specified.
        run_and_get_output([
            "ray", "start", "--head", "--num-workers", "2", "--redis-port",
            "6379", "--redis-shard-ports", "6380,6381,6382",
            "--object-manager-port", "12345", "--num-cpus", "2", "--num-gpus",
            "0", "--redis-max-clients", "100", "--resources", "{\"Custom\": 1}"
        ])
        subprocess.Popen(["ray", "stop"]).wait()

    # Test starting Ray with invalid arguments.
    with pytest.raises(Exception):
        run_and_get_output(
            ["ray", "start", "--head", "--redis-address", "127.0.0.1:6379"])
    subprocess.Popen(["ray", "stop"]).wait()


@pytest.fixture
def ray_start_head_local():
    # Start the Ray processes on this machine.
    run_and_get_output([
        "ray", "start", "--head", "--node-ip-address=localhost",
        "--redis-port=6379"
    ])

    yield None

    # Disconnect from the Ray cluster.
    ray.shutdown()
    # Kill the Ray cluster.
    subprocess.Popen(["ray", "stop"]).wait()


def test_using_hostnames(ray_start_head_local):
    ray.init(node_ip_address="localhost", redis_address="localhost:6379")

    @ray.remote
    def f():
        return 1

    assert ray.get(f.remote()) == 1


@pytest.fixture
def ray_start_regular():
    # Start the Ray processes.
    address_info = ray.init(num_cpus=1)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def test_connecting_in_local_case(ray_start_regular):
    address_info = ray_start_regular

    # Define a driver that just connects to Redis.
    driver_script = """
import ray
ray.init(redis_address="{}")
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

ray.init(redis_address="{}")
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
