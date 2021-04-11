import os
import pytest
import sys
import time

import ray
from ray.test_utils import (RayTestTimeoutException, run_string_as_driver,
                            run_string_as_driver_nonblocking,
                            wait_for_condition, init_error_pubsub,
                            get_error_message)


def test_remote_raylet_cleanup(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node()
    cluster.add_node()
    cluster.add_node()
    cluster.wait_for_nodes()

    def remote_raylets_dead():
        return not cluster.remaining_processes_alive()

    cluster.remove_node(cluster.head_node, allow_graceful=False)
    wait_for_condition(remote_raylets_dead)


def test_error_isolation(call_ray_start):
    address = call_ray_start
    # Connect a driver to the Ray cluster.
    ray.init(address=address)
    p = init_error_pubsub()

    # There shouldn't be any errors yet.
    errors = get_error_message(p, 1, 2)
    assert len(errors) == 0

    error_string1 = "error_string1"
    error_string2 = "error_string2"

    @ray.remote
    def f():
        raise Exception(error_string1)

    # Run a remote function that throws an error.
    with pytest.raises(Exception):
        ray.get(f.remote())

    # Wait for the error to appear in Redis.
    errors = get_error_message(p, 1)

    # Make sure we got the error.
    assert len(errors) == 1
    assert error_string1 in errors[0].error_message

    # Start another driver and make sure that it does not receive this
    # error. Make the other driver throw an error, and make sure it
    # receives that error.
    driver_script = """
import ray
import time
from ray.test_utils import (init_error_pubsub, get_error_message)

ray.init(address="{}")
p = init_error_pubsub()
time.sleep(1)
errors = get_error_message(p, 1, 2)
assert len(errors) == 0

@ray.remote
def f():
    raise Exception("{}")

try:
    ray.get(f.remote())
except Exception as e:
    pass

errors = get_error_message(p, 1)
assert len(errors) == 1

assert "{}" in errors[0].error_message

print("success")
""".format(address, error_string2, error_string2)

    out = run_string_as_driver(driver_script)
    # Make sure the other driver succeeded.
    assert "success" in out

    # Make sure that the other error message doesn't show up for this
    # driver.
    errors = get_error_message(p, 1)
    assert len(errors) == 1
    p.close()


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


@pytest.mark.parametrize(
    "call_ray_start",
    [
        "ray start --head --num-cpus=1 --min-worker-port=0 "
        "--max-worker-port=0 --port 0 --system-config="
        # This test uses ray.objects(), which only works with the GCS-based
        # object directory
        "{\"ownership_based_object_directory_enabled\":false}",
    ],
    indirect=True)
def test_cleanup_on_driver_exit(call_ray_start):
    # This test will create a driver that creates a bunch of objects and then
    # exits. The entries in the object table should be cleaned up.
    address = call_ray_start

    ray.init(address=address)

    # Define a driver that creates a bunch of objects and exits.
    driver_script = """
import time
import ray
import numpy as np
ray.init(address="{}")
object_refs = [ray.put(np.zeros(200 * 1024, dtype=np.uint8))
              for i in range(1000)]
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
counter = Counter.options(name="Counter").remote()
time.sleep(100)
""".format(address)

    # Define a driver that submits to the named actor and exits.
    driver_script2 = """
import ray
import time
ray.init(address="{}")
while True:
    try:
        counter = ray.get_actor("Counter")
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
    "call_ray_start", [
        "ray start --head --num-cpus=1 --num-gpus=1 " +
        "--min-worker-port=0 --max-worker-port=0 --port 0"
    ],
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
            output_line = ray._private.utils.decode(
                process_handle.stdout.readline()).strip()
            print(output_line)
            if output_line == "success":
                return
            time.sleep(1)
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


if __name__ == "__main__":
    import pytest
    # Make subprocess happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    sys.exit(pytest.main(["-v", __file__]))
