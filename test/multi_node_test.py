from __future__ import absolute_import, division, print_function

import os
import subprocess
import sys
import tempfile
import time
import unittest

import ray
from ray.test.test_utils import run_and_get_output


def run_string_as_driver(driver_script):
    """Run a driver as a separate process.

    Args:
        driver_script: A string to run as a Python script.

    Returns:
        The scripts output.
    """
    # Save the driver script as a file so we can call it using subprocess.
    with tempfile.NamedTemporaryFile() as f:
        f.write(driver_script.encode("ascii"))
        f.flush()
        out = ray.utils.decode(
            subprocess.check_output([sys.executable, f.name]))
    return out


class MultiNodeTest(unittest.TestCase):
    def setUp(self):
        out = run_and_get_output(["ray", "start", "--head"])
        # Get the redis address from the output.
        redis_substring_prefix = "redis_address=\""
        redis_address_location = (
            out.find(redis_substring_prefix) + len(redis_substring_prefix))
        redis_address = out[redis_address_location:]
        self.redis_address = redis_address.split("\"")[0]

    def tearDown(self):
        ray.shutdown()
        # Kill the Ray cluster.
        subprocess.Popen(["ray", "stop"]).wait()

    def testErrorIsolation(self):
        # Connect a driver to the Ray cluster.
        ray.init(redis_address=self.redis_address, driver_mode=ray.SILENT_MODE)

        # There shouldn't be any errors yet.
        self.assertEqual(len(ray.error_info()), 0)

        error_string1 = "error_string1"
        error_string2 = "error_string2"

        @ray.remote
        def f():
            raise Exception(error_string1)

        # Run a remote function that throws an error.
        with self.assertRaises(Exception):
            ray.get(f.remote())

        # Wait for the error to appear in Redis.
        while len(ray.error_info()) != 1:
            time.sleep(0.1)
            print("Waiting for error to appear.")

        # Make sure we got the error.
        self.assertEqual(len(ray.error_info()), 1)
        self.assertIn(error_string1, ray.error_info()[0]["message"])

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
""".format(self.redis_address, error_string2, error_string2)

        out = run_string_as_driver(driver_script)
        # Make sure the other driver succeeded.
        self.assertIn("success", out)

        # Make sure that the other error message doesn't show up for this
        # driver.
        self.assertEqual(len(ray.error_info()), 1)
        self.assertIn(error_string1, ray.error_info()[0]["message"])

    def testRemoteFunctionIsolation(self):
        # This test will run multiple remote functions with the same names in
        # two different drivers. Connect a driver to the Ray cluster.
        ray.init(redis_address=self.redis_address, driver_mode=ray.SILENT_MODE)

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
""".format(self.redis_address)

        out = run_string_as_driver(driver_script)

        @ray.remote
        def f():
            return 1

        @ray.remote
        def g(x):
            return 2

        for _ in range(10000):
            result = ray.get([f.remote(), g.remote(0)])
            self.assertEqual(result, [1, 2])

        # Make sure the other driver succeeded.
        self.assertIn("success", out)

    @unittest.skipIf(
        os.environ.get("RAY_USE_XRAY") == "1",
        "This test does not work with xray yet.")
    def testDriverExitingQuickly(self):
        # This test will create some drivers that submit some tasks and then
        # exit without waiting for the tasks to complete.
        ray.init(redis_address=self.redis_address, driver_mode=ray.SILENT_MODE)

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
""".format(self.redis_address)

        # Define a driver that creates some tasks and exits.
        driver_script2 = """
import ray
ray.init(redis_address="{}")
@ray.remote
def f():
    return 1
f.remote()
print("success")
""".format(self.redis_address)

        # Create some drivers and let them exit and make sure everything is
        # still alive.
        for _ in range(3):
            out = run_string_as_driver(driver_script1)
            # Make sure the first driver ran to completion.
            self.assertIn("success", out)
            out = run_string_as_driver(driver_script2)
            # Make sure the first driver ran to completion.
            self.assertIn("success", out)
            self.assertTrue(ray.services.all_processes_alive())


class StartRayScriptTest(unittest.TestCase):
    def testCallingStartRayHead(self):
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
                "ray", "start", "--head", "--redis-shard-ports",
                "6380,6381,6382"
            ])
            subprocess.Popen(["ray", "stop"]).wait()

            # Test starting Ray with all arguments specified.
            run_and_get_output([
                "ray", "start", "--head", "--num-workers", "2", "--redis-port",
                "6379", "--redis-shard-ports", "6380,6381,6382",
                "--object-manager-port", "12345", "--num-cpus", "2",
                "--num-gpus", "0", "--redis-max-clients", "100", "--resources",
                "{\"Custom\": 1}"
            ])
            subprocess.Popen(["ray", "stop"]).wait()

        # Test starting Ray with invalid arguments.
        with self.assertRaises(Exception):
            run_and_get_output([
                "ray", "start", "--head", "--redis-address", "127.0.0.1:6379"
            ])
        subprocess.Popen(["ray", "stop"]).wait()

    def testUsingHostnames(self):
        # Start the Ray processes on this machine.
        run_and_get_output([
            "ray", "start", "--head", "--node-ip-address=localhost",
            "--redis-port=6379"
        ])

        ray.init(node_ip_address="localhost", redis_address="localhost:6379")

        @ray.remote
        def f():
            return 1

        self.assertEqual(ray.get(f.remote()), 1)

        # Kill the Ray cluster.
        subprocess.Popen(["ray", "stop"]).wait()


class MiscellaneousTest(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()

    def testConnectingInLocalCase(self):
        address_info = ray.init(num_cpus=0)

        # Define a driver that just connects to Redis.
        driver_script = """
import ray
ray.init(redis_address="{}")
print("success")
""".format(address_info["redis_address"])

        out = run_string_as_driver(driver_script)
        # Make sure the other driver succeeded.
        self.assertIn("success", out)


class RunDriverForMultipleTimesTest(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()

    def testRunDriverForTwice(self):
        # We used to have issue 2165 and 2288: driver will hang when we run it
        # for the second time.
        # This test is used to verify the fix for above issue, it will run the
        # same driver for twice and verify whether both of them succeed.
        address_info = ray.init()
        driver_script = """
import ray
import ray.tune as tune
import os
import time

def train_func(config, reporter):  # add a reporter arg
    for i in range(2):
        time.sleep(0.1)
        reporter(timesteps_total=i, mean_accuracy=i+97)  # report metrics

ray.init(redis_address="{}", driver_mode=ray.SILENT_MODE)
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
            self.assertIn("success", out)


if __name__ == "__main__":
    unittest.main(verbosity=2)
