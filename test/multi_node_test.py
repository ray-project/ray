from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import os
import unittest
import ray
import subprocess
import sys
import tempfile
import time

start_ray_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../scripts/start_ray.sh")
stop_ray_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../scripts/stop_ray.sh")

class MultiNodeTest(unittest.TestCase):

  def testErrorIsolation(self):
    # Start the Ray processes on this machine.
    out = subprocess.check_output([start_ray_script, "--head"]).decode("ascii")
    # Get the redis address from the output.
    redis_substring_prefix = "redis_address=\""
    redis_address_location = out.find(redis_substring_prefix) + len(redis_substring_prefix)
    redis_address = out[redis_address_location:]
    redis_address = redis_address.split("\"")[0]
    # Connect a driver to the Ray cluster.
    ray.init(redis_address=redis_address, driver_mode=ray.SILENT_MODE)

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
    self.assertIn(error_string1, ray.error_info()[0][b"message"].decode("ascii"))

    # Start another driver and make sure that it does not receive this error.
    # Make the other driver throw an error, and make sure it receives that
    # error.
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

assert "{}" in ray.error_info()[0][b"message"].decode("ascii")

print("success")
""".format(redis_address, error_string2, error_string2)

    # Save the driver script as a file so we can call it using subprocess.
    with tempfile.NamedTemporaryFile() as f:
      f.write(driver_script.encode("ascii"))
      f.flush()
      out = subprocess.check_output(["python", f.name]).decode("ascii")

    # Make sure the other driver succeeded.
    self.assertIn("success", out)

    # Make sure that the other error message doesn't show up for this driver.
    self.assertEqual(len(ray.error_info()), 1)
    self.assertIn(error_string1, ray.error_info()[0][b"message"].decode("ascii"))

    ray.worker.cleanup()
    subprocess.Popen([stop_ray_script]).wait()

if __name__ == "__main__":
  unittest.main(verbosity=2)
