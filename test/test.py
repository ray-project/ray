from __future__ import print_function

import os
import subprocess
import sys
import unittest
import random
import time

import photon

class TestPhotonClient(unittest.TestCase):

  def setUp(self):
    # Start Redis.
    redis_executable = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../common/thirdparty/redis-3.2.3/src/redis-server")
    self.p1 = subprocess.Popen([redis_executable, "--loglevel", "warning"])
    time.sleep(0.1)
    scheduler_executable = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../build/photon_scheduler")
    scheduler_name = "/tmp/scheduler{}".format(random.randint(0, 10000))
    self.p2 = subprocess.Popen([scheduler_executable, "-s", scheduler_name, "-r", "127.0.0.1:6379"])
    time.sleep(0.1)
    # Connect to the scheduler.
    self.photon_client = photon.PhotonClient(scheduler_name)

  def tearDown(self):
    # Kill the Redis server.
    self.p1.kill()
    # Kill the local scheduler.
    self.p2.kill()

  def test_create(self):
    l = [photon.make_id(20 * "a"), photon.make_id(20 * "b"), photon.make_id(20 * "c")]
    self.photon_client.submit(20 * "a", l)

if __name__ == "__main__":
  unittest.main(verbosity=2)
