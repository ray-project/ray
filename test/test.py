from __future__ import print_function

import os
import signal
import subprocess
import sys
import unittest
import random
import time

import photon

USE_VALGRIND = False

class TestPhotonClient(unittest.TestCase):

  def setUp(self):
    # Start Redis.
    redis_executable = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../common/thirdparty/redis-3.2.3/src/redis-server")
    self.p1 = subprocess.Popen([redis_executable, "--loglevel", "warning"])
    time.sleep(0.1)
    scheduler_executable = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../build/photon_scheduler")
    scheduler_name = "/tmp/scheduler{}".format(random.randint(0, 10000))
    command = [scheduler_executable, "-s", scheduler_name, "-r", "127.0.0.1:6379"]
    if USE_VALGRIND:
      self.p2 = subprocess.Popen(["valgrind", "--track-origins=yes", "--leak-check=full", "--show-leak-kinds=all"] + command)
    else:
      self.p2 = subprocess.Popen(command)
    if USE_VALGRIND:
      time.sleep(1.0)
    else:
      time.sleep(0.1)
    # Connect to the scheduler.
    self.photon_client = photon.PhotonClient(scheduler_name)

  def tearDown(self):
    # Kill the Redis server.
    self.p1.kill()
    # Kill the local scheduler.
    if USE_VALGRIND:
      self.p2.send_signal(signal.SIGTERM)
      self.p2.wait()
      os._exit(self.p2.returncode)
    else:
      self.p2.kill()
    

  def test_create(self):
    l = [20 * "a", 20 * "b", 20 * "c"]
    r = [20 * "e", 20 * "f"]
    # Submit a task.
    self.photon_client.submit(20 * "d", l, r)
    # Get the task.
    task = self.photon_client.get_task()

if __name__ == "__main__":
  if len(sys.argv) > 1:
    # pop the argument so we don't mess with unittest's own argument parser
    arg = sys.argv.pop()
    if arg == "valgrind":
      USE_VALGRIND = True
      print("Using valgrind for tests")
  unittest.main(verbosity=2)
