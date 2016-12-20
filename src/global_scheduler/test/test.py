from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import os
import random
import redis
import signal
import subprocess
import sys
import threading
import time
import unittest

import global_scheduler
import photon
import plasma

USE_VALGRIND = False
PLASMA_STORE_MEMORY = 1000000000
ID_SIZE = 20

# These constants must match the schedulign state enum in task.h.
TASK_STATUS_WAITING = 1
TASK_STATUS_SCHEDULED = 2
TASK_STATUS_RUNNING = 4
TASK_STATUS_DONE = 8

def random_object_id():
  return photon.ObjectID(np.random.bytes(ID_SIZE))
object_ids = [random_object_id() for i in range(256)]

def random_task_id():
  return photon.ObjectID(np.random.bytes(ID_SIZE))

def random_function_id():
  return photon.ObjectID(np.random.bytes(ID_SIZE))

def new_port():
  return random.randint(10000, 65535)

class TestGlobalScheduler(unittest.TestCase):

  def setUp(self):
    # Start a Redis server.
    redis_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../../common/thirdparty/redis/src/redis-server")
    redis_module = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../common/redis_module/ray_redis_module.so")
    assert os.path.isfile(redis_path)
    assert os.path.isfile(redis_module)
    node_ip_address = "127.0.0.1"
    redis_port = new_port()
    redis_address = "{}:{}".format(node_ip_address, redis_port)
    self.redis_process = subprocess.Popen([redis_path, "--port", str(redis_port), "--loglevel", "warning", "--loadmodule", redis_module])
    time.sleep(0.1)
    # Create a Redis client.
    self.redis_client = redis.StrictRedis(host=node_ip_address, port=redis_port)
    # Start the global scheduler.
    self.p1 = global_scheduler.start_global_scheduler(redis_address, use_valgrind=USE_VALGRIND)
    # Start the Plasma store.
    plasma_store_name, self.p2 = plasma.start_plasma_store()
    # Start the Plasma manager.
    plasma_manager_name, self.p3, plasma_manager_port = plasma.start_plasma_manager(plasma_store_name, redis_address)
    plasma_address = "{}:{}".format(node_ip_address, plasma_manager_port)
    print("plasma_address={}".format(plasma_address))
    # Start the local scheduler.
    local_scheduler_name, self.p4 = photon.start_local_scheduler(plasma_store_name, plasma_manager_name=plasma_manager_name, plasma_address=plasma_address, redis_address=redis_address)
    # Connect to the scheduler.
    self.photon_client = photon.PhotonClient(local_scheduler_name)

  def tearDown(self):
    # Check that the processes are still alive.
    self.assertEqual(self.p1.poll(), None)
    self.assertEqual(self.p2.poll(), None)
    self.assertEqual(self.p3.poll(), None)
    self.assertEqual(self.p4.poll(), None)
    self.assertEqual(self.redis_process.poll(), None)

    # Kill the global scheduler.
    if USE_VALGRIND:
      self.p1.send_signal(signal.SIGTERM)
      self.p1.wait()
      os._exit(self.p1.returncode)
    else:
      self.p1.kill()
    self.p2.kill()
    self.p3.kill()
    self.p4.kill()
    # Kill Redis. In the event that we are using valgrind, this needs to happen
    # after we kill the global scheduler.
    self.redis_process.kill()

  def test_redis_contents(self):
    # DB_CLIENT_PREFIX is an implementation detail of ray_redis_module.c, so
    # this must be kept in sync with that file.
    DB_CLIENT_PREFIX = "CL:"
    # There should be three db clients, the global scheduler, the local
    # scheduler, and the plasma manager.
    self.assertEqual(len(self.redis_client.keys("{}*".format(DB_CLIENT_PREFIX))), 3)
    args_list = [
      [],
#       #{},
#       #(),
#       1 * [1],
#       10 * [1],
#       100 * [1],
#       1000 * [1],
#       1 * ["a"],
#       10 * ["a"],
#       100 * ["a"],
#       1000 * ["a"],
#       [1, 1.3, 1 << 100, "hi", u"hi", [1, 2]],
      object_ids[:1],
#       object_ids[:2],
#       object_ids[:3],
#       object_ids[:4],
#       object_ids[:5],
#       object_ids[:10],
#       object_ids[:100],
#       object_ids[:256],
#       [1, object_ids[0]],
#       [object_ids[0], "a"],
#       [1, object_ids[0], "a"],
#       [object_ids[0], 1, object_ids[1], "a"],
#       object_ids[:3] + [1, "hi", 2.3] + object_ids[:5],
#       object_ids + 100 * ["a"] + object_ids
    ]
    num_return_vals = [0,1,2,3,5,10]
    # There should not be anything else in Redis yet.
    self.assertEqual(len(self.redis_client.keys("*")), 3)

    # Submit a task to Redis.
    task = photon.Task(random_function_id(), args_list[1], num_return_vals[0], random_task_id(), 0)
    self.photon_client.submit(task)
    # There should now be a task in Redis, and it should get assigned to the
    # local scheduler
    num_retries = 10
    while num_retries > 0:
      task_entries = self.redis_client.keys("task*")
      self.assertLessEqual(len(task_entries), 1)
      if len(task_entries) == 1:
        task_contents = self.redis_client.hgetall(task_entries[0])
        task_status = int(task_contents[b"state"])
        self.assertTrue(task_status in [TASK_STATUS_WAITING, TASK_STATUS_SCHEDULED])
        if task_status == TASK_STATUS_SCHEDULED:
          break
      print("The task has not been scheduled yet, trying again.")
      num_retries -= 1
      time.sleep(0.2)

    if num_retries <= 0 and task_status != TASK_STATUS_SCHEDULED:
        #failed to submit and schedule a single task -- bail
        sys.exit(1)

    # Submit a bunch of tasks to Redis.
    num_tasks = 1000
    for _ in range(num_tasks):
      task = photon.Task(random_function_id(), args_list[1], num_return_vals[0], random_task_id(), 0)
      self.photon_client.submit(task)
    # Check that there are the correct number of tasks in Redis and that they
    # all get assigned to the local scheduler.
    while True:
      task_entries = self.redis_client.keys("task*")
      self.assertLessEqual(len(task_entries), num_tasks + 1)
      if len(task_entries) == num_tasks + 1:
        task_contents = [self.redis_client.hgetall(task_entries[i]) for i in range(len(task_entries))]
        task_statuses = [int(contents[b"state"]) for contents in task_contents]
        self.assertTrue(all([status in [TASK_STATUS_WAITING, TASK_STATUS_SCHEDULED] for status in task_statuses]))
        if all([status == TASK_STATUS_SCHEDULED for status in task_statuses]):
          break
      print("The tasks have not been scheduled yet, trying again.")

if __name__ == "__main__":
  if len(sys.argv) > 1:
    # pop the argument so we don't mess with unittest's own argument parser
    arg = sys.argv.pop()
    if arg == "valgrind":
      USE_VALGRIND = True
      print("Using valgrind for tests")
  unittest.main(verbosity=2)
