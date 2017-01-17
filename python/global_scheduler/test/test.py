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
from plasma.utils import random_object_id, generate_metadata, write_to_data_buffer, create_object_with_id, create_object

USE_VALGRIND = False
PLASMA_STORE_MEMORY = 1000000000
ID_SIZE = 20

# These constants must match the scheduling state enum in task.h.
TASK_STATUS_WAITING = 1
TASK_STATUS_SCHEDULED = 2
TASK_STATUS_RUNNING = 4
TASK_STATUS_DONE = 8

# These constants are an implementation detail of ray_redis_module.c, so this
# must be kept in sync with that file.
DB_CLIENT_PREFIX = "CL:"
TASK_PREFIX = "TT:"

def random_task_id():
  return photon.ObjectID(np.random.bytes(ID_SIZE))

def random_function_id():
  return photon.ObjectID(np.random.bytes(ID_SIZE))

def new_port():
  return random.randint(10000, 65535)

class TestGlobalScheduler(unittest.TestCase):

  def setUp(self):
    # Start a Redis server.
    redis_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../../core/src/common/thirdparty/redis/src/redis-server")
    redis_module = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../core/src/common/redis_module/libray_redis_module.so") # XXX
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
    self.plasma_address = "{}:{}".format(node_ip_address, plasma_manager_port)
    self.plasma_client = plasma.PlasmaClient(plasma_store_name, plasma_manager_name)
    # Start the local scheduler.
    local_scheduler_name, self.p4 = photon.start_local_scheduler(
        plasma_store_name,
        plasma_manager_name=plasma_manager_name,
        plasma_address=self.plasma_address,
        redis_address=redis_address)
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
      if self.p1.returncode != 0:
        os._exit(-1)
    else:
      self.p1.kill()
    self.p2.kill()
    self.p3.kill()
    self.p4.kill()
    # Kill Redis. In the event that we are using valgrind, this needs to happen
    # after we kill the global scheduler.
    self.redis_process.kill()

  def get_plasma_manager_id(self):
    """Get the db_client_id with client_type equal to plasma_manager.

    Iterates over all the client table keys, gets the db_client_id for the
    client with client_type matching plasma_manager. Strips the client table
    prefix. TODO(atumanov): write a separate function to get all plasma manager
    client IDs.

    Returns:
      The db_client_id if one is found and otherwise None.
    """
    db_client_id = None

    client_list = self.redis_client.keys("{}*".format(DB_CLIENT_PREFIX))
    for client_id in client_list:
      response = self.redis_client.hget(client_id, b"client_type")
      if response == b"plasma_manager":
        db_client_id = client_id
        break

    return db_client_id

  def test_redis_only_single_task(self):
    """
    Tests global scheduler functionality by interacting with Redis and checking
    task state transitions in Redis only. TODO(atumanov): implement.
    """
    # Check precondition for this test:
    # There should be three db clients, the global scheduler, the local
    # scheduler, and the plasma manager.
    self.assertEqual(len(self.redis_client.keys("{}*".format(DB_CLIENT_PREFIX))), 3)
    db_client_id = self.get_plasma_manager_id()
    assert(db_client_id != None)
    assert(db_client_id.startswith(b"CL:"))
    db_client_id = db_client_id[len(b"CL:"):] # Remove the CL: prefix.

  def test_integration_single_task(self):
    # There should be three db clients, the global scheduler, the local
    # scheduler, and the plasma manager.
    self.assertEqual(len(self.redis_client.keys("{}*".format(DB_CLIENT_PREFIX))), 3)

    num_return_vals = [0, 1, 2, 3, 5, 10]
    # There should not be anything else in Redis yet.
    self.assertEqual(len(self.redis_client.keys("*")), 3)
    # Insert the object into Redis.
    data_size = 0xf1f0
    metadata_size = 0x40
    object_dep, memory_buffer, metadata = create_object(self.plasma_client, data_size, metadata_size, seal=True)

    # Sleep before submitting task to photon.
    time.sleep(0.1)
    # Submit a task to Redis.
    task = photon.Task(random_function_id(), [photon.ObjectID(object_dep)], num_return_vals[0], random_task_id(), 0)
    self.photon_client.submit(task)
    time.sleep(0.1)
    # There should now be a task in Redis, and it should get assigned to the
    # local scheduler
    num_retries = 10
    while num_retries > 0:
      task_entries = self.redis_client.keys("{}*".format(TASK_PREFIX))
      self.assertLessEqual(len(task_entries), 1)
      if len(task_entries) == 1:
        task_contents = self.redis_client.hgetall(task_entries[0])
        task_status = int(task_contents[b"state"])
        self.assertTrue(task_status in [TASK_STATUS_WAITING, TASK_STATUS_SCHEDULED])
        if task_status == TASK_STATUS_SCHEDULED:
          break
        else:
          print(task_status)
      print("The task has not been scheduled yet, trying again.")
      num_retries -= 1
      time.sleep(1)

    if num_retries <= 0 and task_status != TASK_STATUS_SCHEDULED:
      # Failed to submit and schedule a single task -- bail.
      self.tearDown()
      sys.exit(1)

  def integration_many_tasks_helper(self, timesync=True):
    # There should be three db clients, the global scheduler, the local
    # scheduler, and the plasma manager.
    self.assertEqual(len(self.redis_client.keys("{}*".format(DB_CLIENT_PREFIX))), 3)
    num_return_vals = [0, 1, 2, 3, 5, 10]

    # Submit a bunch of tasks to Redis.
    num_tasks = 1000
    for _ in range(num_tasks):
      # Create a new object for each task.
      data_size = np.random.randint(1 << 20)
      metadata_size = np.random.randint(1 << 10)
      object_dep, memory_buffer, metadata = create_object(self.plasma_client, data_size, metadata_size, seal=True)
      if timesync:
        # Give 10ms for object info handler to fire (long enough to yield CPU).
        time.sleep(0.010)
      task = photon.Task(random_function_id(), [photon.ObjectID(object_dep)], num_return_vals[0], random_task_id(), 0)
      self.photon_client.submit(task)
    # Check that there are the correct number of tasks in Redis and that they
    # all get assigned to the local scheduler.
    num_retries = 10
    num_tasks_done = 0
    while num_retries > 0:
      task_entries = self.redis_client.keys("{}*".format(TASK_PREFIX))
      self.assertLessEqual(len(task_entries), num_tasks)
      # First, check if all tasks made it to Redis.
      if len(task_entries) == num_tasks:
        task_contents = [self.redis_client.hgetall(task_entries[i]) for i in range(len(task_entries))]
        task_statuses = [int(contents[b"state"]) for contents in task_contents]
        self.assertTrue(all([status in [TASK_STATUS_WAITING, TASK_STATUS_SCHEDULED] for status in task_statuses]))
        num_tasks_done = task_statuses.count(TASK_STATUS_SCHEDULED)
        num_tasks_waiting = task_statuses.count(TASK_STATUS_WAITING)
        print("tasks in Redis = {}, tasks waiting = {}, tasks scheduled = {}, retries left = {}"
              .format(len(task_entries), num_tasks_waiting, num_tasks_done, num_retries))
        if all([status == TASK_STATUS_SCHEDULED for status in task_statuses]):
          # We're done, so pass.
          break
      num_retries -= 1
      time.sleep(0.1)

    if num_tasks_done != num_tasks:
      # At least one of the tasks failed to schedule.
      self.tearDown()
      sys.exit(2)

  def test_integration_many_tasks_handler_sync(self):
    self.integration_many_tasks_helper(timesync=True)

  def test_integration_many_tasks(self):
    # More realistic case: should handle out of order object and task
    # notifications.
    self.integration_many_tasks_helper(timesync=False)

if __name__ == "__main__":
  if len(sys.argv) > 1:
    # Pop the argument so we don't mess with unittest's own argument parser.
    arg = sys.argv.pop()
    if arg == "valgrind":
      USE_VALGRIND = True
      print("Using valgrind for tests")
  unittest.main(verbosity=2)
