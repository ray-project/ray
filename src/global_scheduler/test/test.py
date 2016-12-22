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

# DB_CLIENT_PREFIX is an implementation detail of ray_redis_module.c, so
# this must be kept in sync with that file.
DB_CLIENT_PREFIX = "CL:"

# def random_object_id():
#   return photon.ObjectID(np.random.bytes(ID_SIZE))
# object_ids = [random_object_id() for i in range(256)]

def random_object_id():
  return np.random.bytes(ID_SIZE)

def random_task_id():
  return photon.ObjectID(np.random.bytes(ID_SIZE))

def random_function_id():
  return photon.ObjectID(np.random.bytes(ID_SIZE))

def new_port():
  return random.randint(10000, 65535)

def generate_metadata(length):
  metadata_buffer = bytearray(length)
  if length > 0:
    metadata_buffer[0] = random.randint(0, 255)
    metadata_buffer[-1] = random.randint(0, 255)
    for _ in range(100):
      metadata_buffer[random.randint(0, length - 1)] = random.randint(0, 255)
  return metadata_buffer

def write_to_data_buffer(buff, length):
  if length > 0:
    buff[0] = chr(random.randint(0, 255))
    buff[-1] = chr(random.randint(0, 255))
    for _ in range(100):
      buff[random.randint(0, length - 1)] = chr(random.randint(0, 255))

def create_object_with_id(client, object_id, data_size, metadata_size, seal=True):
  metadata = generate_metadata(metadata_size)
  memory_buffer = client.create(object_id, data_size, metadata)
  write_to_data_buffer(memory_buffer, data_size)
  if seal:
    client.seal(object_id)
  return memory_buffer, metadata

def create_object(client, data_size, metadata_size, seal=True):
  object_id = random_object_id()
  memory_buffer, metadata = create_object_with_id(client, object_id, data_size, metadata_size, seal=seal)
  return object_id, memory_buffer, metadata


class TestGlobalScheduler(unittest.TestCase):

  def setUp(self):
    # Start a Redis server.
    redis_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../../common/thirdparty/redis/src/redis-server")
    redis_module = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../common/redis_module/ray_redis_module.so")
    assert os.path.isfile(redis_path)
    assert os.path.isfile(redis_module)
    node_ip_address = "127.0.0.1"
    redis_port = new_port()
    #redis_port = 6379 #default_port=6379
    redis_address = "{}:{}".format(node_ip_address, redis_port)
    self.redis_process = subprocess.Popen([redis_path, "--port", str(redis_port), "--loglevel", "warning", "--loadmodule", redis_module])
    time.sleep(0.1)
    # Create a Redis client.
    self.redis_client = redis.StrictRedis(host=node_ip_address, port=redis_port)
    #time.sleep(0.5)
    # Start the global scheduler.
    self.p1 = global_scheduler.start_global_scheduler(redis_address, use_valgrind=USE_VALGRIND)
    #time.sleep(0.5)
    # Start the Plasma store.
    plasma_store_name, self.p2 = plasma.start_plasma_store()
    # Start the Plasma manager.
    plasma_manager_name, self.p3, plasma_manager_port = plasma.start_plasma_manager(plasma_store_name, redis_address)
    self.plasma_address = "{}:{}".format(node_ip_address, plasma_manager_port)
    self.plasmaclient = plasma.PlasmaClient(plasma_store_name, plasma_manager_name)
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
    #self.assertEqual(self.redis_process.poll(), None)

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

  def get_plasmamgr_id(self):
    ''' iterates over all the client table keys, gets the db_client_id for the client
        with client_type matching plasma_manager. Strips the client table prefix.
        Returns None if plasma_manager client not found.
        Note that it returns db_client id for the first encountered plasma manager.
        TODO(atumanov): write a separate function to get all plasma manager client IDs
    '''
    db_client_id = None

    cli_lst = self.redis_client.keys("{}*".format(DB_CLIENT_PREFIX))
    for client_id in cli_lst:
        rediscmdstr = b'HGET {} client_type'.format(client_id)
        response = self.redis_client.hget(client_id, b'client_type')
        if response == "plasma_manager":
            db_client_id = client_id
            break

    return db_client_id

  def test_redisonly_singletask(self):
    ''' tests global scheduler functionality by interaction with Redis
        and checking task state transitions in Redis only
        TODO(atumanov): implement
    '''
    # Check precondition for this test:
    # There should be three db clients, the global scheduler, the local
    # scheduler, and the plasma manager.
    self.assertEqual(len(self.redis_client.keys("{}*".format(DB_CLIENT_PREFIX))), 3)
    db_client_id = self.get_plasmamgr_id()
    assert(db_client_id != None)
    assert(db_client_id.startswith("CL:"))
    db_client_id = db_client_id[len('CL:'):] #remove the CL: prefix

    #self.redis_client.execute_command("RAY.OBJECT_TABLE_ADD", object_dep, data_size, "hash1", db_client_id)

  def test_integration_singletask(self):

    # There should be three db clients, the global scheduler, the local
    # scheduler, and the plasma manager.
    self.assertEqual(len(self.redis_client.keys("{}*".format(DB_CLIENT_PREFIX))), 3)

    num_return_vals = [0,1,2,3,5,10]
    # There should not be anything else in Redis yet.
    self.assertEqual(len(self.redis_client.keys("*")), 3)
    #insert the object into Redis
    data_size = 0xf1f0
    metadata_size = 0x40
    object_dep, memory_buffer, metadata = create_object(self.plasmaclient, data_size, metadata_size, seal=True)

    #sleep before submitting task to photon
    time.sleep(0.1)
    # Submit a task to Redis.
    task = photon.Task(random_function_id(), [photon.ObjectID(object_dep)], num_return_vals[0], random_task_id(), 0)
    self.photon_client.submit(task)
    time.sleep(0.1)
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
        else:
            print(task_status)
      print("The task has not been scheduled yet, trying again.")
      num_retries -= 1
      time.sleep(1)

    if num_retries <= 0 and task_status != TASK_STATUS_SCHEDULED:
        #failed to submit and schedule a single task -- bail
        self.tearDown()
        sys.exit(1)


  def integration_manytasks_helper(self, timesync = True):
    # There should be three db clients, the global scheduler, the local
    # scheduler, and the plasma manager.
    self.assertEqual(len(self.redis_client.keys("{}*".format(DB_CLIENT_PREFIX))), 3)
    num_return_vals = [0,1,2,3,5,10]

    # Submit a bunch of tasks to Redis.
    num_tasks = 1000
    for _ in range(num_tasks):
      #create a new object for each task
      data_size = np.random.randint(1<<20) #upto 1MB
      metadata_size = np.random.randint(1<<10) #upto 1KB
      object_dep, memory_buffer, metadata = create_object(self.plasmaclient, data_size, metadata_size, seal=True)
      if timesync:
        #give 10ms for object info handler to fire
        time.sleep(0.010) #10ms (yields cpu)
      task = photon.Task(random_function_id(), [photon.ObjectID(object_dep)], num_return_vals[0], random_task_id(), 0)
      self.photon_client.submit(task)
    # Check that there are the correct number of tasks in Redis and that they
    # all get assigned to the local scheduler.
    num_retries = 10
    num_tasks_done = 0
    while num_retries > 0:
      task_entries = self.redis_client.keys("task*")
      self.assertLessEqual(len(task_entries), num_tasks)
      #first, check if all tasks made it to Redis
      if len(task_entries) == num_tasks:
        task_contents = [self.redis_client.hgetall(task_entries[i]) for i in range(len(task_entries))]
        task_statuses = [int(contents[b"state"]) for contents in task_contents]
        self.assertTrue(all([status in [TASK_STATUS_WAITING, TASK_STATUS_SCHEDULED] for status in task_statuses]))
        num_tasks_done = task_statuses.count(TASK_STATUS_SCHEDULED)
        num_tasks_waiting = task_statuses.count(TASK_STATUS_WAITING)
        print("tasks in Redis = {}, tasks waiting = {}, tasks scheduled = {}, retries left = {}"
              .format(len(task_entries), num_tasks_waiting, num_tasks_done, num_retries))
        if all([status == TASK_STATUS_SCHEDULED for status in task_statuses]):
          #we're done : pass
          break
      num_retries -= 1
      time.sleep(.1)

    if num_tasks_done != num_tasks:
        # at least one of the tasks failed to schedule
        self.tearDown()
        sys.exit(2)

  def test_integration_maytasks_handlersync(self):
    self.integration_manytasks_helper(timesync = True)

#  def test_integration_maytasks(self):
#    #more realistic case: should handle out of order object and task notifications
#    self.integration_manytasks_helper(timesync = False))

if __name__ == "__main__":
  if len(sys.argv) > 1:
    # pop the argument so we don't mess with unittest's own argument parser
    arg = sys.argv.pop()
    if arg == "valgrind":
      USE_VALGRIND = True
      print("Using valgrind for tests")
  unittest.main(verbosity=2)
