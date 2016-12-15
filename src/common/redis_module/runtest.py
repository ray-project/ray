from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import random
import subprocess
import time
import unittest
import redis

# Check if the redis-server binary is present.
redis_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../thirdparty/redis/src/redis-server")
if not os.path.exists(redis_path):
  raise Exception("You do not have the redis-server binary. Run `make test` in the plasma directory to get it.")

# Absolute path of the ray redis module.
module_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "ray_redis_module.so")
print("path to the redis module is {}".format(module_path))

class TestGlobalStateStore(unittest.TestCase):

  def setUp(self):
    redis_port = random.randint(2000, 50000)
    self.redis_process = subprocess.Popen([redis_path,
                                           "--port", str(redis_port),
                                           "--loadmodule", module_path])
    time.sleep(0.5)
    self.redis = redis.StrictRedis(host="localhost", port=redis_port, db=0)

  def tearDown(self):
    self.redis_process.kill()

  def testInvalidObjectTableAdd(self):
    # Check that Redis returns an error when RAY.OBJECT_TABLE_ADD is called with
    # the wrong arguments.
    with self.assertRaises(redis.ResponseError):
      self.redis.execute_command("RAY.OBJECT_TABLE_ADD")
    with self.assertRaises(redis.ResponseError):
      self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "hello")
    with self.assertRaises(redis.ResponseError):
      self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id2", "one", "hash2", "manager_id1")
    with self.assertRaises(redis.ResponseError):
      self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id2", 1, "hash2", "manager_id1", "extra argument")
    # Check that Redis returns an error when RAY.OBJECT_TABLE_ADD adds an object
    # ID that is already present with a different hash.
    self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id1", 1, "hash1", "manager_id1")
    with self.assertRaises(redis.ResponseError):
      self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id1", 1, "hash2", "manager_id1")
    # Check that it is fine if we add the same object ID multiple times with the
    # same hash.
    self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id1", 1, "hash1", "manager_id1")
    self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id1", 1, "hash1", "manager_id1")
    self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id1", 1, "hash1", "manager_id2")
    self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id1", 2, "hash1", "manager_id2")

  def testObjectTableAddAndLookup(self):
    # Try calling RAY.OBJECT_TABLE_LOOKUP with an object ID that has not been
    # added yet.
    response = self.redis.execute_command("RAY.OBJECT_TABLE_LOOKUP", "object_id1")
    self.assertEqual(set(response), set([]))
    # Add some managers and try again.
    self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id1", 1, "hash1", "manager_id1")
    self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id1", 1, "hash1", "manager_id2")
    response = self.redis.execute_command("RAY.OBJECT_TABLE_LOOKUP", "object_id1")
    self.assertEqual(set(response), {"manager_id1", "manager_id2"})
    # Add a manager that already exists again and try again.
    self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id1", 1, "hash1", "manager_id2")
    response = self.redis.execute_command("RAY.OBJECT_TABLE_LOOKUP", "object_id1")
    self.assertEqual(set(response), {"manager_id1", "manager_id2"})

  def testObjectTableSubscribe(self):
    p = self.redis.pubsub()
    # Subscribe to an object ID.
    p.subscribe("object_id1")
    self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id1", 1, "hash1", "manager_id1")
    # Receive the acknowledgement message.
    self.assertEqual(p.get_message()["data"], 1)
    # Receive the actual data.
    self.assertEqual(p.get_message()["data"], "MANAGERS manager_id1")

  def testResultTableAddAndLookup(self):
    response = self.redis.execute_command("RAY.RESULT_TABLE_LOOKUP", "object_id1")
    self.assertEqual(set(response), set([]))
    self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id1", 1, "hash1", "manager_id1")
    response = self.redis.execute_command("RAY.RESULT_TABLE_LOOKUP", "object_id1")
    self.assertEqual(set(response), set([]))
    self.redis.execute_command("RAY.RESULT_TABLE_ADD", "object_id1", "task_id1")
    response = self.redis.execute_command("RAY.RESULT_TABLE_LOOKUP", "object_id1")
    self.assertEqual(response, "task_id1")
    self.redis.execute_command("RAY.RESULT_TABLE_ADD", "object_id2", "task_id2")
    response = self.redis.execute_command("RAY.RESULT_TABLE_LOOKUP", "object_id2")
    self.assertEqual(response, "task_id2")

  def testInvalidTaskTableAdd(self):
    # Check that Redis returns an error when RAY.TASK_TABLE_ADD is called with
    # the wrong arguments.
    with self.assertRaises(redis.ResponseError):
      self.redis.execute_command("RAY.TASK_TABLE_ADD")
    with self.assertRaises(redis.ResponseError):
      self.redis.execute_command("RAY.TASK_TABLE_ADD", "hello")
    with self.assertRaises(redis.ResponseError):
      self.redis.execute_command("RAY.TASK_TABLE_ADD", "task_id", 3, "node_id")
    with self.assertRaises(redis.ResponseError):
      # Non-integer scheduling states should not be added.
      self.redis.execute_command("RAY.TASK_TABLE_ADD", "task_id",
                                 "invalid_state", "node_id", "task_spec")
    with self.assertRaises(redis.ResponseError):
      # Scheduling states with invalid width should not be added.
      self.redis.execute_command("RAY.TASK_TABLE_ADD", "task_id", 10,
                                 "node_id", "task_spec")

  def testTaskTableAddAndLookup(self):
    # Check that task table adds, updates, and lookups work correctly.
    task_args = [1, "node_id", "task_spec"]
    response = self.redis.execute_command("RAY.TASK_TABLE_ADD", "task_id",
                                          *task_args)
    response = self.redis.execute_command("RAY.TASK_TABLE_GET", "task_id")
    self.assertEqual(response, task_args)

    task_args[0] = 2
    self.redis.execute_command("RAY.TASK_TABLE_UPDATE", "task_id", *task_args[:2])
    response = self.redis.execute_command("RAY.TASK_TABLE_GET", "task_id")
    self.assertEqual(response, task_args)

if __name__ == "__main__":
  unittest.main(verbosity=2)
