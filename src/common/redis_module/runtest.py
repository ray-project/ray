from __future__ import print_function

import os
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
    redis_port = 6379
    self.redis_process = subprocess.Popen([redis_path,
                                           "--port", str(redis_port),
                                           "--loadmodule", module_path])
    time.sleep(0.5)
    self.redis = redis.StrictRedis(host="localhost", port=6379, db=0)

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
      self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id2", "a1", "hash2", "manager_id1")
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

  def testResultTableAddAndLookup(self):
    response = self.redis.execute_command("RAY.RESULT_TABLE_LOOKUP", "object_id1")
    self.assertEqual(set(response), set([]))
    self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id1", 1, "hash1", "manager_id1")
    response = self.redis.execute_command("RAY.RESULT_TABLE_LOOKUP", "object_id1")
    self.assertEqual(set(response), set([]))

  def testObjectTableSubscribe(self):
    p = self.redis.pubsub()
    # Subscribe to an object ID.
    p.subscribe("object_id1")
    self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id1", 1, "hash1", "manager_id1")
    # Receive the acknowledgement message.
    self.assertEqual(p.get_message()["data"], 1)
    # Receive the actual data.
    self.assertEqual(p.get_message()["data"], "MANAGERS manager_id1")

if __name__ == "__main__":
  unittest.main(verbosity=2)
