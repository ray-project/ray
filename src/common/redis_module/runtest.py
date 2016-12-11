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

class TestGlobalStateStore(unittest.TestCase):

  def setUp(self):
    redis_port = 6379
    with open(os.devnull, "w") as FNULL:
      self.redis_process = subprocess.Popen([redis_path,
                                             "--port", str(redis_port),
                                             "--loadmodule", "ray_redis_module.so"],
                                             stdout=FNULL)
    time.sleep(0.1)
    self.redis = redis.StrictRedis(host='localhost', port=6379, db=0)

  def tearDown(self):
    self.redis_process.kill()

  def testInvalidObjectTableAdd(self):
    self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "hello", 1, "this is a hash", 1)
    with self.assertRaises(redis.ResponseError):
      self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "hello", 1, "this is another hash", 2)

  def testObjectTableAddAndLookup(self):
    self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "world", 1, "hash 1", 42)
    self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "world", 1, "hash 1", 43)
    response = self.redis.execute_command("RAY.OBJECT_TABLE_LOOKUP", "world")
    self.assertEqual(set(response), {"42", "43"})

  def testObjectTableSubscribe(self):
    p = self.redis.pubsub()
    # Subscribe to object ID "berkeley"
    p.subscribe("berkeley")
    self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "berkeley", 1, "hash 2", 42)
    # First the acknowledgement message
    self.assertEqual(p.get_message()['data'], 1)
    # And then the actual data
    self.assertEqual(p.get_message()['data'], "MANAGERS 42")

if __name__ == "__main__":
  unittest.main(verbosity=2)
