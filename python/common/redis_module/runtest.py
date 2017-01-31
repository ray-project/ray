from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import random
import subprocess
import sys
import time
import unittest
import redis

# Check if the redis-server binary is present.
redis_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../../core/src/common/thirdparty/redis/src/redis-server")
if not os.path.exists(redis_path):
  raise Exception("You do not have the redis-server binary. Run `make test` in the plasma directory to get it.")

# Absolute path of the ray redis module.
module_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../../core/src/common/redis_module/libray_redis_module.so")
print("path to the redis module is {}".format(module_path))

OBJECT_INFO_PREFIX = "OI:"
OBJECT_LOCATION_PREFIX = "OL:"
OBJECT_SUBSCRIBE_PREFIX = "OS:"
TASK_PREFIX = "TT:"
OBJECT_CHANNEL_PREFIX = "OC:"

def integerToAsciiHex(num, numbytes):
  retstr = b""
  # Support 32 and 64 bit architecture.
  assert(numbytes == 4 or numbytes == 8)
  for i in range(numbytes):
    curbyte = num & 0xff
    if sys.version_info >= (3, 0):
      retstr += bytes([curbyte])
    else:
      retstr += chr(curbyte)
    num = num >> 8

  return retstr

class TestGlobalStateStore(unittest.TestCase):

  def setUp(self):
    redis_port = random.randint(2000, 50000)
    self.redis_process = subprocess.Popen([redis_path,
                                           "--port", str(redis_port),
                                           "--loglevel", "warning",
                                           "--loadmodule", module_path])
    time.sleep(1.5)
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
    self.assertEqual(response, None)
    # Add some managers and try again.
    self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id1", 1, "hash1", "manager_id1")
    self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id1", 1, "hash1", "manager_id2")
    response = self.redis.execute_command("RAY.OBJECT_TABLE_LOOKUP", "object_id1")
    self.assertEqual(set(response), {b"manager_id1", b"manager_id2"})
    # Add a manager that already exists again and try again.
    self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id1", 1, "hash1", "manager_id2")
    response = self.redis.execute_command("RAY.OBJECT_TABLE_LOOKUP", "object_id1")
    self.assertEqual(set(response), {b"manager_id1", b"manager_id2"})
    # Check that we properly handle NULL characters. In the past, NULL
    # characters were handled improperly causing a "hash mismatch" error if two
    # object IDs that agreed up to the NULL character were inserted with
    # different hashes.
    self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "\x00object_id3", 1, "hash1", "manager_id1")
    self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "\x00object_id4", 1, "hash2", "manager_id1")
    # Check that NULL characters in the hash are handled properly.
    self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id3", 1, "\x00hash1", "manager_id1")
    with self.assertRaises(redis.ResponseError):
      self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id3", 1, "\x00hash2", "manager_id1")

  def testObjectTableAddAndRemove(self):
    # Try removing a manager from an object ID that has not been added yet.
    with self.assertRaises(redis.ResponseError):
      self.redis.execute_command("RAY.OBJECT_TABLE_REMOVE", "object_id1", "manager_id1")
    # Try calling RAY.OBJECT_TABLE_LOOKUP with an object ID that has not been
    # added yet.
    response = self.redis.execute_command("RAY.OBJECT_TABLE_LOOKUP", "object_id1")
    self.assertEqual(response, None)
    # Add some managers and try again.
    self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id1", 1, "hash1", "manager_id1")
    self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id1", 1, "hash1", "manager_id2")
    response = self.redis.execute_command("RAY.OBJECT_TABLE_LOOKUP", "object_id1")
    self.assertEqual(set(response), {b"manager_id1", b"manager_id2"})
    # Remove a manager that doesn't exist, and make sure we still have the same set.
    self.redis.execute_command("RAY.OBJECT_TABLE_REMOVE", "object_id1", "manager_id3")
    response = self.redis.execute_command("RAY.OBJECT_TABLE_LOOKUP", "object_id1")
    self.assertEqual(set(response), {b"manager_id1", b"manager_id2"})
    # Remove a manager that does exist. Make sure it gets removed the first
    # time and does nothing the second time.
    self.redis.execute_command("RAY.OBJECT_TABLE_REMOVE", "object_id1", "manager_id1")
    response = self.redis.execute_command("RAY.OBJECT_TABLE_LOOKUP", "object_id1")
    self.assertEqual(set(response), {b"manager_id2"})
    self.redis.execute_command("RAY.OBJECT_TABLE_REMOVE", "object_id1", "manager_id1")
    response = self.redis.execute_command("RAY.OBJECT_TABLE_LOOKUP", "object_id1")
    self.assertEqual(set(response), {b"manager_id2"})
    # Remove the last manager, and make sure we have an empty set.
    self.redis.execute_command("RAY.OBJECT_TABLE_REMOVE", "object_id1", "manager_id2")
    response = self.redis.execute_command("RAY.OBJECT_TABLE_LOOKUP", "object_id1")
    self.assertEqual(set(response), set())
    # Remove a manager from an empty set, and make sure we now have an empty set.
    self.redis.execute_command("RAY.OBJECT_TABLE_REMOVE", "object_id1", "manager_id3")
    response = self.redis.execute_command("RAY.OBJECT_TABLE_LOOKUP", "object_id1")
    self.assertEqual(set(response), set())

  def testObjectTableSubscribeToNotifications(self):
    data_size = 0xf1f0
    p = self.redis.pubsub()
    # Subscribe to an object ID.
    p.psubscribe("{}manager_id1".format(OBJECT_CHANNEL_PREFIX))
    self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id1", data_size, "hash1", "manager_id2")
    # Receive the acknowledgement message.
    self.assertEqual(p.get_message()["data"], 1)
    # Request a notification and receive the data.
    self.redis.execute_command("RAY.OBJECT_TABLE_REQUEST_NOTIFICATIONS", "manager_id1", "object_id1")
    self.assertEqual(p.get_message()["data"], b"object_id1 %s MANAGERS manager_id2"\
                     %integerToAsciiHex(data_size, 8))
    # Request a notification for an object that isn't there. Then add the object
    # and receive the data. Only the first call to RAY.OBJECT_TABLE_ADD should
    # trigger notifications.
    self.redis.execute_command("RAY.OBJECT_TABLE_REQUEST_NOTIFICATIONS", "manager_id1", "object_id2", "object_id3")
    self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id3", data_size, "hash1", "manager_id1")
    self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id3", data_size, "hash1", "manager_id2")
    self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id3", data_size, "hash1", "manager_id3")
    self.assertEqual(p.get_message()["data"], b"object_id3 %s MANAGERS manager_id1"\
                     %integerToAsciiHex(data_size, 8))
    self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id2", data_size, "hash1", "manager_id3")
    self.assertEqual(p.get_message()["data"], b"object_id2 %s MANAGERS manager_id3"\
                     %integerToAsciiHex(data_size, 8))
    # Request notifications for object_id3 again.
    self.redis.execute_command("RAY.OBJECT_TABLE_REQUEST_NOTIFICATIONS", "manager_id1", "object_id3")
    self.assertEqual(p.get_message()["data"], b"object_id3 %s MANAGERS manager_id1 manager_id2 manager_id3"\
                     %integerToAsciiHex(data_size, 8))

  def testResultTableAddAndLookup(self):
    # Try looking up something in the result table before anything is added.
    response = self.redis.execute_command("RAY.RESULT_TABLE_LOOKUP", "object_id1")
    self.assertIsNone(response)
    # Adding the object to the object table should have no effect.
    self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id1", 1, "hash1", "manager_id1")
    response = self.redis.execute_command("RAY.RESULT_TABLE_LOOKUP", "object_id1")
    self.assertIsNone(response)
    # Add the result to the result table. The lookup now returns the task ID.
    task_id = "task_id1"
    self.redis.execute_command("RAY.RESULT_TABLE_ADD", "object_id1", task_id)
    response = self.redis.execute_command("RAY.RESULT_TABLE_LOOKUP", "object_id1")
    self.assertEqual(response, task_id)
    # Doing it again should still work.
    response = self.redis.execute_command("RAY.RESULT_TABLE_LOOKUP", "object_id1")
    self.assertEqual(response, task_id)
    # Try another result table lookup. This should succeed.
    task_id = "task_id2"
    self.redis.execute_command("RAY.RESULT_TABLE_ADD", "object_id2", task_id)
    response = self.redis.execute_command("RAY.RESULT_TABLE_LOOKUP", "object_id2")
    self.assertEqual(response, task_id)

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
      self.redis.execute_command("RAY.TASK_TABLE_ADD", "task_id", 101,
                                 "node_id", "task_spec")
    with self.assertRaises(redis.ResponseError):
      # Should not be able to update a non-existent task.
      self.redis.execute_command("RAY.TASK_TABLE_UPDATE", "task_id", 10,
                                 "node_id")

  def testTaskTableAddAndLookup(self):
    # Check that task table adds, updates, and lookups work correctly.
    task_args = [1, b"node_id", b"task_spec"]
    response = self.redis.execute_command("RAY.TASK_TABLE_ADD", "task_id",
                                          *task_args)
    response = self.redis.execute_command("RAY.TASK_TABLE_GET", "task_id")
    self.assertEqual(response, task_args)

    task_args[0] = 2
    self.redis.execute_command("RAY.TASK_TABLE_UPDATE", "task_id", *task_args[:2])
    response = self.redis.execute_command("RAY.TASK_TABLE_GET", "task_id")
    self.assertEqual(response, task_args)

    # If the current value, test value, and set value are all the same, the
    # update happens, and the response is still the same task.
    task_args = [task_args[0]] + task_args
    response = self.redis.execute_command("RAY.TASK_TABLE_TEST_AND_UPDATE",
                                          "task_id",
                                          *task_args[:3])
    self.assertEqual(response, task_args[1:])
    # Check that the task entry is still the same.
    get_response = self.redis.execute_command("RAY.TASK_TABLE_GET", "task_id")
    self.assertEqual(get_response, task_args[1:])

    # If the current value is the same as the test value, and the set value is
    # different, the update happens, and the response is the entire task.
    task_args[1] += 1
    response = self.redis.execute_command("RAY.TASK_TABLE_TEST_AND_UPDATE",
                                          "task_id",
                                          *task_args[:3])
    self.assertEqual(response, task_args[1:])
    # Check that the update happened.
    get_response = self.redis.execute_command("RAY.TASK_TABLE_GET", "task_id")
    self.assertEqual(get_response, task_args[1:])

    # If the current value is no longer the same as the test value, the
    # response is nil.
    response = self.redis.execute_command("RAY.TASK_TABLE_TEST_AND_UPDATE",
                                          "task_id",
                                          *task_args[:3])
    self.assertEqual(response, None)
    # Check that the update did not happen.
    get_response2 = self.redis.execute_command("RAY.TASK_TABLE_GET", "task_id")
    self.assertEqual(get_response2, get_response)

  def testTaskTableSubscribe(self):
    scheduling_state = 1
    node_id = "node_id"
    # Subscribe to the task table.
    p = self.redis.pubsub()
    p.psubscribe("{prefix}*:*".format(prefix=TASK_PREFIX))
    p.psubscribe("{prefix}*:{state: >2}".format(prefix=TASK_PREFIX, state=scheduling_state))
    p.psubscribe("{prefix}{node}:*".format(prefix=TASK_PREFIX, node=node_id))
    task_args = [b"task_id", scheduling_state, node_id.encode("ascii"), b"task_spec"]
    self.redis.execute_command("RAY.TASK_TABLE_ADD", *task_args)
    # Receive the acknowledgement message.
    self.assertEqual(p.get_message()["data"], 1)
    self.assertEqual(p.get_message()["data"], 2)
    self.assertEqual(p.get_message()["data"], 3)
    # Receive the actual data.
    for i in range(3):
        message = p.get_message()["data"]
        message = message.split()
        message[1] = int(message[1])
        self.assertEqual(message, task_args)

if __name__ == "__main__":
  unittest.main(verbosity=2)
