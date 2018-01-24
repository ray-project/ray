from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import redis
import sys
import time
import unittest

import ray.services

# Import flatbuffer bindings.
from ray.core.generated.SubscribeToNotificationsReply \
    import SubscribeToNotificationsReply
from ray.core.generated.TaskReply import TaskReply
from ray.core.generated.ResultTableReply import ResultTableReply

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


def get_next_message(pubsub_client, timeout_seconds=10):
    """Block until the next message is available on the pubsub channel."""
    start_time = time.time()
    while True:
        message = pubsub_client.get_message()
        if message is not None:
            return message
        time.sleep(0.1)
        if time.time() - start_time > timeout_seconds:
            raise Exception("Timed out while waiting for next message.")


class TestGlobalStateStore(unittest.TestCase):

    def setUp(self):
        redis_port, _ = ray.services.start_redis_instance()
        self.redis = redis.StrictRedis(host="localhost", port=redis_port, db=0)

    def tearDown(self):
        ray.services.cleanup()

    def testInvalidObjectTableAdd(self):
        # Check that Redis returns an error when RAY.OBJECT_TABLE_ADD is called
        # with the wrong arguments.
        with self.assertRaises(redis.ResponseError):
            self.redis.execute_command("RAY.OBJECT_TABLE_ADD")
        with self.assertRaises(redis.ResponseError):
            self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "hello")
        with self.assertRaises(redis.ResponseError):
            self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id2",
                                       "one", "hash2", "manager_id1")
        with self.assertRaises(redis.ResponseError):
            self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id2", 1,
                                       "hash2", "manager_id1",
                                       "extra argument")
        # Check that Redis returns an error when RAY.OBJECT_TABLE_ADD adds an
        # object ID that is already present with a different hash.
        self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id1", 1,
                                   "hash1", "manager_id1")
        response = self.redis.execute_command("RAY.OBJECT_TABLE_LOOKUP",
                                              "object_id1")
        self.assertEqual(set(response), {b"manager_id1"})
        with self.assertRaises(redis.ResponseError):
            self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id1", 1,
                                       "hash2", "manager_id2")
        # Check that the second manager was added, even though the hash was
        # mismatched.
        response = self.redis.execute_command("RAY.OBJECT_TABLE_LOOKUP",
                                              "object_id1")
        self.assertEqual(set(response), {b"manager_id1", b"manager_id2"})
        # Check that it is fine if we add the same object ID multiple times
        # with the most recent hash.
        self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id1", 1,
                                   "hash2", "manager_id1")
        self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id1", 1,
                                   "hash2", "manager_id1")
        self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id1", 1,
                                   "hash2", "manager_id2")
        self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id1", 2,
                                   "hash2", "manager_id2")
        response = self.redis.execute_command("RAY.OBJECT_TABLE_LOOKUP",
                                              "object_id1")
        self.assertEqual(set(response), {b"manager_id1", b"manager_id2"})

    def testObjectTableAddAndLookup(self):
        # Try calling RAY.OBJECT_TABLE_LOOKUP with an object ID that has not
        # been added yet.
        response = self.redis.execute_command("RAY.OBJECT_TABLE_LOOKUP",
                                              "object_id1")
        self.assertEqual(response, None)
        # Add some managers and try again.
        self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id1", 1,
                                   "hash1", "manager_id1")
        self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id1", 1,
                                   "hash1", "manager_id2")
        response = self.redis.execute_command("RAY.OBJECT_TABLE_LOOKUP",
                                              "object_id1")
        self.assertEqual(set(response), {b"manager_id1", b"manager_id2"})
        # Add a manager that already exists again and try again.
        self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id1", 1,
                                   "hash1", "manager_id2")
        response = self.redis.execute_command("RAY.OBJECT_TABLE_LOOKUP",
                                              "object_id1")
        self.assertEqual(set(response), {b"manager_id1", b"manager_id2"})
        # Check that we properly handle NULL characters. In the past, NULL
        # characters were handled improperly causing a "hash mismatch" error if
        # two object IDs that agreed up to the NULL character were inserted
        # with different hashes.
        self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "\x00object_id3", 1,
                                   "hash1", "manager_id1")
        self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "\x00object_id4", 1,
                                   "hash2", "manager_id1")
        # Check that NULL characters in the hash are handled properly.
        self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id3", 1,
                                   "\x00hash1", "manager_id1")
        with self.assertRaises(redis.ResponseError):
            self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id3", 1,
                                       "\x00hash2", "manager_id1")

    def testObjectTableAddAndRemove(self):
        # Try removing a manager from an object ID that has not been added yet.
        with self.assertRaises(redis.ResponseError):
            self.redis.execute_command("RAY.OBJECT_TABLE_REMOVE", "object_id1",
                                       "manager_id1")
        # Try calling RAY.OBJECT_TABLE_LOOKUP with an object ID that has not
        # been added yet.
        response = self.redis.execute_command("RAY.OBJECT_TABLE_LOOKUP",
                                              "object_id1")
        self.assertEqual(response, None)
        # Add some managers and try again.
        self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id1", 1,
                                   "hash1", "manager_id1")
        self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id1", 1,
                                   "hash1", "manager_id2")
        response = self.redis.execute_command("RAY.OBJECT_TABLE_LOOKUP",
                                              "object_id1")
        self.assertEqual(set(response), {b"manager_id1", b"manager_id2"})
        # Remove a manager that doesn't exist, and make sure we still have the
        # same set.
        self.redis.execute_command("RAY.OBJECT_TABLE_REMOVE", "object_id1",
                                   "manager_id3")
        response = self.redis.execute_command("RAY.OBJECT_TABLE_LOOKUP",
                                              "object_id1")
        self.assertEqual(set(response), {b"manager_id1", b"manager_id2"})
        # Remove a manager that does exist. Make sure it gets removed the first
        # time and does nothing the second time.
        self.redis.execute_command("RAY.OBJECT_TABLE_REMOVE", "object_id1",
                                   "manager_id1")
        response = self.redis.execute_command("RAY.OBJECT_TABLE_LOOKUP",
                                              "object_id1")
        self.assertEqual(set(response), {b"manager_id2"})
        self.redis.execute_command("RAY.OBJECT_TABLE_REMOVE", "object_id1",
                                   "manager_id1")
        response = self.redis.execute_command("RAY.OBJECT_TABLE_LOOKUP",
                                              "object_id1")
        self.assertEqual(set(response), {b"manager_id2"})
        # Remove the last manager, and make sure we have an empty set.
        self.redis.execute_command("RAY.OBJECT_TABLE_REMOVE", "object_id1",
                                   "manager_id2")
        response = self.redis.execute_command("RAY.OBJECT_TABLE_LOOKUP",
                                              "object_id1")
        self.assertEqual(set(response), set())
        # Remove a manager from an empty set, and make sure we now have an
        # empty set.
        self.redis.execute_command("RAY.OBJECT_TABLE_REMOVE", "object_id1",
                                   "manager_id3")
        response = self.redis.execute_command("RAY.OBJECT_TABLE_LOOKUP",
                                              "object_id1")
        self.assertEqual(set(response), set())

    def testObjectTableSubscribeToNotifications(self):
        # Define a helper method for checking the contents of object
        # notifications.
        def check_object_notification(notification_message, object_id,
                                      object_size, manager_ids):
            notification_object = (SubscribeToNotificationsReply
                                   .GetRootAsSubscribeToNotificationsReply(
                                       notification_message, 0))
            self.assertEqual(notification_object.ObjectId(), object_id)
            self.assertEqual(notification_object.ObjectSize(), object_size)
            self.assertEqual(notification_object.ManagerIdsLength(),
                             len(manager_ids))
            for i in range(len(manager_ids)):
                self.assertEqual(notification_object.ManagerIds(i),
                                 manager_ids[i])

        data_size = 0xf1f0
        p = self.redis.pubsub()
        # Subscribe to an object ID.
        p.psubscribe("{}manager_id1".format(OBJECT_CHANNEL_PREFIX))
        self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id1",
                                   data_size, "hash1", "manager_id2")
        # Receive the acknowledgement message.
        self.assertEqual(get_next_message(p)["data"], 1)
        # Request a notification and receive the data.
        self.redis.execute_command("RAY.OBJECT_TABLE_REQUEST_NOTIFICATIONS",
                                   "manager_id1", "object_id1")
        # Verify that the notification is correct.
        check_object_notification(get_next_message(p)["data"],
                                  b"object_id1",
                                  data_size,
                                  [b"manager_id2"])

        # Request a notification for an object that isn't there. Then add the
        # object and receive the data. Only the first call to
        # RAY.OBJECT_TABLE_ADD should trigger notifications.
        self.redis.execute_command("RAY.OBJECT_TABLE_REQUEST_NOTIFICATIONS",
                                   "manager_id1", "object_id2", "object_id3")
        self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id3",
                                   data_size, "hash1", "manager_id1")
        self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id3",
                                   data_size, "hash1", "manager_id2")
        self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id3",
                                   data_size, "hash1", "manager_id3")
        # Verify that the notification is correct.
        check_object_notification(get_next_message(p)["data"],
                                  b"object_id3",
                                  data_size,
                                  [b"manager_id1"])
        self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id2",
                                   data_size, "hash1", "manager_id3")
        # Verify that the notification is correct.
        check_object_notification(get_next_message(p)["data"],
                                  b"object_id2",
                                  data_size,
                                  [b"manager_id3"])
        # Request notifications for object_id3 again.
        self.redis.execute_command("RAY.OBJECT_TABLE_REQUEST_NOTIFICATIONS",
                                   "manager_id1", "object_id3")
        # Verify that the notification is correct.
        check_object_notification(get_next_message(p)["data"],
                                  b"object_id3",
                                  data_size,
                                  [b"manager_id1", b"manager_id2",
                                   b"manager_id3"])

    def testResultTableAddAndLookup(self):
        def check_result_table_entry(message, task_id, is_put):
            result_table_reply = ResultTableReply.GetRootAsResultTableReply(
                message, 0)
            self.assertEqual(result_table_reply.TaskId(), task_id)
            self.assertEqual(result_table_reply.IsPut(), is_put)

        # Try looking up something in the result table before anything is
        # added.
        response = self.redis.execute_command("RAY.RESULT_TABLE_LOOKUP",
                                              "object_id1")
        self.assertIsNone(response)
        # Adding the object to the object table should have no effect.
        self.redis.execute_command("RAY.OBJECT_TABLE_ADD", "object_id1", 1,
                                   "hash1", "manager_id1")
        response = self.redis.execute_command("RAY.RESULT_TABLE_LOOKUP",
                                              "object_id1")
        self.assertIsNone(response)
        # Add the result to the result table. The lookup now returns the task
        # ID.
        task_id = b"task_id1"
        self.redis.execute_command("RAY.RESULT_TABLE_ADD", "object_id1",
                                   task_id, 0)
        response = self.redis.execute_command("RAY.RESULT_TABLE_LOOKUP",
                                              "object_id1")
        check_result_table_entry(response, task_id, False)
        # Doing it again should still work.
        response = self.redis.execute_command("RAY.RESULT_TABLE_LOOKUP",
                                              "object_id1")
        check_result_table_entry(response, task_id, False)
        # Try another result table lookup. This should succeed.
        task_id = b"task_id2"
        self.redis.execute_command("RAY.RESULT_TABLE_ADD", "object_id2",
                                   task_id, 1)
        response = self.redis.execute_command("RAY.RESULT_TABLE_LOOKUP",
                                              "object_id2")
        check_result_table_entry(response, task_id, True)

    def testInvalidTaskTableAdd(self):
        # Check that Redis returns an error when RAY.TASK_TABLE_ADD is called
        # with the wrong arguments.
        with self.assertRaises(redis.ResponseError):
            self.redis.execute_command("RAY.TASK_TABLE_ADD")
        with self.assertRaises(redis.ResponseError):
            self.redis.execute_command("RAY.TASK_TABLE_ADD", "hello")
        with self.assertRaises(redis.ResponseError):
            self.redis.execute_command("RAY.TASK_TABLE_ADD", "task_id", 3,
                                       "node_id")
        with self.assertRaises(redis.ResponseError):
            # Non-integer scheduling states should not be added.
            self.redis.execute_command("RAY.TASK_TABLE_ADD", "task_id",
                                       "invalid_state", "node_id", "task_spec")
        with self.assertRaises(redis.ResponseError):
            # Should not be able to update a non-existent task.
            self.redis.execute_command("RAY.TASK_TABLE_UPDATE", "task_id", 10,
                                       "node_id", b"")

    def testTaskTableAddAndLookup(self):
        TASK_STATUS_WAITING = 1
        TASK_STATUS_SCHEDULED = 2
        TASK_STATUS_QUEUED = 4

        # make sure somebody will get a notification (checked in the redis
        # module)
        p = self.redis.pubsub()
        p.psubscribe("{prefix}*:*".format(prefix=TASK_PREFIX))

        def check_task_reply(message, task_args, updated=False):
            (task_status, local_scheduler_id, execution_dependencies_string,
             spillback_count, task_spec) = task_args
            task_reply_object = TaskReply.GetRootAsTaskReply(message, 0)
            self.assertEqual(task_reply_object.State(), task_status)
            self.assertEqual(task_reply_object.LocalSchedulerId(),
                             local_scheduler_id)
            self.assertEqual(task_reply_object.SpillbackCount(),
                             spillback_count)
            self.assertEqual(task_reply_object.TaskSpec(), task_spec)
            self.assertEqual(task_reply_object.Updated(), updated)

        # Check that task table adds, updates, and lookups work correctly.
        task_args = [TASK_STATUS_WAITING, b"node_id", b"", 0, b"task_spec"]
        response = self.redis.execute_command("RAY.TASK_TABLE_ADD", "task_id",
                                              *task_args)
        response = self.redis.execute_command("RAY.TASK_TABLE_GET", "task_id")
        check_task_reply(response, task_args)

        task_args[0] = TASK_STATUS_SCHEDULED
        self.redis.execute_command("RAY.TASK_TABLE_UPDATE", "task_id",
                                   *task_args[:4])
        response = self.redis.execute_command("RAY.TASK_TABLE_GET", "task_id")
        check_task_reply(response, task_args)

        # If the current value, test value, and set value are all the same, the
        # update happens, and the response is still the same task.
        task_args = [task_args[0]] + task_args
        response = self.redis.execute_command("RAY.TASK_TABLE_TEST_AND_UPDATE",
                                              "task_id",
                                              *task_args[:3])
        check_task_reply(response, task_args[1:], updated=True)
        # Check that the task entry is still the same.
        get_response = self.redis.execute_command("RAY.TASK_TABLE_GET",
                                                  "task_id")
        check_task_reply(get_response, task_args[1:])

        # If the current value is the same as the test value, and the set value
        # is different, the update happens, and the response is the entire
        # task.
        task_args[1] = TASK_STATUS_QUEUED
        response = self.redis.execute_command("RAY.TASK_TABLE_TEST_AND_UPDATE",
                                              "task_id",
                                              *task_args[:3])
        check_task_reply(response, task_args[1:], updated=True)
        # Check that the update happened.
        get_response = self.redis.execute_command("RAY.TASK_TABLE_GET",
                                                  "task_id")
        check_task_reply(get_response, task_args[1:])

        # If the current value is no longer the same as the test value, the
        # response is the same task as before the test-and-set.
        new_task_args = task_args[:]
        new_task_args[1] = TASK_STATUS_WAITING
        response = self.redis.execute_command("RAY.TASK_TABLE_TEST_AND_UPDATE",
                                              "task_id",
                                              *new_task_args[:3])
        check_task_reply(response, task_args[1:], updated=False)
        # Check that the update did not happen.
        get_response2 = self.redis.execute_command("RAY.TASK_TABLE_GET",
                                                   "task_id")
        self.assertEqual(get_response2, get_response)

        # If the test value is a bitmask that matches the current value, the
        # update happens.
        task_args = new_task_args
        task_args[0] = TASK_STATUS_SCHEDULED | TASK_STATUS_QUEUED
        response = self.redis.execute_command("RAY.TASK_TABLE_TEST_AND_UPDATE",
                                              "task_id",
                                              *task_args[:3])
        check_task_reply(response, task_args[1:], updated=True)

        # If the test value is a bitmask that does not match the current value,
        # the update does not happen, and the response is the same task as
        # before the test-and-set.
        new_task_args = task_args[:]
        new_task_args[0] = TASK_STATUS_SCHEDULED
        old_response = response
        response = self.redis.execute_command("RAY.TASK_TABLE_TEST_AND_UPDATE",
                                              "task_id",
                                              *new_task_args[:3])
        check_task_reply(response, task_args[1:], updated=False)
        # Check that the update did not happen.
        get_response = self.redis.execute_command("RAY.TASK_TABLE_GET",
                                                  "task_id")
        self.assertNotEqual(get_response, old_response)
        check_task_reply(get_response, task_args[1:])

    def check_task_subscription(self, p, scheduling_state, local_scheduler_id):
        task_args = [b"task_id", scheduling_state,
                     local_scheduler_id.encode("ascii"), b"", 0, b"task_spec"]
        self.redis.execute_command("RAY.TASK_TABLE_ADD", *task_args)
        # Receive the data.
        message = get_next_message(p)["data"]
        # Check that the notification object is correct.
        notification_object = TaskReply.GetRootAsTaskReply(message, 0)
        self.assertEqual(notification_object.TaskId(), task_args[0])
        self.assertEqual(notification_object.State(), task_args[1])
        self.assertEqual(notification_object.LocalSchedulerId(),
                         task_args[2])
        self.assertEqual(notification_object.ExecutionDependencies(),
                         task_args[3])
        self.assertEqual(notification_object.TaskSpec(), task_args[-1])

    def testTaskTableSubscribe(self):
        scheduling_state = 1
        local_scheduler_id = "local_scheduler_id"
        # Subscribe to the task table.
        p = self.redis.pubsub()
        p.psubscribe("{prefix}*:*".format(prefix=TASK_PREFIX))
        # Receive acknowledgment.
        self.assertEqual(get_next_message(p)["data"], 1)
        self.check_task_subscription(p, scheduling_state, local_scheduler_id)
        # unsubscribe to make sure there is only one subscriber at a given time
        p.punsubscribe("{prefix}*:*".format(prefix=TASK_PREFIX))
        # Receive acknowledgment.
        self.assertEqual(get_next_message(p)["data"], 0)

        p.psubscribe("{prefix}*:{state}".format(
            prefix=TASK_PREFIX, state=scheduling_state))
        # Receive acknowledgment.
        self.assertEqual(get_next_message(p)["data"], 1)
        self.check_task_subscription(p, scheduling_state, local_scheduler_id)
        p.punsubscribe("{prefix}*:{state}".format(
            prefix=TASK_PREFIX, state=scheduling_state))
        # Receive acknowledgment.
        self.assertEqual(get_next_message(p)["data"], 0)

        p.psubscribe("{prefix}{local_scheduler_id}:*".format(
            prefix=TASK_PREFIX, local_scheduler_id=local_scheduler_id))
        # Receive acknowledgment.
        self.assertEqual(get_next_message(p)["data"], 1)
        self.check_task_subscription(p, scheduling_state, local_scheduler_id)
        p.punsubscribe("{prefix}{local_scheduler_id}:*".format(
            prefix=TASK_PREFIX, local_scheduler_id=local_scheduler_id))
        # Receive acknowledgment.
        self.assertEqual(get_next_message(p)["data"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
