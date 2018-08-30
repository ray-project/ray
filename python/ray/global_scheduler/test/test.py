from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import os
import random
import signal
import sys
import time
import unittest

# The ray import must come before the pyarrow import because ray modifies the
# python path so that the right version of pyarrow is found.
import ray.global_scheduler as global_scheduler
import ray.local_scheduler as local_scheduler
import ray.plasma as plasma
from ray.plasma.utils import create_object
from ray import services
from ray.experimental import state
import ray.ray_constants as ray_constants
import pyarrow as pa

USE_VALGRIND = False
PLASMA_STORE_MEMORY = 1000000000
NUM_CLUSTER_NODES = 2

NIL_WORKER_ID = ray_constants.ID_SIZE * b"\xff"
NIL_OBJECT_ID = ray_constants.ID_SIZE * b"\xff"
NIL_ACTOR_ID = ray_constants.ID_SIZE * b"\xff"


def random_driver_id():
    return local_scheduler.ObjectID(np.random.bytes(ray_constants.ID_SIZE))


def random_task_id():
    return local_scheduler.ObjectID(np.random.bytes(ray_constants.ID_SIZE))


def random_function_id():
    return local_scheduler.ObjectID(np.random.bytes(ray_constants.ID_SIZE))


def random_object_id():
    return local_scheduler.ObjectID(np.random.bytes(ray_constants.ID_SIZE))


def new_port():
    return random.randint(10000, 65535)


class TestGlobalScheduler(unittest.TestCase):
    def setUp(self):
        # Start one Redis server and N pairs of (plasma, local_scheduler)
        self.node_ip_address = "127.0.0.1"
        redis_address, redis_shards = services.start_redis(
            self.node_ip_address)
        redis_port = services.get_port(redis_address)
        time.sleep(0.1)
        # Create a client for the global state store.
        self.state = state.GlobalState()
        self.state._initialize_global_state(self.node_ip_address, redis_port)

        # Start one global scheduler.
        self.p1 = global_scheduler.start_global_scheduler(
            redis_address, self.node_ip_address, use_valgrind=USE_VALGRIND)
        self.plasma_store_pids = []
        self.plasma_manager_pids = []
        self.local_scheduler_pids = []
        self.plasma_clients = []
        self.local_scheduler_clients = []

        for i in range(NUM_CLUSTER_NODES):
            # Start the Plasma store. Plasma store name is randomly generated.
            plasma_store_name, p2 = plasma.start_plasma_store()
            self.plasma_store_pids.append(p2)
            # Start the Plasma manager.
            # Assumption: Plasma manager name and port are randomly generated
            # by the plasma module.
            manager_info = plasma.start_plasma_manager(plasma_store_name,
                                                       redis_address)
            plasma_manager_name, p3, plasma_manager_port = manager_info
            self.plasma_manager_pids.append(p3)
            plasma_address = "{}:{}".format(self.node_ip_address,
                                            plasma_manager_port)
            plasma_client = pa.plasma.connect(plasma_store_name,
                                              plasma_manager_name, 64)
            self.plasma_clients.append(plasma_client)
            # Start the local scheduler.
            local_scheduler_name, p4 = local_scheduler.start_local_scheduler(
                plasma_store_name,
                plasma_manager_name=plasma_manager_name,
                plasma_address=plasma_address,
                redis_address=redis_address,
                static_resources={"CPU": 10})
            # Connect to the scheduler.
            local_scheduler_client = local_scheduler.LocalSchedulerClient(
                local_scheduler_name, NIL_WORKER_ID, False, random_task_id(),
                False)
            self.local_scheduler_clients.append(local_scheduler_client)
            self.local_scheduler_pids.append(p4)

    def tearDown(self):
        # Check that the processes are still alive.
        self.assertEqual(self.p1.poll(), None)
        for p2 in self.plasma_store_pids:
            self.assertEqual(p2.poll(), None)
        for p3 in self.plasma_manager_pids:
            self.assertEqual(p3.poll(), None)
        for p4 in self.local_scheduler_pids:
            self.assertEqual(p4.poll(), None)

        redis_processes = services.all_processes[
            services.PROCESS_TYPE_REDIS_SERVER]
        for redis_process in redis_processes:
            self.assertEqual(redis_process.poll(), None)

        # Kill the global scheduler.
        if USE_VALGRIND:
            self.p1.send_signal(signal.SIGTERM)
            self.p1.wait()
            if self.p1.returncode != 0:
                os._exit(-1)
        else:
            self.p1.kill()
        # Kill local schedulers, plasma managers, and plasma stores.
        for p2 in self.local_scheduler_pids:
            p2.kill()
        for p3 in self.plasma_manager_pids:
            p3.kill()
        for p4 in self.plasma_store_pids:
            p4.kill()
        # Kill Redis. In the event that we are using valgrind, this needs to
        # happen after we kill the global scheduler.
        while redis_processes:
            redis_process = redis_processes.pop()
            redis_process.kill()

    def get_plasma_manager_id(self):
        """Get the db_client_id with client_type equal to plasma_manager.

        Iterates over all the client table keys, gets the db_client_id for the
        client with client_type matching plasma_manager. Strips the client
        table prefix. TODO(atumanov): write a separate function to get all
        plasma manager client IDs.

        Returns:
          The db_client_id if one is found and otherwise None.
        """
        db_client_id = None

        client_list = self.state.client_table()[self.node_ip_address]
        for client in client_list:
            if client["ClientType"] == "plasma_manager":
                db_client_id = client["DBClientID"]
                break

        return db_client_id

    def test_task_default_resources(self):
        task1 = local_scheduler.Task(
            random_driver_id(), random_function_id(), [random_object_id()], 0,
            random_task_id(), 0)
        self.assertEqual(task1.required_resources(), {"CPU": 1})
        task2 = local_scheduler.Task(
            random_driver_id(), random_function_id(), [random_object_id()], 0,
            random_task_id(), 0, local_scheduler.ObjectID(NIL_ACTOR_ID),
            local_scheduler.ObjectID(NIL_OBJECT_ID),
            local_scheduler.ObjectID(NIL_ACTOR_ID),
            local_scheduler.ObjectID(NIL_ACTOR_ID), 0, 0, [], {
                "CPU": 1,
                "GPU": 2
            })
        self.assertEqual(task2.required_resources(), {"CPU": 1, "GPU": 2})

    def test_redis_only_single_task(self):
        # Tests global scheduler functionality by interacting with Redis and
        # checking task state transitions in Redis only. TODO(atumanov):
        # implement.

        # Check precondition for this test:
        # There should be 2n+1 db clients: the global scheduler + one local
        # scheduler and one plasma per node.
        self.assertEqual(
            len(self.state.client_table()[self.node_ip_address]),
            2 * NUM_CLUSTER_NODES + 1)
        db_client_id = self.get_plasma_manager_id()
        assert (db_client_id is not None)

    @unittest.skipIf(
        os.environ.get("RAY_USE_NEW_GCS", False),
        "New GCS API doesn't have a Python API yet.")
    def test_integration_single_task(self):
        # There should be three db clients, the global scheduler, the local
        # scheduler, and the plasma manager.
        self.assertEqual(
            len(self.state.client_table()[self.node_ip_address]),
            2 * NUM_CLUSTER_NODES + 1)

        num_return_vals = [0, 1, 2, 3, 5, 10]
        # Insert the object into Redis.
        data_size = 0xf1f0
        metadata_size = 0x40
        plasma_client = self.plasma_clients[0]
        object_dep, memory_buffer, metadata = create_object(
            plasma_client, data_size, metadata_size, seal=True)

        # Sleep before submitting task to local scheduler.
        time.sleep(0.1)
        # Submit a task to Redis.
        task = local_scheduler.Task(
            random_driver_id(), random_function_id(),
            [local_scheduler.ObjectID(object_dep.binary())],
            num_return_vals[0], random_task_id(), 0)
        self.local_scheduler_clients[0].submit(task)
        time.sleep(0.1)
        # There should now be a task in Redis, and it should get assigned to
        # the local scheduler
        num_retries = 10
        while num_retries > 0:
            task_entries = self.state.task_table()
            self.assertLessEqual(len(task_entries), 1)
            if len(task_entries) == 1:
                task_id, task = task_entries.popitem()
                task_status = task["State"]
                self.assertTrue(task_status in [
                    state.TASK_STATUS_WAITING, state.TASK_STATUS_SCHEDULED,
                    state.TASK_STATUS_QUEUED
                ])
                if task_status == state.TASK_STATUS_QUEUED:
                    break
                else:
                    print(task_status)
            print("The task has not been scheduled yet, trying again.")
            num_retries -= 1
            time.sleep(1)

        if num_retries <= 0 and task_status != state.TASK_STATUS_QUEUED:
            # Failed to submit and schedule a single task -- bail.
            self.tearDown()
            sys.exit(1)

    def integration_many_tasks_helper(self, timesync=True):
        # There should be three db clients, the global scheduler, the local
        # scheduler, and the plasma manager.
        self.assertEqual(
            len(self.state.client_table()[self.node_ip_address]),
            2 * NUM_CLUSTER_NODES + 1)
        num_return_vals = [0, 1, 2, 3, 5, 10]

        # Submit a bunch of tasks to Redis.
        num_tasks = 1000
        for _ in range(num_tasks):
            # Create a new object for each task.
            data_size = np.random.randint(1 << 12)
            metadata_size = np.random.randint(1 << 9)
            plasma_client = self.plasma_clients[0]
            object_dep, memory_buffer, metadata = create_object(
                plasma_client, data_size, metadata_size, seal=True)
            if timesync:
                # Give 10ms for object info handler to fire (long enough to
                # yield CPU).
                time.sleep(0.010)
            task = local_scheduler.Task(
                random_driver_id(), random_function_id(),
                [local_scheduler.ObjectID(object_dep.binary())],
                num_return_vals[0], random_task_id(), 0)
            self.local_scheduler_clients[0].submit(task)
        # Check that there are the correct number of tasks in Redis and that
        # they all get assigned to the local scheduler.
        num_retries = 20
        num_tasks_done = 0
        while num_retries > 0:
            task_entries = self.state.task_table()
            self.assertLessEqual(len(task_entries), num_tasks)
            # First, check if all tasks made it to Redis.
            if len(task_entries) == num_tasks:
                task_statuses = [
                    task_entry["State"]
                    for task_entry in task_entries.values()
                ]
                self.assertTrue(
                    all(status in [
                        state.TASK_STATUS_WAITING, state.TASK_STATUS_SCHEDULED,
                        state.TASK_STATUS_QUEUED
                    ] for status in task_statuses))
                num_tasks_done = task_statuses.count(state.TASK_STATUS_QUEUED)
                num_tasks_scheduled = task_statuses.count(
                    state.TASK_STATUS_SCHEDULED)
                num_tasks_waiting = task_statuses.count(
                    state.TASK_STATUS_WAITING)
                print("tasks in Redis = {}, tasks waiting = {}, "
                      "tasks scheduled = {}, "
                      "tasks queued = {}, retries left = {}".format(
                          len(task_entries), num_tasks_waiting,
                          num_tasks_scheduled, num_tasks_done, num_retries))
                if all(status == state.TASK_STATUS_QUEUED
                       for status in task_statuses):
                    # We're done, so pass.
                    break
            num_retries -= 1
            time.sleep(0.1)

        # Tasks can either be queued or in the global scheduler due to
        # spillback.
        self.assertEqual(num_tasks_done + num_tasks_waiting, num_tasks)

    @unittest.skipIf(
        os.environ.get("RAY_USE_NEW_GCS", False),
        "New GCS API doesn't have a Python API yet.")
    def test_integration_many_tasks_handler_sync(self):
        self.integration_many_tasks_helper(timesync=True)

    @unittest.skipIf(
        os.environ.get("RAY_USE_NEW_GCS", False),
        "New GCS API doesn't have a Python API yet.")
    def test_integration_many_tasks(self):
        # More realistic case: should handle out of order object and task
        # notifications.
        self.integration_many_tasks_helper(timesync=False)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Pop the argument so we don't mess with unittest's own argument
        # parser.
        if sys.argv[-1] == "valgrind":
            arg = sys.argv.pop()
            USE_VALGRIND = True
            print("Using valgrind for tests")
    unittest.main(verbosity=2)
