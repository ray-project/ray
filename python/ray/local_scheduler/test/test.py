from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import os
import signal
import sys
import threading
import time
import unittest

from ray.function_manager import FunctionDescriptor
import ray.local_scheduler as local_scheduler
from ray.local_scheduler import Task
import ray.plasma as plasma
import ray.ray_constants as ray_constants
import pyarrow as pa

USE_VALGRIND = False

NIL_WORKER_ID = ray_constants.ID_SIZE * b"\xff"


def random_object_id():
    return local_scheduler.ObjectID(np.random.bytes(ray_constants.ID_SIZE))


def random_driver_id():
    return local_scheduler.ObjectID(np.random.bytes(ray_constants.ID_SIZE))


def random_task_id():
    return local_scheduler.ObjectID(np.random.bytes(ray_constants.ID_SIZE))


def random_function_id():
    return local_scheduler.ObjectID(np.random.bytes(ray_constants.ID_SIZE))


class TestLocalSchedulerClient(unittest.TestCase):
    def setUp(self):
        # Start Plasma store.
        plasma_store_name, self.p1 = plasma.start_plasma_store()
        self.plasma_client = pa.plasma.connect(plasma_store_name, "", 0)
        # Start a local scheduler.
        scheduler_name, self.p2 = local_scheduler.start_local_scheduler(
            plasma_store_name, use_valgrind=USE_VALGRIND)
        # Connect to the scheduler.
        self.local_scheduler_client = local_scheduler.LocalSchedulerClient(
            scheduler_name, NIL_WORKER_ID, False, random_task_id(), False)

    def tearDown(self):
        # Check that the processes are still alive.
        self.assertEqual(self.p1.poll(), None)
        self.assertEqual(self.p2.poll(), None)

        # Kill Plasma.
        self.p1.kill()
        # Kill the local scheduler.
        if USE_VALGRIND:
            self.p2.send_signal(signal.SIGTERM)
            self.p2.wait()
            if self.p2.returncode != 0:
                os._exit(-1)
        else:
            self.p2.kill()

    def test_submit_and_get_task(self):
        function_id = random_function_id()
        func_desc = FunctionDescriptor.from_function_id(random_function_id())
        func_desc_list = func_desc.get_function_descriptor_list()
        object_ids = [random_object_id() for i in range(256)]
        # Create and seal the objects in the object store so that we can
        # schedule all of the subsequent tasks.
        for object_id in object_ids:
            self.plasma_client.create(pa.plasma.ObjectID(object_id.id()), 0)
            self.plasma_client.seal(pa.plasma.ObjectID(object_id.id()))
        # Define some arguments to use for the tasks.
        args_list = [[], [{}], [()], 1 * [1], 10 * [1], 100 * [1], 1000 * [1],
                     1 * ["a"], 10 * ["a"], 100 * ["a"], 1000 * ["a"], [
                         1, 1.3, 1 << 100, "hi", u"hi", [1, 2]
                     ], object_ids[:1], object_ids[:2], object_ids[:3],
                     object_ids[:4], object_ids[:5], object_ids[:10],
                     object_ids[:100], object_ids[:256], [1, object_ids[0]], [
                         object_ids[0], "a"
                     ], [1, object_ids[0], "a"], [
                         object_ids[0], 1, object_ids[1], "a"
                     ], object_ids[:3] + [1, "hi", 2.3] + object_ids[:5],
                     object_ids + 100 * ["a"] + object_ids]

        for args in args_list:
            for num_return_vals in [0, 1, 2, 3, 5, 10, 100]:
                task = local_scheduler.Task(random_driver_id(), func_desc_list,
                                            args, num_return_vals,
                                            random_task_id(), 0)
                # Submit a task.
                self.local_scheduler_client.submit(task)
                # Get the task.
                new_task = self.local_scheduler_client.get_task()
                self.assertEqual(task.function_descriptor_list(),
                                 new_task.function_descriptor_list())
                retrieved_args = new_task.arguments()
                returns = new_task.returns()
                self.assertEqual(len(args), len(retrieved_args))
                self.assertEqual(num_return_vals, len(returns))
                for i in range(len(retrieved_args)):
                    if isinstance(args[i], local_scheduler.ObjectID):
                        self.assertEqual(args[i].id(), retrieved_args[i].id())
                    else:
                        self.assertEqual(args[i], retrieved_args[i])

        # Submit all of the tasks.
        for args in args_list:
            for num_return_vals in [0, 1, 2, 3, 5, 10, 100]:
                task = local_scheduler.Task(random_driver_id(), func_desc_list,
                                            args, num_return_vals,
                                            random_task_id(), 0)
                self.local_scheduler_client.submit(task)
        # Get all of the tasks.
        for args in args_list:
            for num_return_vals in [0, 1, 2, 3, 5, 10, 100]:
                new_task = self.local_scheduler_client.get_task()

    def test_scheduling_when_objects_ready(self):
        # Create a task and submit it.
        object_id = random_object_id()
        function_id = random_function_id()
        func_desc = FunctionDescriptor.from_function_id(random_function_id())
        func_desc_list = func_desc.get_function_descriptor_list()
        task = local_scheduler.Task(random_driver_id(), func_desc_list,
                                    [object_id], 0, random_task_id(), 0)
        self.local_scheduler_client.submit(task)

        # Launch a thread to get the task.
        def get_task():
            self.local_scheduler_client.get_task()

        t = threading.Thread(target=get_task)
        t.start()
        # Sleep to give the thread time to call get_task.
        time.sleep(0.1)
        # Create and seal the object ID in the object store. This should
        # trigger a scheduling event.
        self.plasma_client.create(pa.plasma.ObjectID(object_id.id()), 0)
        self.plasma_client.seal(pa.plasma.ObjectID(object_id.id()))
        # Wait until the thread finishes so that we know the task was
        # scheduled.
        t.join()

    def test_scheduling_when_objects_evicted(self):
        # Create a task with two dependencies and submit it.
        object_id1 = random_object_id()
        object_id2 = random_object_id()
        function_id = random_function_id()
        func_desc = FunctionDescriptor.from_function_id(random_function_id())
        func_desc_list = func_desc.get_function_descriptor_list()
        task = local_scheduler.Task(random_driver_id(), func_desc_list,
                                    [object_id1, object_id2], 0,
                                    random_task_id(), 0)
        self.local_scheduler_client.submit(task)

        # Launch a thread to get the task.
        def get_task():
            self.local_scheduler_client.get_task()

        t = threading.Thread(target=get_task)
        t.start()

        # Make one of the dependencies available.
        buf = self.plasma_client.create(pa.plasma.ObjectID(object_id1.id()), 1)
        self.plasma_client.seal(pa.plasma.ObjectID(object_id1.id()))
        # Release the object.
        del buf
        # Check that the thread is still waiting for a task.
        time.sleep(0.1)
        self.assertTrue(t.is_alive())
        # Force eviction of the first dependency.
        self.plasma_client.evict(plasma.DEFAULT_PLASMA_STORE_MEMORY)
        # Check that the thread is still waiting for a task.
        time.sleep(0.1)
        self.assertTrue(t.is_alive())
        # Check that the first object dependency was evicted.
        object1 = self.plasma_client.get_buffers(
            [pa.plasma.ObjectID(object_id1.id())], timeout_ms=0)
        self.assertEqual(object1, [None])
        # Check that the thread is still waiting for a task.
        time.sleep(0.1)
        self.assertTrue(t.is_alive())

        # Create the second dependency.
        self.plasma_client.create(pa.plasma.ObjectID(object_id2.id()), 1)
        self.plasma_client.seal(pa.plasma.ObjectID(object_id2.id()))
        # Check that the thread is still waiting for a task.
        time.sleep(0.1)
        self.assertTrue(t.is_alive())

        # Create the first dependency again. Both dependencies are now
        # available.
        self.plasma_client.create(pa.plasma.ObjectID(object_id1.id()), 1)
        self.plasma_client.seal(pa.plasma.ObjectID(object_id1.id()))

        # Wait until the thread finishes so that we know the task was
        # scheduled.
        t.join()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Pop the argument so we don't mess with unittest's own argument
        # parser.
        if sys.argv[-1] == "valgrind":
            arg = sys.argv.pop()
            USE_VALGRIND = True
            print("Using valgrind for tests")
    unittest.main(verbosity=2)
