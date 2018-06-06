from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import os
import ray
import numpy as np
import time


class TaskTests(unittest.TestCase):
    def testSubmittingTasks(self):
        for num_local_schedulers in [1, 4]:
            for num_workers_per_scheduler in [4]:
                num_workers = num_local_schedulers * num_workers_per_scheduler
                ray.worker._init(
                    start_ray_local=True,
                    num_workers=num_workers,
                    num_local_schedulers=num_local_schedulers,
                    num_cpus=100)

                @ray.remote
                def f(x):
                    return x

                for _ in range(1):
                    ray.get([f.remote(1) for _ in range(1000)])

                for _ in range(10):
                    ray.get([f.remote(1) for _ in range(100)])

                for _ in range(100):
                    ray.get([f.remote(1) for _ in range(10)])

                for _ in range(1000):
                    ray.get([f.remote(1) for _ in range(1)])

                self.assertTrue(ray.services.all_processes_alive())
                ray.worker.cleanup()

    def testDependencies(self):
        for num_local_schedulers in [1, 4]:
            for num_workers_per_scheduler in [4]:
                num_workers = num_local_schedulers * num_workers_per_scheduler
                ray.worker._init(
                    start_ray_local=True,
                    num_workers=num_workers,
                    num_local_schedulers=num_local_schedulers,
                    num_cpus=100)

                @ray.remote
                def f(x):
                    return x

                x = 1
                for _ in range(1000):
                    x = f.remote(x)
                ray.get(x)

                @ray.remote
                def g(*xs):
                    return 1

                xs = [g.remote(1)]
                for _ in range(100):
                    xs.append(g.remote(*xs))
                    xs.append(g.remote(1))
                ray.get(xs)

                self.assertTrue(ray.services.all_processes_alive())
                ray.worker.cleanup()

    @unittest.skipIf(
        os.environ.get("RAY_USE_XRAY") == "1",
        "This test does not work with xray yet.")
    def testSubmittingManyTasks(self):
        ray.init()

        @ray.remote
        def f(x):
            return 1

        def g(n):
            x = 1
            for i in range(n):
                x = f.remote(x)
            return x

        ray.get([g(1000) for _ in range(100)])
        self.assertTrue(ray.services.all_processes_alive())
        ray.worker.cleanup()

    def testGettingAndPutting(self):
        ray.init(num_workers=1)

        for n in range(8):
            x = np.zeros(10**n)

            for _ in range(100):
                ray.put(x)

            x_id = ray.put(x)
            for _ in range(1000):
                ray.get(x_id)

        self.assertTrue(ray.services.all_processes_alive())
        ray.worker.cleanup()

    def testGettingManyObjects(self):
        ray.init()

        @ray.remote
        def f():
            return 1

        n = 10**4  # TODO(pcm): replace by 10 ** 5 once this is faster.
        lst = ray.get([f.remote() for _ in range(n)])
        self.assertEqual(lst, n * [1])

        self.assertTrue(ray.services.all_processes_alive())
        ray.worker.cleanup()

    def testWait(self):
        for num_local_schedulers in [1, 4]:
            for num_workers_per_scheduler in [4]:
                num_workers = num_local_schedulers * num_workers_per_scheduler
                ray.worker._init(
                    start_ray_local=True,
                    num_workers=num_workers,
                    num_local_schedulers=num_local_schedulers,
                    num_cpus=100)

                @ray.remote
                def f(x):
                    return x

                x_ids = [f.remote(i) for i in range(100)]
                for i in range(len(x_ids)):
                    ray.wait([x_ids[i]])
                for i in range(len(x_ids) - 1):
                    ray.wait(x_ids[i:])

                @ray.remote
                def g(x):
                    time.sleep(x)

                for i in range(1, 5):
                    x_ids = [
                        g.remote(np.random.uniform(0, i))
                        for _ in range(2 * num_workers)
                    ]
                    ray.wait(x_ids, num_returns=len(x_ids))

                self.assertTrue(ray.services.all_processes_alive())
                ray.worker.cleanup()


class ReconstructionTests(unittest.TestCase):

    num_local_schedulers = 1

    def setUp(self):
        # Start the Redis global state store.
        node_ip_address = "127.0.0.1"
        redis_address, redis_shards = ray.services.start_redis(node_ip_address)
        self.redis_ip_address = ray.services.get_ip_address(redis_address)
        self.redis_port = ray.services.get_port(redis_address)
        time.sleep(0.1)

        # Start the Plasma store instances with a total of 1GB memory.
        self.plasma_store_memory = 10**9
        plasma_addresses = []
        objstore_memory = (
            self.plasma_store_memory // self.num_local_schedulers)
        for i in range(self.num_local_schedulers):
            store_stdout_file, store_stderr_file = ray.services.new_log_files(
                "plasma_store_{}".format(i), True)
            manager_stdout_file, manager_stderr_file = (
                ray.services.new_log_files("plasma_manager_{}".format(i),
                                           True))
            plasma_addresses.append(
                ray.services.start_objstore(
                    node_ip_address,
                    redis_address,
                    objstore_memory=objstore_memory,
                    store_stdout_file=store_stdout_file,
                    store_stderr_file=store_stderr_file,
                    manager_stdout_file=manager_stdout_file,
                    manager_stderr_file=manager_stderr_file))

        # Start the rest of the services in the Ray cluster.
        address_info = {
            "redis_address": redis_address,
            "redis_shards": redis_shards,
            "object_store_addresses": plasma_addresses
        }
        ray.worker._init(
            address_info=address_info,
            start_ray_local=True,
            num_workers=1,
            num_local_schedulers=self.num_local_schedulers,
            num_cpus=[1] * self.num_local_schedulers,
            redirect_output=True,
            driver_mode=ray.SILENT_MODE)

    def tearDown(self):
        self.assertTrue(ray.services.all_processes_alive())

        # Determine the IDs of all local schedulers that had a task scheduled
        # or submitted.
        state = ray.experimental.state.GlobalState()
        state._initialize_global_state(self.redis_ip_address, self.redis_port)
        if os.environ.get('RAY_USE_NEW_GCS', False):
            tasks = state.task_table()
            local_scheduler_ids = {
                task["LocalSchedulerID"]
                for task in tasks.values()
            }

        # Make sure that all nodes in the cluster were used by checking that
        # the set of local scheduler IDs that had a task scheduled or submitted
        # is equal to the total number of local schedulers started. We add one
        # to the total number of local schedulers to account for
        # NIL_LOCAL_SCHEDULER_ID. This is the local scheduler ID associated
        # with the driver task, since it is not scheduled by a particular local
        # scheduler.
        if os.environ.get('RAY_USE_NEW_GCS', False):
            self.assertEqual(
                len(local_scheduler_ids), self.num_local_schedulers + 1)

        # Clean up the Ray cluster.
        ray.worker.cleanup()

    @unittest.skipIf(
        os.environ.get('RAY_USE_NEW_GCS', False),
        "Failing with new GCS API on Linux.")
    def testSimple(self):
        # Define the size of one task's return argument so that the combined
        # sum of all objects' sizes is at least twice the plasma stores'
        # combined allotted memory.
        num_objects = 1000
        size = int(self.plasma_store_memory * 1.5 / (num_objects * 8))

        # Define a remote task with no dependencies, which returns a numpy
        # array of the given size.
        @ray.remote
        def foo(i, size):
            array = np.zeros(size)
            array[0] = i
            return array

        # Launch num_objects instances of the remote task.
        args = []
        for i in range(num_objects):
            args.append(foo.remote(i, size))

        # Get each value to force each task to finish. After some number of
        # gets, old values should be evicted.
        for i in range(num_objects):
            value = ray.get(args[i])
            self.assertEqual(value[0], i)
        # Get each value again to force reconstruction.
        for i in range(num_objects):
            value = ray.get(args[i])
            self.assertEqual(value[0], i)
        # Get values sequentially, in chunks.
        num_chunks = 4 * self.num_local_schedulers
        chunk = num_objects // num_chunks
        for i in range(num_chunks):
            values = ray.get(args[i * chunk:(i + 1) * chunk])
            del values

    @unittest.skipIf(
        os.environ.get('RAY_USE_NEW_GCS', False), "Failing with new GCS API.")
    def testRecursive(self):
        # Define the size of one task's return argument so that the combined
        # sum of all objects' sizes is at least twice the plasma stores'
        # combined allotted memory.
        num_objects = 1000
        size = int(self.plasma_store_memory * 1.5 / (num_objects * 8))

        # Define a root task with no dependencies, which returns a numpy array
        # of the given size.
        @ray.remote
        def no_dependency_task(size):
            array = np.zeros(size)
            return array

        # Define a task with a single dependency, which returns its one
        # argument.
        @ray.remote
        def single_dependency(i, arg):
            arg = np.copy(arg)
            arg[0] = i
            return arg

        # Launch num_objects instances of the remote task, each dependent on
        # the one before it.
        arg = no_dependency_task.remote(size)
        args = []
        for i in range(num_objects):
            arg = single_dependency.remote(i, arg)
            args.append(arg)

        # Get each value to force each task to finish. After some number of
        # gets, old values should be evicted.
        for i in range(num_objects):
            value = ray.get(args[i])
            self.assertEqual(value[0], i)
        # Get each value again to force reconstruction.
        for i in range(num_objects):
            value = ray.get(args[i])
            self.assertEqual(value[0], i)
        # Get 10 values randomly.
        for _ in range(10):
            i = np.random.randint(num_objects)
            value = ray.get(args[i])
            self.assertEqual(value[0], i)
        # Get values sequentially, in chunks.
        num_chunks = 4 * self.num_local_schedulers
        chunk = num_objects // num_chunks
        for i in range(num_chunks):
            values = ray.get(args[i * chunk:(i + 1) * chunk])
            del values

    @unittest.skipIf(
        os.environ.get('RAY_USE_NEW_GCS', False), "Failing with new GCS API.")
    def testMultipleRecursive(self):
        # Define the size of one task's return argument so that the combined
        # sum of all objects' sizes is at least twice the plasma stores'
        # combined allotted memory.
        num_objects = 1000
        size = self.plasma_store_memory * 2 // (num_objects * 8)

        # Define a root task with no dependencies, which returns a numpy array
        # of the given size.
        @ray.remote
        def no_dependency_task(size):
            array = np.zeros(size)
            return array

        # Define a task with multiple dependencies, which returns its first
        # argument.
        @ray.remote
        def multiple_dependency(i, arg1, arg2, arg3):
            arg1 = np.copy(arg1)
            arg1[0] = i
            return arg1

        # Launch num_args instances of the root task. Then launch num_objects
        # instances of the multi-dependency remote task, each dependent on the
        # num_args tasks before it.
        num_args = 3
        args = []
        for i in range(num_args):
            arg = no_dependency_task.remote(size)
            args.append(arg)
        for i in range(num_objects):
            args.append(multiple_dependency.remote(i, *args[i:i + num_args]))

        # Get each value to force each task to finish. After some number of
        # gets, old values should be evicted.
        args = args[num_args:]
        for i in range(num_objects):
            value = ray.get(args[i])
            self.assertEqual(value[0], i)
        # Get each value again to force reconstruction.
        for i in range(num_objects):
            value = ray.get(args[i])
            self.assertEqual(value[0], i)
        # Get 10 values randomly.
        for _ in range(10):
            i = np.random.randint(num_objects)
            value = ray.get(args[i])
            self.assertEqual(value[0], i)

    def wait_for_errors(self, error_check):
        # Wait for errors from all the nondeterministic tasks.
        errors = []
        time_left = 100
        while time_left > 0:
            errors = ray.error_info()
            if error_check(errors):
                break
            time_left -= 1
            time.sleep(1)

        # Make sure that enough errors came through.
        self.assertTrue(error_check(errors))
        return errors

    @unittest.skipIf(
        os.environ.get('RAY_USE_NEW_GCS', False), "Hanging with new GCS API.")
    def testNondeterministicTask(self):
        # Define the size of one task's return argument so that the combined
        # sum of all objects' sizes is at least twice the plasma stores'
        # combined allotted memory.
        num_objects = 1000
        size = self.plasma_store_memory * 2 // (num_objects * 8)

        # Define a nondeterministic remote task with no dependencies, which
        # returns a random numpy array of the given size. This task should
        # produce an error on the driver if it is ever reexecuted.
        @ray.remote
        def foo(i, size):
            array = np.random.rand(size)
            array[0] = i
            return array

        # Define a deterministic remote task with no dependencies, which
        # returns a numpy array of zeros of the given size.
        @ray.remote
        def bar(i, size):
            array = np.zeros(size)
            array[0] = i
            return array

        # Launch num_objects instances, half deterministic and half
        # nondeterministic.
        args = []
        for i in range(num_objects):
            if i % 2 == 0:
                args.append(foo.remote(i, size))
            else:
                args.append(bar.remote(i, size))

        # Get each value to force each task to finish. After some number of
        # gets, old values should be evicted.
        for i in range(num_objects):
            value = ray.get(args[i])
            self.assertEqual(value[0], i)
        # Get each value again to force reconstruction.
        for i in range(num_objects):
            value = ray.get(args[i])
            self.assertEqual(value[0], i)

        def error_check(errors):
            if self.num_local_schedulers == 1:
                # In a single-node setting, each object is evicted and
                # reconstructed exactly once, so exactly half the objects will
                # produce an error during reconstruction.
                min_errors = num_objects // 2
            else:
                # In a multinode setting, each object is evicted zero or one
                # times, so some of the nondeterministic tasks may not be
                # reexecuted.
                min_errors = 1
            return len(errors) >= min_errors

        errors = self.wait_for_errors(error_check)
        # Make sure all the errors have the correct type.
        self.assertTrue(
            all(error[b"type"] == b"object_hash_mismatch" for error in errors))

    @unittest.skipIf(
        os.environ.get('RAY_USE_NEW_GCS', False), "Hanging with new GCS API.")
    def testDriverPutErrors(self):
        # Define the size of one task's return argument so that the combined
        # sum of all objects' sizes is at least twice the plasma stores'
        # combined allotted memory.
        num_objects = 1000
        size = self.plasma_store_memory * 2 // (num_objects * 8)

        # Define a task with a single dependency, a numpy array, that returns
        # another array.
        @ray.remote
        def single_dependency(i, arg):
            arg = np.copy(arg)
            arg[0] = i
            return arg

        # Launch num_objects instances of the remote task, each dependent on
        # the one before it. The first instance of the task takes a numpy array
        # as an argument, which is put into the object store.
        args = []
        arg = single_dependency.remote(0, np.zeros(size))
        for i in range(num_objects):
            arg = single_dependency.remote(i, arg)
            args.append(arg)
        # Get each value to force each task to finish. After some number of
        # gets, old values should be evicted.
        for i in range(num_objects):
            value = ray.get(args[i])
            self.assertEqual(value[0], i)

        # Get each value starting from the beginning to force reconstruction.
        # Currently, since we're not able to reconstruct `ray.put` objects that
        # were evicted and whose originating tasks are still running, this
        # for-loop should hang on its first iteration and push an error to the
        # driver.
        ray.worker.global_worker.local_scheduler_client.reconstruct_object(
            args[0].id())

        def error_check(errors):
            return len(errors) > 1

        errors = self.wait_for_errors(error_check)
        self.assertTrue(
            all(error[b"type"] == b"put_reconstruction" for error in errors))


class ReconstructionTestsMultinode(ReconstructionTests):

    # Run the same tests as the single-node suite, but with 4 local schedulers,
    # one worker each.
    num_local_schedulers = 4


# NOTE(swang): This test tries to launch 1000 workers and breaks.
# class WorkerPoolTests(unittest.TestCase):
#
#   def tearDown(self):
#     ray.worker.cleanup()
#
#   def testBlockingTasks(self):
#     @ray.remote
#     def f(i, j):
#       return (i, j)
#
#     @ray.remote
#     def g(i):
#       # Each instance of g submits and blocks on the result of another remote
#       # task.
#       object_ids = [f.remote(i, j) for j in range(10)]
#       return ray.get(object_ids)
#
#     ray.init(num_workers=1)
#     ray.get([g.remote(i) for i in range(1000)])
#     ray.worker.cleanup()

if __name__ == "__main__":
    unittest.main(verbosity=2)
