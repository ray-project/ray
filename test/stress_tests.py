from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import ray
import numpy as np
import time

class TaskTests(unittest.TestCase):

  def testSubmittingTasks(self):
    for num_local_schedulers in [1, 4]:
      for num_workers_per_scheduler in [4]:
        num_workers = num_local_schedulers * num_workers_per_scheduler
        ray.worker._init(start_ray_local=True, num_workers=num_workers,
                         num_local_schedulers=num_local_schedulers)

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
        ray.worker._init(start_ray_local=True, num_workers=num_workers,
                         num_local_schedulers=num_local_schedulers)

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

  def testGettingAndPutting(self):
    ray.init(num_workers=1)

    for n in range(8):
      x = np.zeros(10 ** n)

      for _ in range(100):
        ray.put(x)

      x_id = ray.put(x)
      for _ in range(1000):
        ray.get(x_id)

    self.assertTrue(ray.services.all_processes_alive())
    ray.worker.cleanup()

  def testWait(self):
    for num_local_schedulers in [1, 4]:
      for num_workers_per_scheduler in [4]:
        num_workers = num_local_schedulers * num_workers_per_scheduler
        ray.worker._init(start_ray_local=True, num_workers=num_workers,
                         num_local_schedulers=num_local_schedulers)

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
          x_ids = [g.remote(np.random.uniform(0, i)) for _ in range(2 * num_workers)]
          ray.wait(x_ids, num_returns=len(x_ids))

        self.assertTrue(ray.services.all_processes_alive())
        ray.worker.cleanup()

class ReconstructionTests(unittest.TestCase):

  def setUp(self):
    # Start a Redis instance and a Plasma store instance with 1GB memory.
    node_ip_address = "127.0.0.1"
    redis_address = ray.services.start_redis(node_ip_address)
    self.plasma_store_memory = 10 ** 9
    plasma_address = ray.services.start_objstore(node_ip_address,
                                                 redis_address,
                                                 objstore_memory=self.plasma_store_memory)
    address_info = {
        "redis_address": redis_address,
        "object_store_addresses": [plasma_address],
        }

    # Start the rest of the services in the Ray cluster.
    ray.worker._init(address_info=address_info, start_ray_local=True,
                     num_workers=1)

  def tearDown(self):
    # Clean up the Ray cluster.
    self.assertTrue(ray.services.all_processes_alive())
    ray.worker.cleanup()

  def testSingleNodeSimple(self):
    # Define the size of one task's return argument so that the combined sum of
    # all objects' sizes is twice the plasma store's allotted memory.
    num_objects = 1000
    size = int(self.plasma_store_memory * 2 / (num_objects * 8))

    # Define a remote task with no dependencies, which returns a numpy array of
    # the given size.
    @ray.remote
    def foo(i, size):
      array = np.zeros(size)
      array[0] = i
      return array

    # Launch num_objects instances of the remote task.
    args = []
    for i in range(num_objects):
      args.append(foo.remote(i, size))

    # Get each value to force each task to finish. After some number of gets,
    # old values should be evicted. Get each value again to force
    # reconstruction.
    for i in range(num_objects):
      value = ray.get(args[i])
      self.assertEqual(value[0], i)
    for i in range(num_objects):
      value = ray.get(args[i])
      self.assertEqual(value[0], i)

  def testSingleNodeRecursive(self):
    # Define the size of one task's return argument so that the combined sum of
    # all objects' sizes is twice the plasma store's allotted memory.
    num_iterations = 1000
    size = int(self.plasma_store_memory * 2 / (num_iterations * 8))

    # Define a root task with no dependencies, which returns a numpy array of
    # the given size.
    @ray.remote
    def no_dependency_task(size):
      array = np.zeros(size)
      return array

    # Define a task with a single dependency, which returns its one argument.
    @ray.remote
    def single_dependency(i, arg):
      arg = np.copy(arg)
      arg[0] = i
      return arg

    # Launch num_iterations instances of the remote task, each dependent on the
    # one before it.
    arg = no_dependency_task.remote(size)
    args = []
    for i in range(num_iterations):
      args.append(single_dependency.remote(i, arg))

    # Get each value to force each task to finish. After some number of gets,
    # old values should be evicted. Get each value again to force
    # reconstruction.
    for i in range(num_iterations):
      value = ray.get(args[i])
      self.assertEqual(value[0], i)
    for i in range(num_iterations):
      value = ray.get(args[i])
      self.assertEqual(value[0], i)

    self.assertTrue(ray.services.all_processes_alive())
    ray.worker.cleanup()

  def testSingleNodeMultipleRecursive(self):
    # Define the size of one task's return argument so that the combined sum of
    # all objects' sizes is twice the plasma store's allotted memory.
    num_iterations = 1000
    size = int(self.plasma_store_memory * 2 / (num_iterations * 8))

    # Define a root task with no dependencies, which returns a numpy array of
    # the given size.
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

    # Launch num_args instances of the root task. Then launch num_iterations
    # instances of the multi-dependency remote task, each dependent on the
    # num_args tasks before it.
    num_args = 3
    args = []
    for i in range(num_args):
      arg = no_dependency_task.remote(size)
      args.append(arg)
    for i in range(num_iterations):
      args.append(multiple_dependency.remote(i, *args[i:i+num_args]))

    # Get each value to force each task to finish. After some number of gets,
    # old values should be evicted. Get each value again to force
    # reconstruction.
    args = args[num_args:]
    for i in range(num_iterations):
      value = ray.get(args[i])
      self.assertEqual(value[0], i)
    for i in range(num_iterations):
      value = ray.get(args[i])
      self.assertEqual(value[0], i)

    self.assertTrue(ray.services.all_processes_alive())
    ray.worker.cleanup()

if __name__ == "__main__":
  unittest.main(verbosity=2)
