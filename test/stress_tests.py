from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import ray
import numpy as np
import time
import redis

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

  num_local_schedulers = 1

  def setUp(self):
    # Start a Redis instance and Plasma store instances with a total of 1GB
    # memory.
    node_ip_address = "127.0.0.1"
    self.redis_port = ray.services.new_port()
    print(self.redis_port)
    redis_address = ray.services.address(node_ip_address, self.redis_port)
    self.plasma_store_memory = 10 ** 9
    plasma_addresses = []
    objstore_memory = (self.plasma_store_memory // self.num_local_schedulers)
    for i in range(self.num_local_schedulers):
      plasma_addresses.append(
          ray.services.start_objstore(node_ip_address, redis_address,
                                      objstore_memory=objstore_memory)
          )
    address_info = {
        "redis_address": redis_address,
        "object_store_addresses": plasma_addresses,
        }

    # Start the rest of the services in the Ray cluster. NOTE(swang): We need
    # to use two workers per local scheduler to prevent deadlock for testPut.
    ray.worker._init(address_info=address_info, start_ray_local=True,
                     num_workers=self.num_local_schedulers * 2,
                     num_local_schedulers=self.num_local_schedulers)

  def tearDown(self):
    self.assertTrue(ray.services.all_processes_alive())

    # Make sure that all nodes in the cluster were used by checking where tasks
    # were scheduled and/or submitted from.
    r = redis.StrictRedis(port=self.redis_port)
    task_ids = r.keys("TT:*")
    task_ids = [task_id[3:] for task_id in task_ids]
    node_ids = [r.execute_command("ray.task_table_get", task_id)[1] for task_id
                in task_ids]
    self.assertEqual(len(set(node_ids)), self.num_local_schedulers)

    # Clean up the Ray cluster.
    ray.worker.cleanup()

  def testSimple(self):
    # Define the size of one task's return argument so that the combined sum of
    # all objects' sizes is at least twice the plasma stores' combined allotted
    # memory.
    num_objects = 1000
    size = self.plasma_store_memory * 2 // (num_objects * 8)

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
    # old values should be evicted.
    for i in range(num_objects):
      value = ray.get(args[i])
      self.assertEqual(value[0], i)
    # Get each value again to force reconstruction.
    for i in range(num_objects):
      value = ray.get(args[i])
      self.assertEqual(value[0], i)

  def testRecursive(self):
    # Define the size of one task's return argument so that the combined sum of
    # all objects' sizes is at least twice the plasma stores' combined allotted
    # memory.
    num_objects = 1000
    size = self.plasma_store_memory * 2 // (num_objects * 8)

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

    # Launch num_objects instances of the remote task, each dependent on the
    # one before it.
    arg = no_dependency_task.remote(size)
    args = []
    for i in range(num_objects):
      arg = single_dependency.remote(i, arg)
      args.append(arg)

    # Get each value to force each task to finish. After some number of gets,
    # old values should be evicted.
    for i in range(num_objects):
      value = ray.get(args[i])
      self.assertEqual(value[0], i)
    # Get each value again to force reconstruction.
    for i in range(num_objects):
      value = ray.get(args[i])
      self.assertEqual(value[0], i)
    # Get 10 values randomly.
    for _ in range(10):
      i  = np.random.randint(num_objects)
      value = ray.get(args[i])
      self.assertEqual(value[0], i)

  def testMultipleRecursive(self):
    # Define the size of one task's return argument so that the combined sum of
    # all objects' sizes is at least twice the plasma stores' combined allotted
    # memory.
    num_objects = 1000
    size = self.plasma_store_memory * 2 // (num_objects * 8)

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

    # Get each value to force each task to finish. After some number of gets,
    # old values should be evicted.
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
      i  = np.random.randint(num_objects)
      value = ray.get(args[i])
      self.assertEqual(value[0], i)

  def testPut(self):
    # Define the size of one task's return argument so that the combined sum of
    # all objects' sizes is at least twice the plasma stores' combined allotted
    # memory.
    num_objects = 1000
    size = self.plasma_store_memory * 2 // (num_objects * 8)

    # This task puts an array of size `size`, with the first index equal to
    # `i`, into the object store and returns the resulting object ID.
    @ray.remote
    def put_task(i, size):
      array = np.zeros(size)
      array[0] = i
      put_id = ray.put(array)
      return [put_id]

    # This task copies the given array, updates its first index with `i`, and
    # returns it.
    @ray.remote
    def put_arg_task(i, arg):
      arg = np.copy(arg)
      arg[0] = i
      return arg

    # This task submits a remote task with a large numpy array as the argument.
    # Ray internally puts the large array into the object store. The task
    # returns the result of the remote task, which is the same array with the
    # first index updated.
    @ray.remote
    def put_args_task(i, size):
      array = np.zeros(size)
      return ray.get(put_arg_task.remote(i, array))

    # Test direct calls to ray.put. Launch num_objects instances of the remote
    # task that does a put.
    args = []
    for i in range(num_objects):
      arg = put_task.remote(i, size)
      args.append(arg)

    # Get the result of each put to force each task to finish. After some
    # number of gets, old values should be evicted.
    for i in range(num_objects):
      value = ray.get(args[i])
      value = ray.get(value[0])
      self.assertEqual(value[0], i)
    # Get each value again to force reconstruction.
    for i in range(num_objects):
      value = ray.get(args[i])
      value = ray.get(value[0])
      self.assertEqual(value[0], i)
    # Get 10 values randomly.
    for _ in range(10):
      i  = np.random.randint(num_objects)
      value = ray.get(args[i])
      value = ray.get(value[0])
      self.assertEqual(value[0], i)

    # Test puts of remote task arguments. Launch num_objects instances of the
    # remote task that calls a remote task with a large argument.
    args = []
    for i in range(num_objects):
      arg = put_args_task.remote(i, size)
      # TODO(swang): We get each value as we submit to prevent deadlock, since
      # each of these tasks submits and then blocks on a remote task.
      value = ray.get(arg)
      self.assertEqual(value[0], i)
      args.append(arg)
    # Get each value again to force reconstruction.
    for i in range(num_objects):
      value = ray.get(args[i])
      self.assertEqual(value[0], i)
    # Get 10 values randomly.
    for _ in range(10):
      i  = np.random.randint(num_objects)
      value = ray.get(args[i])
      self.assertEqual(value[0], i)


  # TODO(swang): The following test case is a gnarly example of deadlock due to
  # ray.put! The root_task function does a ray.put to store the initial
  # argument. Then, it launches a chain of dependencies, where each remote task
  # is dependent on the one before it. By the time the last task has finished,
  # the initial argument will have been evicted. To reconstruct the initial
  # argument, root_task needs to be run again. Even if the job manages to
  # return, it will continue to eat up workers, as each instance of root_task
  # will resubmit another instance of root_task to reconstruct the initial
  # argument.
  #def testPut(self):
  #  # Define the size of one task's return argument so that the combined sum of
  #  # all objects' sizes is at least twice the plasma stores' combined allotted
  #  # memory.
  #  num_objects = 1000
  #  size = self.plasma_store_memory * 2 // (num_objects * 8)

  #  # Define a task with a single dependency, which returns its one argument.
  #  @ray.remote
  #  def single_dependency(i, arg):
  #    arg = np.copy(arg)
  #    arg[0] = i
  #    return arg

  #  @ray.remote
  #  def root_task(size):
  #    array = np.zeros(size)
  #    arg = ray.put(array)

  #    # Launch num_objects instances of the remote task, each dependent on the
  #    # one before it.
  #    args = []
  #    for i in range(num_objects):
  #      arg = single_dependency.remote(i, arg)
  #      args.append(arg)

  #    # Get each value to force each task to finish. After some number of gets,
  #    # old values should be evicted.
  #    for i in range(num_objects):
  #      value = ray.get(args[i])
  #      self.assertEqual(value[0], i)
  #    # Get each value again to force reconstruction.
  #    for i in range(num_objects):
  #      value = ray.get(args[i])
  #      self.assertEqual(value[0], i)
  #    # Get 10 values randomly.
  #    for _ in range(10):
  #      i  = np.random.randint(num_objects)
  #      value = ray.get(args[i])
  #      self.assertEqual(value[0], i)

  #    return True

  #  ray.get(root_task.remote(size))

class ReconstructionTestsMultinode(ReconstructionTests):

  # Run the same tests as the single-node suite, but with 4 local schedulers,
  # two workers each.
  num_local_schedulers = 4

if __name__ == "__main__":
  unittest.main(verbosity=2)
