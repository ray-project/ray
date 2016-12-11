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
        ray.init(start_ray_local=True, num_workers=num_workers, num_local_schedulers=num_local_schedulers)

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
        ray.init(start_ray_local=True, num_workers=num_workers, num_local_schedulers=num_local_schedulers)

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
    ray.init(start_ray_local=True, num_workers=1)

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
        ray.init(start_ray_local=True, num_workers=num_workers, num_local_schedulers=num_local_schedulers)

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

if __name__ == "__main__":
  unittest.main(verbosity=2)
