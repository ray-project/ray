from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import time

import ray


if __name__ == "__main__":
  driver_index = int(os.environ["RAY_DRIVER_INDEX"])
  redis_address = os.environ["RAY_REDIS_ADDRESS"]
  print("Driver {} started at {}.".format(driver_index, time.time()))
  ray.worker._init(redis_address=redis_address)

  @ray.remote
  def f(x):
    return x

  x = 1
  for _ in range(1000):
    x = f.remote(x)
  ray.get(x)
  print("Got f", num_local_schedulers)

  @ray.remote
  def g(*xs):
    return 1

  xs = [g.remote(1)]
  for _ in range(100):
    xs.append(g.remote(*xs))
    xs.append(g.remote(1))
  ray.get(xs)
  print("Got g", num_local_schedulers)

  self.assertTrue(ray.services.all_processes_alive())
  ray.worker.cleanup()
  print("Killed ray")
