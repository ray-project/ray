from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import time

import ray
from ray.test.multi_node_tests import (_wait_for_nodes_to_join,
                                       _broadcast_event,
                                       _wait_for_event)

# This test should be run with 5 nodes, which have 0, 1, 2, 3, and 4 GPUs for a
# total of 10 GPUs. It shoudl be run with 3 drivers.
total_num_nodes = 5


@ray.actor
class Actor0(object):
  def __init__(self):
    assert len(ray.get_gpu_ids()) == 0

  def check_ids(self):
    assert len(ray.get_gpu_ids()) == 0


@ray.actor(num_gpus=1)
class Actor1(object):
  def __init__(self):
    assert len(ray.get_gpu_ids()) == 1

  def check_ids(self):
    assert len(ray.get_gpu_ids()) == 1


@ray.actor(num_gpus=2)
class Actor2(object):
  def __init__(self):
    assert len(ray.get_gpu_ids()) == 2

  def check_ids(self):
    assert len(ray.get_gpu_ids()) == 2


def driver_0(redis_address):
  """The script for driver 0.

  This driver should create five actors that each use one GPU and some actors
  that use no GPUs. After a while, it should exit.
  """
  ray.init(redis_address=redis_address)

  # Wait for all the nodes to join the cluster.
  _wait_for_nodes_to_join(total_num_nodes)

  # Create some actors that require one GPU.
  actors_one_gpu = [Actor1() for _ in range(5)]
  # Create some actors that don't require any GPUs.
  actors_no_gpus = [Actor0() for _ in range(5)]

  for _ in range(1000):
    ray.get([actor.check_ids() for actor in actors_one_gpu])
    ray.get([actor.check_ids() for actor in actors_no_gpus])

  _broadcast_event("DRIVER_0_DONE", redis_address)


def driver_1(redis_address):
  """The script for driver 1.

  This driver should create one actor that uses two GPUs, three actors that
  each use one GPU (the one requiring two must be created first), and some
  actors that don't use any GPUs. After a while, it should exit.
  """
  ray.init(redis_address=redis_address)

  # Wait for all the nodes to join the cluster.
  _wait_for_nodes_to_join(total_num_nodes)

  # Create an actor that requires two GPUs.
  actors_two_gpus = [Actor2() for _ in range(1)]
  # Create some actors that require one GPU.
  actors_one_gpu = [Actor1() for _ in range(3)]
  # Create some actors that don't require any GPUs.
  actors_no_gpus = [Actor0() for _ in range(5)]

  for _ in range(1000):
    ray.get([actor.check_ids() for actor in actors_two_gpus])
    ray.get([actor.check_ids() for actor in actors_one_gpu])
    ray.get([actor.check_ids() for actor in actors_no_gpus])

  _broadcast_event("DRIVER_1_DONE", redis_address)


def driver_2(redis_address):
  """The script for driver 2.

  This driver should wait for the first two drivers to finish. Then it should
  create some actors that use a total of ten GPUs.
  """
  ray.init(redis_address=redis_address)

  _wait_for_event("DRIVER_0_DONE", redis_address)
  _wait_for_event("DRIVER_1_DONE", redis_address)

  def try_to_create_actor(actor_class, timeout=20):
    # Try to create an actor, but allow failures while we wait for the monitor
    # to release the resources for the removed drivers.
    start_time = time.time()
    while time.time() - start_time < timeout:
      try:
        actor = actor_class()
      except Exception as e:
        time.sleep(0.1)
      else:
        return actor
    # If we are here, then we timed out while looping.
    raise Exception("Timed out while trying to create actor.")

  # Create some actors that require two GPUs.
  actors_two_gpus = []
  for _ in range(3):
    actors_two_gpus.append(try_to_create_actor(Actor2))
  # Create some actors that require one GPU.
  actors_one_gpu = []
  for _ in range(4):
    actors_one_gpu.append(try_to_create_actor(Actor1))
  # Create some actors that don't require any GPUs.
  actors_no_gpus = [Actor0() for _ in range(5)]

  for _ in range(1000):
    ray.get([actor.check_ids() for actor in actors_two_gpus])
    ray.get([actor.check_ids() for actor in actors_one_gpu])
    ray.get([actor.check_ids() for actor in actors_no_gpus])

  _broadcast_event("DRIVER_2_DONE", redis_address)


if __name__ == "__main__":
  driver_index = int(os.environ["RAY_DRIVER_INDEX"])
  redis_address = os.environ["RAY_REDIS_ADDRESS"]
  print("Driver {} started at {}.".format(driver_index, time.time()))

  if driver_index == 0:
    driver_0(redis_address)
  elif driver_index == 1:
    driver_1(redis_address)
  elif driver_index == 2:
    driver_2(redis_address)
  else:
    raise Exception("This code should be unreachable.")

  print("Driver {} finished at {}.".format(driver_index, time.time()))
