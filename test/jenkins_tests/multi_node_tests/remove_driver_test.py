from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import redis
import time

import ray

# This test should be run with 5 nodes, which have 0, 1, 2, 3, and 4 GPUs for a
# total of 10 GPUs. It shoudl be run with 3 drivers.
total_num_nodes = 5

EVENT_KEY = "RAY_DOCKER_TEST_KEY"


def _wait_for_nodes_to_join(num_nodes, timeout=20):
  """Wait until the nodes have joined the cluster.

  Args:
    num_nodes: The number of nodes to wait for.
    timeout: The amount of time in seconds to wait before failing.
  """
  start_time = time.time()
  while time.time() - start_time < timeout:
    num_ready_nodes = len(ray.global_state.client_table())
    if num_ready_nodes == num_nodes:
      return
    if num_ready_nodes > num_nodes:
      # Too many nodes have joined. Something must be wrong.
      raise Exception("{} nodes have joined the cluster, but we were "
                      "expecting {} nodes.".format(num_ready_nodes, num_nodes))
    time.sleep(0.1)

  # If we get here then we timed out.
  raise Exception("Timed out while waiting for {} nodes to join. Only {} "
                  "nodes have joined so far.".format(num_ready_nodes,
                                                     num_nodes))


def _broadcast_event(event_name, redis_address):
  """Broadcast an event.

  Args:
    event_name: The name of the event to wait for.
    redis_address: The address of the Redis server to use for synchronization.

  This is used to synchronize drivers for the multi-node tests.
  """
  redis_host, redis_port = redis_address.split(":")
  redis_client = redis.StrictRedis(host=redis_host, port=int(redis_port))
  redis_client.rpush(EVENT_KEY, event_name)


def _wait_for_event(event_name, redis_address, extra_buffer=1):
  """Block until an event has been broadcast.

  Args:
    event_name: The name of the event to wait for.
    redis_address: The address of the Redis server to use for synchronization.
    extra_buffer: An amount of time in seconds to wait after the event.

  This is used to synchronize drivers for the multi-node tests.
  """
  redis_host, redis_port = redis_address.split(":")
  redis_client = redis.StrictRedis(host=redis_host, port=int(redis_port))
  while True:
    event_names = redis_client.lrange(EVENT_KEY, 0, -1)
    if event_name.encode("ascii") in event_names:
      break
  time.sleep(extra_buffer)


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
  """The first driver script.

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
  """The second driver script.

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
  """The second driver script.

  This driver should wait for the first two drivers to finish. Then it should
  create some actors that use a total of ten GPUs.
  """
  ray.init(redis_address=redis_address)

  _wait_for_event("DRIVER_0_DONE", redis_address)
  _wait_for_event("DRIVER_1_DONE", redis_address)

  # Create some actors that require two GPUs.
  actors_two_gpus = [Actor2() for _ in range(3)]
  # Create some actors that require one GPU.
  actors_one_gpu = [Actor1() for _ in range(4)]
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
