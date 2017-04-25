from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import psutil
import time

import ray
from ray.test.multi_node_tests import (_wait_for_nodes_to_join,
                                       _broadcast_event,
                                       _wait_for_event)

# This test should be run with 5 nodes, which have 0, 1, 2, 3, and 4 GPUs for a
# total of 10 GPUs. It should be run with 3 drivers. Driver 2 has to run on the
# same node as driver 0 and driver 1 so that it can check if the PIDs of
# certain processes created by those drivers have exited.
total_num_nodes = 5


def pid_alive(pid):
  """Check if the process with this PID is alive or not.

  Args:
    pid: The pid to check.

  Returns:
    This returns false if the process is dead or defunct. Otherwise, it returns
      true.
  """
  try:
    os.kill(pid, 0)
  except OSError:
    return False
  else:
    if psutil.Process(pid).status() == psutil.STATUS_ZOMBIE:
      return False
    else:
      return True


def actor_event_name(driver_index, actor_index):
  return "DRIVER_{}_ACTOR_{}_RUNNING".format(driver_index, actor_index)


def remote_function_event_name(driver_index, task_index):
  return "DRIVER_{}_TASK_{}_RUNNING".format(driver_index, task_index)


@ray.remote
def long_running_task(driver_index, task_index, redis_address):
  _broadcast_event(remote_function_event_name(driver_index, task_index),
                   redis_address, data=os.getpid())
  # Loop forever.
  while True:
    time.sleep(100)


@ray.actor
class Actor0(object):
  def __init__(self, driver_index, actor_index, redis_address):
    _broadcast_event(actor_event_name(driver_index, actor_index),
                     redis_address, data=os.getpid())
    assert len(ray.get_gpu_ids()) == 0

  def check_ids(self):
    assert len(ray.get_gpu_ids()) == 0


@ray.actor(num_gpus=1)
class Actor1(object):
  def __init__(self, driver_index, actor_index, redis_address):
    _broadcast_event(actor_event_name(driver_index, actor_index),
                     redis_address, data=os.getpid())
    assert len(ray.get_gpu_ids()) == 1

  def check_ids(self):
    assert len(ray.get_gpu_ids()) == 1


@ray.actor(num_gpus=2)
class Actor2(object):
  def __init__(self, driver_index, actor_index, redis_address):
    _broadcast_event(actor_event_name(driver_index, actor_index),
                     redis_address, data=os.getpid())
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

  # Start a long running task. Driver 2 will make sure the worker running this
  # task has been killed.
  long_running_task.remote(0, 0, redis_address)

  # Create some actors that require one GPU.
  actors_one_gpu = [Actor1(0, i, redis_address) for i in range(5)]
  # Create some actors that don't require any GPUs.
  actors_no_gpus = [Actor0(0, 5 + i, redis_address) for i in range(5)]

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

  # Start a long running task. Driver 2 will make sure the worker running this
  # task has been killed.
  long_running_task.remote(1, 0, redis_address)

  # Create an actor that requires two GPUs.
  actors_two_gpus = [Actor2(1, i, redis_address) for i in range(1)]
  # Create some actors that require one GPU.
  actors_one_gpu = [Actor1(1, 1 + i, redis_address) for i in range(3)]
  # Create some actors that don't require any GPUs.
  actors_no_gpus = [Actor0(1, 1 + 3 + i, redis_address) for i in range(5)]

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

  # We go ahead and create some actors that don't require any GPUs. We don't
  # need to wait for the other drivers to finish. We call methods on these
  # actors later to make sure they haven't been killed.
  actors_no_gpus = [Actor0(2, i, redis_address) for i in range(10)]

  _wait_for_event("DRIVER_0_DONE", redis_address)
  _wait_for_event("DRIVER_1_DONE", redis_address)

  def try_to_create_actor(actor_class, driver_index, actor_index, timeout=20):
    # Try to create an actor, but allow failures while we wait for the monitor
    # to release the resources for the removed drivers.
    start_time = time.time()
    while time.time() - start_time < timeout:
      try:
        actor = actor_class(driver_index, actor_index, redis_address)
      except Exception as e:
        time.sleep(0.1)
      else:
        return actor
    # If we are here, then we timed out while looping.
    raise Exception("Timed out while trying to create actor.")

  # Create some actors that require two GPUs.
  actors_two_gpus = []
  for i in range(3):
    actors_two_gpus.append(try_to_create_actor(Actor2, 2, 10 + i))
  # Create some actors that require one GPU.
  actors_one_gpu = []
  for i in range(4):
    actors_one_gpu.append(try_to_create_actor(Actor1, 2, 10 + 3 + i))

  def wait_for_pid_to_exit(pid, timeout=20):
    start_time = time.time()
    while time.time() - start_time < timeout:
      if not pid_alive(pid):
        return
      time.sleep(0.1)
    raise Exception("Timed out while waiting for process to exit.")

  # Make sure that the PIDs for the long-running tasks from driver 0 and driver
  # 1 have been killed.
  pid = _wait_for_event(remote_function_event_name(0, 0), redis_address)
  wait_for_pid_to_exit(pid)
  pid = _wait_for_event(remote_function_event_name(1, 0), redis_address)
  wait_for_pid_to_exit(pid)
  # Make sure that the PIDs for the actors from driver 0 and driver 1 have been
  # killed.
  for i in range(10):
    pid = _wait_for_event(actor_event_name(0, i), redis_address)
    wait_for_pid_to_exit(pid)
  for i in range(9):
    pid = _wait_for_event(actor_event_name(0, i), redis_address)
    wait_for_pid_to_exit(pid)

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
