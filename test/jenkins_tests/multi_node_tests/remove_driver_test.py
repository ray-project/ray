from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import time

import ray
from ray.test.test_utils import (_wait_for_nodes_to_join, _broadcast_event,
                                 _wait_for_event, wait_for_pid_to_exit)

# This test should be run with 5 nodes, which have 0, 1, 2, 3, and 4 GPUs for a
# total of 10 GPUs. It should be run with 7 drivers. Drivers 2 through 6 must
# run on different nodes so they can check if all the relevant workers on all
# the nodes have been killed.
total_num_nodes = 5


def actor_event_name(driver_index, actor_index):
    return "DRIVER_{}_ACTOR_{}_RUNNING".format(driver_index, actor_index)


def remote_function_event_name(driver_index, task_index):
    return "DRIVER_{}_TASK_{}_RUNNING".format(driver_index, task_index)


@ray.remote
def long_running_task(driver_index, task_index, redis_address):
    _broadcast_event(
        remote_function_event_name(driver_index, task_index),
        redis_address,
        data=(ray.services.get_node_ip_address(), os.getpid()))
    # Loop forever.
    while True:
        time.sleep(100)


num_long_running_tasks_per_driver = 2


@ray.remote
class Actor0(object):
    def __init__(self, driver_index, actor_index, redis_address):
        _broadcast_event(
            actor_event_name(driver_index, actor_index),
            redis_address,
            data=(ray.services.get_node_ip_address(), os.getpid()))
        assert len(ray.get_gpu_ids()) == 0

    def check_ids(self):
        assert len(ray.get_gpu_ids()) == 0

    def long_running_method(self):
        # Loop forever.
        while True:
            time.sleep(100)


@ray.remote(num_gpus=1)
class Actor1(object):
    def __init__(self, driver_index, actor_index, redis_address):
        _broadcast_event(
            actor_event_name(driver_index, actor_index),
            redis_address,
            data=(ray.services.get_node_ip_address(), os.getpid()))
        assert len(ray.get_gpu_ids()) == 1

    def check_ids(self):
        assert len(ray.get_gpu_ids()) == 1

    def long_running_method(self):
        # Loop forever.
        while True:
            time.sleep(100)


@ray.remote(num_gpus=2)
class Actor2(object):
    def __init__(self, driver_index, actor_index, redis_address):
        _broadcast_event(
            actor_event_name(driver_index, actor_index),
            redis_address,
            data=(ray.services.get_node_ip_address(), os.getpid()))
        assert len(ray.get_gpu_ids()) == 2

    def check_ids(self):
        assert len(ray.get_gpu_ids()) == 2

    def long_running_method(self):
        # Loop forever.
        while True:
            time.sleep(100)


def driver_0(redis_address, driver_index):
    """The script for driver 0.

    This driver should create five actors that each use one GPU and some actors
    that use no GPUs. After a while, it should exit.
    """
    ray.init(redis_address=redis_address)

    # Wait for all the nodes to join the cluster.
    _wait_for_nodes_to_join(total_num_nodes)

    # Start some long running task. Driver 2 will make sure the worker running
    # this task has been killed.
    for i in range(num_long_running_tasks_per_driver):
        long_running_task.remote(driver_index, i, redis_address)

    # Create some actors that require one GPU.
    actors_one_gpu = [
        Actor1.remote(driver_index, i, redis_address) for i in range(5)
    ]
    # Create some actors that don't require any GPUs.
    actors_no_gpus = [
        Actor0.remote(driver_index, 5 + i, redis_address) for i in range(5)
    ]

    for _ in range(1000):
        ray.get([actor.check_ids.remote() for actor in actors_one_gpu])
        ray.get([actor.check_ids.remote() for actor in actors_no_gpus])

    # Start a long-running method on one actor and make sure this doesn't
    # affect anything.
    actors_no_gpus[0].long_running_method.remote()

    _broadcast_event("DRIVER_0_DONE", redis_address)


def driver_1(redis_address, driver_index):
    """The script for driver 1.

    This driver should create one actor that uses two GPUs, three actors that
    each use one GPU (the one requiring two must be created first), and some
    actors that don't use any GPUs. After a while, it should exit.
    """
    ray.init(redis_address=redis_address)

    # Wait for all the nodes to join the cluster.
    _wait_for_nodes_to_join(total_num_nodes)

    # Start some long running task. Driver 2 will make sure the worker running
    # this task has been killed.
    for i in range(num_long_running_tasks_per_driver):
        long_running_task.remote(driver_index, i, redis_address)

    # Create an actor that requires two GPUs.
    actors_two_gpus = [
        Actor2.remote(driver_index, i, redis_address) for i in range(1)
    ]
    # Create some actors that require one GPU.
    actors_one_gpu = [
        Actor1.remote(driver_index, 1 + i, redis_address) for i in range(3)
    ]
    # Create some actors that don't require any GPUs.
    actors_no_gpus = [
        Actor0.remote(driver_index, 1 + 3 + i, redis_address) for i in range(5)
    ]

    for _ in range(1000):
        ray.get([actor.check_ids.remote() for actor in actors_two_gpus])
        ray.get([actor.check_ids.remote() for actor in actors_one_gpu])
        ray.get([actor.check_ids.remote() for actor in actors_no_gpus])

    # Start a long-running method on one actor and make sure this doesn't
    # affect anything.
    actors_one_gpu[0].long_running_method.remote()

    _broadcast_event("DRIVER_1_DONE", redis_address)


def cleanup_driver(redis_address, driver_index):
    """The script for drivers 2 through 6.

    This driver should wait for the first two drivers to finish. Then it should
    create some actors that use a total of ten GPUs.
    """
    ray.init(redis_address=redis_address)

    # Only one of the cleanup drivers should create more actors.
    if driver_index == 2:
        # We go ahead and create some actors that don't require any GPUs. We
        # don't need to wait for the other drivers to finish. We call methods
        # on these actors later to make sure they haven't been killed.
        actors_no_gpus = [
            Actor0.remote(driver_index, i, redis_address) for i in range(10)
        ]

    _wait_for_event("DRIVER_0_DONE", redis_address)
    _wait_for_event("DRIVER_1_DONE", redis_address)

    def try_to_create_actor(actor_class, driver_index, actor_index,
                            timeout=20):
        # Try to create an actor, but allow failures while we wait for the
        # monitor to release the resources for the removed drivers.
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                actor = actor_class.remote(driver_index, actor_index,
                                           redis_address)
            except Exception as e:
                time.sleep(0.1)
            else:
                return actor
        # If we are here, then we timed out while looping.
        raise Exception("Timed out while trying to create actor.")

    # Only one of the cleanup drivers should create more actors.
    if driver_index == 2:
        # Create some actors that require one GPU.
        actors_one_gpu = []
        for i in range(10):
            actors_one_gpu.append(
                try_to_create_actor(Actor1, driver_index, 10 + 3 + i))

    removed_workers = 0

    # Make sure that the PIDs for the long-running tasks from driver 0 and
    # driver 1 have been killed.
    for i in range(num_long_running_tasks_per_driver):
        node_ip_address, pid = _wait_for_event(
            remote_function_event_name(0, i), redis_address)
        if node_ip_address == ray.services.get_node_ip_address():
            wait_for_pid_to_exit(pid)
            removed_workers += 1
    for i in range(num_long_running_tasks_per_driver):
        node_ip_address, pid = _wait_for_event(
            remote_function_event_name(1, i), redis_address)
        if node_ip_address == ray.services.get_node_ip_address():
            wait_for_pid_to_exit(pid)
            removed_workers += 1
    # Make sure that the PIDs for the actors from driver 0 and driver 1 have
    # been killed.
    for i in range(10):
        node_ip_address, pid = _wait_for_event(
            actor_event_name(0, i), redis_address)
        if node_ip_address == ray.services.get_node_ip_address():
            wait_for_pid_to_exit(pid)
            removed_workers += 1
    for i in range(9):
        node_ip_address, pid = _wait_for_event(
            actor_event_name(1, i), redis_address)
        if node_ip_address == ray.services.get_node_ip_address():
            wait_for_pid_to_exit(pid)
            removed_workers += 1

    print("{} workers/actors were removed on this node."
          .format(removed_workers))

    # Only one of the cleanup drivers should create and use more actors.
    if driver_index == 2:
        for _ in range(1000):
            ray.get([actor.check_ids.remote() for actor in actors_one_gpu])
            ray.get([actor.check_ids.remote() for actor in actors_no_gpus])

    _broadcast_event("DRIVER_{}_DONE".format(driver_index), redis_address)


if __name__ == "__main__":
    driver_index = int(os.environ["RAY_DRIVER_INDEX"])
    redis_address = os.environ["RAY_REDIS_ADDRESS"]
    print("Driver {} started at {}.".format(driver_index, time.time()))

    if driver_index == 0:
        driver_0(redis_address, driver_index)
    elif driver_index == 1:
        driver_1(redis_address, driver_index)
    elif driver_index in [2, 3, 4, 5, 6]:
        cleanup_driver(redis_address, driver_index)
    else:
        raise Exception("This code should be unreachable.")

    print("Driver {} finished at {}.".format(driver_index, time.time()))
