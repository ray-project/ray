from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import time

import ray
from ray.test.test_utils import (_wait_for_nodes_to_join, _broadcast_event,
                                 _wait_for_event)

# This test should be run with 5 nodes, which have 0, 0, 1, 5, and 10 GPUs for
# a total of 16 GPUs. It should be run with a large number of drivers (e.g.,
# 100). At most 10 drivers will run at a time, and each driver will use at most
# 3 GPUs (this is ceil(16 / 7), which guarantees that we will always be able
# to make progress).
total_num_nodes = 5
max_concurrent_drivers = 7
num_gpus_per_driver = 3


@ray.remote(num_cpus=0, num_gpus=1)
class Actor1(object):
    def __init__(self):
        assert len(ray.get_gpu_ids()) == 1

    def check_ids(self):
        assert len(ray.get_gpu_ids()) == 1


def driver(redis_address, driver_index):
    """The script for all drivers.

    This driver should create five actors that each use one GPU. After a while,
    it should exit.
    """
    ray.init(redis_address=redis_address)

    # Wait for all the nodes to join the cluster.
    _wait_for_nodes_to_join(total_num_nodes)

    # Limit the number of drivers running concurrently.
    for i in range(driver_index - max_concurrent_drivers + 1):
        _wait_for_event("DRIVER_{}_DONE".format(i), redis_address)

    def try_to_create_actor(actor_class, timeout=500):
        # Try to create an actor, but allow failures while we wait for the
        # monitor to release the resources for the removed drivers.
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                actor = actor_class.remote()
            except Exception as e:
                time.sleep(0.1)
            else:
                return actor
        # If we are here, then we timed out while looping.
        raise Exception("Timed out while trying to create actor.")

    # Create some actors that require one GPU.
    actors_one_gpu = []
    for _ in range(num_gpus_per_driver):
        actors_one_gpu.append(try_to_create_actor(Actor1))

    for _ in range(100):
        ray.get([actor.check_ids.remote() for actor in actors_one_gpu])

    _broadcast_event("DRIVER_{}_DONE".format(driver_index), redis_address)


if __name__ == "__main__":
    driver_index = int(os.environ["RAY_DRIVER_INDEX"])
    redis_address = os.environ["RAY_REDIS_ADDRESS"]
    print("Driver {} started at {}.".format(driver_index, time.time()))

    # In this test, all drivers will run the same script.
    driver(redis_address, driver_index)

    print("Driver {} finished at {}.".format(driver_index, time.time()))
