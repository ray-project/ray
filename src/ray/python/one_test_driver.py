import argparse

import ray
from worker import Worker, logger
from ray.utils import random_string


parser = argparse.ArgumentParser()
parser.add_argument("raylet_socket_name")
parser.add_argument("object_store_socket_name")

if __name__ == '__main__':
    args = parser.parse_args()

    driver = Worker(args.raylet_socket_name, args.object_store_socket_name,
                    is_worker=False)

    task1 = ray.local_scheduler.Task(
        ray.local_scheduler.ObjectID(random_string()),
        ray.local_scheduler.ObjectID(random_string()),
        [],
        1,
        ray.local_scheduler.ObjectID(random_string()),
        0)
    logger.debug("submitting", task1.task_id())
    driver.node_manager_client.submit(task1)

    logger.debug("Return values were", task1.returns())
    print("[DRIVER] Return values were", task1.returns())
    # Make sure the tasks get executed and we can get the result of the
    # last task
    obj = driver.get(task1.returns(), timeout_ms=1000)
    print("[DRIVER]: task1 driver.get result ", obj)
