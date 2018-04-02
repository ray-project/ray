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

    task = ray.local_scheduler.Task(
        ray.local_scheduler.ObjectID(random_string()),
        ray.local_scheduler.ObjectID(random_string()),
        [],
        1,
        ray.local_scheduler.ObjectID(random_string()),
        0)
    logger.debug("submitting %s", task.task_id())
    driver.node_manager_client.submit(task)

    logger.debug("Return values were %s", task.returns())
    task2 = ray.local_scheduler.Task(
        ray.local_scheduler.ObjectID(random_string()),
        ray.local_scheduler.ObjectID(random_string()),
        task.returns(),
        1,
        ray.local_scheduler.ObjectID(random_string()),
        0)
    logger.debug("Submitting dependent task 2 %s", task2.task_id())
    driver.node_manager_client.submit(task2)

    # Make sure the tasks get executed and we can get the result of the last
    # task.
    obj = driver.get(task2.returns(), timeout_ms=1000)
