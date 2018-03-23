import argparse

import ray
from worker import Worker, logger
from ray.utils import random_string


parser = argparse.ArgumentParser()
parser.add_argument("raylet_socket_name")
parser.add_argument("object_store_socket_name")

def submit_task(driver_handle, task_object_dependencies=[]):
    ''' submit a task that depend on a list of @args'''
    task = ray.local_scheduler.Task(
        ray.local_scheduler.ObjectID(random_string()),
        ray.local_scheduler.ObjectID(random_string()),
        task_object_dependencies,
        1, #num_returns
        ray.local_scheduler.ObjectID(random_string()),
        0)
    print("[DRIVER]: submitting task ", task.task_id())
    driver_handle.node_manager_client.submit(task)
    print("[DRIVER]: task return values", task.returns())
    return task.returns()

if __name__ == '__main__':
    args = parser.parse_args()

    driver = Worker(args.raylet_socket_name, args.object_store_socket_name,
                    is_worker=False)

    chain_returns = []
    num_chains=10
    chain_length=100
    for chain in range(num_chains):
        last_task_returns = []
        for i in range(chain_length):
            task_returns = submit_task(driver, last_task_returns)
            print("[DRIVER] task return values", task_returns)
            prev_task_returns = task_returns
        chain_returns.append(last_task_returns)

    chain_results = [driver.get(_, timeout_ms=1000) for _ in chain_returns]

    print("[DRIVER]: return values from each chain ", chain_results)
