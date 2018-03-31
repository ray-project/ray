import argparse

import ray
from worker import Worker, logger
from ray.utils import random_string


parser = argparse.ArgumentParser()
parser.add_argument("raylet_socket_name")
parser.add_argument("object_store_socket_name")


def submit_task_withdep(driver_handle, task_object_dependencies=[]):
    ''' submit a task that depend on a list of @args'''
    task = ray.local_scheduler.Task(
        ray.local_scheduler.ObjectID(random_string()),
        ray.local_scheduler.ObjectID(random_string()),
        task_object_dependencies,
        1,  # num_returns
        ray.local_scheduler.ObjectID(random_string()),
        0)
    logger.debug("[DRIVER]: submitting task ", task.task_id())
    driver_handle.node_manager_client.submit(task)
    logger.debug("[DRIVER]: task return values", task.returns())
    return task.returns()


def submit_tasks_nodep(driver_handle, num_tasks):
    ''' submit a task that depend on a list of @args'''
    for i in range(num_tasks):
        task = ray.local_scheduler.Task(
            ray.local_scheduler.ObjectID(random_string()),
            ray.local_scheduler.ObjectID(random_string()),
            [],
            1,  # num_returns
            ray.local_scheduler.ObjectID(random_string()),
            0)

        logger.debug("[DRIVER]: submitting task ", task.task_id())
        driver_handle.node_manager_client.submit(task)
        logger.debug("[DRIVER]: task return values", task.returns())


def submit_task_chains(num_chains, tasks_per_chain):
    # return task placement map on output
    chain_returns = []
    task_placement_map_ = {}
    for chain_num in range(num_chains):
        last_task_returns = []
        task_placement_map_[chain_num] = []
        for i in range(tasks_per_chain):
            task_returns = submit_task_withdep(
                driver,
                task_object_dependencies=last_task_returns)
            last_task_returns = task_returns
            task_placement_map_[chain_num].append(task_returns[0])
        chain_returns.append(last_task_returns)

    logger.debug("chain_returns=", chain_returns)
    chain_results = driver.get([r[0] for r in chain_returns], timeout_ms=5000)
    print("[DRIVER]: chain return values: ", chain_results)

    return task_placement_map_


def TEST_run_task_chains(num_chains, tasks_per_chain):
    task_placement_map = submit_task_chains(num_chains=num_chains,
                                            tasks_per_chain=tasks_per_chain)
    logger.debug("[DRIVER]: task placement information, per chain:")
    task_placement_total = []
    for chain_num in range(len(task_placement_map)):
        task_placement_list = driver.get(task_placement_map[chain_num],
                                         timeout_ms=5000)
        task_placement_total += [t[1] for t in task_placement_list]
        logger.debug(chain_num, task_placement_list)
    logger.debug("task placement overall: ", task_placement_total)
    task_placement_stats = [(v, task_placement_total.count(v))
                            for v in set(task_placement_total)]
    num_total_tasks = sum([t[1] for t in task_placement_stats])
    print("total tasks executed = ", num_total_tasks)
    assert(num_total_tasks == num_chains * tasks_per_chain)
    print("task placement breakdown: total=", task_placement_stats)


def TEST_run_tasks_nodep(num_tasks):
    # This test is the same as having num_tasks chains with 1 task per chain
    # In this test we assume the num_tasks x 1 chain structure.
    task_placement_map = submit_task_chains(num_chains=num_tasks,
                                            tasks_per_chain=1)
    logger.debug("[DRIVER]: task placement information, per chain:")
    task_placement_total = []
    for chain_num in range(len(task_placement_map)):
        task_placement_list = driver.get(task_placement_map[chain_num],
                                         timeout_ms=5000)
        task_placement_total += [t[1] for t in task_placement_list]
        logger.debug(chain_num, task_placement_list)
    logger.debug("task placement overall: ", task_placement_total)
    task_placement_stats = [(v, task_placement_total.count(v)) for v in
                            set(task_placement_total)]
    num_total_tasks = sum([t[1] for t in task_placement_stats])
    print("total tasks executed = ", num_total_tasks)
    assert(num_total_tasks == num_tasks)
    print("task placement breakdown: total=", task_placement_stats)


if __name__ == '__main__':
    args = parser.parse_args()

    driver = Worker(args.raylet_socket_name, args.object_store_socket_name,
                    is_worker=False)

    # Set up the experiment : number of chains and tasks per chain.
    # TEST_run_task_chains(num_chains=10, tasks_per_chain=100)

    TEST_run_tasks_nodep(10000)
