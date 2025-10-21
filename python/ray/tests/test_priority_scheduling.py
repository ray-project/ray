import sys
import time

import pytest

import ray


def test_single_node_two_priorities(ray_start_cluster):
    """
    Kicks off 100 low priority tasks followed by 100 high priority tasks.
    Ensures that after the first 20 tasks complete, the completion
    ordering should be high priority tasks -> low priority tasks.
    """
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=5)
    ray.init(address=cluster.address)

    @ray.remote
    def task(priority):
        time.sleep(1)
        return priority

    num_high_priority_tasks = 100
    num_low_priority_tasks = 100

    not_ready = [
        task.options(priority=1).remote(1) for _ in range(num_low_priority_tasks)
    ]
    not_ready.extend(
        [task.options(priority=0).remote(0) for _ in range(num_high_priority_tasks)]
    )

    num_completed = 0
    last_priority = 0
    while len(not_ready) > 0:
        ready, not_ready = ray.wait(not_ready)
        this_priority = ray.get(ready)[0]

        if num_completed > 20:
            if this_priority != last_priority:
                assert this_priority > last_priority
                last_priority = this_priority

        num_completed += 1


def test_many_nodes_many_priorities(ray_start_cluster):
    """
    Start a cluster with 1 head node and 5 workers with 1CPU each.
    Kicks off 100 tasks with priorities 0 to 4.
    Ensures that after the first 20 tasks complete, the completion
    ordering should be high priority tasks -> low priority tasks.
    """

    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    ray.init(address=cluster.address)
    _ = [cluster.add_node(num_cpus=1) for _ in range(5)]

    @ray.remote(num_cpus=1)
    def task(x):
        time.sleep(1)
        return x

    total_num_tasks = 100

    not_ready = []
    task_num = 0
    for i in range(total_num_tasks):
        not_ready.append(task.options(priority=task_num % 5).remote(task_num % 5))
        task_num += 1

    num_completed = 0
    last_priority = 0
    while len(not_ready) > 0:
        ready, not_ready = ray.wait(not_ready)
        this_priority = ray.get(ready)[0]
        if num_completed > 30 and this_priority != last_priority:
            assert this_priority > last_priority
            last_priority = this_priority

        num_completed += 1


def test_many_nodes_many_priorities_with_injection(ray_start_cluster):
    """
    Start a cluster with 1 head node and 5 workers with 1CPU each.
    Kicks off 100 tasks with priorities 0 to 4.
    Injects 50 tasks with priority 0 at the 50th task.
    Ensures that 20 tasks after injection, the completion order is
    high priority tasks -> low priority tasks.
    """
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    ray.init(address=cluster.address)
    _ = [cluster.add_node(num_cpus=1) for _ in range(5)]

    @ray.remote(num_cpus=1)
    def task(x):
        time.sleep(1)
        return x

    total_num_tasks = 100
    num_waiting_when_inject = 50
    num_to_inject_with_p0 = 100

    not_ready = []
    task_num = 0
    for i in range(total_num_tasks):
        not_ready.append(task.options(priority=task_num % 5).remote(task_num % 5))
        task_num += 1

    injected = False
    num_completed_after_injection = 0
    last_priority = 0
    while len(not_ready) > 0:
        ready, not_ready = ray.wait(not_ready)
        this_priority = ray.get(ready)[0]
        if len(not_ready) == num_waiting_when_inject and not injected:
            injected = True
            for i in range(num_to_inject_with_p0):
                not_ready.append(task.options(priority=0).remote(0))
        if injected:
            num_completed_after_injection += 1
        if num_completed_after_injection > 30 and this_priority != last_priority:
            assert this_priority > last_priority
            last_priority = this_priority


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
