#!/usr/bin/env python

import argparse
from collections import defaultdict
import numpy as np
import json
import logging
import os
import time

import ray

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ray.init(address="auto")


@ray.remote(num_cpus=1)
def f(size, *xs):
    return np.ones(size, dtype=np.uint8)


@ray.remote(num_cpus=1)
class Actor(object):
    def method(self, size, *xs):
        return np.ones(size, dtype=np.uint8)


# Stage 0: Submit a bunch of small tasks with large returns.
def stage0(smoke=False):
    num_tasks = 1000
    size = 1000000

    if smoke:
        num_tasks //= 25
        size //= 25

    stage_0_iterations = []
    start_time = time.time()
    logger.info("Submitting many tasks with large returns.")
    for i in range(10):
        iteration_start = time.time()
        logger.info("Iteration %s", i)
        ray.get([f.remote(size) for _ in range(num_tasks)])
        stage_0_iterations.append(time.time() - iteration_start)

    return time.time() - start_time


# Stage 1: Launch a bunch of tasks.
def stage1(smoke=False):
    num_tasks = 100000

    if smoke:
        num_tasks //= 25

    stage_1_iterations = []
    start_time = time.time()
    logger.info("Submitting many tasks.")
    for i in range(10):
        iteration_start = time.time()
        logger.info("Iteration %s", i)
        ray.get([f.remote(0) for _ in range(num_tasks)])
        stage_1_iterations.append(time.time() - iteration_start)

    return time.time() - start_time, stage_1_iterations


# Launch a bunch of tasks, each with a bunch of dependencies. TODO(rkn): This
# test starts to fail if we increase the number of tasks in the inner loop from
# 500 to 1000. (approximately 615 seconds)
def stage2(smoke=False):
    num_tasks_per_iteration = 500

    if smoke:
        num_tasks_per_iteration //= 25

    stage_2_iterations = []
    start_time = time.time()
    logger.info("Submitting tasks with many dependencies.")
    x_ids = []
    for _ in range(5):
        iteration_start = time.time()
        for i in range(20):
            logger.info(
                "Iteration %s. Cumulative time %s seconds", i, time.time() - start_time
            )
            x_ids = [f.remote(0, *x_ids) for _ in range(num_tasks_per_iteration)]
        ray.get(x_ids)
        stage_2_iterations.append(time.time() - iteration_start)
        logger.info("Finished after %s seconds.", time.time() - start_time)
    return time.time() - start_time, stage_2_iterations


# Create a bunch of actors.
def stage3(total_num_remote_cpus, smoke=False):
    start_time = time.time()
    logger.info("Creating %s actors.", total_num_remote_cpus)
    actors = [Actor.remote() for _ in range(total_num_remote_cpus)]
    stage_3_creation_time = time.time() - start_time
    logger.info("Finished stage 3 actor creation in %s seconds.", stage_3_creation_time)

    num_tasks = 1000

    if smoke:
        num_tasks //= 25

    # Submit a bunch of small tasks to each actor. (approximately 1070 seconds)
    start_time = time.time()
    logger.info("Submitting many small actor tasks.")
    for N in [num_tasks, num_tasks * 100]:
        x_ids = []
        for i in range(N):
            x_ids = [a.method.remote(0) for a in actors]
            if i % 100 == 0:
                logger.info("Submitted {}".format(i * len(actors)))
        ray.get(x_ids)
    return time.time() - start_time, stage_3_creation_time


# This tests https://github.com/ray-project/ray/issues/10150. The only way to
# integration test this is via performance. The goal is to fill up the cluster
# so that all tasks can be run, but spillback is required. Since the driver
# submits all these tasks it should easily be able to schedule each task in
# O(1) iterative spillback queries. If spillback behavior is incorrect, each
# task will require O(N) queries. Since we limit the number of inflight
# requests, we will run into head of line blocking and we should be able to
# measure this timing.
def stage4():
    num_tasks = int(ray.cluster_resources()["CPU"])
    logger.info("Scheduling many tasks for spillback.")

    @ray.remote(num_cpus=1)
    def func(t):
        if t % 100 == 0:
            logger.info(f"[spillback test] {t}/{num_tasks}")
        start = time.perf_counter()
        time.sleep(1)
        end = time.perf_counter()
        return start, end, ray.worker.global_worker.node.unique_id

    results = ray.get([func.remote(i) for i in range(num_tasks)])

    host_to_start_times = defaultdict(list)
    for start, end, host in results:
        host_to_start_times[host].append(start)

    spreads = []
    for host in host_to_start_times:
        last = max(host_to_start_times[host])
        first = min(host_to_start_times[host])
        spread = last - first
        spreads.append(spread)
        logger.info(f"Spread: {last - first}\tLast: {last}\tFirst: {first}")

    avg_spread = sum(spreads) / len(spreads)
    logger.info(f"Avg spread: {sum(spreads)/len(spreads)}")
    return avg_spread


def parse_script_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-nodes", type=int, default=100)
    parser.add_argument("--smoke-test", action="store_true")
    return parser.parse_known_args()


if __name__ == "__main__":
    args, unknown = parse_script_args()
    # These numbers need to correspond with the autoscaler config file.
    # The number of remote nodes in the autoscaler should upper bound
    # these because sometimes nodes fail to update.
    num_remote_nodes = args.num_nodes
    num_remote_cpus = 2
    total_num_remote_cpus = num_remote_nodes * num_remote_cpus
    is_smoke_test = args.smoke_test

    result = {"success": 0}
    # Wait until the expected number of nodes have joined the cluster.
    while True:
        num_nodes = len(ray.nodes())
        logger.info("Waiting for nodes {}/{}".format(num_nodes, num_remote_nodes + 1))
        if num_nodes >= num_remote_nodes + 1:
            break
        time.sleep(5)
    logger.info(
        "Nodes have all joined. There are %s resources.", ray.cluster_resources()
    )

    stage_0_time = stage0(smoke=is_smoke_test)
    logger.info("Finished stage 0 after %s seconds.", stage_0_time)
    result["stage_0_time"] = stage_0_time

    stage_1_time, stage_1_iterations = stage1(smoke=is_smoke_test)
    logger.info("Finished stage 1 after %s seconds.", stage_1_time)
    result["stage_1_time"] = stage_1_time
    result["stage_1_avg_iteration_time"] = sum(stage_1_iterations) / len(
        stage_1_iterations
    )
    result["stage_1_max_iteration_time"] = max(stage_1_iterations)
    result["stage_1_min_iteration_time"] = min(stage_1_iterations)

    stage_2_time, stage_2_iterations = stage2(smoke=is_smoke_test)
    logger.info("Finished stage 2 after %s seconds.", stage_2_time)
    result["stage_2_time"] = stage_2_time
    result["stage_2_avg_iteration_time"] = sum(stage_2_iterations) / len(
        stage_2_iterations
    )
    result["stage_2_max_iteration_time"] = max(stage_2_iterations)
    result["stage_2_min_iteration_time"] = min(stage_2_iterations)

    stage_3_time, stage_3_creation_time = stage3(
        total_num_remote_cpus, smoke=is_smoke_test
    )
    logger.info("Finished stage 3 in %s seconds.", stage_3_time)
    result["stage_3_creation_time"] = stage_3_creation_time
    result["stage_3_time"] = stage_3_time

    stage_4_spread = stage4()
    # avg_spread ~ 115 with Ray 1.0 scheduler. ~695 with (buggy) 0.8.7
    # scheduler.
    result["stage_4_spread"] = stage_4_spread
    result["success"] = 1
    print("PASSED.")

    # TODO(rkn): The test below is commented out because it currently
    # does not pass.
    # # Submit a bunch of actor tasks with all-to-all communication.
    # start_time = time.time()
    # logger.info("Submitting actor tasks with all-to-all communication.")
    # x_ids = []
    # for _ in range(50):
    #     for size_exponent in [0, 1, 2, 3, 4, 5, 6]:
    #         x_ids = [a.method.remote(10**size_exponent, *x_ids) for a
    #                  in actors]
    # ray.get(x_ids)
    # logger.info("Finished after %s seconds.", time.time() - start_time)

    with open(os.environ["TEST_OUTPUT_JSON"], "w") as out_put:
        out_put.write(json.dumps(result))
