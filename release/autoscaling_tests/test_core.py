import ray
from ray._private.test_utils import wait_for_condition
from ray.autoscaler.v2.tests.util import (
    NodeCountCheck,
    TotalResourceCheck,
    check_cluster,
)
import time
from logger import logger

ray.init("auto")

# Sync with the compute config.
HEAD_NODE_CPU = 0
WORKER_NODE_CPU = 4
IDLE_TERMINATION_S = 60 * 5  # 5 min
DEFAULT_RETRY_INTERVAL_MS = 15 * 1000  # 15 sec


ctx = {
    "num_cpus": 0,
    "num_nodes": 1,
}
logger.info(f"Starting cluster with {ctx['num_nodes']} nodes, {ctx['num_cpus']} cpus")
check_cluster(
    [
        NodeCountCheck(ctx["num_nodes"]),
        TotalResourceCheck({"CPU": ctx["num_cpus"]}),
    ]
)


# Request for cluster resources
def test_request_cluster_resources(ctx: dict):
    from ray.autoscaler._private.commands import request_resources

    request_resources(num_cpus=8)

    ctx["num_cpus"] += 8
    ctx["num_nodes"] += 8 // WORKER_NODE_CPU

    # Assert on number of worker nodes.
    logger.info(
        f"Requesting cluster constraints: {ctx['num_nodes']} nodes, "
        f"{ctx['num_cpus']} cpus"
    )
    wait_for_condition(
        check_cluster,
        timeout=60 * 5,  # 5min
        retry_interval_ms=DEFAULT_RETRY_INTERVAL_MS,
        targets=[
            NodeCountCheck(ctx["num_nodes"]),
            TotalResourceCheck({"CPU": ctx["num_cpus"]}),
        ],
    )

    # Reset the cluster constraints.
    request_resources(num_cpus=0)

    ctx["num_cpus"] -= 8
    ctx["num_nodes"] -= 8 // WORKER_NODE_CPU
    logger.info(
        f"Waiting for cluster go idle after constraint cleared: {ctx['num_nodes']} "
        f"nodes, {ctx['num_cpus']} cpus"
    )
    wait_for_condition(
        check_cluster,
        timeout=60 + IDLE_TERMINATION_S,  # 1min + idle timeout
        retry_interval_ms=DEFAULT_RETRY_INTERVAL_MS,
        targets=[
            NodeCountCheck(ctx["num_nodes"]),
            TotalResourceCheck({"CPU": ctx["num_cpus"]}),
        ],
    )


# Run actors/tasks that exceed the cluster resources should upscale the cluster
def test_run_tasks_concurrent(ctx: dict):
    num_tasks = 2
    num_actors = 2

    @ray.remote(num_cpus=WORKER_NODE_CPU)
    def f():
        while True:
            time.sleep(1)

    @ray.remote(num_cpus=WORKER_NODE_CPU)
    class Actor:
        def __init__(self):
            pass

    tasks = [f.remote() for _ in range(num_tasks)]
    actors = [Actor.remote() for _ in range(num_actors)]

    ctx["num_cpus"] += (num_tasks + num_actors) * WORKER_NODE_CPU
    ctx["num_nodes"] += num_tasks + num_actors

    logger.info(f"Waiting for {ctx['num_nodes']} nodes, {ctx['num_cpus']} cpus")
    wait_for_condition(
        check_cluster,
        timeout=60 * 5,  # 5min
        retry_interval_ms=DEFAULT_RETRY_INTERVAL_MS,
        targets=[
            NodeCountCheck(ctx["num_nodes"]),
            TotalResourceCheck({"CPU": ctx["num_cpus"]}),
        ],
    )

    [ray.cancel(task) for task in tasks]
    [ray.kill(actor) for actor in actors]

    ctx["num_cpus"] -= (num_actors + num_tasks) * WORKER_NODE_CPU
    ctx["num_nodes"] -= num_actors + num_tasks

    logger.info(
        f"Waiting for cluster to scale down to {ctx['num_nodes']} nodes, "
        f"{ctx['num_cpus']} cpus"
    )
    wait_for_condition(
        check_cluster,
        timeout=60 + IDLE_TERMINATION_S,
        retry_interval_ms=DEFAULT_RETRY_INTERVAL_MS,
        targets=[
            NodeCountCheck(ctx["num_nodes"]),
            TotalResourceCheck({"CPU": ctx["num_cpus"]}),
        ],
    )


test_request_cluster_resources(ctx)
test_run_tasks_concurrent(ctx)
