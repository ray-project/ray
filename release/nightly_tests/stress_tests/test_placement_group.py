# This is stress test to run placement group.
# Please don't run it in the cluster
# setup yet. This test uses the cluster util to simulate the
# cluster environment.

import time

from time import perf_counter
from random import random
import json
import logging
import os
import ray

from ray.util.placement_group import placement_group, remove_placement_group

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# TODO(sang): Increase the number in the actual stress test.
# This number should be divisible by 3.
RESOURCE_QUANTITY = 666
NUM_NODES = 5
CUSTOM_RESOURCES = {"pg_custom": RESOURCE_QUANTITY}
# Create pg that uses 1 resource of cpu & custom resource.
NUM_PG = RESOURCE_QUANTITY


@ray.remote(num_cpus=0, resources={"pg_custom": 1}, max_calls=0)
def mock_task():
    time.sleep(0.1)
    return True


@ray.remote(num_cpus=0, resources={"pg_custom": 1}, max_restarts=0)
class MockActor:
    def __init__(self):
        pass

    def ping(self):
        pass


@ray.remote(num_cpus=0)
def pg_launcher(pre_created_pgs, num_pgs_to_create):
    pgs = []
    pgs += pre_created_pgs
    for i in range(num_pgs_to_create):
        pgs.append(placement_group(BUNDLES, strategy="STRICT_SPREAD"))

    pgs_removed = []
    pgs_unremoved = []
    # Randomly choose placement groups to remove.
    for pg in pgs:
        if random() < 0.5:
            pgs_removed.append(pg)
        else:
            pgs_unremoved.append(pg)

    tasks = []
    max_actor_cnt = 5
    actor_cnt = 0
    actors = []
    # Randomly schedule tasks or actors on placement groups that
    # are not removed.
    for pg in pgs_unremoved:
        # TODO(sang): Comment in this line causes GCS actor management
        # failure. We need to fix it.
        if random() < 0.5:
            tasks.append(mock_task.options(placement_group=pg).remote())
        else:
            if actor_cnt < max_actor_cnt:
                actors.append(MockActor.options(placement_group=pg).remote())
                actor_cnt += 1

    # Remove the rest of placement groups.
    for pg in pgs_removed:
        remove_placement_group(pg)

    ray.get([pg.ready() for pg in pgs_unremoved])
    ray.get(tasks)
    ray.get([actor.ping.remote() for actor in actors])
    # Since placement groups are scheduled, remove them.
    for pg in pgs_unremoved:
        remove_placement_group(pg)


if __name__ == "__main__":
    result = {"success": 0}

    # Wait until the expected number of nodes have joined the cluster.
    ray.init(address="auto")
    while True:
        num_nodes = len(ray.nodes())
        logger.info("Waiting for nodes {}/{}".format(num_nodes, NUM_NODES + 1))
        if num_nodes >= NUM_NODES + 1:
            break
        time.sleep(5)
    logger.info(
        "Nodes have all joined. There are %s resources.", ray.cluster_resources()
    )

    # Scenario 1: Create bunch of placement groups and measure how long
    # it takes.
    total_creating_time = 0
    total_removing_time = 0
    repeat = 1
    total_trial = repeat * NUM_PG
    BUNDLES = [{"pg_custom": 1}] * NUM_NODES

    # Create and remove placement groups.
    for _ in range(repeat):
        pgs = []
        for i in range(NUM_PG):
            start = perf_counter()
            pgs.append(placement_group(BUNDLES, strategy="PACK"))
            end = perf_counter()
            logger.info(f"append_group iteration {i}")
            total_creating_time += end - start

        ray.get([pg.ready() for pg in pgs])

        for i, pg in enumerate(pgs):
            start = perf_counter()
            remove_placement_group(pg)
            end = perf_counter()
            logger.info(f"remove_group iteration {i}")
            total_removing_time += end - start

    # Validate the correctness.
    assert ray.cluster_resources()["pg_custom"] == NUM_NODES * RESOURCE_QUANTITY

    # Scenario 2:
    # - Launch 30% of placement group in the driver and pass them.
    # - Launch 70% of placement group at each remote tasks.
    # - Randomly remove placement groups and schedule tasks and actors.
    #
    # Goal:
    # - Make sure jobs are done without breaking GCS server.
    # - Make sure all the resources are recovered after the job is done.
    # - Measure the creation latency in the stressful environment.
    pre_created_num_pgs = round(NUM_PG * 0.3)
    num_pgs_to_create = NUM_PG - pre_created_num_pgs
    pg_launchers = []
    for i in range(3):
        pre_created_pgs = [
            placement_group(BUNDLES, strategy="STRICT_SPREAD")
            for _ in range(pre_created_num_pgs // 3)
        ]
        pg_launchers.append(pg_launcher.remote(pre_created_pgs, num_pgs_to_create // 3))

    ray.get(pg_launchers)
    assert ray.cluster_resources()["pg_custom"] == NUM_NODES * RESOURCE_QUANTITY

    result["avg_pg_create_time_ms"] = total_creating_time / total_trial * 1000
    result["avg_pg_remove_time_ms"] = total_removing_time / total_trial * 1000
    result["success"] = 1
    print(
        "Avg placement group creating time: "
        f"{total_creating_time / total_trial * 1000} ms"
    )
    print(
        "Avg placement group removing time: "
        f"{total_removing_time / total_trial* 1000} ms"
    )
    print("PASSED.")

    with open(os.environ["TEST_OUTPUT_JSON"], "w") as out_put:
        out_put.write(json.dumps(result))
