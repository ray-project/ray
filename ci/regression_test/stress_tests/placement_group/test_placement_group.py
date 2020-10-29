# This is stress test to run placement group.
# Please don't run it in the cluster
# setup yet. This test uses the cluster util to simulate the
# cluster environment.

import time

from time import perf_counter
from random import random

import ray

from ray.cluster_utils import Cluster
from ray.util.placement_group import (placement_group, remove_placement_group)

# TODO(sang): Increase the number in the actual stress test.
# This number should be divisible by 3.
resource_quantity = 666
num_nodes = 5
custom_resources = {"pg_custom": resource_quantity}
# Create pg that uses 1 resource of cpu & custom resource.
num_pg = resource_quantity

# TODO(sang): Cluster setup. Remove when running in real clusters.
cluster = Cluster()
nodes = []
for _ in range(num_nodes):
    nodes.append(
        cluster.add_node(
            num_cpus=3, num_gpus=resource_quantity,
            resources=custom_resources))
cluster.wait_for_nodes()

ray.init(address=cluster.address)
while not ray.is_initialized():
    time.sleep(0.1)

# Scenario 1: Create bunch of placement groups and measure how long it takes.
total_creating_time = 0
total_removing_time = 0
repeat = 1
total_trial = repeat * num_pg
bundles = [{"GPU": 1, "pg_custom": 1}] * num_nodes

# Create and remove placement groups.
for _ in range(repeat):
    pgs = []
    for i in range(num_pg):
        start = perf_counter()
        pgs.append(placement_group(bundles, strategy="PACK", name=str(i)))
        end = perf_counter()
        total_creating_time += (end - start)

    ray.get([pg.ready() for pg in pgs])

    for pg in pgs:
        start = perf_counter()
        remove_placement_group(pg)
        end = perf_counter()
        total_removing_time += (end - start)

# Validate the correctness.
assert ray.cluster_resources()["GPU"] == num_nodes * resource_quantity
assert ray.cluster_resources()["pg_custom"] == num_nodes * resource_quantity


# Scenario 2:
# - Launch 30% of placement group in the driver and pass them.
# - Launch 70% of placement group at each remote tasks.
# - Randomly remove placement groups and schedule tasks and actors.
#
# Goal:
# - Make sure jobs are done without breaking GCS server.
# - Make sure all the resources are recovered after the job is done.
# - Measure the creation latency in the stressful environment.
@ray.remote(num_cpus=0, num_gpus=1, max_calls=0)
def mock_task():
    time.sleep(0.1)
    return True


@ray.remote(num_cpus=0, num_gpus=1, max_restarts=0)
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
        pgs.append(
            placement_group(bundles, strategy="STRICT_SPREAD", name=str(i)))

    pgs_removed = []
    pgs_unremoved = []
    # Randomly choose placement groups to remove.
    for pg in pgs:
        if random() < .5:
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
        if random() < .5:
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


pre_created_num_pgs = round(num_pg * 0.3)
num_pgs_to_create = num_pg - pre_created_num_pgs
pg_launchers = []
for i in range(3):
    pre_created_pgs = [
        placement_group(bundles, strategy="STRICT_SPREAD")
        for _ in range(pre_created_num_pgs // 3)
    ]
    pg_launchers.append(
        pg_launcher.remote(pre_created_pgs, num_pgs_to_create // 3))

ray.get(pg_launchers)
assert ray.cluster_resources()["GPU"] == num_nodes * resource_quantity
assert ray.cluster_resources()["pg_custom"] == num_nodes * resource_quantity
ray.shutdown()
print("Avg placement group creating time: "
      f"{total_creating_time / total_trial * 1000} ms")
print("Avg placement group removing time: "
      f"{total_removing_time / total_trial* 1000} ms")
print("Stress Test succeed.")
