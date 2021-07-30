import ray
import ray.autoscaler.sdk

import json
import os
from time import sleep, perf_counter
from tqdm import tqdm, trange

TEST_NUM_NODES = 1000
MAX_ACTORS_IN_CLUSTER = 20000


def num_alive_nodes():
    n = 0
    for node in ray.nodes():
        if node["Alive"]:
            n += 1
    return n


def scale_to(target):
    while num_alive_nodes() != target:
        ray.autoscaler.sdk.request_resources(bundles=[{"node": 1}] * target)
        print(f"Current # nodes: {num_alive_nodes()}, target: {target}")
        print("Waiting ...")
        sleep(5)


def test_max_actors_launch():
    # m5.xlarge has 4 CPUs
    # We are targeting: 4 / 0.2 = 20 actors per node
    cpus_per_actor = 0.2

    @ray.remote(num_cpus=cpus_per_actor)
    class Actor:
        def foo(self):
            pass

    actors = [
        Actor.remote()
        for _ in trange(MAX_ACTORS_IN_CLUSTER, desc="Launching actors")
    ]
    return actors

def test_actor_ready(actors):
    remaining = [actor.foo.remote() for actor in actors]
    pbar = tqdm(total=len(remaining), desc="Executing actor.foo")
    while len(remaining) != 0:
        ready, remaining = ray.wait(remaining, num_returns=100)
        for r in ray.get(ready):
            assert r is None
        pbar.update(len(ready))


ray.init(address="auto")

scale_to(TEST_NUM_NODES + 1)
assert num_alive_nodes(
) == TEST_NUM_NODES + 1, f"Wrong number of nodes in cluster {len(ray.nodes())}"

cluster_resources = ray.cluster_resources()

available_resources = ray.available_resources()
assert available_resources == cluster_resources, (
    str(available_resources) + " != " + str(cluster_resources))

actor_launch_start = perf_counter()
actors = test_max_actors_launch()
actor_launch_end = perf_counter()
actor_launch_time = actor_launch_end - actor_launch_start

actor_ready_start = perf_counter()
test_actor_ready(actors)
actor_ready_end = perf_counter()
actor_ready_time = actor_ready_end - actor_ready_start

print(f"Actor launch time: {actor_launch_time} ({MAX_ACTORS_IN_CLUSTER} actors)")
print(f"Actor ready time: {actor_ready_time} ({MAX_ACTORS_IN_CLUSTER} actors)")
print(f"Total time: {actor_launch_time + actor_ready_time} ({MAX_ACTORS_IN_CLUSTER} actors)")

if "TEST_OUTPUT_JSON" in os.environ:
    out_file = open(os.environ["TEST_OUTPUT_JSON"], "w")
    results = {
        "actor_launch_time": actor_launch_time,
        "actor_ready_time": actor_ready_time,
        "total_time": actor_launch_time + actor_ready_time,
        "num_actors": MAX_ACTORS_IN_CLUSTER,
        "success": "1"
    }
    json.dump(results, out_file)
