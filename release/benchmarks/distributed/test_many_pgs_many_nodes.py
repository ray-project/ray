import os
import time

import tqdm
from many_nodes_tests.dashboard_test import DashboardTestAtScale

import ray
import ray._common.test_utils
import ray._private.test_utils as test_utils
from ray.util.placement_group import placement_group, remove_placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

is_smoke_test = True
if "SMOKE_TEST" in os.environ:
    NUM_PGS = 5
    NUM_ACTORS_PER_PG = 4
    MIN_NODES = 5
    MIN_CPUS_PER_NODE = 4
else:
    NUM_PGS = 1250
    NUM_ACTORS_PER_PG = 4
    MIN_NODES = 1250
    MIN_CPUS_PER_NODE = 4
    is_smoke_test = False

TOTAL_ACTORS = NUM_PGS * NUM_ACTORS_PER_PG


def test_many_pgs_many_nodes():
    @ray.remote(num_cpus=0.25)
    class Actor:
        def ping(self):
            return "pong"

    nodes = [node for node in ray.nodes() if node.get("Alive")]
    if len(nodes) < MIN_NODES:
        raise Exception(
            f"Not enough nodes. Expected at least {MIN_NODES} nodes, "
            f"got {len(nodes)}."
        )

    for node in tqdm.tqdm(nodes, desc="Verifying minimum resources per Node"):
        cpus = node.get("Resources", {}).get("CPU", 0)
        if cpus < MIN_CPUS_PER_NODE:
            raise Exception(
                f"Node {node.get('NodeID')} has {cpus} CPUs, "
                f"expected at least {MIN_CPUS_PER_NODE}."
            )

    print(f"Creating {NUM_PGS} rollout placement groups...")
    pgs = [
        placement_group(
            name=f"pg_{i}",
            bundles=[{"CPU": 0.25}] * NUM_ACTORS_PER_PG,
            strategy="STRICT_PACK",
        )
        for i in tqdm.trange(NUM_PGS, desc="Creating PGs")
    ]

    for pg in tqdm.tqdm(pgs, desc="Waiting for PGs to be ready"):
        ray.get(pg.ready())

    print("Scheduling actors...")

    actors = [
        Actor.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=pg, placement_group_bundle_index=j
            )
        ).remote()
        for pg, j in tqdm.tqdm(
            [(pg, j) for pg in pgs for j in range(NUM_ACTORS_PER_PG)],
            desc="Scheduling Actors",
        )
    ]

    not_ready = [actor.ping.remote() for actor in actors]
    for _ in tqdm.trange(len(actors), desc="Waiting for actors to respond"):
        ready, not_ready = ray.wait(not_ready)
        assert ray.get(*ready) == "pong"

    for pg in pgs:
        remove_placement_group(pg)


def no_resource_leaks():
    return test_utils.no_resource_leaks_excluding_node_resources()


addr = ray.init(address="auto")

ray._common.test_utils.wait_for_condition(no_resource_leaks, timeout=60)
monitor_actor = test_utils.monitor_memory_usage()
dashboard_test = DashboardTestAtScale(addr)

start_time = time.time()
test_many_pgs_many_nodes()
end_time = time.time()

ray.get(monitor_actor.stop_run.remote())
used_gb, usage = ray.get(monitor_actor.get_peak_memory_info.remote())
print(f"Peak memory usage: {round(used_gb, 2)}GB")
print(f"Peak memory usage per processes:\n {usage}")
del monitor_actor
ray._common.test_utils.wait_for_condition(no_resource_leaks)

rate = TOTAL_ACTORS / (end_time - start_time)
print(
    f"Success! Started {TOTAL_ACTORS} actors on placement groups in "
    f"{end_time - start_time}s. ({rate} actors/s)"
)

results = {
    "actors_per_second": rate,
    "num_actors": TOTAL_ACTORS,
    "time": end_time - start_time,
    "_peak_memory": round(used_gb, 2),
    "_peak_process_memory": usage,
}
if not is_smoke_test:
    results["perf_metrics"] = [
        {
            "perf_metric_name": "actors_per_second",
            "perf_metric_value": rate,
            "perf_metric_type": "THROUGHPUT",
        }
    ]
dashboard_test.update_release_test_result(results)
test_utils.safe_write_to_results_json(results)
