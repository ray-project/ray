import os
import time

import tqdm
from many_nodes_tests.dashboard_test import DashboardTestAtScale

import ray
import ray._common.test_utils
import ray._private.test_utils as test_utils
from ray._private.state_api_test_utils import summarize_worker_startup_time

is_smoke_test = True
if "SMOKE_TEST" in os.environ:
    MAX_ACTORS_IN_CLUSTER = 10
    MIN_NODES = 1
    MIN_CPUS_PER_NODE = 1
else:
    MAX_ACTORS_IN_CLUSTER = 20000
    MIN_NODES = 1250
    MIN_CPUS_PER_NODE = 4
    is_smoke_test = False


def test_many_actors_many_nodes():
    cpus_per_actor = 0.25

    @ray.remote(num_cpus=cpus_per_actor)
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

    actors = [
        Actor.remote()
        for _ in tqdm.trange(MAX_ACTORS_IN_CLUSTER, desc="Launching actors")
    ]

    not_ready = [actor.ping.remote() for actor in actors]
    for _ in tqdm.trange(len(actors), desc="Waiting for actors to respond"):
        ready, not_ready = ray.wait(not_ready)
        assert ray.get(*ready) == "pong"


def no_resource_leaks():
    return test_utils.no_resource_leaks_excluding_node_resources()


addr = ray.init(address="auto")

ray._common.test_utils.wait_for_condition(no_resource_leaks)
monitor_actor = test_utils.monitor_memory_usage()
dashboard_test = DashboardTestAtScale(addr)

start_time = time.time()
test_many_actors_many_nodes()
end_time = time.time()

ray.get(monitor_actor.stop_run.remote())
used_gb, usage = ray.get(monitor_actor.get_peak_memory_info.remote())
print(f"Peak memory usage: {round(used_gb, 2)}GB")
print(f"Peak memory usage per processes:\n {usage}")
del monitor_actor

# Get the dashboard result
ray._common.test_utils.wait_for_condition(no_resource_leaks)

rate = MAX_ACTORS_IN_CLUSTER / (end_time - start_time)
try:
    summarize_worker_startup_time()
except Exception as e:
    print("Failed to summarize worker startup time.")
    print(e)

print(
    f"Success! Started {MAX_ACTORS_IN_CLUSTER} actors in "
    f"{end_time - start_time}s. ({rate} actors/s)"
)

results = {
    "actors_per_second": rate,
    "num_actors": MAX_ACTORS_IN_CLUSTER,
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
